/*-
 * #%L
 * Spark-based parallel BigStitcher project.
 * %%
 * Copyright (C) 2021 - 2024 Developers.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */
package net.preibisch.bigstitcher.spark;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.SetupImgLoader;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.ViewSetup;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.flatfield.BasicFlatfield;
import net.preibisch.bigstitcher.spark.flatfield.BasicFlatfieldParams;
import net.preibisch.bigstitcher.spark.flatfield.BasicFlatfieldResult;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

/**
 * Estimate a BaSiC flatfield / darkfield per illumination group (typically per
 * {@code (channel, illumination)}) from a selection of views, and write the
 * fields to an N5 / Zarr / HDF5 container.
 * <p>
 * The estimation is a single, in-memory optimization (see
 * {@link BasicFlatfield}); it is intentionally <b>not</b> distributed via Spark.
 * View selection (via the inherited {@link AbstractSelectableViews} flags),
 * random sub-sampling and frame loading/downsampling all run on the driver.
 * A later Spark <i>apply</i> phase is out of scope here.
 */
public class BasicFlatfieldEstimation extends AbstractSelectableViews
{
	private static final long serialVersionUID = 1L;

	@Option(names = { "--maxViews" }, description = "max frames (z-slices) to use per group; random subsample (default: all)")
	protected Integer maxViews = null;

	@Option(names = { "--seed" }, description = "RNG seed for random sub-sampling, for reproducibility (default: 42)")
	protected Long seed = 42L;

	@Option(names = { "--workingSize" }, description = "downsample size (square) for estimation; 0 = full resolution (default: 128)")
	protected Integer workingSize = 0;

	@Option(names = { "--estimateDarkfield" }, description = "estimate the additive darkfield (default: true)")
	protected Boolean estimateDarkfield = true;

	@Option(names = { "--lambda" }, description = "flatfield regularization strength; 0 = auto (default: 0)")
	protected Double lambda = 0.0;

	@Option(names = { "--lambdaDarkfield" }, description = "darkfield regularization strength; 0 = auto (default: 0)")
	protected Double lambdaDarkfield = 0.0;

	@Option(names = { "--maxIterations" }, description = "max inner ALM iterations (default: 500)")
	protected Integer maxIterations = 500;

	@Option(names = { "--optTol" }, description = "inner ALM convergence tolerance (default: 1e-6)")
	protected Double optTol = 1e-6;

	@Option(names = { "--reweightTol" }, description = "outer reweighting convergence tolerance (default: 1e-3)")
	protected Double reweightTol = 1e-3;

	@Option(names = { "--maxReweightIterations" }, description = "max outer reweighting iterations (default: 10)")
	protected Integer maxReweightIterations = 10;

	@Option(names = { "--epsilon" }, description = "reweighting stability term (default: 0.1)")
	protected Double epsilon = 0.1;

	@Option(names = { "-o", "--output" }, required = true, description = "N5/ZARR/HDF5 container path for the estimated fields (e.g. s3://myBucket/flatfield.zarr)")
	protected String outputPathURIString = null;

	@Option(names = { "-s", "--storage" }, description = "output storage type: ZARR (v3) | ZARR2 (v2) | N5 | HDF5 (default: guess from path: .zarr=ZARR v3, .zarr2=ZARR2 v2, .n5=N5, .h5/.hdf5=HDF5)")
	protected StorageFormat storageType = null;

	@Option(names = { "-c", "--compression" }, showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "dataset compression")
	protected Compressions compressionType = Compressions.Zstandard;

	@Option(names = { "-cl", "--compressionLevel" }, description = "compression level, if supported by the codec (default: gzip 1, Zstandard 3, xz 6)")
	protected Integer compressionLevel = null;

	@Option(names = { "--blockSize" }, description = "inner chunk size (X,Y) of the written fields (default: 128,128)")
	protected String blockSizeString = "128,128";

	@Option(names = { "--blockScale" }, description = "shard-size factor (X,Y) when ZARR v3 sharding is enabled: shard size = blockSize * blockScale (default: 16,16)")
	protected String blockScaleString = "16,16";

	@Option(names = { "--useSharding" }, description = "enable Zarr v3 sharding using blockScale as shard size factor (default: enabled for ZARR v3, disabled otherwise)")
	protected Boolean useSharding = null; // null = auto-detect

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		final URI outPathURI = URITools.toURI( outputPathURIString );

		if ( storageType == null )
		{
			final String lc = outputPathURIString.toLowerCase();
			if ( lc.endsWith( ".zarr2" ) )
				storageType = StorageFormat.ZARR2;
			else if ( lc.endsWith( ".zarr" ) )
				storageType = StorageFormat.ZARR;
			else if ( lc.endsWith( ".n5" ) )
				storageType = StorageFormat.N5;
			else if ( lc.endsWith( ".h5" ) || lc.endsWith( ".hdf5" ) )
				storageType = StorageFormat.HDF5;
			else
			{
				System.out.println( "Unable to guess format from URI '" + outPathURI + "', please specify using '-s'" );
				return null;
			}
			System.out.println( "Guessed format " + storageType + " for '" + outPathURI + "', override with '-s'" );
		}

		// -- compression + sharding --
		final Compression compression = N5Util.getCompression( this.compressionType, this.compressionLevel );
		final int[] blockSize = Import.csvStringToIntArray( blockSizeString );
		final int[] blockScale = Import.csvStringToIntArray( blockScaleString );

		// auto-detect sharding: enabled for ZARR v3, disabled otherwise
		if ( useSharding == null )
			useSharding = ( storageType == StorageFormat.ZARR );

		if ( useSharding && storageType != StorageFormat.ZARR )
		{
			System.out.println( "WARNING: Sharding is only supported for ZARR v3. Disabling sharding." );
			useSharding = false;
		}

		System.out.println( "Compression: " + this.compressionType + ( compressionLevel == null ? "" : " (level " + compressionLevel + ")" ) );
		System.out.println( "Inner chunk size: " + Arrays.toString( blockSize ) + ( useSharding ? ", shard factor: " + Arrays.toString( blockScale ) : "" ) );

		final SpimData2 spimData = this.loadSpimData2();
		if ( spimData == null )
			throw new IllegalArgumentException( "Couldn't load SpimData XML project." );

		final List< ViewId > views = loadViewIds( spimData );
		if ( views == null || views.isEmpty() )
			throw new IllegalArgumentException( "No ViewIds found." );

		final N5Writer n5Writer = N5Util.createN5Writer( outPathURI, storageType );
		if ( n5Writer == null )
			throw new IllegalArgumentException( "Couldn't create output container '" + outPathURI + "'." );

		final BasicFlatfieldParams params = new BasicFlatfieldParams(
				estimateDarkfield,
				lambda.floatValue(),
				lambdaDarkfield.floatValue(),
				maxIterations,
				optTol.floatValue(),
				reweightTol.floatValue(),
				maxReweightIterations,
				epsilon.floatValue(),
				workingSize );

		// group by (channel, illumination)
		final Map< String, List< ViewId > > groups = groupByChannelIllumination( spimData, views );
		System.out.println( "Estimating flatfield/darkfield for " + groups.size() + " (channel,illumination) group(s)." );

		for ( final Entry< String, List< ViewId > > group : groups.entrySet() )
		{
			final String groupKey = group.getKey();
			final List< ViewId > groupViews = group.getValue();

			System.out.println( "\n=== Group " + groupKey + " (" + groupViews.size() + " view(s)) ===" );

			// build frames: every z-slice of every group view is one frame that shares the profile
			final List< RandomAccessibleInterval< ? extends RealType< ? > > > frames = loadFrames( spimData, groupViews );

			if ( frames.isEmpty() )
			{
				System.out.println( "  no frames for group " + groupKey + ", skipping." );
				continue;
			}

			// random subsample to maxViews
			if ( maxViews != null && maxViews > 0 && frames.size() > maxViews )
			{
				Collections.shuffle( frames, new java.util.Random( seed ) );
				while ( frames.size() > maxViews )
					frames.remove( frames.size() - 1 );
				System.out.println( "  sub-sampled to " + frames.size() + " frame(s)." );
			}

			System.out.println( "  estimating from " + frames.size() + " frame(s)..." );
			final BasicFlatfieldResult result = BasicFlatfield.estimate( frames, params );

			if ( dryRun )
			{
				System.out.println( "  dry run: skipped writing fields for group " + groupKey );
				continue;
			}

			writeField( n5Writer, groupKey + "/flatfield", result.flatfield, params, result, compression, blockSize, blockScale, useSharding );
			writeField( n5Writer, groupKey + "/darkfield", result.darkfield, params, result, compression, blockSize, blockScale, useSharding );
			System.out.println( "  wrote " + groupKey + "/flatfield and " + groupKey + "/darkfield" );
		}

		if ( storageType == StorageFormat.HDF5 )
			n5Writer.close();

		System.out.println( "\nDone." );
		return null;
	}

	/** Group selected views by their (channel id, illumination id). */
	private static Map< String, List< ViewId > > groupByChannelIllumination( final SpimData2 spimData, final List< ViewId > views )
	{
		final Map< String, List< ViewId > > groups = new LinkedHashMap<>();
		for ( final ViewId v : views )
		{
			final ViewDescription vd = spimData.getSequenceDescription().getViewDescription( v );
			if ( vd == null || !vd.isPresent() )
				continue;
			final ViewSetup vs = vd.getViewSetup();
			final int ch = ( vs.getChannel() != null ) ? vs.getChannel().getId() : 0;
			final int il = ( vs.getIllumination() != null ) ? vs.getIllumination().getId() : 0;
			final String key = "channel" + ch + "/illumination" + il;
			groups.computeIfAbsent( key, k -> new ArrayList<>() ).add( v );
		}
		return groups;
	}

	/**
	 * Load all views of a group and split each into 2D frames. A 3D view yields
	 * one frame per z-slice (all share the same illumination profile); a 2D view
	 * yields a single frame. All frames must share the same X/Y size, so views of
	 * differing X/Y size in a group are skipped with a warning.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static List< RandomAccessibleInterval< ? extends RealType< ? > > > loadFrames( final SpimData2 spimData, final List< ViewId > groupViews )
	{
		final List< RandomAccessibleInterval< ? extends RealType< ? > > > frames = new ArrayList<>();
		long[] frameSize = null;

		for ( final ViewId v : groupViews )
		{
			final SetupImgLoader< ? > sil = spimData.getSequenceDescription().getImgLoader().getSetupImgLoader( v.getViewSetupId() );
			final RandomAccessibleInterval< ? > imgRaw = sil.getImage( v.getTimePointId() );

			if ( !( imgRaw.randomAccess().get() instanceof RealType ) )
			{
				System.out.println( "  view " + v.getViewSetupId() + " is not a RealType image, skipping." );
				continue;
			}
			final RandomAccessibleInterval< ? extends RealType< ? > > img = ( RandomAccessibleInterval ) imgRaw;

			final long sx = img.dimension( 0 );
			final long sy = img.dimension( 1 );
			if ( frameSize == null )
				frameSize = new long[] { sx, sy };
			else if ( frameSize[ 0 ] != sx || frameSize[ 1 ] != sy )
			{
				System.out.println( "  view " + v.getViewSetupId() + " has different X/Y size " + sx + "x" + sy
						+ " (expected " + frameSize[ 0 ] + "x" + frameSize[ 1 ] + "), skipping." );
				continue;
			}

			if ( img.numDimensions() == 2 )
			{
				frames.add( img );
			}
			else if ( img.numDimensions() == 3 )
			{
				final long zMin = img.min( 2 );
				final long zMax = img.max( 2 );
				for ( long z = zMin; z <= zMax; ++z )
					frames.add( Views.hyperSlice( img, 2, z ) );
			}
			else
			{
				System.out.println( "  view " + v.getViewSetupId() + " has " + img.numDimensions() + " dims, skipping." );
			}
		}

		return frames;
	}

	private static void writeField(
			final N5Writer n5Writer,
			final String dataset,
			final RandomAccessibleInterval< FloatType > field,
			final BasicFlatfieldParams params,
			final BasicFlatfieldResult result,
			final Compression compression,
			final int[] blockSize,
			final int[] blockScale,
			final boolean useSharding ) throws Exception
	{
		if ( useSharding )
		{
			// Zarr v3 sharded write: shard size = blockSize * blockScale, inner chunk = blockSize.
			// The whole (small) field is written in a single saveRegion call, so there is no
			// concurrent partial-shard write to worry about.
			final long[] dims = new long[] { field.dimension( 0 ), field.dimension( 1 ) };
			final int[] shardSize = new int[] { blockSize[ 0 ] * blockScale[ 0 ], blockSize[ 1 ] * blockScale[ 1 ] };
			final DatasetAttributes attributes = ZarrV3DatasetAttributes.builder( dims, DataType.FLOAT32 )
					.blockSize( shardSize )   // shard dimensions
					.chunkSize( blockSize )   // inner chunk size within shards
					.compression( compression )
					.shardIndexDataCodecInfos( new Crc32cChecksumCodec() )
					.build();
			n5Writer.createDataset( dataset, attributes );
			N5Utils.saveRegion( field, n5Writer, dataset );
		}
		else
		{
			N5Utils.save( field, n5Writer, dataset, blockSize, compression );
		}

		// params + QC as attributes
		n5Writer.setAttribute( dataset, "estimateDarkfield", params.estimateDarkfield );
		n5Writer.setAttribute( dataset, "lambda", params.lambda );
		n5Writer.setAttribute( dataset, "lambdaDarkfield", params.lambdaDarkfield );
		n5Writer.setAttribute( dataset, "maxIterations", params.maxIterations );
		n5Writer.setAttribute( dataset, "optimizationTol", params.optimizationTol );
		n5Writer.setAttribute( dataset, "reweightTol", params.reweightTol );
		n5Writer.setAttribute( dataset, "maxReweightIterations", params.maxReweightIterations );
		n5Writer.setAttribute( dataset, "epsilon", params.epsilon );
		n5Writer.setAttribute( dataset, "workingSize", params.workingSize );
		n5Writer.setAttribute( dataset, "baseline", result.baseline );
		n5Writer.setAttribute( dataset, "numFrames", result.frameScales.length );
	}

	public static void main( final String... args ) throws SpimDataException
	{
		System.out.println( Arrays.toString( args ) );
		System.exit( new CommandLine( new BasicFlatfieldEstimation() ).execute( args ) );
	}
}
