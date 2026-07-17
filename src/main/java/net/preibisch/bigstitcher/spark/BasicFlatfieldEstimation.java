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
import java.util.Random;
import java.util.stream.IntStream;

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
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes;
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
	private static final long MAX_N5_BLOCK_ELEMENTS = Integer.MAX_VALUE;

	@Option(names = { "--numViewSetups" }, description = "number of view setups to randomly select per (channel,illumination) group to draw frames from; combine with --channelId/--tileId/-vi etc. to restrict the candidate pool (default: all view setups in the group)")
	protected Integer numViewSetups = null;

	@Option(names = { "--numFrames" }, description = "total number of frames (z-slices) to randomly select per group from the chosen view setups (default: all frames)")
	protected Integer numFrames = null;

	@Option(names = { "--seed" }, description = "RNG seed for random sub-sampling of view setups and frames, for reproducibility (default: 42)")
	protected Long seed = 42L;

	@Option(names = { "--workingSize" }, description = "downsample size (square) for estimation; 0 = full resolution (default: 128)")
	protected Integer workingSize = 128;

	@Option(names = { "--estimateDarkfield" }, description = "estimate the additive darkfield (default: true)")
	protected Boolean estimateDarkfield = true;

	@Option(names = { "--lambdaFlatfield" }, description = "flatfield regularization strength; 0 = auto (default: 0)")
	protected Double lambdaFlatfield = 0.0;

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

	@Option(names = { "--useSharding" }, description = "enable Zarr v3 sharding using blockScale as shard size factor (default: false)")
	protected Boolean useSharding = null; // null = default false

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
		validateBlockSizeOption( "--blockSize", blockSize, 2 );
		validateBlockSizeOption( "--blockScale", blockScale, 2 );

		// Flatfield outputs are 2D calibration fields. Normal chunks are the safer
		// default; sharding can create very large merge blocks for little benefit.
		if ( useSharding == null )
			useSharding = false;

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
				lambdaFlatfield.floatValue(),
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

		final Random rng = new Random( seed );
		for ( final Entry< String, List< ViewId > > group : groups.entrySet() )
		{
			final String groupKey = group.getKey();
			final List< ViewId > groupViews = group.getValue();

			System.out.println( "\n=== Group " + groupKey + " (" + groupViews.size() + " view(s)) ===" );

			// (1) randomly select numViewSetups view setups from this group's candidate pool
			final List< ViewId > selectedViews = selectViewSetups( groupViews, numViewSetups, rng );

			System.out.println( "\n=== Group " + groupKey + " selected views: " + selectedViews + " ===" );

			// (2) build frames: every z-slice of every selected view is one frame that shares the profile
			final BasicFlatfield.FramesStack frames = loadFrames( spimData, selectedViews, rng, numFrames, workingSize );

			if ( frames.isEmpty() )
			{
				System.out.println( "  no frames for group " + groupKey + ", skipping." );
				continue;
			}

			System.out.println( "  estimating from " + frames.size() + " frame(s)..." );

			writeField( n5Writer, groupKey + "/frames", frames.asImage(), params, null, compression, new int[]{128,128,128}, new int[]{1,1,1}, false );
			final BasicFlatfieldResult result = BasicFlatfield.estimate( frames, params, rng );

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
	 * Randomly select {@code numViewSetups} distinct view setups from a group's
	 * candidate views and return all their ViewIds (all timepoints of the chosen
	 * setups). Returns the input unchanged when {@code numViewSetups} is null/<=0 or
	 * the group has that many or fewer distinct setups.
	 */
	private static List< ViewId > selectViewSetups( final List< ViewId > groupViews, final Integer numViewSetups, final java.util.Random rng )
	{
		// distinct view setups present in this group (preserve encounter order)
		final Map< Integer, List< ViewId > > bySetup = new LinkedHashMap<>();
		for ( final ViewId v : groupViews )
			bySetup.computeIfAbsent( v.getViewSetupId(), k -> new ArrayList<>() ).add( v );

		final List< Integer > setupIds = new ArrayList<>( bySetup.keySet() );

		if ( numViewSetups == null || numViewSetups <= 0 || setupIds.size() <= numViewSetups )
		{
			System.out.println( "  using all " + setupIds.size() + " view setup(s) in the group." );
			return groupViews;
		}

		Collections.shuffle( setupIds, rng );
		final List< Integer > chosen = setupIds.subList( 0, numViewSetups );
		final List< ViewId > selected = new ArrayList<>();
		for ( final Integer sid : chosen )
			selected.addAll( bySetup.get( sid ) );

		System.out.println( "  randomly selected " + chosen.size() + " of " + setupIds.size() + " view setup(s): " + chosen );
		return selected;
	}

	/**
	 * Load all views of a group and split each into 2D frames. A 3D view yields
	 * one frame per z-slice (all share the same illumination profile); a 2D view
	 * yields a single frame. All frames must share the same X/Y size, so views of
	 * differing X/Y size in a group are skipped with a warning.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static BasicFlatfield.FramesStack loadFrames( final SpimData2 spimData,
														  final List< ViewId > groupViews,
														  final Random rng,
														  final Integer nframesPerView,
														  final Integer workingSize)
	{
		int nViews = groupViews.size();
		BasicFlatfield.FramesStack framesStack = null;
		int frameIndex = 0;
		int viewDims = 0;
		for ( final ViewId v : groupViews )
		{
			final SetupImgLoader< ? > sil = spimData.getSequenceDescription().getImgLoader().getSetupImgLoader( v.getViewSetupId() );
			final RandomAccessibleInterval< ? > imgRaw = sil.getImage( v.getTimePointId() );

			if ( !( imgRaw.randomAccess().get() instanceof RealType ) )
			{
				throw new IllegalArgumentException( "  view " + v.getViewSetupId() + " is not a RealType image" );
			}

			final RandomAccessibleInterval< ? extends RealType< ? > > img = ( RandomAccessibleInterval ) imgRaw;

			if (framesStack == null) {
				viewDims = img.numDimensions();
				int sx = (int) img.dimension( 0  );
				int sy = (int) img.dimension( 1  );
				int sz = viewDims == 2 ? 1 : (int) img.dimension( 2 ) ;
				int frameSize = workingSize == null ? 0 : workingSize;
				int nframes;
				if (viewDims == 2) {
					nframes = nViews;
				} else if (viewDims == 3) {
					if ( nframesPerView != null && nframesPerView > 0 && nframesPerView < sz ) {
						nframes = nViews * nframesPerView;
					} else {
						nframes = nViews * sz;
					}
				} else {
					throw new IllegalArgumentException( "view " + v.getViewSetupId() + " has " + img.numDimensions() + " dimensions. Currently it only supports 2 or 3 dimensions." );
				}
				framesStack = BasicFlatfield.createFramesStack(sx, sy, frameSize, frameSize, nframes);
			}

			if ( img.numDimensions() != viewDims )
			{
				System.out.println("Warning: all views are expected to have the same dimensions: " + viewDims + " vs. " + img.numDimensions());
			}

			if ( img.numDimensions() == 2 )
			{
				framesStack.setFrame(frameIndex++, img);
			}
			else // numDimensions == 3 otherwise it would've thrown IllegalArgument already
			{
				int[] zs;
				int zSize = (int) img.dimension(2);
				if ( nframesPerView != null && nframesPerView > 0 && nframesPerView < zSize )
				{
					System.out.println( "  Sample " + nframesPerView + " out of " + zSize + " frame(s) from " + v );
					zs = rng.ints(nframesPerView, 0, zSize).toArray();
				}
				else
				{
					System.out.println( "  Select all " + zSize + " frame(s) from " + v );
					zs = IntStream.range(0, zSize).toArray();
				}
				for ( int z : zs ) {
					framesStack.setFrame(frameIndex++, Views.hyperSlice( img, 2, z ));
				}
			}
		}

		return framesStack;
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
			final int[] shardSize = shardSize( blockSize, blockScale );
			validateN5BlockElementCount( "Zarr v3 shard", shardSize );
			final DatasetAttributes attributes = ZarrV3DatasetAttributes.builder( dims, DataType.FLOAT32 )
					.blockSize( shardSize ) // shard dimensions
					.chunkSize( blockSize ) // inner chunk size within shards
					.compression( compression )
					.shardIndexDataCodecInfos( new Crc32cChecksumCodec() )
					.build();
			n5Writer.createDataset( dataset, attributes );
			N5Utils.saveRegion( field, n5Writer, dataset );
		}
		else
		{
			validateN5BlockElementCount( "N5/Zarr chunk", blockSize );
			N5Utils.save( field, n5Writer, dataset, blockSize, compression );
		}

		// params + QC as attributes
		n5Writer.setAttribute( dataset, "estimateDarkfield", params.estimateDarkfield );
		n5Writer.setAttribute( dataset, "lambda", params.lambdaFlatfield);
		n5Writer.setAttribute( dataset, "lambdaDarkfield", params.lambdaDarkfield );
		n5Writer.setAttribute( dataset, "maxIterations", params.maxIterations );
		n5Writer.setAttribute( dataset, "optimizationTol", params.optimizationTol );
		n5Writer.setAttribute( dataset, "reweightTol", params.reweightTol );
		n5Writer.setAttribute( dataset, "maxReweightIterations", params.maxReweightIterations );
		n5Writer.setAttribute( dataset, "epsilon", params.epsilon );
		n5Writer.setAttribute( dataset, "workingSize", params.workingSize );
//		n5Writer.setAttribute( dataset, "baseline", result.baseline );
//		n5Writer.setAttribute( dataset, "numFrames", result.frameScales.length );
	}

	static void validateBlockSizeOption( final String option, final int[] values, final int numDimensions )
	{
		if ( values == null || values.length != numDimensions )
			throw new IllegalArgumentException( option + " must contain exactly " + numDimensions + " comma-separated positive integers." );
		for ( final int value : values )
			if ( value <= 0 )
				throw new IllegalArgumentException( option + " must contain only positive integers, got " + Arrays.toString( values ) );
	}

	static int[] shardSize( final int[] blockSize, final int[] blockScale )
	{
		final int[] shardSize = new int[ blockSize.length ];
		for ( int d = 0; d < blockSize.length; ++d )
		{
			final long value = ( long ) blockSize[ d ] * blockScale[ d ];
			if ( value > Integer.MAX_VALUE )
				throw new IllegalArgumentException( "Zarr v3 shard size overflows int in dimension " + d + ": "
						+ blockSize[ d ] + " * " + blockScale[ d ] );
			shardSize[ d ] = ( int ) value;
		}
		return shardSize;
	}

	static void validateN5BlockElementCount( final String label, final int[] blockSize )
	{
		long elements = 1;
		for ( final int size : blockSize )
			elements *= size;
		if ( elements > MAX_N5_BLOCK_ELEMENTS )
			throw new IllegalArgumentException( label + " size " + Arrays.toString( blockSize )
					+ " contains " + elements + " elements, which exceeds N5's maximum block allocation of "
					+ MAX_N5_BLOCK_ELEMENTS + ". Reduce --blockSize/--blockScale or disable sharding." );
	}

	public static void main( final String... args ) throws SpimDataException
	{
		System.out.println( Arrays.toString( args ) );
		System.exit( new CommandLine( new BasicFlatfieldEstimation() ).execute( args ) );
	}
}
