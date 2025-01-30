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

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.N5Factory.StorageFormat;
import org.janelia.scicomp.n5.zstandard.ZstandardCompression;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converter;
import net.imglib2.converter.RealUnsignedByteConverter;
import net.imglib2.converter.RealUnsignedShortConverter;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractInfrastructure;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.fusion.WriteSuperBlock;
import net.preibisch.bigstitcher.spark.fusion.WriteSuperBlockMasks;
import net.preibisch.bigstitcher.spark.util.BDVSparkInstantiateViewSetup;
import net.preibisch.bigstitcher.spark.util.Downsampling;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.plugin.fusion.FusionGUI.FusionType;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBox;
import net.preibisch.mvrecon.process.export.ExportN5Api;
import net.preibisch.mvrecon.process.fusion.blk.BlkAffineFusion;
import net.preibisch.mvrecon.process.fusion.transformed.FusedRandomAccessibleInterval.Fusion;
import net.preibisch.mvrecon.process.fusion.transformed.TransformVirtual;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import net.preibisch.mvrecon.process.n5api.N5ApiTools;
import net.preibisch.mvrecon.process.n5api.N5ApiTools.MultiResolutionLevelInfo;
import net.preibisch.mvrecon.process.n5api.SpimData2Tools;
import net.preibisch.mvrecon.process.n5api.SpimData2Tools.InstantiateViewSetup;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.Grid;
import util.URITools;

public class SparkAffineFusion extends AbstractInfrastructure implements Callable<Void>, Serializable
{
	public static enum DataTypeFusion
	{
		UINT8, UINT16, FLOAT32
	}

	private static final long serialVersionUID = -6103761116219617153L;

	@Option(names = { "-o", "--n5Path" }, required = true, description = "N5/ZARR/HDF5 basse path for saving (must be combined with the option '-d' or '--bdv'), e.g. -o /home/fused.n5 or e.g. s3://myBucket/data.n5")
	private String outputPathURIString = null;

	@Option(names = {"-s", "--storage"}, description = "Dataset storage type, can be used to override guessed format (default: guess from file/directory-ending)")
	private StorageFormat storageType = null;

	@Option(names = "--blockScale", description = "how many blocks to use for a single processing step, e.g. 4,4,1 means for blockSize a 128,128,64 that each spark thread writes 512,512,64 (default: 2,2,1)")
	private String blockScaleString = "2,2,1";

	//@Option(names = { "--masks" }, description = "save only the masks (this will not fuse the images)")
	//private boolean masks = false;

	//@Option(names = "--maskOffset", description = "allows to make masks larger (+, the mask will include some background) or smaller (-, some fused content will be cut off), warning: in the non-isotropic coordinate space of the raw input images (default: 0.0,0.0,0.0)")
	//private String maskOffset = "0.0,0.0,0.0";

	@Option(names = { "--firstTileWins" }, description = "use firstTileWins fusion strategy (default: false - using weighted average blending fusion)")
	private boolean firstTileWins = false;


	URI outPathURI = null;
	/**
	 * Prefetching now works with a Executors.newCachedThreadPool();
	 */
	//static final int N_PREFETCH_THREADS = 72;

	@Override
	public Void call() throws Exception
	{
		if (dryRun)
		{
			System.out.println( "dry-run not supported for affine fusion.");
			System.exit( 0 );
		}

		this.outPathURI = URITools.toURI( outputPathURIString );
		System.out.println( "Fused volume: " + outPathURI );

		if ( storageType == null )
		{
			if ( outputPathURIString.toLowerCase().endsWith( ".zarr" ) )
				storageType = StorageFormat.ZARR;
			else if ( outputPathURIString.toLowerCase().endsWith( ".n5" ) )
				storageType = StorageFormat.N5;
			else if ( outputPathURIString.toLowerCase().endsWith( ".h5" ) || outPathURI.toString().toLowerCase().endsWith( ".hdf5" ) )
				storageType = StorageFormat.HDF5;
			else
			{
				System.out.println( "Unable to guess format from URI '" + outPathURI + "', please specify using '-s'");
				return null;
			}

			System.out.println( "Guessed format " + storageType + " will be used to open URI '" + outPathURI + "', you can override it using '-s'");
		}
		else
		{
			System.out.println( "Format " + storageType + " will be used to open " + outPathURI );
		}

		final N5Writer driverVolumeWriter = N5Util.createN5Writer( outPathURI, storageType );

		final String fusionFormat = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/FusionFormat", String.class );

		final boolean bdv = fusionFormat.toLowerCase().contains( "BDV" );

		final URI xmlURI = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/InputXML", URI.class );
		final int numTimepoints = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/NumTimepoints", int.class );
		final int numChannels = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/NumChannels", int.class );

		final long[] bbMin = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/Boundingbox_min", long[].class );
		final long[] bbMax = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/Boundingbox_max", long[].class );
 
		final BoundingBox boundingBox = new BoundingBox( new FinalInterval( bbMin, bbMax ) );

		final boolean preserveAnisotropy = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/PreserveAnisotropy", boolean.class );
		final double anisotropyFactor = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/AnisotropyFactor", double.class );
		final int[] blockSize = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/BlockSize", int[].class );

		final DataType dataType = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/DataType", DataType.class );

		System.out.println( "FusionFormat: " + fusionFormat );
		System.out.println( "Input XML: " + xmlURI );
		System.out.println( "BDV project: " + bdv );
		System.out.println( "numTimepoints of fused dataset(s): " + numTimepoints );
		System.out.println( "numChannels of fused dataset(s): " + numChannels );
		System.out.println( "BoundingBox: " + boundingBox );
		System.out.println( "preserveAnisotropy: " + preserveAnisotropy );
		System.out.println( "anisotropyFactor: " + anisotropyFactor );
		System.out.println( "blockSize: " + Arrays.toString( blockSize ) );
		System.out.println( "dataType: " + dataType );

		final double minIntensity, maxIntensity;

		if ( driverVolumeWriter.listAttributes( "Bigstitcher-Spark").containsKey( "MinIntensity" ) )
		{
			minIntensity = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/MinIntensity", double.class );
			maxIntensity = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/MaxIntensity", double.class );

			System.out.println( "minIntensity: " + minIntensity );
			System.out.println( "maxIntensity: " + maxIntensity );
		}
		else
		{
			minIntensity = maxIntensity = Double.NaN;
		}

		final MultiResolutionLevelInfo[][] mrInfos =
				driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/MultiResolutionInfos", MultiResolutionLevelInfo[][].class );

		System.out.println( "Loaded " + mrInfos.length + " metadata object for fused " + storageType + " volume(s)" );

		final SpimData2 dataGlobal = Spark.getJobSpimData2( xmlURI, 0 );

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIdsGlobal;

		if (
			dataGlobal.getSequenceDescription().getAllChannelsOrdered().size() != numChannels || 
			dataGlobal.getSequenceDescription().getTimePoints().getTimePointsOrdered().size() != numTimepoints )
		{
			System.out.println(
					"The number of channels and timepoint in XML does not match the number in the export dataset.\n"
					+ "You have to specify which ViewIds/Channels/Illuminations/Tiles/Angles/Timepoints should be fused into\n"
					+ "a specific 3D volume in the fusion dataset:\n");
			// TODO: support that
			/*
			final ArrayList< ViewId > viewIdsGlobal = this.loadViewIds( dataGlobal );

			if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
				return null;
			*/

			return null;
		}
		else
		{
			viewIdsGlobal = Import.getViewIds( dataGlobal );
		}

		final int[] blocksPerJob = Import.csvStringToIntArray(blockScaleString);
		System.out.println( "Fusing: " + boundingBox.getTitle() + ": " + Util.printInterval( boundingBox ) +
				" with blocksize " + Util.printCoordinates( blockSize ) + " and " + Util.printCoordinates( blocksPerJob ) + " blocks per job" );

		if ( dataType == DataType.UINT8 )
			System.out.println( "Fusing to UINT8, min intensity = " + minIntensity + ", max intensity = " + maxIntensity );
		else if ( dataType == DataType.UINT16 )
			System.out.println( "Fusing to UINT16, min intensity = " + minIntensity + ", max intensity = " + maxIntensity );
		else
			System.out.println( "Fusing to FLOAT32" );

 		/*
		final double[] maskOff = Import.csvStringToDoubleArray(maskOffset);
		if ( masks )
			System.out.println( "Fusing ONLY MASKS! Mask offset: " + Util.printCoordinates( maskOff ) );
		*/

		//
		// final variables for Spark
		//
		final long[] minBB = boundingBox.minAsLongArray();
		final long[] maxBB = boundingBox.maxAsLongArray();

		final long[] dimensions = boundingBox.dimensionsAsLongArray();

		// TODO: do we still need this?
		try
		{
			// trigger the N5-blosc error, because if it is triggered for the first
			// time inside Spark, everything crashes
			new N5FSWriter(null);
		}
		catch (Exception e ) {}

		/*
		final String n5Dataset = this.n5Dataset != null ? this.n5Dataset : N5ApiTools.createBDVPath( this.bdvString, 0, this.storageType );

		// TODO: expose
		final Compression compression = new ZstandardCompression( 3 );// new GzipCompression( 1 );

		final double minIntensity = ( dataTypeFusion != DataTypeFusion.FLOAT32 ) ? this.minIntensity : 0;

		System.out.println( "Format being written: " + storageType );
		final N5Writer driverVolumeWriter = N5Util.createN5Writer( n5PathURI, storageType );

		driverVolumeWriter.createDataset(
				n5Dataset,
				dimensions,
				blockSize,
				dataType,
				compression );

		// saving metadata if it is bdv-compatible (we do this first since it might fail)
		if ( bdvString != null )
		{
			// A Functional Interface that converts a ViewId to a ViewSetup, only called if the ViewSetup does not exist
			final InstantiateViewSetup instantiate =
					new BDVSparkInstantiateViewSetup( angleIds, illuminationIds, channelIds, tileIds );

			final ViewId viewId = Import.getViewId( bdvString );

			try
			{
				if ( SpimData2Tools.writeBDVMetaData(
						driverVolumeWriter,
						storageType,
						dataType,
						dimensions,
						compression,
						blockSize,
						downsamplings,
						viewId,
						n5PathURI,
						xmlOutURI,
						instantiate ) == null )
				{
					System.out.println( "Failed to write metadata for '" + n5Dataset + "'." );
					return null;
				}
			}
			catch (SpimDataException | IOException e)
			{
				e.printStackTrace();
				System.out.println( "Failed to write metadata for '" + n5Dataset + "': " + e );
				return null;
			}

			System.out.println( "Done writing BDV metadata.");
		}
		*/

		final SparkConf conf = new SparkConf().setAppName("AffineFusion");

		if (localSparkBindAddress)
			conf.set("spark.driver.bindAddress", "127.0.0.1");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final SequenceDescription sd = dataGlobal.getSequenceDescription();

		final HashMap< Integer, Integer > tpIdToTpIndex = new HashMap<>();
		final HashMap< Integer, Integer > chIdToChIndex = new HashMap<>();

		for ( int t = 0; t < sd.getTimePoints().getTimePointsOrdered().size(); ++t )
			tpIdToTpIndex.put( sd.getTimePoints().getTimePointsOrdered().get( t ).getId(), t );

		for ( int c = 0; c < sd.getAllChannelsOrdered().size(); ++c )
			chIdToChIndex.put( sd.getAllChannelsOrdered().get( c ).getId(), c );

		for ( int c = 0; c < numChannels; ++c )
			for ( int t = 0; t < numTimepoints; ++t )
			{
				System.out.println( "\nProcessing channel " + c + ", timepoint " + t );
				System.out.println( "\n-----------------------------------" );

				final int tIndex = t;
				final int cIndex = c;

				final ArrayList< ViewId > viewIds = new ArrayList<>();
				viewIdsGlobal.forEach( viewId -> {
					final ViewDescription vd = sd.getViewDescription( viewId );

					if ( tpIdToTpIndex.get( vd.getTimePointId() ) == tIndex && chIdToChIndex.get( vd.getViewSetup().getChannel().getId() ) == cIndex )
						viewIds.add( viewId );
				});

				System.out.println( "Fusing " + viewIds.size() + " views for this 3D volume ... " );

				final MultiResolutionLevelInfo[] mrInfo = mrInfos[ c + t*numChannels  ];

				// using bigger blocksizes than being stored for efficiency (needed for very large datasets)
				final int[] superBlockSize = new int[ 3 ];
				Arrays.setAll( superBlockSize, d -> blockSize[ d ] * blocksPerJob[ d ] );
				final List<long[][]> grid = Grid.create(dimensions,
						superBlockSize,
						blockSize);

				System.out.println( "numJobs = " + grid.size() );

				//driverVolumeWriter.setAttribute( n5Dataset, "offset", minBB );

				final JavaRDD<long[][]> rdd = sc.parallelize( grid );

				final long time = System.currentTimeMillis();

				//TODO: prefetchExecutor!!

				rdd.foreach(
						gridBlock ->
						{
							final SpimData2 dataLocal = Spark.getSparkJobSpimData2(xmlURI);

							final HashMap< ViewId, AffineTransform3D > registrations =
									TransformVirtual.adjustAllTransforms(
											viewIds,
											dataLocal.getViewRegistrations().getViewRegistrations(),
											anisotropyFactor,
											Double.NaN );

							final Converter conv;
							final Type type;

							if ( dataType == DataType.UINT8 )
							{
								conv = new RealUnsignedByteConverter<>( minIntensity, maxIntensity );
								type = new UnsignedByteType();
							}
							else if ( dataType == DataType.UINT16 )
							{
								conv = new RealUnsignedShortConverter<>( minIntensity, maxIntensity );
								type = new UnsignedShortType();
							}
							else
							{
								conv = null;
								type = new FloatType();
							}

							RandomAccessibleInterval img = BlkAffineFusion.init(
									conv,
									dataLocal.getSequenceDescription().getImgLoader(),
									viewIds,
									registrations,
									dataLocal.getSequenceDescription().getViewDescriptions(),
									firstTileWins ? FusionType.FIRST : FusionType.AVG_BLEND,//fusion.getFusionType(),
									1, // linear interpolation
									null, // intensity correction
									boundingBox,
									(RealType & NativeType)type,
									blockSize );

							final long[] blockOffset, blockSizeExport, gridOffset;

							final RandomAccessible image;

							// 5D OME-ZARR CONTAINER
							if ( storageType == StorageFormat.ZARR )
							{
								// gridBlock is 3d, make it 5d
								blockOffset = new long[] { gridBlock[0][0], gridBlock[0][1], gridBlock[0][2], cIndex, tIndex };
								blockSizeExport = new long[] { gridBlock[1][0], gridBlock[1][1], gridBlock[1][2], 1, 1 };
								gridOffset = new long[] { gridBlock[2][0], gridBlock[2][1], gridBlock[2][2], cIndex, tIndex }; // because blocksize in C & T is 1

								// img is 3d, make it 5d
								// the same information is returned no matter which index is queried in C and T
								image = Views.addDimension( Views.addDimension( img ) );
							}
							else
							{
								blockOffset = gridBlock[0];
								blockSizeExport = gridBlock[1];
								gridOffset = gridBlock[2];

								image = img;
							}

							final Interval block =
									Intervals.translate(
											new FinalInterval( blockSizeExport ),
											blockOffset );

							final RandomAccessibleInterval source =
									Views.interval( image, block );

							final RandomAccessibleInterval sourceGridBlock =
									Views.offsetInterval(source, blockOffset, blockSizeExport);

							final N5Writer driverVolumeWriterLocal = N5Util.createN5Writer( outPathURI, storageType );

							N5Utils.saveBlock(sourceGridBlock, driverVolumeWriterLocal, mrInfo[ 0 ].dataset, gridOffset );

							if ( N5Util.sharedHDF5Writer == null )
								driverVolumeWriterLocal.close();
						} );
				/*
				if ( this.downsamplings != null )
				{
					// TODO: run common downsampling code (affine, non-rigid, downsampling-only)
					Downsampling.createDownsampling(
							n5PathURI,
							n5Dataset,
							driverVolumeWriter,
							dimensions,
							storageType,
							blockSize,
							dataType,
							compression,
							downsamplings,
							bdvString != null,
							sc );
				}

				// close main writer (is shared over Spark-threads if it's HDF5, thus just closing it here)
				driverVolumeWriter.close();

				if ( multiRes )
					System.out.println( "Saved, e.g. view with './n5-view -i " + n5PathURI + " -d " + n5Dataset.substring( 0, n5Dataset.length() - 3) + "'" );
				else
					System.out.println( "Saved, e.g. view with './n5-view -i " + n5PathURI + " -d " + n5Dataset + "'" );

				System.out.println( "done, took: " + (System.currentTimeMillis() - time ) + " ms." );
				*/
			}

		sc.close();

		return null;
	}

	public static void main(final String... args) throws SpimDataException {

		//final XmlIoSpimData io = new XmlIoSpimData();
		//final SpimData spimData = io.load( "/Users/preibischs/Documents/Microscopy/Stitching/Truman/standard/output/dataset.xml" );
		//BdvFunctions.show( spimData );
		//SimpleMultiThreading.threadHaltUnClean();

		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new SparkAffineFusion()).execute(args));
	}
}
