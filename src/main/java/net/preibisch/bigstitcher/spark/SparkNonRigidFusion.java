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
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import org.janelia.scicomp.n5.zstandard.ZstandardCompression;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.registration.ViewTransformAffine;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.loops.LoopBuilder;
import net.imglib2.parallel.SequentialExecutorService;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.SparkFusion.DataTypeFusion;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.util.BDVSparkInstantiateViewSetup;
import net.preibisch.bigstitcher.spark.util.Downsampling;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.bigstitcher.spark.util.RetryTrackerSpark;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.bigstitcher.spark.util.ViewUtil;
import net.preibisch.mvrecon.fiji.plugin.fusion.FusionGUI.FusionType;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBox;
import net.preibisch.mvrecon.process.downsampling.lazy.LazyHalfPixelDownsample2x;
import net.preibisch.mvrecon.process.export.ExportN5Api;
import net.preibisch.mvrecon.process.fusion.transformed.nonrigid.NonRigidTools;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import net.preibisch.mvrecon.process.n5api.N5ApiTools;
import net.preibisch.mvrecon.process.n5api.SpimData2Tools;
import net.preibisch.mvrecon.process.n5api.SpimData2Tools.InstantiateViewSetup;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import scala.Tuple2;
import util.Grid;
import util.URITools;

public class SparkNonRigidFusion extends AbstractSelectableViews implements Callable<Void>, Serializable
{
	/**
	 *
	 */
	private static final long serialVersionUID = 385486695284409953L;

	@Option(names = { "-o", "--n5Path" }, required = true, description = "N5/ZARR/HDF5 basse path for saving (must be combined with the option '-d' or '--bdv'), e.g. -o /home/fused.n5 or e.g. s3://myBucket/data.n5")
	private String n5PathURIString = null;

	@Option(names = { "-d", "--n5Dataset" }, required = true, description = "N5 dataset - it is highly recommended to add s0 to be able to compute a multi-resolution pyramid later, e.g. /ch488/s0")
	private String n5Dataset = null;

	@Option(names = { "--bdv" }, required = false, description = "Write a BigDataViewer-compatible dataset specifying TimepointID, ViewSetupId, e.g. -b 0,0 or -b 4,1")
	private String bdvString = null;

	@Option(names = { "-xo", "--xmlout" }, required = false, description = "path to the new BigDataViewer xml project (only valid if --bdv was selected), "
			+ "e.g. -xo /home/project.xml or -xo s3://myBucket/project.xml (default: dataset.xml in basepath for H5, dataset.xml one directory level above basepath for N5)")
	private String xmlOutURIString = null;

	@Option(names = {"-s", "--storage"}, defaultValue = "N5", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Dataset storage type, currently supported N5, ZARR (and ONLY for local, multithreaded Spark: HDF5)")
	private StorageFormat storageType = null;

	@Option(names = "--blockSize", description = "blockSize, you can use smaller blocks for HDF5 (default: 128,128,128)")
	private String blockSizeString = "128,128,128";

	@Option(names = "--blockScale", description = "how many blocks to use for a single processing step, e.g. 4,4,1 means for blockSize a 128,128,64 that each spark thread writes 512,512,64 (default: 2,2,1, ignored when sharding is enabled)")
	private String blockScaleString = "2,2,1";

	@Option(names = { "--useSharding" }, description = "Enable Zarr v3 sharding (only for ZARR v3, default: auto - ON for ZARR v3, OFF otherwise)")
	private Boolean useSharding = null;

	@Option(names = { "--shardSize" }, description = "Absolute shard size in voxels, e.g. 1024,1024,128 (default: 1024,1024,128)")
	private String shardSizeString = "1024,1024,128";

	@Option(names = { "-b", "--boundingBox" }, description = "fuse a specific bounding box listed in the XML (default: fuse everything)")
	private String boundingBoxName = null;

	@Option(names = { "-ip", "--interestPoints" }, required = true, description = "provide a list of corresponding interest points to be used for the fusion (e.g. -ip 'beads' -ip 'nuclei'")
	private ArrayList<String> interestPoints = null;

	@Option(names = {"-p", "--dataType"}, defaultValue = "FLOAT32", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Data type, UINT8 [0...255], UINT16 [0...65535] and FLOAT32 are supported, when choosing UINT8 or UINT16 you must define min and max intensity (default: FLOAT32)")
	private DataTypeFusion dataTypeFusion = null;

	@Option(names = { "--minIntensity" }, description = "min intensity for scaling values to the desired range (required for UINT8 and UINT16), e.g. 0.0")
	private Double minIntensity = null;

	@Option(names = { "--maxIntensity" }, description = "max intensity for scaling values to the desired range (required for UINT8 and UINT16), e.g. 2048.0")
	private Double maxIntensity = null;

	@Option(names = { "--multiRes" }, description = "Automatically create a multi-resolution pyramid (default: false)")
	private boolean multiRes = false;

	@Option(names = { "-ds", "--downsampling" }, split = ";", required = false,
			description = "Manually define steps to create a multi-resolution pyramid (e.g. -ds 1,1,1 -ds 2,2,1 -ds 4,4,2 -ds 8,8,4)")
	private List<String> downsampling = null;

	@Option(names = { "--preserveAnisotropy" }, description = "preserve the anisotropy of the data (default: false)")
	private boolean preserveAnisotropy = false;

	@Option(names = { "--anisotropyFactor" }, description = "define the anisotropy factor if preserveAnisotropy is set to true (default: compute from data)")
	private double anisotropyFactor = Double.NaN;

	URI n5PathURI = null, xmlOutURI = null;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		if (dryRun)
		{
			System.out.println( "dry-run not supported for non-rigid fusion.");
			System.exit( 0 );
		}

		if ( (this.n5Dataset == null && this.bdvString == null) || (this.n5Dataset != null && this.bdvString != null) )
		{
			System.out.println( "You must define either the n5dataset (e.g. -d /ch488/s0) OR the BigDataViewer specification (e.g. --bdv 0,1)");
			System.exit( 0 );
		}

		if ( this.bdvString != null && xmlOutURIString == null )
		{
			System.out.println( "Please specify the output XML for the BDV dataset: -xo");
			return null;
		}

		Import.validateInputParameters(dataTypeFusion, minIntensity, maxIntensity);

		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIdsGlobal = this.loadViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			return null;

		if ( interestPoints == null || interestPoints.size() == 0 )
		{
			throw new IllegalArgumentException( "no interest points defined, exiting.");
		}

		for ( final String ip : interestPoints )
			System.out.println( "nonrigid using interestpoint label: " + ip );

		// validate downsampling parameters before doing any expensive work
		if ( !Downsampling.testDownsamplingParameters( this.multiRes, this.downsampling ) )
			return null;

		// get bounding box (may be adjusted below for anisotropy)
		BoundingBox bb = Import.getBoundingBox( dataGlobal, viewIdsGlobal, boundingBoxName );

		// anisotropy preservation: compress z so output voxels are isotropic
		if ( preserveAnisotropy )
		{
			System.out.println( "Preserving anisotropy." );

			if ( Double.isNaN( anisotropyFactor ) )
			{
				anisotropyFactor = TransformationTools.getAverageAnisotropyFactor( dataGlobal, viewIdsGlobal );
				System.out.println( "Anisotropy factor [computed from data]: " + anisotropyFactor );
			}
			else
			{
				System.out.println( "Anisotropy factor [provided]: " + anisotropyFactor );
			}

			final long[] minBB = bb.minAsLongArray();
			final long[] maxBB = bb.maxAsLongArray();
			minBB[ 2 ] = Math.round( Math.floor( minBB[ 2 ] / anisotropyFactor ) );
			maxBB[ 2 ] = Math.round( Math.ceil(  maxBB[ 2 ] / anisotropyFactor ) );
			bb = new BoundingBox( new FinalInterval( minBB, maxBB ) );
			System.out.println( "Adjusted bounding box (anisotropy preserved): " + Util.printInterval( bb ) );
		}

		// build the multi-resolution downsampling table
		final int[][] downsamplings;
		if ( multiRes )
			downsamplings = ExportN5Api.estimateMultiResPyramid( new FinalDimensions( bb ), anisotropyFactor );
		else if ( this.downsampling != null )
			downsamplings = Import.csvStringListToDownsampling( this.downsampling );
		else
			downsamplings = new int[][]{{ 1, 1, 1 }};

		if ( downsamplings == null )
			return null;

		System.out.println( "The following downsampling pyramid will be created:" );
		System.out.println( Arrays.deepToString( downsamplings ) );

		this.n5PathURI = URITools.toURI( n5PathURIString );
		System.out.println( "Fused volume: " + n5PathURI );

		if ( this.bdvString != null )
		{
			this.xmlOutURI = URITools.toURI( xmlOutURIString );
			System.out.println( "XML: " + xmlOutURI );
		}

		final int[] blockSize = Import.csvStringToIntArray(blockSizeString);
		final int[] blocksPerJob = Import.csvStringToIntArray(blockScaleString);
		System.out.println( "Fusing: " + bb.getTitle() + ": " + Util.printInterval( bb ) +
				" with blocksize " + Util.printCoordinates( blockSize ) + " and " + Util.printCoordinates( blocksPerJob ) + " blocks per job" );

		final DataType dataType;

		if ( dataTypeFusion == DataTypeFusion.UINT8 )
		{
			System.out.println( "Fusing to UINT8, min intensity = " + minIntensity + ", max intensity = " + maxIntensity );
			dataType = DataType.UINT8;
		}
		else if ( dataTypeFusion == DataTypeFusion.UINT16 )
		{
			System.out.println( "Fusing to UINT16, min intensity = " + minIntensity + ", max intensity = " + maxIntensity );
			dataType = DataType.UINT16;
		}
		else
		{
			System.out.println( "Fusing to FLOAT32" );
			dataType = DataType.FLOAT32;
		}

		final long[] dimensions = bb.dimensionsAsLongArray();
		final long[] min = bb.minAsLongArray();

		//
		// final variables for Spark
		//
		final URI n5PathURI = this.n5PathURI;
		final String n5Dataset = this.n5Dataset != null ? this.n5Dataset : N5ApiTools.createBDVPath( this.bdvString, 0, this.storageType );
		final URI xmlURI = this.xmlURI;
		final URI xmloutURI = this.xmlOutURI;
		final StorageFormat storageType = this.storageType;
		final Compression compression = new ZstandardCompression( 3 );

		// sharding setup
		if ( useSharding == null )
			useSharding = (storageType == StorageFormat.ZARR);
		if ( useSharding && storageType != StorageFormat.ZARR )
		{
			System.out.println( "WARNING: Sharding is only supported for ZARR v3. Disabling sharding." );
			useSharding = false;
		}
		final int[] shardSize = useSharding ? Import.csvStringToIntArray( shardSizeString ) : null;
		System.out.println( useSharding
				? "Sharding ENABLED, shard size: " + Util.printCoordinates( shardSize )
				: "Sharding DISABLED" );

		// In the sharded two-stage path, --blockScale defines the size of the sub-block each Spark
		// task fuses (compute granularity), while the shard stays the write granularity. The
		// sub-block must tile the shard exactly, i.e. computeBlockSize must divide shardSize (it is
		// a multiple of blockSize by construction).
		final int[] computeBlockSize;
		if ( useSharding )
		{
			computeBlockSize = new int[ 3 ];
			for ( int d = 0; d < 3; ++d )
			{
				computeBlockSize[ d ] = blockSize[ d ] * blocksPerJob[ d ];

				if ( shardSize[ d ] % computeBlockSize[ d ] != 0 )
				{
					System.out.println( "ERROR: compute block size (blockSize * blockScale = " + Util.printCoordinates( computeBlockSize )
							+ ") must divide the shard size " + Util.printCoordinates( shardSize ) + " in every dimension. "
							+ "Adjust --blockScale and/or --shardSize." );
					return null;
				}
			}
			System.out.println( "Two-stage sharded fusion: compute sub-block " + Util.printCoordinates( computeBlockSize )
					+ " (blockSize * blockScale), shard write unit " + Util.printCoordinates( shardSize ) );
		}
		else
		{
			computeBlockSize = null;
		}

		final ArrayList< String > labels = new ArrayList<>(interestPoints);
		final boolean uint8 = (dataTypeFusion == DataTypeFusion.UINT8);
		final boolean uint16 = (dataTypeFusion == DataTypeFusion.UINT16);
		final double minIntensity = (uint8 || uint16 ) ? this.minIntensity : 0;
		final double range;
		if ( uint8 )
			range = ( this.maxIntensity - this.minIntensity ) / 255.0;
		else if ( uint16 )
			range = ( this.maxIntensity - this.minIntensity ) / 65535.0;
		else
			range = 0;
		final int[][] serializedViewIds = Spark.serializeViewIds(viewIdsGlobal);

		// capture anisotropy state for Spark lambdas
		final boolean preserveAnisotropy = this.preserveAnisotropy;
		final double anisotropyFactorFinal = this.anisotropyFactor;

		try
		{
			// trigger the N5-blosc error, because if it is triggered for the first
			// time inside Spark, everything crashes
			new N5FSWriter(null);
		}
		catch (Exception e ) {}

		System.out.println( "Format being written: " + storageType );

		final N5Writer driverVolumeWriter = N5Util.createN5Writer(n5PathURI, storageType);

		if ( driverVolumeWriter == null )
			return null;

		// build a level → dataset-path function (used by setupMultiResolutionPyramid for all levels)
		final Function<Integer, String> levelToName;
		if ( bdvString != null )
		{
			levelToName = level -> level == 0
					? n5Dataset
					: N5ApiTools.createDownsampledBDVPath( n5Dataset, level, storageType );
		}
		else if ( n5Dataset.endsWith( "/s0" ) )
		{
			final String base = n5Dataset.substring( 0, n5Dataset.length() - 3 );
			levelToName = level -> base + "/s" + level;
		}
		else
		{
			levelToName = level -> level == 0 ? n5Dataset : n5Dataset + "/s" + level;
		}

		// create all dataset levels (s0..sN) with sharding codec when useSharding=true
		N5ApiTools.setupMultiResolutionPyramid(
				driverVolumeWriter,
				levelToName,
				dataType,
				dimensions,
				compression,
				blockSize,
				downsamplings,
				useSharding,
				shardSize );

		// superBlockSize = write granularity per Spark task.
		// When sharding ON it must equal shardSize; when OFF it is blockSize * blocksPerJob.
		final int[] superBlockSize = new int[ 3 ];
		Arrays.setAll( superBlockSize, d -> useSharding ? shardSize[ d ] : blockSize[ d ] * blocksPerJob[ d ] );
		final List<long[][]> grid = new ArrayList<>( Grid.create(dimensions, superBlockSize, blockSize) );

		System.out.println( "numJobs = " + grid.size() );

		driverVolumeWriter.setAttribute( n5Dataset, "offset", min);

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
						xmloutURI,
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

		final SparkConf conf = new SparkConf().setAppName("NonRigidFusion");

		if (localSparkBindAddress)
		{
			conf.set("spark.driver.bindAddress", "127.0.0.1");
			conf.set("spark.driver.host", "localhost");
			org.apache.spark.util.Utils.setCustomHostname("localhost");
		}

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		//
		// s0 fusion
		//
		final long time = System.currentTimeMillis();

		if ( useSharding )
		{
			// ===================================================================================
			// Two-stage sharded fusion: decouple COMPUTE granularity from WRITE granularity.
			//
			// A shard is written atomically as a single file, so it must be handed to N5 fully
			// assembled. But the expensive non-rigid rendering that fills it does NOT have to run
			// in one task. We therefore:
			//   Stage 1 (map):        fuse small (computeBlockSize) sub-blocks in parallel -- one
			//                         Spark task per sub-block, one thread each (no oversubscription).
			//   shuffle:              key each rendered sub-block by the shard it belongs to.
			//   Stage 2 (groupByKey): gather all sub-blocks of a shard into one task, assemble the
			//                         shard buffer, and write it with a single saveBlock (identical
			//                         contract to the single-stage path: shard-sized zero-min source
			//                         + inner-block grid offset).
			// ===================================================================================

			final long[] shardSizeL = new long[] { shardSize[ 0 ], shardSize[ 1 ], shardSize[ 2 ] };
			final long nS0 = ( dimensions[ 0 ] + shardSize[ 0 ] - 1 ) / shardSize[ 0 ];
			final long nS1 = ( dimensions[ 1 ] + shardSize[ 1 ] - 1 ) / shardSize[ 1 ];

			final List<long[][]> computeGrid = new ArrayList<>( Grid.create( dimensions, computeBlockSize, blockSize ) );

			// Determine which shards actually overlap data, mirroring the per-block expand-50 test the
			// render tasks use. An all-background shard produces no sub-block output and therefore no
			// groupByKey entry, so it must be excluded from the (shard-level) retry universe -- otherwise
			// RetryTrackerSpark would treat it as a permanent failure. Also avoids launching render tasks
			// for empty shards.
			if ( preserveAnisotropy )
			{
				// inject the same z-scale the render tasks use so bounding boxes are in output space
				// (safe: dataGlobal is not used for fusion afterwards -- tasks reload their own copy)
				final AffineTransform3D zScale = new AffineTransform3D();
				zScale.set( 1,0,0,0, 0,1,0,0, 0,0,1.0/anisotropyFactorFinal,0 );
				final ViewTransformAffine at = new ViewTransformAffine( "anisotropy_correction", zScale );
				for ( final ViewId viewId : viewIdsGlobal )
					dataGlobal.getViewRegistrations().getViewRegistration( viewId ).getTransformList().add( at );
			}

			final List<long[][]> shardsToWrite = new ArrayList<>();
			for ( final long[][] shardBlock : grid )
			{
				final Interval shardInterval =
						Intervals.translate( Intervals.translate( new FinalInterval( shardBlock[ 1 ] ), shardBlock[ 0 ] ), min );
				for ( final ViewId viewId : viewIdsGlobal )
				{
					dataGlobal.getViewRegistrations().getViewRegistration( viewId ).updateModel();
					final Interval vb = Intervals.expand(
							ViewUtil.getTransformedBoundingBox( dataGlobal, viewId, dataGlobal.getViewRegistrations().getViewRegistration( viewId ).getModel() ), 50 );
					if ( ViewUtil.overlaps( shardInterval, vb ) )
					{
						shardsToWrite.add( shardBlock );
						break;
					}
				}
			}

			System.out.println( "Stage 1: " + computeGrid.size() + " compute sub-blocks; Stage 2: "
					+ shardsToWrite.size() + "/" + grid.size() + " shards overlap data and will be written." );

			// Retry at the SHARD level (the write unit). Render (Stage 1) failures are left to Spark's
			// native task retry, so the shuffle only ever delivers complete shard data to Stage 2; this
			// tracker only re-drives transient Stage-2 write failures.
			final List<long[][]> gridRetry = new ArrayList<>( shardsToWrite );
			final RetryTrackerSpark<long[][]> retryTracker =
					RetryTrackerSpark.forGridBlocks( "s0 non-rigid shard write", gridRetry.size() );

			do
			{
				if ( !retryTracker.beginAttempt() )
				{
					System.out.println( "Stopping." );
					System.exit( 1 );
				}

				// shards still to write this attempt, and the sub-blocks that feed them
				final java.util.Set<Long> pendingShards = new java.util.HashSet<>();
				for ( final long[][] shardBlock : gridRetry )
					pendingShards.add( shardKeyOf( shardBlock[ 0 ], shardSizeL, nS0, nS1 ) );

				final List<long[][]> subBlocks = new ArrayList<>();
				for ( final long[][] sb : computeGrid )
					if ( pendingShards.contains( shardKeyOf( sb[ 0 ], shardSizeL, nS0, nS1 ) ) )
						subBlocks.add( sb );

				// Stage 1: render sub-blocks in parallel, keyed by linear shard index.
				final JavaPairRDD<Long, FusedBlock> rendered = sc
						.parallelize( subBlocks, Math.min( Spark.maxPartitions, subBlocks.size() ) )
						.flatMapToPair( gridBlock -> {

						// The non-rigid deformation lattice is anchored at boundingBox.min and viewsToUse
						// is chosen per boundingBox (ModelGrid: pos = index * cpd + min). To keep sub-blocks
						// of the same shard seamless, we build the model over the WHOLE SHARD (identical to
						// what the original per-shard fusion does) and only render this sub-block's region.
						// virtualGrid=true means the cached grid computes only the control points this
						// sub-block touches (plus a one-spacing halo neighboring tasks recompute), so the
						// per-task cost stays proportional to the sub-block, not the whole shard.
						final long[] shardStart = new long[ 3 ];
						final long[] shardDims = new long[ 3 ];
						final long[] localOffset = new long[ 3 ];
						final long[] sk = new long[ 3 ];
						for ( int d = 0; d < 3; ++d )
						{
							sk[ d ] = gridBlock[ 0 ][ d ] / shardSizeL[ d ];
							shardStart[ d ] = sk[ d ] * shardSizeL[ d ];
							shardDims[ d ] = Math.min( shardSizeL[ d ], dimensions[ d ] - shardStart[ d ] );
							localOffset[ d ] = gridBlock[ 0 ][ d ] - shardStart[ d ];
						}
						final long shardKey = shardKeyOf( gridBlock[ 0 ], shardSizeL, nS0, nS1 );

						final SpimData2 dataLocal = Spark.getSparkJobSpimData2( xmlURI );

						// model + view selection over the whole shard; zero-min source spanning the shard
						final RandomAccessibleInterval< FloatType > shardSource =
								fuseNonRigidBlock( dataLocal, serializedViewIds, min, shardStart, shardDims,
										labels, preserveAnisotropy, anisotropyFactorFinal, true );

						// no view overlaps this shard at all -> emit nothing (shard stays background)
						if ( shardSource == null )
							return java.util.Collections.emptyIterator();

						final long[] size = gridBlock[ 1 ];
						final long[] sizeL = new long[] { size[ 0 ], size[ 1 ], size[ 2 ] };
						final int[] sizeI = new int[] { (int)size[ 0 ], (int)size[ 1 ], (int)size[ 2 ] };
						final int numEle = sizeI[ 0 ] * sizeI[ 1 ] * sizeI[ 2 ];

						// crop the shard-spanning source down to this sub-block (zero-min)
						final RandomAccessibleInterval< FloatType > source = Views.offsetInterval( shardSource, localOffset, sizeL );

						// materialize + convert to the target type (also halves/quarters shuffle size for UINT16/UINT8)
						final Object data;
						if ( uint8 )
						{
							final byte[] arr = new byte[ numEle ];
							final ArrayImg< UnsignedByteType, ? > tgt = ArrayImgs.unsignedBytes( arr, sizeL );
							LoopBuilder.setImages( source, tgt ).forEachPixel( ( i, o ) -> o.setReal( ( i.get() - minIntensity ) / range ) );
							data = arr;
						}
						else if ( uint16 )
						{
							final short[] arr = new short[ numEle ];
							final ArrayImg< UnsignedShortType, ? > tgt = ArrayImgs.unsignedShorts( arr, sizeL );
							LoopBuilder.setImages( source, tgt ).forEachPixel( ( i, o ) -> o.setReal( ( i.get() - minIntensity ) / range ) );
							data = arr;
						}
						else
						{
							final float[] arr = new float[ numEle ];
							final ArrayImg< FloatType, ? > tgt = ArrayImgs.floats( arr, sizeL );
							LoopBuilder.setImages( source, tgt ).forEachPixel( ( i, o ) -> o.set( i.get() ) );
							data = arr;
						}

						return java.util.Collections.singletonList(
								new Tuple2<>( shardKey, new FusedBlock( localOffset, sizeI, data ) ) ).iterator();
					});

				// Stage 2: gather a shard's sub-blocks, assemble, write it once. Returns the shard's
				// grid block on success / null on a transient write failure, so RetryTrackerSpark can
				// re-drive just the failed shards (and their feeding sub-blocks) on the next attempt.
				final JavaRDD<long[][]> shardResult = rendered
						.groupByKey( Math.min( Spark.maxPartitions, gridRetry.size() ) )
						.map( entry -> {

							final N5Writer w = N5Util.createN5Writer( n5PathURI, storageType );

							try
							{
								// decode linear shard index -> shard grid position
								final long key = entry._1();
								final long plane = nS0 * nS1;
								final long[] sk = new long[] { key % nS0, ( key % plane ) / nS0, key / plane };

								// shard offset / dimensions (cropped at the dataset edge) / inner-block write offset
								final long[] shardOffset = new long[ 3 ];
								final long[] shardDims = new long[ 3 ];
								final long[] shardGridOffset = new long[ 3 ];
								for ( int d = 0; d < 3; ++d )
								{
									shardOffset[ d ] = sk[ d ] * shardSize[ d ];
									shardDims[ d ] = Math.min( shardSize[ d ], dimensions[ d ] - shardOffset[ d ] );
									shardGridOffset[ d ] = shardOffset[ d ] / blockSize[ d ];
								}

								if ( uint8 )
								{
									final ArrayImg< UnsignedByteType, ? > shardImg = ArrayImgs.unsignedBytes( shardDims );
									for ( final FusedBlock fb : entry._2() )
									{
										final long[] fbDims = new long[] { fb.size[ 0 ], fb.size[ 1 ], fb.size[ 2 ] };
										final ArrayImg< UnsignedByteType, ? > blk = ArrayImgs.unsignedBytes( (byte[])fb.data, fbDims );
										LoopBuilder.setImages( blk, Views.offsetInterval( shardImg, fb.localOffset, fbDims ) ).forEachPixel( ( s, t ) -> t.set( s ) );
									}
									N5Utils.saveBlock( shardImg, w, n5Dataset, shardGridOffset );
								}
								else if ( uint16 )
								{
									final ArrayImg< UnsignedShortType, ? > shardImg = ArrayImgs.unsignedShorts( shardDims );
									for ( final FusedBlock fb : entry._2() )
									{
										final long[] fbDims = new long[] { fb.size[ 0 ], fb.size[ 1 ], fb.size[ 2 ] };
										final ArrayImg< UnsignedShortType, ? > blk = ArrayImgs.unsignedShorts( (short[])fb.data, fbDims );
										LoopBuilder.setImages( blk, Views.offsetInterval( shardImg, fb.localOffset, fbDims ) ).forEachPixel( ( s, t ) -> t.set( s ) );
									}
									N5Utils.saveBlock( shardImg, w, n5Dataset, shardGridOffset );
								}
								else
								{
									final ArrayImg< FloatType, ? > shardImg = ArrayImgs.floats( shardDims );
									for ( final FusedBlock fb : entry._2() )
									{
										final long[] fbDims = new long[] { fb.size[ 0 ], fb.size[ 1 ], fb.size[ 2 ] };
										final ArrayImg< FloatType, ? > blk = ArrayImgs.floats( (float[])fb.data, fbDims );
										LoopBuilder.setImages( blk, Views.offsetInterval( shardImg, fb.localOffset, fbDims ) ).forEachPixel( ( s, t ) -> t.set( s ) );
									}
									N5Utils.saveBlock( shardImg, w, n5Dataset, shardGridOffset );
								}

								return new long[][] { shardOffset, shardDims, shardGridOffset };
							}
							catch ( Throwable t )
							{
								System.out.println( "Error writing shard key=" + entry._1() + " (will be re-tried): " + t );
								t.printStackTrace();
								return null;
							}
							finally
							{
								if ( w != N5Util.sharedHDF5Writer )
									w.close();
							}
						});

				shardResult.cache();
				shardResult.count();

				final java.util.Set<long[][]> failedShards = retryTracker.processWithSpark( shardResult, gridRetry );
				if ( !retryTracker.processFailures( failedShards ) )
				{
					System.out.println( "Stopping." );
					System.exit( 1 );
				}
				gridRetry.clear();
				gridRetry.addAll( failedShards );
			}
			while ( gridRetry.size() > 0 );
		}
		else
		{
			// ===================================================================================
			// Single-stage path (no sharding): compute block == write block, so each task fuses
			// and writes its own block independently, with app-level retry.
			// ===================================================================================
			final List<long[][]> gridRetry = new ArrayList<>( grid );
			final RetryTrackerSpark<long[][]> retryTracker =
					RetryTrackerSpark.forGridBlocks( "s0 non-rigid block processing", gridRetry.size() );

			do
			{
				if ( !retryTracker.beginAttempt() )
				{
					System.out.println( "Stopping." );
					System.exit( 1 );
				}

				final JavaRDD<long[][]> rdd = sc.parallelize( gridRetry, Math.min( Spark.maxPartitions, gridRetry.size() ) );

				final JavaRDD<long[][]> rddResult = rdd.map(
						gridBlock -> {
							try
							{
								final SpimData2 dataLocal = Spark.getSparkJobSpimData2( xmlURI );

								final RandomAccessibleInterval< FloatType > source =
										fuseNonRigidBlock( dataLocal, serializedViewIds, min, gridBlock[ 0 ], gridBlock[ 1 ],
												labels, preserveAnisotropy, anisotropyFactorFinal, false );

								// nothing to save...
								if ( source == null )
									return gridBlock.clone();

								final N5Writer executorVolumeWriter = N5Util.createN5Writer(n5PathURI, storageType);

								if ( uint8 )
								{
									final RandomAccessibleInterval< UnsignedByteType > sourceUINT8 =
											Converters.convert(
													source,(i, o) -> o.setReal( ( i.get() - minIntensity ) / range ),
													new UnsignedByteType());

									N5Utils.saveBlock(sourceUINT8, executorVolumeWriter, n5Dataset, gridBlock[2]);
								}
								else if ( uint16 )
								{
									final RandomAccessibleInterval< UnsignedShortType > sourceUINT16 =
											Converters.convert(
													source,(i, o) -> o.setReal( ( i.get() - minIntensity ) / range ),
													new UnsignedShortType());

									N5Utils.saveBlock(sourceUINT16, executorVolumeWriter, n5Dataset, gridBlock[2]);
								}
								else
								{
									N5Utils.saveBlock(source, executorVolumeWriter, n5Dataset, gridBlock[2]);
								}

								if ( executorVolumeWriter != N5Util.sharedHDF5Writer )
									executorVolumeWriter.close();

								return gridBlock.clone();
							}
							catch ( Throwable t )
							{
								System.out.println( "Error processing block offset=" + Arrays.toString( gridBlock[0] ) + " (will be re-tried): " + t );
								t.printStackTrace();
								return null;
							}
						});

				rddResult.cache();
				rddResult.count();

				final Set<long[][]> failedBlocks = retryTracker.processWithSpark( rddResult, gridRetry );
				if ( !retryTracker.processFailures( failedBlocks ) )
				{
					System.out.println( "Stopping." );
					System.exit( 1 );
				}
				gridRetry.clear();
				gridRetry.addAll( failedBlocks );
			}
			while ( gridRetry.size() > 0 );
		}

		System.out.println( new Date() + ": Saved full resolution, took: " + (System.currentTimeMillis() - time ) + " ms." );

		//
		// multi-resolution pyramid (s1 .. sN)
		//
		if ( downsamplings.length > 1 )
		{
			long[] previousDim = dimensions.clone();
			String previousDataset = n5Dataset;

			for ( int level = 1; level < downsamplings.length; ++level )
			{
				final int[] relativeDs = new int[ downsamplings[ 0 ].length ];
				for ( int d = 0; d < relativeDs.length; ++d )
					relativeDs[ d ] = downsamplings[ level ][ d ] / downsamplings[ level - 1 ][ d ];

				final long[] dimDS = new long[ previousDim.length ];
				for ( int d = 0; d < dimDS.length; ++d )
					dimDS[ d ] = previousDim[ d ] / relativeDs[ d ];

				final String datasetDS = levelToName.apply( level );

				// dataset already created by setupMultiResolutionPyramid; just ensure attribute is set
				driverVolumeWriter.setAttribute( datasetDS, "downsamplingFactors", downsamplings[ level ] );

				System.out.println( new Date() + ": Downsampling: " + Arrays.toString( downsamplings[ level ] )
						+ " with relative downsampling of " + Arrays.toString( relativeDs ) );
				System.out.println( new Date() + ": Loading '" + previousDataset + "', downsampled will be written as '" + datasetDS + "'." );

				final List<long[][]> gridDS = new ArrayList<>( Grid.create( dimDS, superBlockSize, blockSize ) );
				System.out.println( new Date() + ": s" + level + " numBlocks=" + gridDS.size() );

				final List<long[][]> gridDSRetry = new ArrayList<>( gridDS );
				final RetryTrackerSpark<long[][]> retryTrackerDS =
						RetryTrackerSpark.forGridBlocks( "s" + level + " non-rigid block processing", gridDSRetry.size() );

				final long timeDS = System.currentTimeMillis();
				final String prevDataset = previousDataset;
				final int[] relDsCapture = relativeDs;

				do
				{
					if ( !retryTrackerDS.beginAttempt() )
					{
						System.out.println( "Stopping." );
						System.exit( 1 );
					}

					final JavaRDD<long[][]> rddDS = sc.parallelize( gridDSRetry, Math.min( Spark.maxPartitions, gridDSRetry.size() ) );

					final JavaRDD<long[][]> rddDSResult = rddDS.map( gridBlock ->
					{
						final N5Writer executorVolumeWriterDS = N5Util.createN5Writer( n5PathURI, storageType );

						try
						{
							if ( dataType == DataType.UINT16 )
							{
								RandomAccessibleInterval<UnsignedShortType> downsampled =
										N5Utils.open( executorVolumeWriterDS, prevDataset );
								for ( int d = 0; d < downsampled.numDimensions(); ++d )
									if ( relDsCapture[ d ] > 1 )
										downsampled = LazyHalfPixelDownsample2x.init(
												downsampled,
												new FinalInterval( downsampled ),
												new UnsignedShortType(),
												blockSize,
												d );
								N5Utils.saveNonEmptyBlock(
										Views.offsetInterval( downsampled, gridBlock[0], gridBlock[1] ),
										executorVolumeWriterDS, datasetDS, gridBlock[2], new UnsignedShortType() );
							}
							else if ( dataType == DataType.UINT8 )
							{
								RandomAccessibleInterval<UnsignedByteType> downsampled =
										N5Utils.open( executorVolumeWriterDS, prevDataset );
								for ( int d = 0; d < downsampled.numDimensions(); ++d )
									if ( relDsCapture[ d ] > 1 )
										downsampled = LazyHalfPixelDownsample2x.init(
												downsampled,
												new FinalInterval( downsampled ),
												new UnsignedByteType(),
												blockSize,
												d );
								N5Utils.saveNonEmptyBlock(
										Views.offsetInterval( downsampled, gridBlock[0], gridBlock[1] ),
										executorVolumeWriterDS, datasetDS, gridBlock[2], new UnsignedByteType() );
							}
							else // FLOAT32
							{
								RandomAccessibleInterval<FloatType> downsampled =
										N5Utils.open( executorVolumeWriterDS, prevDataset );
								for ( int d = 0; d < downsampled.numDimensions(); ++d )
									if ( relDsCapture[ d ] > 1 )
										downsampled = LazyHalfPixelDownsample2x.init(
												downsampled,
												new FinalInterval( downsampled ),
												new FloatType(),
												blockSize,
												d );
								N5Utils.saveNonEmptyBlock(
										Views.offsetInterval( downsampled, gridBlock[0], gridBlock[1] ),
										executorVolumeWriterDS, datasetDS, gridBlock[2], new FloatType() );
							}
						}
						catch ( Exception e )
						{
							System.out.println( "Error writing downsampled block offset=" + Arrays.toString( gridBlock[0] ) + ": " + e );
							e.printStackTrace();
							if ( executorVolumeWriterDS != N5Util.sharedHDF5Writer )
								executorVolumeWriterDS.close();
							return null; // signals failure to retry tracker
						}

						if ( executorVolumeWriterDS != N5Util.sharedHDF5Writer )
							executorVolumeWriterDS.close();

						return gridBlock.clone();
					});

					rddDSResult.cache();
					rddDSResult.count();

					final Set<long[][]> failedDS = retryTrackerDS.processWithSpark( rddDSResult, gridDSRetry );
					if ( !retryTrackerDS.processFailures( failedDS ) )
					{
						System.out.println( "Stopping." );
						System.exit( 1 );
					}
					gridDSRetry.clear();
					gridDSRetry.addAll( failedDS );
				}
				while ( gridDSRetry.size() > 0 );

				System.out.println( new Date() + ": Saved level s" + level + ", took: " + (System.currentTimeMillis() - timeDS ) + " ms." );

				previousDim = dimDS;
				previousDataset = datasetDS;
			}
		}

		sc.close();

		// close main writer (is shared over Spark-threads if it's HDF5, thus just closing it here)
		driverVolumeWriter.close();

		System.out.println( "Saved non-rigid, e.g. view with './n5-view -i " + n5PathURI + " -d " + n5Dataset );
		System.out.println( "done, took: " + (System.currentTimeMillis() - time ) + " ms." );

		return null;
	}

	/**
	 * Linear shard index for the shard that contains the given pixel offset, matching the
	 * {@code shardKey = s0 + s1*nS0 + s2*nS0*nS1} encoding used as the Stage-1 shuffle key.
	 */
	private static long shardKeyOf( final long[] offset, final long[] shardSize, final long nS0, final long nS1 )
	{
		final long s0 = offset[ 0 ] / shardSize[ 0 ];
		final long s1 = offset[ 1 ] / shardSize[ 1 ];
		final long s2 = offset[ 2 ] / shardSize[ 2 ];
		return s0 + s1 * nS0 + s2 * nS0 * nS1;
	}

	/**
	 * Serializable carrier for one rendered sub-block, shuffled from the compute stage to the
	 * shard-assembly stage. {@code data} is a {@code byte[]}/{@code short[]}/{@code float[]}
	 * depending on the output data type; {@code localOffset} is the zero-min offset of this
	 * sub-block within its shard, {@code size} its dimensions.
	 */
	public static class FusedBlock implements Serializable
	{
		private static final long serialVersionUID = 1L;

		final long[] localOffset;
		final int[] size;
		final Object data;

		public FusedBlock( final long[] localOffset, final int[] size, final Object data )
		{
			this.localOffset = localOffset;
			this.size = size;
			this.data = data;
		}
	}

	/**
	 * Builds the non-rigid model over {@code [blockOffset, blockOffset+blockSize)} (in the dataset's
	 * 0-based space, shifted by {@code min}) and returns a lazy, zero-min {@link FloatType} fusion
	 * spanning exactly that interval, or {@code null} if no view overlaps it.
	 *
	 * The deformation lattice is anchored at the boundingBox min (see {@code ModelGrid}), so the
	 * caller controls seam behaviour through the interval it passes here: the single-stage path
	 * passes the block it writes; the two-stage sharded path passes the whole shard (and renders
	 * sub-regions of the returned source) so all sub-blocks of a shard share one lattice.
	 *
	 * @param virtualGrid if true, use a cached (lazy) control-point grid so only the queried region
	 *                    is computed; if false, the full grid over the interval is materialized.
	 */
	private static RandomAccessibleInterval< FloatType > fuseNonRigidBlock(
			final SpimData2 dataLocal,
			final int[][] serializedViewIds,
			final long[] min,
			final long[] blockOffset,
			final long[] blockSize,
			final ArrayList< String > labels,
			final boolean preserveAnisotropy,
			final double anisotropyFactorFinal,
			final boolean virtualGrid )
	{
		// inject z-scale into every view registration so the non-rigid fusion
		// works in isotropic output space (same effect as adjustAllTransforms
		// in the affine fusion path)
		if ( preserveAnisotropy )
		{
			final AffineTransform3D zScale = new AffineTransform3D();
			zScale.set( 1,0,0,0, 0,1,0,0, 0,0,1.0/anisotropyFactorFinal,0 );
			final ViewTransformAffine anisotropyTransform =
					new ViewTransformAffine( "anisotropy_correction", zScale );
			for ( int i = 0; i < serializedViewIds.length; ++i )
				dataLocal.getViewRegistrations()
						.getViewRegistration( Spark.deserializeViewIds( serializedViewIds, i ) )
						.getTransformList().add( anisotropyTransform );
			// updateModel() is called per-view in the overlap-detection loops below;
			// the z-scale is in the list so every recomposition includes it
		}

		// be smarter, test which ViewIds are actually needed for the block we want to fuse
		final Interval fusedBlock =
				Intervals.translate(
						Intervals.translate(
								new FinalInterval( blockSize ), // blocksize
								blockOffset ), // block offset
						min ); // min of the randomaccessbileinterval

		// recover views to process
		final List< ViewId > viewsToFuse = new ArrayList<>(); // fuse
		final List< ViewId > allViews = new ArrayList<>();

		for ( int i = 0; i < serializedViewIds.length; ++i )
		{
			final ViewId viewId = Spark.deserializeViewIds(serializedViewIds, i);

			// expand by 50 to be conservative for non-rigid overlaps
			dataLocal.getViewRegistrations().getViewRegistration( viewId ).updateModel();
			final Interval boundingBox = ViewUtil.getTransformedBoundingBox( dataLocal, viewId, dataLocal.getViewRegistrations().getViewRegistration( viewId ).getModel() );
			final Interval bounds = Intervals.expand( boundingBox, 50 );
			// TODO: estimate the "50" from the distance of corresponding, transformed interest points

			if ( ViewUtil.overlaps( fusedBlock, bounds ) )
				viewsToFuse.add( viewId );

			allViews.add( viewId );
		}

		// nothing to save...
		if ( viewsToFuse.size() == 0 )
			return null;

		// test with which views the viewsToFuse overlap
		// TODO: use the actual interest point correspondences maybe (i.e. change in mvr)
		final List< ViewId > viewsToUse = new ArrayList<>(); // used to compute the non-rigid transform

		for ( final ViewId viewId : allViews )
		{
			dataLocal.getViewRegistrations().getViewRegistration( viewId ).updateModel();
			final Interval boundingBoxView = ViewUtil.getTransformedBoundingBox( dataLocal, viewId, dataLocal.getViewRegistrations().getViewRegistration( viewId ).getModel() );
			final Interval boundsView = Intervals.expand( boundingBoxView, 25 );

			for ( final ViewId fusedId : viewsToFuse )
			{
				dataLocal.getViewRegistrations().getViewRegistration( fusedId ).updateModel();
				final Interval boundingBoxFused = ViewUtil.getTransformedBoundingBox( dataLocal, fusedId, dataLocal.getViewRegistrations().getViewRegistration( fusedId ).getModel() );
				final Interval boundsFused = Intervals.expand( boundingBoxFused, 25 );

				if ( ViewUtil.overlaps( boundsView, boundsFused ))
				{
					viewsToUse.add( viewId );
					break;
				}
			}
		}

		final double ds = 1.0;
		final int cpd = Math.max( 1, (int)Math.round( 10 / ds ) );

		final int interpolation = 1;
		final long[] controlPointDistance = new long[] { cpd, cpd, cpd };
		final double alpha = 1.0;

		final FusionType fusionType = FusionType.AVG_BLEND;
		final boolean displayDistances = false;

		final ExecutorService service = new SequentialExecutorService();

		final RandomAccessibleInterval< FloatType > source =
				NonRigidTools.fuseVirtualInterpolatedNonRigid(
						dataLocal,
						viewsToFuse,
						viewsToUse,
						labels,
						fusionType,
						displayDistances,
						controlPointDistance,
						alpha,
						virtualGrid,
						interpolation,
						fusedBlock,
						null,
						service );

		service.shutdown();

		return source;
	}

	public static void main(final String... args) {

		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new SparkNonRigidFusion()).execute(args));
	}
}
