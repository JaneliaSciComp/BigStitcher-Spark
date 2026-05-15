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

import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import bdv.img.n5.N5ImageLoader;
import bdv.util.MipmapTransforms;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.bigstitcher.spark.util.RetryTrackerSpark;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.plugin.resave.Resave_HDF5;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.AllenOMEZarrLoader;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.AllenOMEZarrLoader.OMEZARREntry;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.OMEZarrAttributes;
import net.preibisch.mvrecon.process.n5api.N5ApiTools;
import net.preibisch.mvrecon.process.n5api.N5ApiTools.MultiResolutionLevelInfo;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bigdataviewer.n5.N5CloudImageLoader;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.shard.ShardCodecInfo;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import org.janelia.saalfeldlab.n5.universe.metadata.ome.ngff.v04.OmeNgffMultiScaleMetadata;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

public class SparkResaveN5 extends AbstractBasic implements Callable<Void>, Serializable
{
	/*
	-x '/Users/preibischs/Documents/Microscopy/Stitching/Truman/testspark/dataset.xml'
	-xo '/Users/preibischs/Documents/Microscopy/Stitching/Truman/testspark/dataset-n5.xml'
	-ds '1,1,1; 2,2,1; 4,4,1; 8,8,2'
	*/

	// TOOD: there is a bug:
	// -x s3://janelia-bigstitcher-spark/Stitching/dataset.xml -xo /Users/preibischs/SparkTest/Stitching/dataset.xml
	// sets the wrong path for the N5:
	// file:/Users/preibischs/workspace/BigStitcher-Spark/file:/Users/preibischs/SparkTest/Stitching-fromcloud/dataset.n5
	
	private static final long serialVersionUID = 1890656279324908516L;

	@Option(names = { "-xo", "--xmlout" }, required = false, description = "path to the output BigStitcher xml, e.g. /home/project-n5.xml or s3://myBucket/dataset.xml (default: overwrite input and keep a backup ~1)")
	private String xmlOutURIString = null;

	private URI xmlOutURI = null;

	@Option(names = {"-s", "--storage"}, defaultValue = "ZARR", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Dataset storage type: N5, ZARR2 (Zarr v2), or ZARR (Zarr v3 with sharding support, default)")
	private StorageFormat storageFormat = StorageFormat.ZARR;

	@Option(names = "--blockSize", description = "blockSize, you can use smaller blocks for HDF5 (default: 128,128,64)")
	private String blockSizeString = "128,128,64";

	@Option(names = "--blockScale", description = "how many blocks to use for a single processing step, e.g. 4,4,1 means for blockSize 128,128,32 that each spark thread writes 512,512,32; also serves as shard size factor when ZARR v3 sharding is enabled (default: 16,16,1)")
	private String blockScaleString = "16,16,1";

	@Option(names = { "-ds", "--downsampling" }, description = "downsampling pyramid (must contain full res 1,1,1 that is always created), e.g. 1,1,1; 2,2,1; 4,4,1; 8,8,2 (default: automatically computed)")
	private String downsampling = null;

	@Option(names = {"-c", "--compression"}, defaultValue = "Zstandard", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Dataset compression")
	private Compressions compressionType = null;

	@Option(names = {"-cl", "--compressionLevel" }, description = "compression level, if supported by the codec (default: gzip 1, Zstandard 3, xz 6)")
	private Integer compressionLevel = null;

	@Option(names = { "-o", "--n5Path" }, description = "N5/OME-ZARR path for saving, (default: 'folder of the xml'/dataset.n5 or e.g. s3://myBucket/data.n5)")
	private String n5PathURIString = null;

	@Option(names = { "--downsampleS0" }, description = "Skip writing the s0/full-resolution level and only generate downsampling pyramid levels; requires existing s0 datasets for all selected views (default: false)")
	private boolean downsampleS0 = false;

	@Option(names = { "--useSharding" },
			description = "Enable Zarr v3 sharding using blockScale as shard size factor (default: enabled for ZARR v3, disabled for N5/ZARR v2)")
	private Boolean useSharding = null; // null = auto-detect

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		System.out.println( "com.google.common.collect.ImmutableList: " +  com.google.common.collect.ImmutableList.class.getProtectionDomain().getCodeSource().getLocation() );
		System.out.println( "com.google.common.collect.Streams: " + com.google.common.collect.Streams.class.getProtectionDomain().getCodeSource().getLocation() );

		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		if ( xmlOutURIString == null )
			xmlOutURIString = xmlURIString;

		xmlOutURI = URITools.toURI( xmlOutURIString );
		System.out.println( "xmlout: " + xmlOutURI );

		// process all views
		List< ViewId > viewIdsGlobal = Import.getViewIds( dataGlobal );

		if ( viewIdsGlobal.isEmpty() )
		{
			throw new IllegalArgumentException( "No views to resave." );
		}
		else
		{
			System.out.println( "Following ViewIds will be resaved: ");
			for ( final ViewId v : viewIdsGlobal )
				System.out.print( "[" + v.getTimePointId() + "," + v.getViewSetupId() + "] " );
			System.out.println();
		}

		// Determine default output filename based on storage format
		final String defaultFilename = storageFormat == StorageFormat.N5 ? "dataset.n5" : "dataset.ome.zarr";
		final URI n5PathURI = URITools.toURI( this.n5PathURIString == null ? URITools.appendName( URITools.getParentURI( xmlOutURI ), defaultFilename ) : n5PathURIString );
		final Compression compression = N5Util.getCompression( this.compressionType, this.compressionLevel );

		final int[] blockSize = Import.csvStringToIntArray(blockSizeString);
		final int[] blockScale = Import.csvStringToIntArray(blockScaleString);

		final int[] computeBlockSize = new int[] {
				blockSize[0] * blockScale[ 0 ],
				blockSize[1] * blockScale[ 1 ],
				blockSize[2] * blockScale[ 2 ] };

		// Auto-detect sharding based on storage format (enabled for ZARR v3, disabled for N5/ZARR2)
		if ( useSharding == null )
			useSharding = (storageFormat == StorageFormat.ZARR);

		// Validate: sharding only for ZARR v3
		if ( useSharding && storageFormat != StorageFormat.ZARR )
		{
			System.out.println( "WARNING: Sharding only supported for ZARR v3. Disabling sharding." );
			useSharding = false;
		}

		// Calculate shard size (shard size = blockSize * blockScale when sharding is enabled)
		final int[] shardSize;
		if ( useSharding )
		{
			shardSize = new int[] {
				blockSize[0] * blockScale[0],
				blockSize[1] * blockScale[1],
				blockSize[2] * blockScale[2]
			};
			System.out.println( "Sharding enabled. Shard size: " + Util.printCoordinates( shardSize ) + " (blockSize * blockScale)" );
			System.out.println( "Note: For Zarr v3 sharding, computeBlockSize equals shardSize for shard-aware writing." );
		}
		else
		{
			shardSize = null;
			System.out.println( "Sharding disabled." );
		}
		final N5Writer n5Writer = URITools.instantiateN5Writer( storageFormat, n5PathURI );

		System.out.println( "Compression: " + this.compressionType );
		System.out.println( "Compression level: " + ( compressionLevel == null ? "default" : compressionLevel ) );
		System.out.println( "N5/ZARR block size=" + Util.printCoordinates( blockSize ) );
		System.out.println( "Compute/Shard block size=" + Util.printCoordinates( computeBlockSize ) );
		System.out.println( "Setting up XML at: " + xmlOutURI );
		System.out.println( "Setting up N5 writing to basepath: " + n5PathURI );

		// all ViewSetupIds (needed to create N5 datasets)
		final HashMap<Integer, long[]> dimensions =
				N5ApiTools.assembleDimensions( dataGlobal, viewIdsGlobal );

		// all grids across all ViewId's
		final List<long[][]> gridS0 =
				viewIdsGlobal.stream().map( viewId ->
						N5ApiTools.assembleJobs(
								viewId,
								dimensions.get( viewId.getViewSetupId() ),
								blockSize,
								computeBlockSize ) ).flatMap(List::stream).collect( Collectors.toList() );
		System.out.printf("Process %d S0 grid blocks\n", gridS0.size());

		final Map<Integer, DataType> dataTypes =
				N5ApiTools.assembleDataTypes( dataGlobal, dimensions.keySet() );

		// estimate or read downsampling factors
		final int[][] downsamplings;

		if ( this.downsampling == null )
			downsamplings = N5ApiTools.mipMapInfoToDownsamplings( Resave_HDF5.proposeMipmaps( N5ApiTools.assembleViewSetups(dataGlobal, viewIdsGlobal) ) );
		else
			downsamplings = Import.csvStringToDownsampling( this.downsampling );

		if ( !Import.testFirstDownsamplingIsPresent( downsamplings ) )
			throw new RuntimeException( "First downsampling step must be full resolution [1,1,...1], stopping." );

		System.out.println( "Downsamplings: " + Arrays.deepToString( downsamplings ) );

		if ( dryRun )
		{
			System.out.println( "This is a dry-run, stopping here.");
			return null;
		}

		if (downsampleS0)
		{
			validateExistingS0Datasets(
					n5Writer,
					storageFormat,
					viewIdsGlobal,
					dimensions,
					dataTypes,
					blockSize,
					compression );
			System.out.println( "Skipping s0/full-resolution write; existing s0 datasets will be used to generate the downsampling pyramid." );
		}

		// create all datasets and write BDV metadata for all ViewIds (including downsampling) in parallel
		long time = System.currentTimeMillis();

		final Map< ViewId, MultiResolutionLevelInfo[] > viewIdToMrInfo =
				viewIdsGlobal.parallelStream().map( viewId ->
				{
					final MultiResolutionLevelInfo[] mrInfo;

					if ( storageFormat == StorageFormat.N5 )
					{
						if (downsampleS0)
						{
							mrInfo = setupBdvDatasetsN5SkippingS0(
									n5Writer,
									viewId,
									dataTypes.get(viewId.getViewSetupId()),
									dimensions.get(viewId.getViewSetupId()),
									compression,
									blockSize,
									downsamplings);
						}
						else
						{
							mrInfo = N5ApiTools.setupBdvDatasetsN5(
									n5Writer,
									viewId,
									dataTypes.get(viewId.getViewSetupId()),
									dimensions.get(viewId.getViewSetupId()),
									compression,
									blockSize,
									downsamplings);
						}
					}
					else
					{
						System.out.println( Arrays.toString( blockSize ) );
						VoxelDimensions vx = dataGlobal.getSequenceDescription().getViewDescription( viewId ).getViewSetup().getVoxelSize();
						if (downsampleS0)
						{
							System.out.println( "Save view setups downsampling" );
							mrInfo = setupBdvDatasetsOMEZARRResaveRawSkippingS0(
									n5Writer,
									viewId,
									dataTypes.get(viewId.getViewSetupId()),
									dimensions.get(viewId.getViewSetupId()),
									vx.dimensionsAsDoubleArray(),
									vx.unit(),
									compression,
									blockSize,
									downsamplings,
									useSharding,
									shardSize);
						}
						else
							mrInfo = N5ApiTools.setupBdvDatasetsOMEZARR_ResaveRaw(
									n5Writer,
									viewId,
									dataTypes.get( viewId.getViewSetupId() ),
									dimensions.get( viewId.getViewSetupId() ),
									vx.dimensionsAsDoubleArray(),
									vx.unit(),
									compression,
									blockSize,
									downsamplings,
									useSharding,
									shardSize );
					}

					return new ValuePair<>(
						new ViewId( viewId.getTimePointId(), viewId.getViewSetupId() ), // viewId is actually a ViewDescripton object, thus not serializable
						mrInfo );
				}).collect(Collectors.toMap( e -> e.getA(), e -> e.getB() ));

		System.out.println( "Created BDV-metadata, took " + (System.currentTimeMillis() - time ) + " ms." );
		System.out.println( "Number of compute blocks = " + gridS0.size() );

		final SparkConf conf = new SparkConf().setAppName("SparkResaveN5");

		if ( localSparkBindAddress )
		{
			conf.set("spark.driver.bindAddress", "127.0.0.1");
			conf.set("spark.driver.host", "localhost");
			org.apache.spark.util.Utils.setCustomHostname("localhost");
		}

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		//
		// Save s0 level
		//
		if ( !downsampleS0 )
			time = processSNBlocks(
					sc,
					gridS0,
					blockCount -> Math.min( Math.max( sc.defaultParallelism(), 1 ), blockCount ),
					"s0 n5-api dataset resaving",
					"Resaved " + (storageFormat == StorageFormat.N5 ? "N5 s0" : "OME-ZARR 0") + "-level, took: ",
					true,
					true,
					gridBlock ->
					{
						final SpimData2 dataLocal = Spark.getSparkJobSpimData2(xmlURI);
						final N5Writer n5Lcl = URITools.instantiateN5Writer( storageFormat, n5PathURI );

						N5ApiTools.resaveS0Block(
								dataLocal,
								n5Lcl,
								storageFormat,
								dataTypes.get( N5ApiTools.gridBlockToViewId( gridBlock ).getViewSetupId() ),
								N5ApiTools.gridToDatasetBdv( 0, storageFormat ), // a function mapping the gridblock to the dataset name for level 0 and N5
								gridBlock );

						n5Lcl.close();
					} );

		//
		// Save remaining downsampling levels (s1 ... sN)
		//
		processDownsampledViews(
				sc,
				downsamplings,
				viewIdsGlobal,
				viewIdToMrInfo,
				storageFormat,
				n5PathURI );

		sc.close();

		System.out.println( "resaved successfully." );

		// things look good, let's save the new XML
		System.out.println( "Saving new xml to: " + xmlOutURI );

		if ( storageFormat == StorageFormat.N5 && URITools.isFile( n5PathURI ))
		{
			dataGlobal.getSequenceDescription().setImgLoader(
					new N5ImageLoader( n5PathURI, dataGlobal.getSequenceDescription()));
		}
		else if ( storageFormat == StorageFormat.N5 )
		{
			dataGlobal.getSequenceDescription().setImgLoader(
					new N5CloudImageLoader( null, n5PathURI, dataGlobal.getSequenceDescription())); // null is OK because the instance is not used now
		}
		else
		{
			final Map< ViewId, OMEZARREntry > viewIdToPath = new HashMap<>();

			viewIdToMrInfo.forEach( (viewId, mrInfo ) ->
				viewIdToPath.put(
						viewId,
						new OMEZARREntry( mrInfo[ 0 ].dataset.substring(0,  mrInfo[ 0 ].dataset.lastIndexOf( "/" ) ), new int[] { 0, 0 } ) )
			);

			dataGlobal.getSequenceDescription().setImgLoader(
					new AllenOMEZarrLoader( n5PathURI, storageFormat, dataGlobal.getSequenceDescription(), viewIdToPath )); // null is OK because the instance is not used now
		}

		new XmlIoSpimData2().save( dataGlobal, xmlOutURI );

		n5Writer.close();

		Thread.sleep( 100 );
		System.out.println( "Resaved project, in total took: " + (System.currentTimeMillis() - time ) + " ms." );
		System.out.println( "done." );

		return null;
	}

	@FunctionalInterface
	private interface GridBlockProcessor extends Serializable
	{
		void process( long[][] gridBlock ) throws Exception;
	}

	private static long processSNBlocks(
			final JavaSparkContext sc,
			final List<long[][]> blocks,
			final IntUnaryOperator partitionCount,
			final String retryDescription,
			final String completedMessage,
			final boolean printPartitionCount,
			final boolean printRetryCount,
			final GridBlockProcessor processBlock )
	{
		final long time = System.currentTimeMillis();

		final RetryTrackerSpark<long[][]> retryTracker =
				RetryTrackerSpark.forGridBlocks(retryDescription, blocks.size());

		do
		{
			if (!retryTracker.beginAttempt())
			{
				System.out.println( "Stopping." );
				System.exit( 1 );
			}

			final int nPartitions = partitionCount.applyAsInt( blocks.size() );

			if ( printPartitionCount )
				System.out.printf("Use %d partitions to process %d blocks\n", nPartitions, blocks.size());

			final JavaRDD<long[][]> rdds = sc.parallelize( blocks, nPartitions );

			final JavaRDD<long[][]> rddsResult = rdds.map( gridBlock ->
			{
				processBlock.process( gridBlock );
				return gridBlock.clone();
			});

			// extract all blocks that failed
			final Set<long[][]> failedBlocksSet =
					retryTracker.processResults( rddsResult.collect(), blocks );

			// Use RetryTracker to handle retry counting and removal
			if (!retryTracker.processFailures(failedBlocksSet))
			{
				System.out.println( "Stopping." );
				System.exit( 1 );
			}

			// Update grid for next iteration with remaining failed blocks
			blocks.clear();
			if ( printRetryCount && !failedBlocksSet.isEmpty() )
				System.out.printf("Retry %d failed blocks\n", failedBlocksSet.size());
			blocks.addAll(failedBlocksSet);
		}
		while (!blocks.isEmpty());

		System.out.println( completedMessage + (System.currentTimeMillis() - time ) + " ms." );

		return time;
	}

	private static void processDownsampledViews(
			final JavaSparkContext sc,
			final int[][] downsamplings,
			final List< ViewId > viewIdsGlobal,
			final Map< ViewId, MultiResolutionLevelInfo[] > viewIdToMrInfo,
			final StorageFormat storageFormat,
			final URI n5PathURI )
	{
		for ( int level = 1; level < downsamplings.length; ++level )
		{
			final int s = level;

			//mrInfo.dimensions, mrInfo.blockSize, mrInfo.blockSize
			final List<long[][]> allBlocks =
					viewIdsGlobal.stream().map( viewId ->
							N5ApiTools.assembleJobs(
									viewId,
									viewIdToMrInfo.get( viewId )[s]) ).flatMap(List::stream).collect( Collectors.toList() );

			System.out.println( "Downsampling level " + (storageFormat == StorageFormat.N5 ? "s" : "") + s + "... " );
			System.out.println( "Number of compute blocks: " + allBlocks.size() );

			processSNBlocks(
					sc,
					allBlocks,
					blockCount -> Math.min( Spark.maxPartitions, blockCount ),
					"s" + s +" n5-api dataset resaving",
					"Resaved " + (storageFormat == StorageFormat.N5 ? "N5 s" : "OME-ZARR ") + s + " level, took: ",
					false,
					false,
					gridBlock ->
					{
						final N5Writer n5Lcl = URITools.instantiateN5Writer( storageFormat, n5PathURI );

						if ( storageFormat == StorageFormat.N5 )
						{
							N5ApiTools.writeDownsampledBlock(
									n5Lcl,
									viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s ], //N5ResaveTools.gridToDatasetBdv( s, StorageType.N5 ),
									viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s - 1 ],//N5ResaveTools.gridToDatasetBdv( s - 1, StorageType.N5 ),
									gridBlock );
						}
						else
						{
							N5ApiTools.writeDownsampledBlock5dOMEZARR(
									n5Lcl,
									viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s ], //N5ResaveTools.gridToDatasetBdv( s, StorageType.N5 ),
									viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s - 1 ],//N5ResaveTools.gridToDatasetBdv( s - 1, StorageType.N5 ),
									gridBlock,
									0,
									0 );
						}

						n5Lcl.close();
					} );
		}
	}

	private static void validateExistingS0Datasets(
			final N5Reader n5Reader,
			final StorageFormat storageFormat,
			final List< ViewId > viewIds,
			final Map< Integer, long[] > dimensions,
			final Map< Integer, DataType > dataTypes,
			final int[] blockSize,
			final Compression compression )
	{
		final StringBuilder errors = new StringBuilder();

		for ( final ViewId viewId : viewIds )
		{
			final String dataset = N5ApiTools.createBDVPath( viewId, 0, storageFormat );

			if ( !n5Reader.datasetExists( dataset ) )
			{
				errors.append( "\n  - missing dataset " ).append( dataset );
				continue;
			}

			final DatasetAttributes attributes = n5Reader.getDatasetAttributes( dataset );
			final long[] expectedDimensions = expectedS0Dimensions( storageFormat, dimensions.get( viewId.getViewSetupId() ) );
			final DataType expectedDataType = dataTypes.get( viewId.getViewSetupId() );

			if ( !Arrays.equals( expectedDimensions, attributes.getDimensions() ) )
				errors.append( "\n  - " ).append( dataset )
						.append( " dimensions are " ).append( Util.printCoordinates( attributes.getDimensions() ) )
						.append( ", expected " ).append( Util.printCoordinates( expectedDimensions ) );

			if ( expectedDataType != attributes.getDataType() )
				errors.append( "\n  - " ).append( dataset )
						.append( " data type is " ).append( attributes.getDataType() )
						.append( ", expected " ).append( expectedDataType );

			if ( StorageFormat.N5.equals( storageFormat ) && !Arrays.equals( blockSize, attributes.getBlockSize() ) )
				errors.append( "\n  - " ).append( dataset )
						.append( " block size is " ).append( Util.printCoordinates( attributes.getBlockSize() ) )
						.append( ", expected " ).append( Util.printCoordinates( blockSize ) );

			if ( StorageFormat.N5.equals( storageFormat ) && !attributes.getCompression().getType().equals( compression.getType() ) )
				errors.append( "\n  - " ).append( dataset )
						.append( " compression is " ).append( attributes.getCompression().getType() )
						.append( ", expected " ).append( compression.getType() );
		}

		if ( errors.length() > 0 )
			throw new IllegalStateException( "Cannot use --skipS0IfExists because existing s0 datasets are missing or incompatible:" + errors );
	}

	private static MultiResolutionLevelInfo[] setupBdvDatasetsN5SkippingS0(
			final N5Writer n5Writer,
			final ViewId viewId,
			final DataType dataType,
			final long[] dimensions,
			final Compression compression,
			final int[] blockSize,
			int[][] downsamplings )
	{
		final MultiResolutionLevelInfo[] mrInfo = setupMultiResolutionPyramidSkippingS0(
				n5Writer,
				viewId,
				N5ApiTools.viewIdToDatasetBdv( StorageFormat.N5 ),
				dataType,
				dimensions,
				compression,
				blockSize,
				downsamplings,
				false,
				null );

		final String s0Dataset = N5ApiTools.createBDVPath( viewId, 0, StorageFormat.N5 );
		final String setupDataset = s0Dataset.substring(0, s0Dataset.indexOf( "/timepoint" ));
		final String timepointDataset = s0Dataset.substring(0, s0Dataset.indexOf("/s0" ));
		final DatasetAttributes s0Attributes = n5Writer.getDatasetAttributes( s0Dataset );

		final Map<String, Class<?>> attribs = n5Writer.listAttributes( setupDataset );

		if ( !attribs.containsKey( DatasetAttributes.DATA_TYPE_KEY ) || !attribs.containsKey( DatasetAttributes.BLOCK_SIZE_KEY ) || !attribs.containsKey( DatasetAttributes.DIMENSIONS_KEY ) || !attribs.containsKey( DatasetAttributes.COMPRESSION_KEY ) || !attribs.containsKey( "downsamplingFactors" ) )
		{
			final HashMap<String, Object > attribs2 = new HashMap<>();
			attribs2.put(DatasetAttributes.DATA_TYPE_KEY, s0Attributes.getDataType() );
			attribs2.put(DatasetAttributes.BLOCK_SIZE_KEY, s0Attributes.getBlockSize() );
			attribs2.put(DatasetAttributes.DIMENSIONS_KEY, s0Attributes.getDimensions() );
			attribs2.put(DatasetAttributes.COMPRESSION_KEY, s0Attributes.getCompression() );
			attribs2.put( "downsamplingFactors", downsamplings );

			n5Writer.setAttributes( setupDataset, attribs2 );
		}

		n5Writer.setAttribute(timepointDataset, "resolution", new double[] {1,1,1} );
		n5Writer.setAttribute(timepointDataset, "saved_completely", true );
		n5Writer.setAttribute(timepointDataset, "multiScale", downsamplings != null && downsamplings.length != 0 );

		for ( int level = 1; level < downsamplings.length; ++level )
			n5Writer.setAttribute( mrInfo[ level ].dataset, "downsamplingFactors", mrInfo[ level ].absoluteDownsampling );

		return mrInfo;
	}

	private static MultiResolutionLevelInfo[] setupBdvDatasetsOMEZARRResaveRawSkippingS0(
			final N5Writer n5Writer,
			final ViewId viewId,
			final DataType dataType,
			final long[] dimensions,
			final double[] resolutionS0,
			final String resolutionUnit,
			final Compression compression,
			final int[] blockSize,
			final int[][] downsamplings,
			final boolean useSharding,
			final int[] shardSize )
	{
		final String s0Dataset = N5ApiTools.viewIdToDatasetBdv( StorageFormat.ZARR ).apply( viewId, 0 );
		final String baseDataset = s0Dataset.substring(0, s0Dataset.lastIndexOf( "/" ) + 1);

		System.out.println( "Creating 5D OME-ZARR metadata for '" + baseDataset + "' ... " );

		final long[] dim5d = new long[] { dimensions[ 0 ], dimensions[ 1 ], dimensions[ 2 ], 1, 1 };
		final int[] blockSize5d = new int[] { blockSize[ 0 ], blockSize[ 1 ], blockSize[ 2 ], 1, 1 };
		final int[][] ds5d = new int[ downsamplings.length ][];

		for ( int d = 0; d < ds5d.length; ++d )
			ds5d[ d ] = new int[] { downsamplings[ d ][ 0 ], downsamplings[ d ][ 1 ], downsamplings[ d ][ 2 ], 1, 1 };

		final int[] shardSize5d = useSharding && shardSize != null
				? new int[] { shardSize[ 0 ], shardSize[ 1 ], shardSize[ 2 ], 1, 1 }
				: null;

		final MultiResolutionLevelInfo[] mrInfo = setupMultiResolutionPyramidSkippingS0(
				n5Writer,
				viewId,
				N5ApiTools.viewIdToDatasetBdv( StorageFormat.ZARR ),
				dataType,
				dim5d,
				compression,
				blockSize5d,
				ds5d,
				useSharding,
				shardSize5d );

		final Function<Integer, String> levelToName = level -> "/" + level;
		final Function<Integer, AffineTransform3D> levelToMipmapTransform =
				level -> MipmapTransforms.getMipmapTransformDefault( mrInfo[level].absoluteDownsamplingDouble() );

		final OmeNgffMultiScaleMetadata[] meta = OMEZarrAttributes.createOMEv04ZarrMetadata(
				5,
				"/",
				resolutionS0,
				resolutionUnit,
				mrInfo.length,
				levelToName,
				levelToMipmapTransform );

		n5Writer.setAttribute( baseDataset, "multiscales", meta );

		return mrInfo;
	}

	private static MultiResolutionLevelInfo[] setupMultiResolutionPyramidSkippingS0(
			final N5Writer n5Writer,
			final ViewId viewId,
			final BiFunction<ViewId, Integer, String> viewIdToDataset,
			final DataType dataType,
			final long[] dimensionsS0,
			final Compression compression,
			final int[] blockSize,
			final int[][] downsamplings,
			final boolean useSharding,
			final int[] shardSize )
	{
		final MultiResolutionLevelInfo[] mrInfo = new MultiResolutionLevelInfo[ downsamplings.length ];
		final int[] relativeDownsampling = downsamplings[ 0 ].clone();
		Arrays.setAll( relativeDownsampling, i -> 1 );

		final String datasetS0 = viewIdToDataset.apply( viewId, 0 );
		final DatasetAttributes s0Attributes = n5Writer.getDatasetAttributes( datasetS0 );

		mrInfo[ 0 ] = new MultiResolutionLevelInfo(
				datasetS0,
				s0Attributes.getDimensions().clone(),
				s0Attributes.getDataType(),
				relativeDownsampling,
				downsamplings[ 0 ],
				s0Attributes.getBlockSize(),
				null );

		long[] previousDim = dimensionsS0.clone();

		for ( int level = 1; level < downsamplings.length; ++level )
		{
			final int[] relativeDownsamplingLevel = N5ApiTools.computeRelativeDownsampling( downsamplings, level );
			final String datasetLevel = viewIdToDataset.apply( viewId, level );
			final long[] dim = new long[ previousDim.length ];

			for ( int d = 0; d < dim.length; ++d )
				dim[ d ] = previousDim[ d ] / relativeDownsamplingLevel[ d ];

			final DatasetAttributes attributes;

			if ( n5Writer.datasetExists( datasetLevel ) )
			{
				attributes = n5Writer.getDatasetAttributes( datasetLevel );
				validateExistingPyramidDataset(
						datasetLevel,
						attributes,
						dim,
						dataType,
						blockSize,
						shardSize,
						useSharding );
			}
			else if ( useSharding )
			{
				attributes = ZarrV3DatasetAttributes.builder(dim, dataType)
						.blockSize(blockSize)
						.shardShape(shardSize)
						.compression(compression)
						.shardIndexDataCodecInfos(new Crc32cChecksumCodec())
						.build();
				n5Writer.createDataset( datasetLevel, attributes );
			}
			else
			{
				attributes = n5Writer.createDataset(
						datasetLevel,
						dim,
						blockSize,
						dataType,
						compression );
			}

			mrInfo[ level ] = new MultiResolutionLevelInfo(
					datasetLevel,
					attributes.getDimensions().clone(),
					attributes.getDataType(),
					relativeDownsamplingLevel,
					downsamplings[ level ],
					blockSize,
					useSharding ? shardSize : null );

			n5Writer.setAttribute( datasetLevel, "downsamplingFactors", downsamplings[ level ] );

			previousDim = dim;
		}

		return mrInfo;
	}

	private static void validateExistingPyramidDataset(
			final String dataset,
			final DatasetAttributes attributes,
			final long[] expectedDimensions,
			final DataType expectedDataType,
			final int[] expectedBlockSize,
			final int[] expectedShardSize,
			final boolean useSharding )
	{
		final StringBuilder errors = new StringBuilder();

		if ( !Arrays.equals( expectedDimensions, attributes.getDimensions() ) )
			errors.append( "\n  - dimensions are " ).append( Util.printCoordinates( attributes.getDimensions() ) )
					.append( ", expected " ).append( Util.printCoordinates( expectedDimensions ) );

		if ( expectedDataType != attributes.getDataType() )
			errors.append( "\n  - data type is " ).append( attributes.getDataType() )
					.append( ", expected " ).append( expectedDataType );

		if ( attributes.isSharded() != useSharding )
			errors.append( "\n  - existing dataset " )
					.append( attributes.isSharded() ? "is sharded" : "is not sharded" )
					.append( ", but --useSharding is " ).append( useSharding );

		if ( useSharding )
		{
			if ( !Arrays.equals( expectedShardSize, attributes.getBlockSize() ) )
				errors.append( "\n  - shard size is " ).append( Util.printCoordinates( attributes.getBlockSize() ) )
						.append( ", expected " ).append( Util.printCoordinates( expectedShardSize ) );

			if ( attributes.getBlockCodecInfo() instanceof ShardCodecInfo )
			{
				final int[] innerBlockSize = ((ShardCodecInfo)attributes.getBlockCodecInfo()).getInnerBlockSize();
				if ( !Arrays.equals( expectedBlockSize, innerBlockSize ) )
					errors.append( "\n  - inner block size is " ).append( Util.printCoordinates( innerBlockSize ) )
							.append( ", expected " ).append( Util.printCoordinates( expectedBlockSize ) );
			}
			else
			{
				errors.append( "\n  - existing dataset is missing shard codec metadata" );
			}
		}
		else if ( !Arrays.equals( expectedBlockSize, attributes.getBlockSize() ) )
		{
			errors.append( "\n  - block size is " ).append( Util.printCoordinates( attributes.getBlockSize() ) )
					.append( ", expected " ).append( Util.printCoordinates( expectedBlockSize ) );
		}

		if ( errors.length() > 0 )
			throw new IllegalStateException( "Cannot reuse existing pyramid dataset '" + dataset + "' because it is incompatible:" + errors );
	}

	private static long[] expectedS0Dimensions( final StorageFormat storageFormat, final long[] dimensions )
	{
		if ( StorageFormat.ZARR.equals( storageFormat ) || StorageFormat.ZARR2.equals( storageFormat ) )
			return new long[] { dimensions[ 0 ], dimensions[ 1 ], dimensions[ 2 ], 1, 1 };

		return dimensions.clone();
	}

	public static void main(final String... args) {

		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new SparkResaveN5()).execute(args));
	}

}
