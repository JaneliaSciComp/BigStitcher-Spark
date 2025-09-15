package net.preibisch.bigstitcher.spark;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import bdv.img.n5.N5ImageLoader;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractInfrastructure;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.bigstitcher.spark.util.RetryTrackerSpark;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.dataset.DatasetBuilder;
import net.preibisch.mvrecon.fiji.plugin.resave.ParametersResaveN5Api;
import net.preibisch.mvrecon.fiji.plugin.resave.Resave_HDF5;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.AllenOMEZarrLoader;
import net.preibisch.mvrecon.process.n5api.N5ApiTools;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bigdataviewer.n5.N5CloudImageLoader;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

public class CreateStitchingDataset extends AbstractInfrastructure implements Callable<Void>, Serializable {
	private static final long serialVersionUID = -9140450542904228386L;

	@Option(names = {"-o", "--outputPath"}, required = true, description = "OME-ZARR/N5/HDF5 path for saving, e.g. -o /home/fused.zarr, file:/home/fused.n5 or e.g. s3://myBucket/data.zarr")
	private String outputPathURIString = null;

	@Option(names = {"-s", "--storage"}, defaultValue = "ZARR", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Dataset storage type, currently supported OME-ZARR, N5, and ONLY for local, multithreaded Spark HDF5 (default: OME-ZARR)")
	private StorageFormat storageType = null;

	@Option(names = {"--intermediate-xml"}, description = "name of the intermediate BigDataViewer xml project")
	private String tmpXmlOut = null;

	@Option(names = {"-xo", "--xmlout"}, description = "path to the output xml, e.g. file:/data/dataset.xml or s3://myBucket/data/dataset.xml")
	private String xmlOutURIString = null;

	@Option(names = {"--input-path"}, required = true, description = "Path to the input images, e.g. /data/images/")
	private String inputPath = "/Users/goinac/Work/HHMI/stitching/datasets/tiny_4_bigstitcher/t1/";

	@Option(names = {"--input-pattern"}, description = "Glob pattern for input images, e.g. /data/images/*.tif")
	private String inputPattern = "*";

	@Option(names = {"--output-container"}, description = "Output container")
	private String outputContainer;

	@Option(names = "--blockSize", description = "blockSize, you can use smaller blocks for HDF5 (default: 128,128,64)")
	private String blockSizeString = "128,128,64";

	@Option(names = "--blockScale", description = "how many blocks to use for a single processing step, e.g. 4,4,1 means for blockSize a 128,128,32 that each spark thread writes 512,512,32 (default: 16,16,1)")
	private String blockScaleString = "16,16,1";

	@Option(names = {"-ds", "--downsampling"}, description = "downsampling pyramid (must contain full res 1,1,1 that is always created), e.g. 1,1,1; 2,2,1; 4,4,1; 8,8,2 (default: automatically computed)")
	private String downsampling = null;

	@Option(names = {"-c", "--compression"}, defaultValue = "Zstandard", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Dataset compression")
	private Compressions compression = null;

	@Option(names = {"-cl", "--compressionLevel"}, description = "compression level, if supported by the codec (default: gzip 1, Zstandard 3, xz 6)")
	private Integer compressionLevel = null;

	@Override
	public Void call() throws Exception {
		this.setRegion();

		URI outPathURI = URITools.toURI(outputPathURIString + "/");
		SpimData2 spimData = createDataset();

		URI xmlOutURI = xmlOutURIString != null ? URITools.toURI(xmlOutURIString) : outPathURI.resolve("dataset.xml");

		System.out.println("Save spimData with original tiles to " + xmlOutURI);
		new XmlIoSpimData2().save(spimData, xmlOutURI);

		if (outputContainer != null) {
			if (tmpXmlOut != null) {
				URI tmpXmlLocation = outPathURI.resolve(tmpXmlOut);
				System.out.println("Save intermediate spimData to " + tmpXmlLocation);
				new XmlIoSpimData2().save(spimData, tmpXmlLocation);
			}
			URI outputContainerURI = outPathURI.resolve(outputContainer);
			System.out.println("Re-Save data to " + outputContainerURI);
			saveDatasetAsN5(spimData, xmlOutURI, outputContainerURI);
		}
		return null;
	}

	private SpimData2 createDataset() {
		DatasetBuilder datasetBuilder = new DatasetBuilder(inputPattern);
		return datasetBuilder.createDataset(inputPath);
	}

	private void saveDatasetAsN5(SpimData2 spimData, URI outputXMLURI, URI outputContainerURI) {
		List<ViewId> viewIds = Import.getViewIds(spimData);
		if (viewIds.isEmpty()) {
			throw new IllegalArgumentException("No views could be generated.");
		}

		Collections.sort(viewIds);

		final Compression dataCompression = N5Util.getCompression(this.compression, this.compressionLevel);

		final int[] blockSize = Import.csvStringToIntArray(blockSizeString);
		final int[] blockScale = Import.csvStringToIntArray(blockScaleString);

		final int[] computeBlockSize = new int[]{
				blockSize[0] * blockScale[0],
				blockSize[1] * blockScale[1],
				blockSize[2] * blockScale[2]};

		final N5Writer n5Writer = URITools.instantiateN5Writer(
				storageType == StorageFormat.ZARR ? StorageFormat.ZARR : StorageFormat.N5,
				outputContainerURI
		);

		System.out.println("Compression: " + this.compression);
		System.out.println("Compression level: " + (compressionLevel == null ? "default" : compressionLevel));
		System.out.println("N5 block size=" + Util.printCoordinates(blockSize));
		System.out.println("Compute block size=" + Util.printCoordinates(computeBlockSize));
		System.out.println("Setting up XML at: " + outputXMLURI);
		System.out.println("Setting up N5 writing to basepath: " + outputContainerURI);

		// all ViewSetupIds (needed to create N5 datasets)
		final HashMap<Integer, long[]> dimensions =
				N5ApiTools.assembleDimensions(spimData, viewIds);

		// all grids across all ViewId's
		final List<long[][]> gridS0 =
				viewIds.stream().map(viewId ->
						N5ApiTools.assembleJobs(
								viewId,
								dimensions.get(viewId.getViewSetupId()),
								blockSize,
								computeBlockSize)).flatMap(List::stream).collect(Collectors.toList());

		final Map<Integer, DataType> dataTypes =
				N5ApiTools.assembleDataTypes(spimData, dimensions.keySet());

		// estimate or read downsampling factors
		final int[][] downsamplings;

		if (this.downsampling == null)
			downsamplings = N5ApiTools.mipMapInfoToDownsamplings(
					Resave_HDF5.proposeMipmaps(N5ApiTools.assembleViewSetups(spimData, viewIds))
			);
		else
			downsamplings = Import.csvStringToDownsampling(this.downsampling);

		if (!Import.testFirstDownsamplingIsPresent(downsamplings))
			throw new IllegalStateException("First downsampling step must be full resolution [1,1,...1], stopping.");

		System.out.println("Downsamplings: " + Arrays.deepToString(downsamplings));

		// create all datasets and write BDV metadata for all ViewIds (including downsampling) in parallel
		long time = System.currentTimeMillis();

		final Map<ViewId, N5ApiTools.MultiResolutionLevelInfo[]> viewIdToMrInfo =
				viewIds.parallelStream().map(viewId -> {
					final N5ApiTools.MultiResolutionLevelInfo[] mrInfo;

					if (storageType == StorageFormat.ZARR) {
						mrInfo = N5ApiTools.setupBdvDatasetsOMEZARR(
								n5Writer,
								viewId,
								dataTypes.get(viewId.getViewSetupId()),
								dimensions.get(viewId.getViewSetupId()),
								dataCompression,
								blockSize,
								downsamplings);
					} else {
						System.out.println(Arrays.toString(blockSize));
						mrInfo = N5ApiTools.setupBdvDatasetsN5(
								n5Writer,
								viewId,
								dataTypes.get(viewId.getViewSetupId()),
								dimensions.get(viewId.getViewSetupId()),
								dataCompression,
								blockSize,
								downsamplings);
					}

					return new ValuePair<>(
							new ViewId( viewId.getTimePointId(), viewId.getViewSetupId() ), // viewId is actually a ViewDescripton object, thus not serializable
							mrInfo );
				}).collect(Collectors.toMap(e -> e.getA(), e -> e.getB()));

		System.out.println( "Created BDV-metadata, took " + (System.currentTimeMillis() - time ) + " ms." );
		System.out.println( "Number of compute blocks = " + gridS0.size() );

		final SparkConf conf = new SparkConf().setAppName("SparkSaveDataset");

		if ( localSparkBindAddress )
			conf.set("spark.driver.bindAddress", "127.0.0.1");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		//
		// Save s0 level
		//
		time = System.currentTimeMillis();
		final RetryTrackerSpark<long[][]> retryTracker =
				RetryTrackerSpark.forGridBlocks("s0 n5-api dataset resaving", gridS0.size());

		do {
			if (!retryTracker.beginAttempt()) {
				System.out.println( "Stopping." );
				System.exit( 1 );
			}

			final JavaRDD<long[][]> rdds0 = sc.parallelize( gridS0, Math.min( Spark.maxPartitions, gridS0.size() ) );

			final JavaRDD<long[][]> rdds0Result = rdds0.map( gridBlock -> {
				final SpimData2 dataLocal = Spark.getSparkJobSpimData2(outputXMLURI);
				final N5Writer n5Lcl = URITools.instantiateN5Writer( storageType, outputContainerURI );

				N5ApiTools.resaveS0Block(
						dataLocal,
						n5Lcl,
						storageType,
						dataTypes.get( N5ApiTools.gridBlockToViewId( gridBlock ).getViewSetupId() ),
						N5ApiTools.gridToDatasetBdv( 0, storageType ), // a function mapping the gridblock to the dataset name for level 0 and N5
						gridBlock );

				n5Lcl.close();

				return gridBlock.clone();
			});

			rdds0Result.cache();
			rdds0Result.count();

			// extract all blocks that failed
			final Set<long[][]> failedBlocksSet =
					retryTracker.processWithSpark( rdds0Result, gridS0 );

			// Use RetryTracker to handle retry counting and removal
			if (!retryTracker.processFailures(failedBlocksSet)) {
				System.out.println( "Stopping." );
				System.exit( 1 );
			}

			// Update grid for next iteration with remaining failed blocks
			gridS0.clear();
			gridS0.addAll(failedBlocksSet);
		}
		while ( gridS0.size() > 0 );

		System.out.println( "Saved " + (storageType == StorageFormat.ZARR ? "OME-ZARR 0" : "N5 s0") + "-level, took: " + (System.currentTimeMillis() - time ) + " ms." );

		//
		// Save remaining downsampling levels (s1 ... sN)
		//
		for ( int level = 1; level < downsamplings.length; ++level ) {
			final int s = level;

			//mrInfo.dimensions, mrInfo.blockSize, mrInfo.blockSize
			final List<long[][]> allBlocks =
					viewIds.stream().map( viewId ->
							N5ApiTools.assembleJobs(
									viewId,
									viewIdToMrInfo.get(viewId)[s] )).flatMap(List::stream).collect( Collectors.toList() );

			System.out.println( "Downsampling level " + (storageType == StorageFormat.N5 ? "s" : "") + s + "... " );
			System.out.println( "Number of compute blocks: " + allBlocks.size() );

			final RetryTrackerSpark<long[][]> retryTrackerDS =
					RetryTrackerSpark.forGridBlocks( "s" + s +" n5-api dataset resaving", allBlocks.size());

			final long timeS = System.currentTimeMillis();

			do
			{
				if (!retryTrackerDS.beginAttempt())
				{
					System.out.println( "Stopping." );
					System.exit( 1 );
				}

				final JavaRDD<long[][]> rddsN = sc.parallelize(allBlocks, Math.min( Spark.maxPartitions, allBlocks.size() ) );

				final JavaRDD<long[][]> rdds0Result = rddsN.map( gridBlock ->
				{
					final N5Writer n5Lcl = URITools.instantiateN5Writer( storageType, outputContainerURI );

					if ( storageType == StorageFormat.ZARR ) {
						N5ApiTools.writeDownsampledBlock5dOMEZARR(
								n5Lcl,
								viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s ], //N5ResaveTools.gridToDatasetBdv( s, StorageType.N5 ),
								viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s - 1 ],//N5ResaveTools.gridToDatasetBdv( s - 1, StorageType.N5 ),
								gridBlock,
								0,
								0 );

					} else {
						N5ApiTools.writeDownsampledBlock(
								n5Lcl,
								viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s ], //N5ResaveTools.gridToDatasetBdv( s, StorageType.N5 ),
								viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s - 1 ],//N5ResaveTools.gridToDatasetBdv( s - 1, StorageType.N5 ),
								gridBlock );
					}

					n5Lcl.close();

					return gridBlock.clone();
				});

				rdds0Result.cache();
				rdds0Result.count();

				// extract all blocks that failed
				final Set<long[][]> failedBlocksSet =
						retryTrackerDS.processWithSpark( rdds0Result, allBlocks );

				// Use RetryTracker to handle retry counting and removal
				if (!retryTrackerDS.processFailures(failedBlocksSet))
				{
					System.out.println( "Stopping." );
					System.exit( 1 );
				}

				// Update grid for next iteration with remaining failed blocks
				allBlocks.clear();
				allBlocks.addAll(failedBlocksSet);

			} while ( allBlocks.size() > 0 );

			System.out.println( "Saved as " + (storageType == StorageFormat.N5 ? "N5 s" : "OME-ZARR ") + s + " level, took: " + (System.currentTimeMillis() - timeS ) + " ms." );
		}

		sc.close();

		System.out.println( "Saved the entire dataset successfully." );

		System.out.println( "Saving new xml to: " + outputXMLURI );

		if (storageType == StorageFormat.N5) {
			if (URITools.isFile(outputContainerURI)) {
				spimData.getSequenceDescription().setImgLoader(
						new N5ImageLoader( outputContainerURI, spimData.getSequenceDescription()));
			} else {
				spimData.getSequenceDescription().setImgLoader(
						new N5CloudImageLoader( null, outputContainerURI, spimData.getSequenceDescription())); // null is OK because the instance is not used now
			}
		} else {
			final Map< ViewId, AllenOMEZarrLoader.OMEZARREntry> viewIdToPath = new HashMap<>();

			viewIdToMrInfo.forEach( (viewId, mrInfo ) ->
					viewIdToPath.put(
							viewId,
							new AllenOMEZarrLoader.OMEZARREntry( mrInfo[ 0 ].dataset.substring(0,  mrInfo[ 0 ].dataset.lastIndexOf( "/" ) ), new int[] { 0, 0 } ) )
			);
			spimData.getSequenceDescription().setImgLoader(
					new AllenOMEZarrLoader( outputContainerURI, spimData.getSequenceDescription(), viewIdToPath )); // null is OK because the instance is not used now
		}
		new XmlIoSpimData2().save( spimData, outputXMLURI );

		n5Writer.close();

		System.out.println( "Resaved project, in total took: " + (System.currentTimeMillis() - time ) + " ms." );
		System.out.println( "done." );
	}

	public static void main(final String... args) throws SpimDataException {
		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new CreateStitchingDataset()).execute(args));
	}
}
