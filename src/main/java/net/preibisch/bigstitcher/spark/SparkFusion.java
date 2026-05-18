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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.blocks.BlockAlgoUtils;
import net.imglib2.algorithm.blocks.BlockSupplier;
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
import net.preibisch.bigstitcher.spark.fusion.GenerateComputeBlockMasks;
import net.preibisch.bigstitcher.spark.fusion.OverlappingBlocks;
import net.preibisch.bigstitcher.spark.fusion.OverlappingViews;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.bigstitcher.spark.util.RetryTrackerSpark;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.plugin.fusion.FusionGUI.FusionType;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBox;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.splitting.SplitViewerImgLoader;
import net.preibisch.mvrecon.process.fusion.blk.BlkAffineFusion;
import net.preibisch.mvrecon.process.fusion.blk.SplitImgLoaderThinPlateSplineFusion;
import net.preibisch.mvrecon.process.fusion.intensity.Coefficients;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrection;
import net.preibisch.mvrecon.process.fusion.transformed.TransformVirtual;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import net.preibisch.mvrecon.process.n5api.N5ApiTools;
import net.preibisch.mvrecon.process.n5api.N5ApiTools.MultiResolutionLevelInfo;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.Grid;
import util.URITools;

public class SparkFusion extends AbstractInfrastructure implements Callable<Void>, Serializable
{
	public enum DataTypeFusion { UINT8, UINT16, FLOAT32 }
	public enum FusionMethod { AFFINE, THIN_PLATE_SPLINE }

	private static final long serialVersionUID = -6103761116219617153L;

	@Option(names = { "-o", "--n5Path" }, required = true, description = "N5/ZARR/HDF5 base path for saving (must be combined with the option '-d' or '--bdv'), e.g. -o /home/fused.n5 or e.g. s3://myBucket/data.n5")
	String outputPathURIString = null;

	@Option(names = {"-s", "--storage"}, description = "Dataset storage type, can be used to override guessed format (default: guess from file/directory-ending; NOTE: does not work for ZARR2, you need to specify since .zarr would default to ZARR v3)")
	StorageFormat storageType = null;

	@Option(names = "--blockScale", description = "how many blocks to use for a single processing step, e.g. 4,4,1 means for blockSize a 128,128,64 that each spark thread writes 512,512,64 (default: 4,4,1; NOT compatible with sharded containers)")
	String blockScaleString = null;

	@Option(names = { "--masks" }, description = "save only the masks (this will not fuse the images, currently only works for FusionMethod.AFFINE)")
	boolean masks = false;

	@Option(names = "--maskOffset", description = "allows to make masks larger (+, the mask will include some background) or smaller (-, some fused content will be cut off), warning: in the non-isotropic coordinate space of the raw input images (default: 0.0,0.0,0.0)")
	String maskOffset = "0.0,0.0,0.0";

	@Option(names = {"-f", "--fusion"}, description = "Strategy for merging overlapping views during fusion, supported: AVG, AVG_BLEND, "/*AVG_CONTENT, AVG_BLEND_CONTENT*/+", MAX_INTENSITY, LOWEST_VIEWID_WINS, HIGHEST_VIEWID_WINS, CLOSEST_PIXEL_WINS (default: AVG_BLEND)")
	FusionType fusionType = FusionType.AVG_BLEND;

	@Option(names = {"-fm", "--fusionMethod"}, description = "The transformation models used for fusion, AFFINE or THIN_PLATE_SPLINE. TPS requires a split dataset with at least 2x2x2 split views (default: AFFINE).")
	FusionMethod fusionMethod = FusionMethod.AFFINE;

	@Option(names = { "-oe", "--overlapExpansion" }, description = "Number of pixels by which intervals are expanded when testing for overlap (default: 2 for FusionMethod.AFFINE; 50 for FusionMethod.THIN_PLATE_SPLINE).")
	Integer overlapExpansion = null;

	@Option(names = { "-t", "--timepointIndex" }, description = "specify a specific timepoint index of the output container that should be fused, usually you would also specify what --angleId, --tileId, ... or ViewIds -vi are being fused.")
	Integer timepointIndex = null;

	@Option(names = { "-c", "--channelIndex" }, description = "specify a specific channel index of the output container that should be fused, usually you would also specify what --angleId, --tileId, ... or ViewIds -vi are being fused.")
	Integer channelIndex = null;

	// To specify what goes into the current 3D volume
	@Option(names = { "--angleId" }, description = "list the angle ids that should be processed, you can find them in the XML, e.g. --angleId '0,1,2' (default: all angles)")
	String angleIds = null;

	@Option(names = { "--tileId" }, description = "list the tile ids that should be processed, you can find them in the XML, e.g. --tileId '0,1,2' (default: all tiles)")
	String tileIds = null;

	@Option(names = { "--illuminationId" }, description = "list the illumination ids that should be processed, you can find them in the XML, e.g. --illuminationId '0,1,2' (default: all illuminations)")
	String illuminationIds = null;

	@Option(names = { "--channelId" }, description = "list the channel ids that should be processed, you can find them in the XML (usually just one when fusing), e.g. --channelId '0,1,2' (default: all channels)")
	String channelIds = null;

	@Option(names = { "--timepointId" }, description = "list the timepoint ids that should be processed, you can find them in the XML (usually just one when fusing), e.g. --timepointId '0,1,2' (default: all time points)")
	String timepointIds = null;

	@Option(names = { "-vi" }, description = "specifically list the view ids (time point, view setup) that should be fused into a single image, e.g. -vi '0,0' -vi '0,1' (default: all view ids)")
	String[] vi = null;

	@Option(names = { "--prefetch" }, description = "prefetch all blocks required for fusion in each Spark job using unlimited threads, useful in cloud environments (default: false)")
	boolean prefetch = false;

	// TODO: add support for loading coefficients during fusion
	@CommandLine.Option(names = { "--intensityN5Path" }, description = "N5/ZARR/HDF5 base path for loading coefficients (e.g. s3://myBucket/coefficients.n5)")
	String intensityN5PathURIString = null;

	@CommandLine.Option(names = { "--intensityN5Storage" }, description = "output storage type, can be used to override guessed format (default: guess from n5Path file/directory-ending; NOTE: does not work for ZARR2, you need to specify since .zarr would default to ZARR v3)")
	StorageFormat intensityN5StorageType = null;

	@CommandLine.Option(names = { "--intensityN5Group" }, description = "group under which coefficient datasets are stored (default: \"\")")
	String intensityN5Group = "";

	@CommandLine.Option(names = { "--intensityN5Dataset" }, description = "dataset name for each coefficient dataset (default: \"intensity\"). The coefficients for view(s,t) are stored in dataset \"{-n5Group}/setup{s}/timepoint{t}/{n5Dataset}\"")
	String intensityN5Dataset = "intensity";

	@Option(names = { "--group" }, description = "Container group path")
	String groupPath = "";

	URI outPathURI = null;
	/**
	 * Prefetching now works with a Executors.newCachedThreadPool();
	 */
	//static final int N_PREFETCH_THREADS = 72;

	URI intensityN5PathURI = null;

	/**
	 * @return container group path always terminated with a '/'
	 */
	private String getContainerGroupPath()
	{
		if (!groupPath.endsWith("/")) {
			return groupPath + "/";
		} else {
			return groupPath;
		}
	}

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		if (dryRun)
		{
			System.out.println( "dry-run not supported for affine fusion.");
			return null;
		}

		if ( timepointIndex != null && channelIndex == null || timepointIndex == null && channelIndex != null )
		{
			System.out.println( "You have to specify timepointId and channelId together, one alone does not work. timepointId =" + timepointIndex + ", channelId=" + channelIndex );
			return null;
		}

		// TODO: Why should we need to provide timepointIndex if timepointIds is set!?
		if ( timepointIndex == null && ( vi != null || timepointIds != null || channelIds != null || illuminationIds != null || tileIds != null || angleIds != null ) )

		{
			System.out.println( "You can only specify specify angles, tiles, ..., ViewIds if you provided a specific timepointIndex & channelIndex.");
			return null;
		}

		this.outPathURI = URITools.toURI( outputPathURIString );
		System.out.println( "Fused volume: " + outPathURI );

		if ( storageType == null )
		{
			if ( outputPathURIString.toLowerCase().endsWith( ".zarr" ) )
				storageType = StorageFormat.ZARR;
			else if ( outputPathURIString.toLowerCase().endsWith( ".zarr2" ) )
				storageType = StorageFormat.ZARR2;
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

		// test that the container exists
		try( final N5Reader r = URITools.instantiateN5Reader( storageType, outPathURI  ) )
		{
			System.out.println( "Found container '" + outPathURI + "'.");
		}
		catch ( Exception e )
		{
			System.out.println( "Exception: " + e);
			System.out.println( "Error, container '" + outPathURI + "' does not exist/could not be loaded. If you are using ZARR2, you need specify it "
					+ "since the default for .zarr is ZARR v3. If it doesn't exist, you need create an output container with create-fusion-container.");
			return null;
		}

		final N5Writer driverVolumeWriter = N5Util.createN5Writer( outPathURI, storageType );

		final String fusionFormat = driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/FusionFormat", String.class );

		if ( fusionFormat == null )
		{
			System.out.println( "Could not load 'Bigstitcher-Spark/FusionFormat' from metadata of specified output '" + outPathURI + "'.");
			System.out.println( "Note: this metadata is created by ./create-fusion-container in the previous step." );
			return null;
		}

		final boolean bdv = fusionFormat.toLowerCase().contains( "BDV" );

		final URI xmlURI = driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/InputXML", URI.class );

		final SpimData2 dataGlobal = Spark.getJobSpimData2( xmlURI, 0 );

		if ( fusionMethod == FusionMethod.THIN_PLATE_SPLINE )
		{
			if ( fusionType != FusionType.CLOSEST_PIXEL_WINS && fusionType != FusionType.AVG_BLEND )
			{
				System.out.println( "FusionMethod.THIN_PLATE_SPLINE: only FusionType.CLOSEST_PIXEL_WINS and FusionType.AVG_BLEND supported right now." );
				return null;
			}

			if ( masks )
			{
				System.out.println( "FusionMethod.THIN_PLATE_SPLINE: masks not yet supported." );
				return null;
			}

			if ( SplitImgLoaderThinPlateSplineFusion.getUnderlyingImageLoader( dataGlobal ) == null )
			{
				System.out.println( "FusionMethod.THIN_PLATE_SPLINE: ImageLoader is not a SplitViewerImgLoader, this is required for thin plate spline fusion. Please split the dataset first." );
				return null;
			}

			if ( overlapExpansion == null )
				overlapExpansion = OverlappingViews.defaultTPSExpansion;
		}
		else
		{
			if ( overlapExpansion == null )
				overlapExpansion = OverlappingViews.defaultAffineExpansion;
		}

		final int numTimepoints, numChannels;

		if ( timepointIndex == null )
		{
			numTimepoints = driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/NumTimepoints", int.class );
			numChannels = driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/NumChannels", int.class );
		}
		else
		{
			System.out.println( "Overriding numChannels and numTimepoints from metadata, instead processing timepointIndex=" + timepointIndex + ", channelIndex=" + channelIndex + " only.");
			numTimepoints = numChannels = 1;
		}

		final long[] bbMin = driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/Boundingbox_min", long[].class );
		final long[] bbMax = driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/Boundingbox_max", long[].class );
 
		final BoundingBox boundingBox = new BoundingBox( new FinalInterval( bbMin, bbMax ) );

		final boolean preserveAnisotropy = driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/PreserveAnisotropy", boolean.class );
		final double anisotropyFactor = preserveAnisotropy ? driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/AnisotropyFactor", double.class ) : Double.NaN;
		final int[] blockSize = driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/BlockSize", int[].class );

		final DataType dataType = driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/DataType", DataType.class );
		final boolean useSharding = driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/UseSharding", boolean.class );

		System.out.println( "FusionFormat: " + fusionFormat );
		System.out.println( "FusionMethod: " + fusionMethod );
		System.out.println( "OverlapExpansion: " + overlapExpansion );
		System.out.println( "FusionType: " + fusionType );
		System.out.println( "Input XML: " + xmlURI );
		System.out.println( "BDV project: " + bdv );
		System.out.println( "numTimepoints of fused dataset(s): " + numTimepoints );
		System.out.println( "numChannels of fused dataset(s): " + numChannels );
		System.out.println( "BoundingBox: " + boundingBox );
		System.out.println( "preserveAnisotropy: " + preserveAnisotropy );
		System.out.println( "anisotropyFactor: " + anisotropyFactor );
		System.out.println( "blockSize: " + Arrays.toString( blockSize ) );
		System.out.println( "useSharding: " + useSharding );
		System.out.println( "dataType: " + dataType );

		double minI = Double.NaN, maxI = Double.NaN;
		try
		{
			minI = driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/MinIntensity", double.class );
			maxI = driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/MaxIntensity", double.class );
		}
		catch ( Exception e )
		{
			System.out.println( "Min/Max intensity not stored." );
		}

		final double minIntensity = minI;
		final double maxIntensity = maxI;

		System.out.println( "minIntensity: " + minI );
		System.out.println( "maxIntensity: " + maxI );

		final MultiResolutionLevelInfo[][] mrInfos =
				driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/MultiResolutionInfos", MultiResolutionLevelInfo[][].class );

		System.out.println( "Loaded " + mrInfos.length + " metadata object for fused " + storageType + " volume(s)" );

		final int[] shardSize;
		final int[] shardSizeFactor;

		if ( useSharding )
		{
			// Load sharding metadata
			shardSize = driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/ShardSize", int[].class );
			shardSizeFactor = driverVolumeWriter.getAttribute( getContainerGroupPath(), "Bigstitcher-Spark/ShardSizeFactor", int[].class );
			System.out.println( "Sharding enabled. Shard size: " + Util.printCoordinates( shardSize ) + " (factor: " + Util.printCoordinates( shardSizeFactor ) + ")" );

			// Validate: blockScale cannot be specified when sharding is enabled
			if ( blockScaleString != null )
			{
				System.err.println( "ERROR: Sharding is enabled in this container (shard size: " + Util.printCoordinates( shardSize ) + ")." );
				System.err.println( "       The --blockScale parameter cannot be used with sharded containers." );
				System.err.println( "       Shard size automatically determines the compute block size for shard-aware writing." );
				throw new IllegalArgumentException( "Cannot specify --blockScale when sharding is enabled." );
			}
		}
		else
		{
			shardSize = null;
			shardSizeFactor = null;
			System.out.println( "Sharding disabled." );

			// Set default blockScale if not specified
			if ( blockScaleString == null )
				blockScaleString = "4,4,1";
		}

		final double[] maskOff = Import.csvStringToDoubleArray(maskOffset);

		final ArrayList< ViewId > viewIdsGlobal;

		if ( vi != null || angleIds != null || channelIds != null || illuminationIds != null || tileIds != null || timepointIds != null )
		{
			// TODO: With current logic, I could never get to AbstractSelectableViews.loadViewIds, no matter which CLI arguments are specified.
			//       The following is a workaround for my example.
			//       But the whole logic is flawed and needs to be revised.
			viewIdsGlobal = AbstractSelectableViews.loadViewIds( dataGlobal, vi, angleIds, channelIds, illuminationIds, tileIds, timepointIds );
		}
		else if (
			dataGlobal.getSequenceDescription().getAllChannelsOrdered().size() != numChannels ||
			dataGlobal.getSequenceDescription().getTimePoints().getTimePointsOrdered().size() != numTimepoints )
		{
			System.out.println(
					"The number of channels and timepoint in XML does not match the number in the export dataset."
					+ "You have to specify which ViewIds/Channels/Illuminations/Tiles/Angles/Timepoints should be fused into"
					+ "a specific 3D volume in the fusion dataset:");

			viewIdsGlobal = AbstractSelectableViews.loadViewIds( dataGlobal, vi, angleIds, channelIds, illuminationIds, tileIds, timepointIds );

			if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
				return null;
		}
		else
		{
			viewIdsGlobal = Import.getViewIds( dataGlobal );
		}

		final int[] blocksPerJob;
		if (shardSize != null) {
			blocksPerJob = null; // blocks per job is not used when sharding, shardsize will be used for computeBlockSize
			System.out.println( "Fusing: " + boundingBox.getTitle() + ": " + Util.printInterval( boundingBox ) +
					" with shardsize " + Util.printCoordinates( shardSize ) + " and blocksize " + Util.printCoordinates( blockSize ) );
		} else {
			blocksPerJob = Import.csvStringToIntArray(blockScaleString);
			System.out.println( "Fusing: " + boundingBox.getTitle() + ": " + Util.printInterval( boundingBox ) +
					" with blocksize " + Util.printCoordinates( blockSize ) + " and " + Util.printCoordinates( blocksPerJob ) + " blocks per job/shard" );
		}

		if ( dataType == DataType.UINT8 )
			System.out.println( "Fusing to UINT8, min intensity = " + minIntensity + ", max intensity = " + maxIntensity );
		else if ( dataType == DataType.UINT16 )
			System.out.println( "Fusing to UINT16, min intensity = " + minIntensity + ", max intensity = " + maxIntensity );
		else
			System.out.println( "Fusing to FLOAT32" );

		//
		// intensity correction coefficients dataset
		//
		if ( intensityN5PathURIString != null )
		{
			intensityN5PathURI = URITools.toURI( intensityN5PathURIString );
			System.out.println( "Intensity coefficients: " + intensityN5PathURI );

			if ( intensityN5StorageType == null )
			{
				if ( intensityN5PathURIString.toLowerCase().endsWith( ".zarr" ) )
					intensityN5StorageType = StorageFormat.ZARR;
				else if ( intensityN5PathURIString.toLowerCase().endsWith( ".zarr2" ) )
					intensityN5StorageType = StorageFormat.ZARR2;
				else if ( intensityN5PathURIString.toLowerCase().endsWith( ".n5" ) )
					intensityN5StorageType = StorageFormat.N5;
				else if ( intensityN5PathURIString.toLowerCase().endsWith( ".h5" ) || intensityN5PathURI.toString().toLowerCase().endsWith( ".hdf5" ) )
					intensityN5StorageType = StorageFormat.HDF5;
				else
				{
					System.out.println( "Unable to guess format from URI '" + intensityN5PathURI + "', please specify using '-s'");
					return null;
				}

				System.out.println( "Guessed format " + intensityN5StorageType + " will be used to open URI '" + intensityN5PathURI + "', you can override it using '-s'");
			}
			else
			{
				System.out.println( "Format " + intensityN5StorageType + " will be used to open " + intensityN5PathURI );
			}

			// test that the intensity coefficients exists
			try( final N5Reader r = URITools.instantiateN5Reader( intensityN5StorageType, intensityN5PathURI  ) )
			{
				System.out.println( "Found intensity container '" + intensityN5PathURI + "'.");
			}
			catch ( Exception e )
			{
				System.out.println( "Exception: " + e);
				System.out.println( "Error, intensity coefficients '" + intensityN5PathURI + "' does not exist/could not be loaded. If you are using ZARR2, you need specify it "
						+ "since the default for .zarr is ZARR v3.");
				return null;
			}
		}

		//
		// final variables for Spark
		//
		final long[] dimensions = boundingBox.dimensionsAsLongArray();
		final StorageFormat intensityN5StorageType = this.intensityN5StorageType;
		final URI intensityN5PathURI = this.intensityN5PathURI;
		final String intensityN5Group = this.intensityN5Group;
		final String intensityN5Dataset = this.intensityN5Dataset;

		// TODO: do we still need this?
		try
		{
			// trigger the N5-blosc error, because if it is triggered for the first
			// time inside Spark, everything crashes
			new N5FSWriter(null);
		}
		catch (Exception e ) {}

		final SparkConf conf = new SparkConf().setAppName("SparkFusion");

		if (localSparkBindAddress)
		{
			conf.set("spark.driver.bindAddress", "127.0.0.1");
			conf.set("spark.driver.host", "localhost");
			org.apache.spark.util.Utils.setCustomHostname("localhost");
		}

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final SequenceDescription sd = dataGlobal.getSequenceDescription();

		final HashMap< Integer, Integer > tpIdToTpIndex = new HashMap<>();
		final HashMap< Integer, Integer > chIdToChIndex = new HashMap<>();

		for ( int t = 0; t < sd.getTimePoints().getTimePointsOrdered().size(); ++t )
			tpIdToTpIndex.put( sd.getTimePoints().getTimePointsOrdered().get( t ).getId(), t );

		for ( int c = 0; c < sd.getAllChannelsOrdered().size(); ++c )
			chIdToChIndex.put( sd.getAllChannelsOrdered().get( c ).getId(), c );

		final long totalTime = System.currentTimeMillis();

		for ( int t = 0; t < numTimepoints; ++t )
			for ( int c = 0; c < numChannels; ++c )
			{
				final int tIndex = (timepointIndex == null) ? t : timepointIndex;
				final int cIndex = (channelIndex == null) ? c : channelIndex;

				System.out.println( "\nProcessing channel " + cIndex + ", timepoint " + tIndex );
				System.out.println( "-----------------------------------" );

				final ArrayList< ViewId > viewIds = new ArrayList<>();
				viewIdsGlobal.forEach( viewId -> {
					final ViewDescription vd = sd.getViewDescription( viewId );

					if ( timepointIndex != null || tpIdToTpIndex.get( vd.getTimePointId() ) == tIndex && chIdToChIndex.get( vd.getViewSetup().getChannel().getId() ) == cIndex )
						viewIds.add( new ViewId( viewId.getTimePointId(), viewId.getViewSetupId() ) );
				});

				if ( masks )
					System.out.println( "Creating masks for " + viewIds.size() + " views of this 3D volume ... " );
				else
					System.out.println( "Fusing " + viewIds.size() + " views for this 3D volume ... " );

				viewIds.forEach( vd -> System.out.println( Group.pvid( vd ) ) );
				final MultiResolutionLevelInfo[] mrInfo;

				if ( storageType == StorageFormat.ZARR || storageType == StorageFormat.ZARR2 )
					mrInfo = mrInfos[ 0 ];
				else
					mrInfo = mrInfos[ cIndex + tIndex*numChannels ];

				// CRITICAL: When sharding enabled, computeBlockSize MUST equal shardSize for shard-aware writing
				final int[] computeBlockSize;
				if ( useSharding )
				{
					computeBlockSize = shardSize;
					System.out.println( "Sharding enabled: computeBlockSize = shardSize = " + Util.printCoordinates( computeBlockSize ) );
				}
				else
				{
					// using bigger blocksizes than being stored for efficiency (needed for very large datasets)
					computeBlockSize = new int[ 3 ];
					Arrays.setAll( computeBlockSize, d -> blockSize[ d ] * blocksPerJob[ d ] );
					System.out.println( "No sharding: computeBlockSize = blockSize * blocksPerJob = " + Util.printCoordinates( computeBlockSize ) );
				}

				final List<long[][]> grid = Grid.create(dimensions,
						computeBlockSize,
						blockSize);

				System.out.println( "numJobs = " + grid.size() );

				final RetryTrackerSpark<long[][]> retryTracker =
						RetryTrackerSpark.forGridBlocks("s0 block processing", grid.size());

				long time = System.currentTimeMillis();

				do
				{
					if (!retryTracker.beginAttempt())
					{
						System.out.println( "Stopping." );
						System.exit( 1 );
					}

					final JavaRDD<long[][]> rddS0 = sc.parallelize( grid, Math.min( Spark.maxPartitions, grid.size() ) );

					final JavaRDD<long[][]> rddS0Result = rddS0.map( gridBlock ->
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
						final boolean uint8, uint16;

						if ( dataType == DataType.UINT8 )
						{
							conv = new RealUnsignedByteConverter<>( minIntensity, maxIntensity );
							type = new UnsignedByteType();
							uint8 = true;
							uint16 = false;
						}
						else if ( dataType == DataType.UINT16 )
						{
							conv = new RealUnsignedShortConverter<>( minIntensity, maxIntensity );
							type = new UnsignedShortType();
							uint8 = false;
							uint16 = true;
						}
						else
						{
							conv = null;
							type = new FloatType();
							uint8 = false;
							uint16 = false;
						}

						// The min coordinates of the block that this job renders (in pixels)
						final int n = gridBlock[ 0 ].length;
						final long[] superBlockOffset = new long[ n ];
						Arrays.setAll( superBlockOffset, d -> gridBlock[ 0 ][ d ] + bbMin[ d ] );

						// The size of the block that this job renders (in pixels)
						final long[] superBlockSize = gridBlock[ 1 ];

						//TODO: prefetchExecutor!!
						final long[] fusedBlockMin = new long[ n ];
						final long[] fusedBlockMax = new long[ n ];
						final Interval fusedBlock = FinalInterval.wrap( fusedBlockMin, fusedBlockMax );

						// pre-filter views that overlap the superBlock
						Arrays.setAll( fusedBlockMin, d -> superBlockOffset[ d ] );
						Arrays.setAll( fusedBlockMax, d -> superBlockOffset[ d ] + superBlockSize[ d ] - 1 );

						final List< ViewId > overlappingViews =
								OverlappingViews.findOverlappingViews( dataLocal, viewIds, registrations, fusedBlock, overlapExpansion );

						if ( overlappingViews.isEmpty() )
							return gridBlock.clone();

						// load intensity correction coefficients for all overlapping views
						final Map< ViewId, Coefficients > coefficients;

						if ( intensityN5PathURI != null )
						{
							coefficients = new HashMap<>();
							try ( N5Reader intensityN5Reader = URITools.instantiateN5Reader( intensityN5StorageType, intensityN5PathURI ) )
							{
								overlappingViews.forEach( v ->
									coefficients.put( v, IntensityCorrection.readCoefficients( intensityN5Reader, intensityN5Group, intensityN5Dataset, v ) ) );
							}
						} else
						{
							coefficients = null;
						}

						//final RandomAccessibleInterval img;
						final BlockSupplier blockSupplier;
						final FinalInterval interval = new FinalInterval( bbMin, bbMax );

						if ( masks )
						{
							System.out.println( "Creating masks for block: offset=" + Util.printCoordinates( gridBlock[0] ) + ", dimension=" + Util.printCoordinates( gridBlock[1] ) );

							blockSupplier = BlockSupplier.of( Views.zeroMin(
									new GenerateComputeBlockMasks(
											dataLocal,
											registrations,
											overlappingViews,
											bbMin,
											bbMax,
											uint8,
											uint16,
											maskOff ).call( gridBlock ) ) );
						}
						else
						{
							//
							// PREFETCHING, TODO: should be part of BlkAffineFusion.init
							//
							// I think we only need this if PREFETCH == TRUE, we already test for overlapping views above
							// this should be fine for TPS since it is using overlapExpansion
							if ( prefetch ) //fusionMethod == FusionMethod.AFFINE )
							{
								final OverlappingBlocks overlappingBlocks = OverlappingBlocks.find( dataLocal, registrations, overlappingViews, fusedBlock, overlapExpansion );

								// my current thinking is that this never happens because we already test for overlapping views above
								if ( overlappingBlocks.overlappingViews().isEmpty() )
									return gridBlock.clone();

								//if ( prefetch )
								{
									System.out.println( "Prefetching: " + overlappingBlocks.numPrefetchBlocks() + " block(s) from " + overlappingBlocks.overlappingViews().size() + " overlapping view(s) in the input data." );

									final ExecutorService prefetchExecutor = Executors.newCachedThreadPool();
									overlappingBlocks.prefetch(prefetchExecutor);
									prefetchExecutor.shutdown();
								}
							}

							System.out.println( "Fusing block: offset=" + Util.printCoordinates( gridBlock[0] ) + ", dimension=" + Util.printCoordinates( gridBlock[1] ) );

							// returns a zero-min interval
							if ( fusionMethod == FusionMethod.AFFINE )
							{
								blockSupplier = BlkAffineFusion.initWithIntensityCoefficients(
										conv,
										dataLocal.getSequenceDescription().getImgLoader(),
                                        overlappingViews,
										registrations,
										dataLocal.getSequenceDescription().getViewDescriptions(),
										fusionType,//fusion.getFusionType(),
										Double.NaN, // only for content-based
										null, // map<old,new> will go here
										1, // linear interpolation
										coefficients, // intensity correction
										new BoundingBox( interval ),
										(RealType & NativeType)type,
										blockSize );
							}
							else
							{
								blockSupplier = SplitImgLoaderThinPlateSplineFusion.init(
										conv,
										(SplitViewerImgLoader)dataLocal.getSequenceDescription().getImgLoader(),
                                        overlappingViews,
										dataLocal.getViewRegistrations().getViewRegistrations(), // already adjusted for anisotropy
										dataLocal.getSequenceDescription().getViewDescriptions(),
										fusionType,
										overlapExpansion,
										Double.NaN,
										1, // linear interpolation
										null, // old setupId > new setupId for fusion order, only makes sense with FusionType.FIRST_LOW or FusionType.FIRST_HIGH
										coefficients, // intensity correction
										new BoundingBox( interval ), // already adjusted for anisotropy???
										(RealType & NativeType)type,
										blockSize );
							}
						}

						final long[] gridOffset;

						final long[] blockMin = gridBlock[0].clone();
						final long[] blockMax = new long[ blockMin.length ];

						for ( int d = 0; d < blockMin.length; ++d )
							blockMax[ d ] = Math.min( Intervals.zeroMin( interval ).max( d ), blockMin[ d ] + gridBlock[1][ d ] - 1 );

						final RandomAccessibleInterval image;
						final RandomAccessibleInterval img = BlockAlgoUtils.arrayImg( blockSupplier, new FinalInterval( blockMin, blockMax ) );

						// 5D OME-ZARR CONTAINER
						if ( storageType == StorageFormat.ZARR || storageType == StorageFormat.ZARR2 )
						{
							// gridBlock is 3d, make it 5d
							//blockOffset = new long[] { gridBlock[0][0], gridBlock[0][1], gridBlock[0][2], cIndex, tIndex };
							//blockSizeExport = new long[] { gridBlock[1][0], gridBlock[1][1], gridBlock[1][2], 1, 1 };
							gridOffset = new long[] { gridBlock[2][0], gridBlock[2][1], gridBlock[2][2], cIndex, tIndex }; // because blocksize in C & T is 1

							// img is 3d, make it 5d
							// the same information is returned no matter which index is queried in C and T
							//image = Views.addDimension( Views.addDimension( img ) );
							image = Views.interval(
									Views.addDimension( Views.addDimension( img ) ),
									new FinalInterval( new long[] { gridBlock[1][0], gridBlock[1][1], gridBlock[1][2], cIndex+1, tIndex+1 } ) ); // blocksize is used here
						}
						else
						{
							//blockOffset = gridBlock[0];
							//blockSizeExport = gridBlock[1];
							gridOffset = gridBlock[2];

							image = img;
						}

						final N5Writer driverVolumeWriterLocal = N5Util.createN5Writer( outPathURI, storageType );

						// TODO: is this multithreaded??
						// TODO: should we catch the N5 exception and throw a general one?
						N5Utils.saveBlock( image, driverVolumeWriterLocal, mrInfo[ 0 ].dataset, gridOffset );

						if ( N5Util.sharedHDF5Writer == null )
							driverVolumeWriterLocal.close();

						return gridBlock.clone();
					});

					// extract all blocks that failed
					final Set<long[][]> failedBlocksSet =
							retryTracker.processResults( rddS0Result.collect(), grid );

					// Use RetryTracker to handle retry counting and removal
					if (!retryTracker.processFailures(failedBlocksSet))
					{
						System.out.println( "Stopping." );
						System.exit( 1 );
					}

					// Update grid for next iteration with remaining failed blocks
					grid.clear();
					grid.addAll(failedBlocksSet);
				}
				while ( grid.size() > 0 );

				System.out.println( new Date( System.currentTimeMillis() ) +
						" timepoint: "  + t +
						" channel: " + c +
						" full resolution, took: " + (System.currentTimeMillis() - time ) / 1000. + " s." );

				//
				// save multiresolution pyramid (s1 ... sN)
				//
				for ( int level = 1; level < mrInfo.length; ++level )
				{
					final int s = level;

					final List<long[][]> allBlocks = Grid.create(
							new long[] { mrInfo[ level ].dimensions[ 0 ], mrInfo[ level ].dimensions[ 1 ], mrInfo[ level ].dimensions[ 2 ] },
							computeBlockSize,
							blockSize);

					System.out.println( new Date( System.currentTimeMillis() ) + ": Downsampling: " + Util.printCoordinates( mrInfo[ level ].absoluteDownsampling ) + " with relative downsampling of " + Util.printCoordinates( mrInfo[ level ].relativeDownsampling ));
					System.out.println( new Date( System.currentTimeMillis() ) + ": s" + level + " num blocks=" + allBlocks.size() );
					System.out.println( new Date( System.currentTimeMillis() ) + ": Loading '" + mrInfo[ level - 1 ].dataset + "', downsampled will be written as '" + mrInfo[ level ].dataset + "'." );

					time = System.currentTimeMillis();

					final RetryTrackerSpark<long[][]> retryTrackerDS =
							RetryTrackerSpark.forGridBlocks("s" + level + " block processing", allBlocks.size());

					do
					{
						if (!retryTrackerDS.beginAttempt())
						{
							System.out.println( "Stopping." );
							System.exit( 1 );
						}

						final JavaRDD<long[][]> rddDS = sc.parallelize( allBlocks, Math.min( Spark.maxPartitions, allBlocks.size() ) );
	
						final JavaRDD<long[][]> rddDSResult = rddDS.map( gridBlock ->
						{
							final N5Writer driverVolumeWriterLocal = N5Util.createN5Writer( outPathURI, storageType );
	
							// 5D OME-ZARR CONTAINER
							if ( storageType == StorageFormat.ZARR || storageType == StorageFormat.ZARR2 )
							{
								N5ApiTools.writeDownsampledBlock5dOMEZARR(
										driverVolumeWriterLocal,
										mrInfo[ s ],
										mrInfo[ s - 1 ],
										gridBlock,
										cIndex,
										tIndex );
							}
							else
							{
								N5ApiTools.writeDownsampledBlock(
										driverVolumeWriterLocal,
										mrInfo[ s ],
										mrInfo[ s - 1 ],
										gridBlock );
							}
	
							if ( N5Util.sharedHDF5Writer == null )
								driverVolumeWriterLocal.close();
	
							return gridBlock.clone();
						});
						
						// extract all blocks that failed
						final Set<long[][]> failedBlocksSet =
								retryTrackerDS.processResults( rddDSResult.collect(), grid );

						// Use RetryTracker to handle retry counting and removal
						if (!retryTrackerDS.processFailures(failedBlocksSet))
						{
							System.out.println( "Stopping." );
							System.exit( 1 );
						}

						// Update grid for next iteration with remaining failed blocks
						grid.clear();
						grid.addAll(failedBlocksSet);
					}
					while ( grid.size() > 0 );

					System.out.println( new Date( System.currentTimeMillis() ) +
							" timepoint: "  + t +
							" channel: " + c +
							" saving level : " + s +
							" took " + (System.currentTimeMillis() - time ) / 1000. + " s." );
					System.out.println( new Date( System.currentTimeMillis() ) + ": Saved level s " + level + ", took: " + (System.currentTimeMillis() - time ) + " ms." );
				}
			}

		// close main writer (is shared over Spark-threads if it's HDF5, thus just closing it here)
		driverVolumeWriter.close();

		System.out.println( "done, took: " + (System.currentTimeMillis() - totalTime ) / 1000. + " s." );

		sc.close();

		return null;
	}

	public static void main(final String... args) throws SpimDataException {

		//final XmlIoSpimData io = new XmlIoSpimData();
		//final SpimData spimData = io.load( "/Users/preibischs/Documents/Microscopy/Stitching/Truman/standard/output/dataset.xml" );
		//BdvFunctions.show( spimData );
		//SimpleMultiThreading.threadHaltUnClean();

		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new SparkFusion()).execute(args));
	}
}
