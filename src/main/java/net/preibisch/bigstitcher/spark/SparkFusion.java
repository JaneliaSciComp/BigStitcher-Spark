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
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Dimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.algorithm.blocks.dfield.DisplacementFields.TransformedDisplacementField;
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
import net.imglib2.type.numeric.real.DoubleType;
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
import net.imglib2.realtransform.ThinplateSplineTransform;
import net.preibisch.mvrecon.process.fusion.blk.BlkAffineFusion;
import net.preibisch.mvrecon.process.fusion.blk.BlkThinPlateSplineFusion;
import net.preibisch.mvrecon.process.fusion.blk.DisplacementFieldN5Tools;
import net.preibisch.mvrecon.process.fusion.blk.SplitImgLoaderThinPlateSplineFusion;
import net.preibisch.mvrecon.process.fusion.tps.Landmarks;
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

	@Option(names = { "-o", "--n5Path" }, required = true, description = "N5/ZARR/HDF5 base path of the output container, which must already have been created with create-fusion-container "
			+ "(data type, BDV/plain format, sharding etc. are read from its metadata), e.g. -o /home/fused.n5 or e.g. s3://myBucket/data.n5")
	private String outputPathURIString = null;

	@Option(names = {"-s", "--storage"}, description = "Dataset storage type, can be used to override guessed format (default: guess from file/directory-ending; NOTE: does not work for ZARR2, you need to specify since .zarr would default to ZARR v3)")
	private StorageFormat storageType = null;

	@Option(names = "--blockScale", description = "how many blocks to use for a single processing step, e.g. 4,4,1 means for blockSize a 128,128,64 that each spark thread writes 512,512,64 (default: 4,4,1; NOT compatible with sharded containers)")
	private String blockScaleString = null;

	@Option(names = { "--masks" }, description = "save only the masks (this will not fuse the images, currently only works for FusionMethod.AFFINE)")
	private boolean masks = false;

	@Option(names = "--maskOffset", description = "allows to make masks larger (+, the mask will include some background) or smaller (-, some fused content will be cut off), warning: in the non-isotropic coordinate space of the raw input images (default: 0.0,0.0,0.0)")
	private String maskOffset = "0.0,0.0,0.0";

	@Option(names = {"-f", "--fusion"}, description = "Strategy for merging overlapping views during fusion, supported: AVG, AVG_BLEND, "/*AVG_CONTENT, AVG_BLEND_CONTENT*/+", MAX_INTENSITY, LOWEST_VIEWID_WINS, HIGHEST_VIEWID_WINS, CLOSEST_PIXEL_WINS (default: AVG_BLEND)")
	private FusionType fusionType = FusionType.AVG_BLEND;

	@Option(names = {"-fm", "--fusionMethod"}, description = "The transformation models used for fusion, AFFINE or THIN_PLATE_SPLINE. TPS requires a split dataset with at least 2x2x2 split views (default: AFFINE).")
	private FusionMethod fusionMethod = FusionMethod.AFFINE;

	@Option(names = { "-oe", "--overlapExpansion" }, description = "Number of pixels by which intervals are expanded when testing for overlap (default: 2 for FusionMethod.AFFINE; 50 for FusionMethod.THIN_PLATE_SPLINE).")
	private Integer overlapExpansion = null;

	@Option(names = { "-t", "--timepointIndex" }, description = "specify a specific timepoint index of the output container that should be fused, usually you would also specify what --angleId, --tileId, ... or ViewIds -vi are being fused.")
	private Integer timepointIndex = null;

	@Option(names = { "-c", "--channelIndex" }, description = "specify a specific channel index of the output container that should be fused, usually you would also specify what --angleId, --tileId, ... or ViewIds -vi are being fused.")
	private Integer channelIndex = null;


	// To specify what goes into the current 3D volume
	@Option(names = { "--angleId" }, description = "list the angle ids that should be processed, you can find them in the XML, e.g. --angleId '0,1,2' (default: all angles)")
	protected String angleIds = null;

	@Option(names = { "--tileId" }, description = "list the tile ids that should be processed, you can find them in the XML, e.g. --tileId '0,1,2' (default: all tiles)")
	protected String tileIds = null;

	@Option(names = { "--illuminationId" }, description = "list the illumination ids that should be processed, you can find them in the XML, e.g. --illuminationId '0,1,2' (default: all illuminations)")
	protected String illuminationIds = null;

	@Option(names = { "--channelId" }, description = "list the channel ids that should be processed, you can find them in the XML (usually just one when fusing), e.g. --channelId '0,1,2' (default: all channels)")
	protected String channelIds = null;

	@Option(names = { "--timepointId" }, description = "list the timepoint ids that should be processed, you can find them in the XML (usually just one when fusing), e.g. --timepointId '0,1,2' (default: all time points)")
	protected String timepointIds = null;

	@Option(names = { "-vi" }, description = "specifically list the view ids (time point, view setup) that should be fused into a single image, e.g. -vi '0,0' -vi '0,1' (default: all view ids)")
	protected String[] vi = null;

	@Option(names = { "--prefetch" }, description = "prefetch all blocks required for fusion in each Spark job using unlimited threads, useful in cloud environments (default: false)")
	protected boolean prefetch = false;

	// === Thin-Plate-Spline displacement-field cache (only used with -fm THIN_PLATE_SPLINE) ===

	public enum DfieldType { FLOAT32, FLOAT64 }

	@Option(names = { "--dfieldSpacing" }, description = "TPS only: dfield grid spacing per dim (default: 8,8,8). Larger = less memory but more interpolation error.")
	private String dfieldSpacingString = "8,8,8";

	@Option(names = { "--dfieldType" }, description = "TPS only: dfield component type, FLOAT32 or FLOAT64 (default: FLOAT64, matching the pre-cache behavior). FLOAT32 halves on-disk and in-RAM size at the cost of some precision.")
	private DfieldType dfieldType = DfieldType.FLOAT64;

	@Option(names = { "--reuseDfieldCache" }, description = "TPS only: skip recomputing per-view dfields if already present in the output container with matching parameters (default: true).")
	private boolean reuseDfieldCache = true;

	@Option(names = { "--dfieldBlockSize" }, description = "TPS only: N5 block size of the cached dfield datasets (default: 128,128,128).")
	private String dfieldBlockSizeString = "128,128,128";


	// TODO: add support for loading coefficients during fusion
	@CommandLine.Option(names = { "--intensityN5Path" }, description = "N5/ZARR/HDF5 base path for loading coefficients (e.g. s3://myBucket/coefficients.n5)")
	private String intensityN5PathURIString = null;

	@CommandLine.Option(names = { "--intensityN5Storage" }, description = "storage type of the intensity coefficients container, can be used to override guessed format (default: guess from --intensityN5Path file/directory-ending; NOTE: does not work for ZARR2, you need to specify since .zarr would default to ZARR v3)")
	private StorageFormat intensityN5StorageType = null;

	@CommandLine.Option(names = { "--intensityN5Group" }, description = "group under which coefficient datasets are stored (default: \"\")")
	private String intensityN5Group = "";

	@CommandLine.Option(names = { "--intensityN5Dataset" }, description = "dataset name for each coefficient dataset (default: \"intensity\"). The coefficients for view(s,t) are stored in dataset \"{--intensityN5Group}/setup{s}/timepoint{t}/{--intensityN5Dataset}\"")
	private String intensityN5Dataset = "intensity";

	URI outPathURI = null;
	/**
	 * Prefetching now works with a Executors.newCachedThreadPool();
	 */
	//static final int N_PREFETCH_THREADS = 72;

	URI intensityN5PathURI = null;

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

		final String fusionFormat = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/FusionFormat", String.class );

		if ( fusionFormat == null )
		{
			System.out.println( "Could not load 'Bigstitcher-Spark/FusionFormat' from metadata of specified output '" + outPathURI + "'.");
			System.out.println( "Note: this metadata is created by ./create-fusion-container in the previous step." );
			return null;
		}

		final boolean bdv = fusionFormat.toLowerCase().contains( "BDV" );

		final URI xmlURI = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/InputXML", URI.class );

		final SpimData2 dataGlobal = Spark.getJobSpimData2( xmlURI, 0 );

		if ( dataGlobal == null )
		{
			System.out.println( "Could not load XML from:" + xmlURI );
			return null;
		}

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
			numTimepoints = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/NumTimepoints", int.class );
			numChannels = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/NumChannels", int.class );
		}
		else
		{
			System.out.println( "Overriding numChannels and numTimepoints from metadata, instead processing timepointIndex=" + timepointIndex + ", channelIndex=" + channelIndex + " only.");
			numTimepoints = numChannels = 1;
		}

		final long[] bbMin = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/Boundingbox_min", long[].class );
		final long[] bbMax = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/Boundingbox_max", long[].class );

		final BoundingBox boundingBox = new BoundingBox( new FinalInterval( bbMin, bbMax ) );

		final boolean preserveAnisotropy = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/PreserveAnisotropy", boolean.class );
		final double anisotropyFactor = preserveAnisotropy ? driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/AnisotropyFactor", double.class ) : Double.NaN;
		final int[] blockSize = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/BlockSize", int[].class );

		final DataType dataType = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/DataType", DataType.class );

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
		System.out.println( "dataType: " + dataType );

		double minI = Double.NaN, maxI = Double.NaN;
		try
		{
			minI = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/MinIntensity", double.class );
			maxI = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/MaxIntensity", double.class );
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
				driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/MultiResolutionInfos", MultiResolutionLevelInfo[][].class );

		System.out.println( "Loaded " + mrInfos.length + " metadata object for fused " + storageType + " volume(s)" );

		// Load sharding metadata
		final boolean useSharding = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/UseSharding", boolean.class );
		final int[] shardSize;
		final int[] shardSizeFactor;

		if ( useSharding )
		{
			shardSize = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/ShardSize", int[].class );
			shardSizeFactor = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/ShardSizeFactor", int[].class );
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

		final int[] blocksPerJob = Import.csvStringToIntArray(blockScaleString);
		System.out.println( "Fusing: " + boundingBox.getTitle() + ": " + Util.printInterval( boundingBox ) +
				" with blocksize " + Util.printCoordinates( blockSize ) + " and " + Util.printCoordinates( blocksPerJob ) + " blocks per job/shard" );

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

		for ( int c = 0; c < numChannels; ++c )
			for ( int t = 0; t < numTimepoints; ++t )
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

				// === Phase 1.5: materialize per-underlying-view dfields if needed (TPS only) ===
				if ( fusionMethod == FusionMethod.THIN_PLATE_SPLINE )
				{
					materializeDisplacementFields( sc, dataGlobal, viewIds, xmlURI, outPathURI, storageType, anisotropyFactor );
				}

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

				//driverVolumeWriter.setAttribute( n5Dataset, "offset", minBB );

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

					final JavaRDD<long[][]> rdd = sc.parallelize( grid, Math.min( Spark.maxPartitions, grid.size() ) );

					final JavaRDD<long[][]> rddResult = rdd.map( gridBlock ->
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

						if ( overlappingViews.size() == 0 )
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
								// THIN_PLATE_SPLINE: load per-underlying-view dfields from the cache
								// materialized in Phase 1.5, then call initWithLoadedDfields.
								// Loading as a full ArrayImg (not lazy) is required by Tobias's
								// perf-tuned per-block evaluation in BlkThinPlateSplineFusion.
								final SplitViewerImgLoader splitImgLoaderLambda =
										( SplitViewerImgLoader ) dataLocal.getSequenceDescription().getImgLoader();
								final List< ViewId > overlappingUnderlying =
										SplitImgLoaderThinPlateSplineFusion.underlyingViewIds(
												overlappingViews, splitImgLoaderLambda.new2oldSetupId() );

								final Map< ViewId, Interval > viewBoundsLoaded = new HashMap<>();
								@SuppressWarnings( { "unchecked", "rawtypes" } )
								final Map rawDfields = new HashMap<>();
								try ( final N5Reader r = URITools.instantiateN5Reader( storageType, outPathURI ) )
								{
									for ( final ViewId uvid : overlappingUnderlying )
									{
										final String dsPath = DisplacementFieldN5Tools.datasetPath( uvid );
										viewBoundsLoaded.put( uvid, DisplacementFieldN5Tools.readBbox( r, dsPath ) );
										final TransformedDisplacementField< ? > df =
												( dfieldType == DfieldType.FLOAT32 )
														? DisplacementFieldN5Tools.readDisplacementFieldAsCellImg( r, dsPath, new FloatType() )
														: DisplacementFieldN5Tools.readDisplacementFieldAsCellImg( r, dsPath, new DoubleType() );
										rawDfields.put( uvid, df );
									}
								}

								@SuppressWarnings( { "unchecked", "rawtypes" } )
								final BlockSupplier rawSupplier = SplitImgLoaderThinPlateSplineFusion.initWithLoadedDfields(
										conv,
										splitImgLoaderLambda,
										overlappingViews,
										dataLocal.getViewRegistrations().getViewRegistrations(),
										dataLocal.getSequenceDescription().getViewDescriptions(),
										viewBoundsLoaded,
										rawDfields,
										fusionType,
										overlapExpansion,
										anisotropyFactor,
										1, // linear interpolation
										null, // fusion order — only for FIRST_LOW / FIRST_HIGH
										coefficients,
										new BoundingBox( interval ),
										(RealType & NativeType) type,
										blockSize );
								blockSupplier = rawSupplier;
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

					rddResult.cache();
					rddResult.count();

					// extract all blocks that failed
					final Set<long[][]> failedBlocksSet =
							retryTracker.processWithSpark( rddResult, grid );

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

				System.out.println( new Date( System.currentTimeMillis() ) + ": Saved full resolution, took: " + (System.currentTimeMillis() - time ) + " ms." );

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
						
						rddDSResult.cache();
						rddDSResult.count();
	
						// extract all blocks that failed
						final Set<long[][]> failedBlocksSet =
								retryTrackerDS.processWithSpark( rddDSResult, grid );
	
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

					System.out.println( new Date( System.currentTimeMillis() ) + ": Saved level s " + level + ", took: " + (System.currentTimeMillis() - time ) + " ms." );
				}
			}

		// close main writer (is shared over Spark-threads if it's HDF5, thus just closing it here)
		driverVolumeWriter.close();

		/*
		if ( multiRes )
			System.out.println( "Saved, e.g. view with './n5-view -i " + n5PathURI + " -d " + n5Dataset.substring( 0, n5Dataset.length() - 3) + "'" );
		else
			System.out.println( "Saved, e.g. view with './n5-view -i " + n5PathURI + " -d " + n5Dataset + "'" );
		*/

		System.out.println( "done, took: " + (System.currentTimeMillis() - totalTime ) + " ms." );

		sc.close();

		return null;
	}

	/**
	 * Phase 1.5 of TPS fusion: for each underlying view of the current (channel,
	 * timepoint), build the TPS displacement field once and persist it under
	 * the output container at {@code _displacement_field_cache/...}. Subsequent
	 * Spark block-fusion tasks load these dfields directly instead of
	 * rebuilding them per task. Skips views whose cache entries already match.
	 */
	/**
	 * Serializable description of one dfield block-task: which underlying view,
	 * which N5 chunk position, and the per-view inputs needed to rebuild the
	 * TPS on the executor (landmarks + bbox). One {@code DfieldBlockSpec} per
	 * N5 chunk per view; many of these get parallelized across Spark slots so
	 * the dfield rasterization scales beyond the small number of views.
	 *
	 * Landmarks and bbox are duplicated per spec (one view contributes many
	 * specs), but the data is small (a few hundred bytes per view) so the
	 * extra closure size is negligible compared to a broadcast.
	 */
	private static final class DfieldBlockSpec implements Serializable
	{
		private static final long serialVersionUID = 1L;
		final int viewSetupId;
		final int timepointId;
		final long[] blockGridPos3d;
		final long[] bboxMin;
		final long[] bboxMax;
		final double[][] sourcePoints;
		final double[][] targetPoints;
		final String datasetPath;

		DfieldBlockSpec(
				final ViewId v,
				final long[] blockGridPos3d,
				final Interval bbox,
				final Landmarks landmarks,
				final String datasetPath )
		{
			this.viewSetupId = v.getViewSetupId();
			this.timepointId = v.getTimePointId();
			this.blockGridPos3d = blockGridPos3d;
			this.bboxMin = bbox.minAsLongArray();
			this.bboxMax = bbox.maxAsLongArray();
			this.sourcePoints = landmarks.getSourcePoints();
			this.targetPoints = landmarks.getTargetPoints();
			this.datasetPath = datasetPath;
		}
	}

	/**
	 * Phase 1.5: materialize per-underlying-view displacement fields under the
	 * output container. Parallelized at the N5-chunk level: one Spark task per
	 * dfield block. Driver pre-creates the empty datasets so executors don't
	 * race on dataset creation. Skips views whose cache entry already matches.
	 */
	private void materializeDisplacementFields(
			final JavaSparkContext sc,
			final SpimData2 dataGlobal,
			final List< ViewId > splitViewIds,
			final URI xmlURIFinal,
			final URI outPathURIFinal,
			final StorageFormat storageTypeFinal,
			final double anisotropyFactor )
	{
		final int[] dfieldSpacingInt = Import.csvStringToIntArray( dfieldSpacingString );
		if ( dfieldSpacingInt.length != 3 )
			throw new IllegalArgumentException( "--dfieldSpacing must have 3 values (got " + dfieldSpacingInt.length + ")" );
		final double[] dfieldSpacing = new double[] { dfieldSpacingInt[ 0 ], dfieldSpacingInt[ 1 ], dfieldSpacingInt[ 2 ] };
		final int[] dfieldBlockSize = Import.csvStringToIntArray( dfieldBlockSizeString );
		if ( dfieldBlockSize.length != 3 )
			throw new IllegalArgumentException( "--dfieldBlockSize must have 3 values (got " + dfieldBlockSize.length + ")" );

		if ( !( dataGlobal.getSequenceDescription().getImgLoader() instanceof SplitViewerImgLoader ) )
			throw new IllegalStateException( "THIN_PLATE_SPLINE requires SplitViewerImgLoader." );
		final SplitViewerImgLoader splitImgLoader = ( SplitViewerImgLoader ) dataGlobal.getSequenceDescription().getImgLoader();
		final Map< Integer, java.util.List< Integer > > old2newSetupId =
				SplitImgLoaderThinPlateSplineFusion.old2newSetupId( splitImgLoader.new2oldSetupId() );
		final List< ViewId > underlyingViewIds =
				SplitImgLoaderThinPlateSplineFusion.underlyingViewIds( splitViewIds, splitImgLoader.new2oldSetupId() );
		final SequenceDescription underlyingSD = splitImgLoader.underlyingSequenceDescription();
		final Map< ViewId, ViewRegistration > splitRegMap = dataGlobal.getViewRegistrations().getViewRegistrations();

		// Pass 1 (driver): compute landmarks + bbox per view, check cache, pre-create N5 datasets.
		final List< DfieldBlockSpec > allSpecs = new ArrayList<>();
		int viewsToCompute = 0;
		final Compression compression = new GzipCompression( 1 );

		try ( final N5Reader r = URITools.instantiateN5Reader( storageTypeFinal, outPathURIFinal );
		      final N5Writer w = URITools.instantiateN5Writer( storageTypeFinal, outPathURIFinal ) )
		{
			for ( final ViewId uvid : underlyingViewIds )
			{
				final String dsPath = DisplacementFieldN5Tools.datasetPath( uvid );
				final Landmarks lm = SplitImgLoaderThinPlateSplineFusion.getCoefficients(
						splitImgLoader, old2newSetupId, splitRegMap, uvid, anisotropyFactor, Double.NaN );
				final ThinplateSplineTransform tps = new ThinplateSplineTransform( lm.getTargetPoints(), lm.getSourcePoints() );
				final Dimensions dims = underlyingSD.getViewDescriptions().get( uvid ).getViewSetup().getSize();
				final Interval bbox = BlkThinPlateSplineFusion.inverseTransformedBoundingBox( tps, dims );

				if ( reuseDfieldCache && DisplacementFieldN5Tools.datasetMatches( r, dsPath, bbox, dfieldSpacing ) )
				{
					System.out.println( "Phase 1.5: reusing cached dfield for " + Group.pvid( uvid ) + " (" + dsPath + ")" );
					continue;
				}

				// Pre-create the empty 4D dataset on the driver, including all attrs.
				if ( dfieldType == DfieldType.FLOAT32 )
					DisplacementFieldN5Tools.createEmptyDataset(
							w, dsPath, bbox, dfieldSpacing, dfieldBlockSize, new FloatType(), compression );
				else
					DisplacementFieldN5Tools.createEmptyDataset(
							w, dsPath, bbox, dfieldSpacing, dfieldBlockSize, new DoubleType(), compression );

				// Enumerate every block of this view's dfield grid into the flat spec list.
				final long[] numBlocks = DisplacementFieldN5Tools.gridBlockCount( bbox, dfieldSpacing, dfieldBlockSize );
				long viewBlockCount = 0;
				for ( long bz = 0; bz < numBlocks[ 2 ]; ++bz )
					for ( long by = 0; by < numBlocks[ 1 ]; ++by )
						for ( long bx = 0; bx < numBlocks[ 0 ]; ++bx )
						{
							allSpecs.add( new DfieldBlockSpec( uvid, new long[] { bx, by, bz }, bbox, lm, dsPath ) );
							++viewBlockCount;
						}

				System.out.println( "Phase 1.5: " + Group.pvid( uvid )
						+ " bbox=" + Util.printInterval( bbox )
						+ " gridBlocks=" + Arrays.toString( numBlocks )
						+ " (" + viewBlockCount + " tasks) -> " + dsPath );
				++viewsToCompute;
			}
		}

		if ( allSpecs.isEmpty() )
		{
			System.out.println( "Phase 1.5: all " + underlyingViewIds.size() + " underlying-view dfield(s) already cached, skipping." );
			return;
		}

		System.out.println( "Phase 1.5: dispatching " + allSpecs.size() + " block task(s) across "
				+ viewsToCompute + " of " + underlyingViewIds.size() + " underlying view(s)." );

		// Pass 2 (executors): rebuild TPS, sample one block, saveBlock.
		final double[] dfieldSpacingFinal = dfieldSpacing;
		final int[] dfieldBlockSizeFinal = dfieldBlockSize;
		final DfieldType dfieldTypeFinal = dfieldType;
		final long phase15Start = System.currentTimeMillis();

		sc.parallelize( allSpecs, allSpecs.size() ).foreach( spec ->
		{
			final ThinplateSplineTransform tps = new ThinplateSplineTransform( spec.targetPoints, spec.sourcePoints );
			final Interval bbox = new FinalInterval( spec.bboxMin, spec.bboxMax );
			try ( final N5Writer ww = URITools.instantiateN5Writer( storageTypeFinal, outPathURIFinal ) )
			{
				if ( dfieldTypeFinal == DfieldType.FLOAT32 )
					DisplacementFieldN5Tools.writeDisplacementFieldBlock(
							tps, bbox, dfieldSpacingFinal, dfieldBlockSizeFinal, spec.blockGridPos3d,
							ww, spec.datasetPath, new FloatType() );
				else
					DisplacementFieldN5Tools.writeDisplacementFieldBlock(
							tps, bbox, dfieldSpacingFinal, dfieldBlockSizeFinal, spec.blockGridPos3d,
							ww, spec.datasetPath, new DoubleType() );
			}
		} );

		System.out.println( "Phase 1.5 complete in " + ( System.currentTimeMillis() - phase15Start ) + " ms." );
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
