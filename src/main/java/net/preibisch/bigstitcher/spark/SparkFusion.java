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
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;

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
import net.imglib2.util.Cast;
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
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPoints;
import net.imglib2.realtransform.ThinplateSplineTransform;
import net.preibisch.mvrecon.process.fusion.blk.BlkAffineFusion;
import net.preibisch.mvrecon.process.fusion.blk.BlkThinPlateSplineFusion;
import net.preibisch.mvrecon.process.fusion.blk.DisplacementFieldN5Tools;
import net.preibisch.mvrecon.process.fusion.blk.SplitImgLoaderThinPlateSplineFusion;
import net.preibisch.mvrecon.process.fusion.blk.SplitImgLoaderThinPlateSplineFusion.LandmarkRecord;
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

	@Option(names = { "--reuseDeformationFields" }, description = "TPS only: if set, reuse any per-underlying-view dfield datasets that already exist in the output container (no parameter-match check is performed — the caller is asserting the cache is valid). Default: false (always recompute from scratch).")
	private boolean reuseDeformationFields = false;

	@Option(names = { "--dfieldBlockSize" }, description = "TPS only: N5 block size of the cached dfield datasets (default: 128,128,128).")
	private String dfieldBlockSizeString = "128,128,128";

	@Option(names = { "--tpsCorrespondenceLabel" }, description = "TPS only: interest-point label whose cross-split correspondences are used to add cross-view 'tie' landmarks to each underlying view's TPS. Typically the splitting-suffixed label, e.g. 'beads_split'. Default: null (no tie landmarks, behave as pure split-center TPS).")
	private String tpsCorrespondenceLabel = null;

	@Option(names = { "--tpsMinNumCorrespondences" }, description = "TPS only: minimum number of corresponding interest points required per overlapping split-pair before adding a tie landmark (default: 4). Only used when --tpsCorrespondenceLabel is set.")
	private int tpsMinNumCorrespondences = 4;

	@Option(names = { "--tpsAnchorOverlapCorners" }, description = "TPS only: when set, additionally place anchor 'nail' landmarks at the 8 zero-min corners of each split sub-view that overlaps another underlying view. Default: false. Only meaningful when --tpsCorrespondenceLabel is also set.")
	private boolean tpsAnchorOverlapCorners = false;

	@Option(names = { "--tpsCornerCoverageRadius" }, description = "TPS only: render-space distance threshold (in output pixels). A candidate corner is nailed iff its distance to the split's correspondence center-of-mass (in render coords) is greater than this value. 0 = nail every candidate corner. Default: 0.")
	private double tpsCornerCoverageRadius = 0.0;

	@Option(names = { "--tpsSeamSamplesPerAxis" }, description = "TPS only: samples per axis on each face of every split sub-view when --tpsAnchorOverlapCorners is set. 2 = corners only (8 candidate nails per split, current default). 3 = also edge midpoints + face centers (26 unique surface positions). Larger = denser surface coverage. Cost grows roughly O(N^3 - (N-2)^3). Default: 2. Per-split override via --tpsSeamSamplesSchedule.")
	private int tpsSeamSamplesPerAxis = 2;

	@Option(names = { "--tpsSeamSamplesSchedule" }, split = ",",
			description = "TPS only: adapt --tpsSeamSamplesPerAxis per split based on the total number of correspondences feeding that split. "
					+ "Format: a comma-separated list of 'threshold=value' entries, sorted ascending by threshold. "
					+ "For each split, the first entry whose threshold >= correspondence-count is used; if none match, --tpsSeamSamplesPerAxis is the fallback. "
					+ "Example: --tpsSeamSamplesSchedule 100=4,200=3 (with --tpsSeamSamplesPerAxis 2) means: "
					+ "<=100 -> 4 samples/axis, <=200 -> 3 samples/axis, otherwise 2. Default: null (use --tpsSeamSamplesPerAxis for every split).")
	private java.util.Map< Integer, Integer > tpsSeamSamplesSchedule = null;

	@Option(names = { "--tpsLandmarksOut" }, description = "TPS only: write all per-underlying-view landmarks (corrCOM, midpoints, nails) to a CSV file. Columns: view_setup_id, timepoint_id, type, source_x, source_y, source_z, target_x, target_y, target_z, donor_view_setup_id. Nail rows whose donor differs from view_setup_id are cross-view tie 'partner' nails. Target coords are in render/global space, ready to overlay on the fused image. Default: null (no output).")
	private String tpsLandmarksOut = null;

	// TODO: add support for loading coefficients during fusion
	@CommandLine.Option(names = { "--intensityN5Path" }, description = "N5/ZARR/HDF5 base path for loading coefficients (e.g. s3://myBucket/coefficients.n5)")
	private String intensityN5PathURIString = null;

	@CommandLine.Option(names = { "--intensityN5Storage" }, description = "output storage type of the intensity coefficients container, can be used to override guessed format (default: guess from n5Path file/directory-ending; NOTE: does not work for ZARR2, you need to specify since .zarr would default to ZARR v3)")
	private StorageFormat intensityN5StorageType = null;

	@CommandLine.Option(names = { "--intensityN5Group" }, description = "group under which coefficient datasets are stored (default: \"\")")
	private String intensityN5Group = "";

	@CommandLine.Option(names = { "--intensityN5Dataset" }, description = "dataset name for each coefficient dataset (default: \"intensity\"). The coefficients for view(s,t) are stored in dataset \"{--intensityN5Group}/setup{s}/timepoint{t}/{--intensityN5Dataset}\"")
	private String intensityN5Dataset = "intensity";

	@Option(names = { "--group" }, description = "Container group path")
	private String groupPath = "";

	private URI outPathURI = null;
	/**
	 * Prefetching now works with a Executors.newCachedThreadPool();
	 */
	//static final int N_PREFETCH_THREADS = 72;

	private URI intensityN5PathURI = null;

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
			System.out.println( "Found container '" + outPathURI + "'. -> " + r.getURI());
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

		if ( dataGlobal == null )
		{
			System.out.println( "Could not load XML from:" + xmlURI );
			return null;
		}

		if ( fusionMethod == FusionMethod.THIN_PLATE_SPLINE )
		{
			if ( fusionType != FusionType.CLOSEST_PIXEL_WINS && fusionType != FusionType.AVG_BLEND && fusionType != FusionType.MAX_INTENSITY )
			{
				System.out.println( "FusionMethod.THIN_PLATE_SPLINE: only FusionType.CLOSEST_PIXEL_WINS, FusionType.AVG_BLEND and FusionType.MAX_INTENSITY supported right now." );
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

		final TpsSeamSchedule tpsSchedule = buildSeamSamplesSchedule( tpsSeamSamplesSchedule, tpsSeamSamplesPerAxis );

		final TpsLandmarksSink landmarksSink = openLandmarksSink( tpsLandmarksOut, fusionMethod );

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

				// === Phase 1.5: materialize per-underlying-view dfields if needed (TPS only) ===
				if ( fusionMethod == FusionMethod.THIN_PLATE_SPLINE )
				{
					materializeDisplacementFields( sc, dataGlobal, viewIds, xmlURI, outPathURI, storageType, anisotropyFactor,
							tpsCorrespondenceLabel, tpsMinNumCorrespondences,
							tpsAnchorOverlapCorners, tpsCornerCoverageRadius,
							tpsSeamSamplesPerAxis, tpsSchedule.thresholds, tpsSchedule.values, landmarksSink.visitor );
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
								final Map< ViewId, AffineTransform3D > approxAffines = new HashMap<>();
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

										final AffineTransform3D a = DisplacementFieldN5Tools.readApproxAffine( r, dsPath );
										if ( a == null )
											throw new RuntimeException( "dfield dataset '" + dsPath
													+ "' is missing the 'approx_affine_row_major' attribute — Phase 1.5 should have written it." );
										approxAffines.put( uvid, a );
									}
								}

								@SuppressWarnings( { "unchecked", "rawtypes" } )
								final BlockSupplier rawSupplier = SplitImgLoaderThinPlateSplineFusion.initWithLoadedDfields(
										conv,
										splitImgLoaderLambda,
										overlappingViews,
										dataLocal.getSequenceDescription().getViewDescriptions(),
										viewBoundsLoaded,
										rawDfields,
										fusionType,
										anisotropyFactor,
										1, // linear interpolation
										null, // fusion order — only for FIRST_LOW / FIRST_HIGH
										coefficients,
										new BoundingBox( interval ),
										(RealType & NativeType) type,
										blockSize,
										approxAffines );

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

		landmarksSink.close();

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
		final long[] blockGridPos3d;
		final long[] bboxMin;
		final long[] bboxMax;
		final double[][] sourcePoints;
		final double[][] targetPoints;
		final String datasetPath;

		DfieldBlockSpec(
				final long[] blockGridPos3d,
				final Interval bbox,
				final Landmarks landmarks,
				final String datasetPath )
		{
			this.blockGridPos3d = blockGridPos3d;
			this.bboxMin = bbox.minAsLongArray();
			this.bboxMax = bbox.maxAsLongArray();
			this.sourcePoints = landmarks.getSourcePoints();
			this.targetPoints = landmarks.getTargetPoints();
			this.datasetPath = datasetPath;
		}
	}

	/**
	 * Serializable description of all per-underlying-view dfield-preamble work
	 * for Phase 1.5: getCoefficients (loads correspondence interest points) +
	 * TPS bbox/affine + N5 createEmptyDataset + writeApproxAffine + dfield
	 * grid enumeration. One {@code PerUTaskSpec} per uncached underlying view,
	 * parallelized across executors so the IP loading and N5 dataset registration
	 * happen in parallel rather than serially on the driver.
	 */
	private static final class PerUTaskSpec implements Serializable
	{
		private static final long serialVersionUID = 1L;
		final int[] uvidSerialized;
		final URI xmlURI;
		final URI outPathURI;
		final StorageFormat storageType;
		final double anisotropyFactor;
		final String correspondenceLabel;             // nullable
		final int minNumCorrespondences;
		final boolean anchorOverlapCorners;
		final double cornerCoverageRadius;
		final int seamSamplesPerAxis;
		final int[] seamSamplesScheduleThresholds;   // nullable
		final int[] seamSamplesScheduleValues;       // nullable
		final double[] dfieldSpacing;
		final int[] dfieldBlockSize;
		final DfieldType dfieldType;
		final List< SplitImgLoaderThinPlateSplineFusion.DonatedNail > donatedNails;

		PerUTaskSpec(
				final int[] uvidSerialized,
				final URI xmlURI, final URI outPathURI, final StorageFormat storageType,
				final double anisotropyFactor,
				final String correspondenceLabel, final int minNumCorrespondences,
				final boolean anchorOverlapCorners, final double cornerCoverageRadius,
				final int seamSamplesPerAxis,
				final int[] seamSamplesScheduleThresholds, final int[] seamSamplesScheduleValues,
				final double[] dfieldSpacing, final int[] dfieldBlockSize, final DfieldType dfieldType,
				final List< SplitImgLoaderThinPlateSplineFusion.DonatedNail > donatedNails )
		{
			this.uvidSerialized = uvidSerialized;
			this.xmlURI = xmlURI;
			this.outPathURI = outPathURI;
			this.storageType = storageType;
			this.anisotropyFactor = anisotropyFactor;
			this.correspondenceLabel = correspondenceLabel;
			this.minNumCorrespondences = minNumCorrespondences;
			this.anchorOverlapCorners = anchorOverlapCorners;
			this.cornerCoverageRadius = cornerCoverageRadius;
			this.seamSamplesPerAxis = seamSamplesPerAxis;
			this.seamSamplesScheduleThresholds = seamSamplesScheduleThresholds;
			this.seamSamplesScheduleValues = seamSamplesScheduleValues;
			this.dfieldSpacing = dfieldSpacing;
			this.dfieldBlockSize = dfieldBlockSize;
			this.dfieldType = dfieldType;
			this.donatedNails = donatedNails;
		}
	}

	/**
	 * Per-underlying-view task result: the dfield-block specs that go to the
	 * next Spark stage plus the {@code LandmarkRecord}s collected on the
	 * executor (for the optional CSV visitor, fanned out on the driver to
	 * keep the single BufferedWriter single-threaded).
	 */
	private static final class PerUTaskResult implements Serializable
	{
		private static final long serialVersionUID = 1L;
		final int[] uvidSerialized;
		final String dsPath;
		final long[] bboxMin;
		final long[] bboxMax;
		final long[] numBlocks;
		final List< DfieldBlockSpec > blockSpecs;
		final List< SplitImgLoaderThinPlateSplineFusion.LandmarkRecord > records;

		PerUTaskResult(
				final int[] uvidSerialized, final String dsPath,
				final long[] bboxMin, final long[] bboxMax, final long[] numBlocks,
				final List< DfieldBlockSpec > blockSpecs,
				final List< SplitImgLoaderThinPlateSplineFusion.LandmarkRecord > records )
		{
			this.uvidSerialized = uvidSerialized;
			this.dsPath = dsPath;
			this.bboxMin = bboxMin;
			this.bboxMax = bboxMax;
			this.numBlocks = numBlocks;
			this.blockSpecs = blockSpecs;
			this.records = records;
		}
	}

	/**
	 * Phase 1.5: materialize per-underlying-view displacement fields under the
	 * output container. Parallelized at the N5-chunk level: one Spark task per
	 * dfield block. Driver pre-creates the empty datasets so executors don't
	 * race on dataset creation. Always recomputes from scratch, unless the user
	 * passed {@code --reuseDeformationFields} in which case any existing dfield
	 * dataset at the per-view path is reused as-is (no parameter-match check).
	 */
	private void materializeDisplacementFields(
			final JavaSparkContext sc,
			final SpimData2 dataGlobal,
			final List< ViewId > splitViewIds,
			final URI xmlURIFinal,
			final URI outPathURIFinal,
			final StorageFormat storageTypeFinal,
			final double anisotropyFactor,
			final String correspondenceLabel,
			final int minNumCorrespondences,
			final boolean anchorOverlapCorners,
			final double cornerCoverageRadius,
			final int seamSamplesPerAxis,
			final int[] seamSamplesScheduleThresholds,
			final int[] seamSamplesScheduleValues,
			final Consumer< LandmarkRecord > landmarkVisitor )
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
		final Map< Integer, List< Integer > > old2newSetupId =
				SplitImgLoaderThinPlateSplineFusion.old2newSetupId( splitImgLoader.new2oldSetupId() );
		final List< ViewId > underlyingViewIds =
				SplitImgLoaderThinPlateSplineFusion.underlyingViewIds( splitViewIds, splitImgLoader.new2oldSetupId() );
		final SequenceDescription underlyingSD = splitImgLoader.underlyingSequenceDescription();
		final Map< ViewId, ViewRegistration > splitRegMap = dataGlobal.getViewRegistrations().getViewRegistrations();
		final ViewInterestPoints viewInterestPoints = ( correspondenceLabel != null ) ? dataGlobal.getViewInterestPoints() : null;

		// Cross-view nail donations are global: each (split S, partner S') overlap-corner emits
		// a pair of landmarks pointing at the same render-space target (one into U's TPS, one
		// into U''s TPS). Done once at the driver before per-view landmark assembly. The
		// landmarkVisitor receives one record per emitted nail (with donor=U for both copies).
		final Map< ViewId, List< SplitImgLoaderThinPlateSplineFusion.DonatedNail > > nailDonations;
		if ( anchorOverlapCorners )
		{
			nailDonations = SplitImgLoaderThinPlateSplineFusion.computeCrossViewNailDonations(
					splitImgLoader, old2newSetupId, splitRegMap, underlyingViewIds,
					anisotropyFactor, Double.NaN,
					viewInterestPoints, correspondenceLabel, minNumCorrespondences,
					cornerCoverageRadius, seamSamplesPerAxis,
					seamSamplesScheduleThresholds, seamSamplesScheduleValues, landmarkVisitor );
		}
		else
		{
			System.out.println( "[TPS] cross-view nail donations: --tpsAnchorOverlapCorners is OFF; "
					+ "no corner/surface nails will be added (corrCOM + correspondence midpoints only)." );
			nailDonations = Collections.emptyMap();
		}

		// Pass 1: parallelize per-underlying-view dfield-preamble work across Spark
		// executors. Each task does: getCoefficients (loads IPs for its U's splits) ->
		// TPS bbox/affine -> N5 createEmptyDataset -> writeApproxAffine -> enumerate
		// dfield grid blocks. Cache hits are filtered on the driver before dispatch so
		// no executor is launched for already-materialized views.
		final List< DfieldBlockSpec > allSpecs = new ArrayList<>();
		final List< PerUTaskSpec > perUSpecs = new ArrayList<>();

		try ( final N5Reader r = URITools.instantiateN5Reader( storageTypeFinal, outPathURIFinal ) )
		{
			for ( final ViewId uvid : underlyingViewIds )
			{
				final String dsPath = DisplacementFieldN5Tools.datasetPath( uvid );

				// Trust the cache: a complete dfield dataset includes the approx_affine_row_major
				// attribute written by a previous Phase 1.5, so there's no reason to recompute
				// landmarks (which would re-load all interest points + correspondences) just to
				// rewrite the same affine.
				if ( reuseDeformationFields && r.exists( dsPath ) )
				{
					System.out.println( "Phase 1.5: reusing cached dfield for " + Group.pvid( uvid ) + " (" + dsPath + ")" );
					continue;
				}

				perUSpecs.add( new PerUTaskSpec(
						Spark.serializeViewId( uvid ),
						xmlURIFinal, outPathURIFinal, storageTypeFinal,
						anisotropyFactor,
						correspondenceLabel, minNumCorrespondences,
						anchorOverlapCorners, cornerCoverageRadius, seamSamplesPerAxis,
						seamSamplesScheduleThresholds, seamSamplesScheduleValues,
						dfieldSpacing, dfieldBlockSize, dfieldType,
						nailDonations.get( uvid ) ) );
			}
		}

		final int viewsToCompute = perUSpecs.size();
		if ( !perUSpecs.isEmpty() )
		{
			System.out.println( "Phase 1.5 preamble: parallelizing per-U work across "
					+ Math.min( Spark.maxPartitions, perUSpecs.size() ) + " partition(s) for "
					+ perUSpecs.size() + " underlying view(s)." );

			final long preambleStart = System.currentTimeMillis();
			final List< PerUTaskResult > perUResults = sc.parallelize(
					perUSpecs, Math.min( Spark.maxPartitions, perUSpecs.size() ) ).map( spec ->
			{
				final ViewId uvid = Spark.deserializeViewId( spec.uvidSerialized );
				final SpimData2 data = Spark.getSparkJobSpimData2( spec.xmlURI );
				final SplitViewerImgLoader sil = ( SplitViewerImgLoader ) data.getSequenceDescription().getImgLoader();
				final SequenceDescription uSD = sil.underlyingSequenceDescription();
				final Map< Integer, List< Integer > > o2n =
						SplitImgLoaderThinPlateSplineFusion.old2newSetupId( sil.new2oldSetupId() );
				final Map< ViewId, ViewRegistration > regs = data.getViewRegistrations().getViewRegistrations();
				final ViewInterestPoints vip = ( spec.correspondenceLabel != null ) ? data.getViewInterestPoints() : null;

				// Collect LandmarkRecords locally; driver fans them out to the CSV
				// visitor after collect() so the single BufferedWriter stays
				// single-threaded.
				final List< SplitImgLoaderThinPlateSplineFusion.LandmarkRecord > records = new ArrayList<>();
				final Consumer< SplitImgLoaderThinPlateSplineFusion.LandmarkRecord > sink = records::add;

				final Landmarks lm = SplitImgLoaderThinPlateSplineFusion.getCoefficients(
						sil, o2n, regs, uvid, spec.anisotropyFactor, Double.NaN,
						vip, spec.correspondenceLabel, spec.minNumCorrespondences,
						spec.anchorOverlapCorners, spec.cornerCoverageRadius, spec.seamSamplesPerAxis,
						spec.seamSamplesScheduleThresholds, spec.seamSamplesScheduleValues, sink,
						spec.donatedNails );

				final ThinplateSplineTransform tps = new ThinplateSplineTransform( lm.getTargetPoints(), lm.getSourcePoints() );
				final Dimensions dims = uSD.getViewDescriptions().get( uvid ).getViewSetup().getSize();
				final Interval bbox = BlkThinPlateSplineFusion.inverseTransformedBoundingBox( tps, dims );
				final AffineTransform3D approxAffine = BlkThinPlateSplineFusion.fitAffineTransform(
						lm.getSourcePoints(), lm.getTargetPoints() );

				final String dsPath = DisplacementFieldN5Tools.datasetPath( uvid );
				try ( final N5Writer w = URITools.instantiateN5Writer( spec.storageType, spec.outPathURI ) )
				{
					// Wipe any stale dataset at this path so createEmptyDataset gets a clean slot.
					if ( w.exists( dsPath ) )
						w.remove( dsPath );

					final Compression compression = new GzipCompression( 1 );
					if ( spec.dfieldType == DfieldType.FLOAT32 )
						DisplacementFieldN5Tools.createEmptyDataset(
								w, dsPath, bbox, spec.dfieldSpacing, spec.dfieldBlockSize, new FloatType(), compression,
								spec.correspondenceLabel, spec.minNumCorrespondences,
								spec.anchorOverlapCorners, spec.cornerCoverageRadius, spec.seamSamplesPerAxis,
								spec.seamSamplesScheduleThresholds, spec.seamSamplesScheduleValues );
					else
						DisplacementFieldN5Tools.createEmptyDataset(
								w, dsPath, bbox, spec.dfieldSpacing, spec.dfieldBlockSize, new DoubleType(), compression,
								spec.correspondenceLabel, spec.minNumCorrespondences,
								spec.anchorOverlapCorners, spec.cornerCoverageRadius, spec.seamSamplesPerAxis,
								spec.seamSamplesScheduleThresholds, spec.seamSamplesScheduleValues );
					DisplacementFieldN5Tools.writeApproxAffine( w, dsPath, approxAffine );
				}

				final long[] numBlocks = DisplacementFieldN5Tools.gridBlockCount( bbox, spec.dfieldSpacing, spec.dfieldBlockSize );
				final List< DfieldBlockSpec > blockSpecs = new ArrayList<>();
				for ( long bz = 0; bz < numBlocks[ 2 ]; ++bz )
					for ( long by = 0; by < numBlocks[ 1 ]; ++by )
						for ( long bx = 0; bx < numBlocks[ 0 ]; ++bx )
							blockSpecs.add( new DfieldBlockSpec( new long[] { bx, by, bz }, bbox, lm, dsPath ) );

				return new PerUTaskResult(
						spec.uvidSerialized, dsPath,
						bbox.minAsLongArray(), bbox.maxAsLongArray(), numBlocks,
						blockSpecs, records );
			} ).collect();

			// Driver-side fan-out: visitor records + per-U Phase 1.5 log line + allSpecs aggregation.
			for ( final PerUTaskResult res : perUResults )
			{
				if ( landmarkVisitor != null )
					for ( final SplitImgLoaderThinPlateSplineFusion.LandmarkRecord rec : res.records )
						landmarkVisitor.accept( rec );

				final ViewId uvid = Spark.deserializeViewId( res.uvidSerialized );
				final Interval bbox = new FinalInterval( res.bboxMin, res.bboxMax );
				System.out.println( "Phase 1.5: " + Group.pvid( uvid )
						+ " bbox=" + Util.printInterval( bbox )
						+ " gridBlocks=" + Arrays.toString( res.numBlocks )
						+ " (" + res.blockSpecs.size() + " tasks) -> " + res.dsPath );
				allSpecs.addAll( res.blockSpecs );
			}

			System.out.println( "Phase 1.5 preamble complete in "
					+ ( System.currentTimeMillis() - preambleStart ) + " ms." );
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

	/**
	 * Sort the TPS seam-samples schedule by threshold ascending into parallel {@code int[]}
	 * arrays. Returns {@link TpsSeamSchedule#EMPTY} (both arrays null) when {@code schedule}
	 * is null or empty. The fallback value is only used for the diagnostic log line.
	 */
	private static TpsSeamSchedule buildSeamSamplesSchedule( final Map< Integer, Integer > schedule, final int fallbackSamplesPerAxis )
	{
		if ( schedule == null || schedule.isEmpty() )
			return TpsSeamSchedule.EMPTY;

		final List< Entry< Integer, Integer > > entries = new ArrayList<>( schedule.entrySet() );
		entries.sort( Comparator.comparingInt( Entry::getKey ) );

		final int[] thresholds = new int[ entries.size() ];
		final int[] values = new int[ entries.size() ];
		for ( int i = 0; i < entries.size(); ++i )
		{
			thresholds[ i ] = entries.get( i ).getKey();
			values[ i ] = Math.max( 2, entries.get( i ).getValue() );
		}

		System.out.println( "[--tpsSeamSamplesSchedule] thresholds=" + Arrays.toString( thresholds )
				+ " values=" + Arrays.toString( values )
				+ " fallback=" + fallbackSamplesPerAxis );

		return new TpsSeamSchedule( thresholds, values );
	}

	/**
	 * Holds the per-split TPS seam-samples schedule as parallel sorted-by-threshold arrays.
	 * The {@link #EMPTY} singleton (both fields null) means "no schedule — use the global
	 * {@code --tpsSeamSamplesPerAxis} fallback for every split".
	 */
	static final class TpsSeamSchedule
	{
		static final TpsSeamSchedule EMPTY = new TpsSeamSchedule( null, null );

		final int[] thresholds;     // sorted ascending; null when empty
		final int[] values;         // parallel to thresholds, each value clamped to >= 2; null when empty

		private TpsSeamSchedule( final int[] thresholds, final int[] values )
		{
			this.thresholds = thresholds;
			this.values = values;
		}
	}

	/**
	 * Open the optional TPS landmarks CSV file and return a {@link TpsLandmarksSink} carrying
	 * its writer + visitor. Returns {@link TpsLandmarksSink#NOOP} when {@code path} is null,
	 * when {@code fusionMethod} isn't TPS, or when the file cannot be opened (an error is
	 * printed to stderr and fusion continues without landmark export).
	 */
	private static TpsLandmarksSink openLandmarksSink( final String path, final FusionMethod fusionMethod )
	{
		if ( path == null || fusionMethod != FusionMethod.THIN_PLATE_SPLINE )
			return TpsLandmarksSink.NOOP;

		try
		{
			final BufferedWriter w = Files.newBufferedWriter( Paths.get( path ), StandardCharsets.UTF_8 );
			// view_setup_id is the RECIPIENT underlying view (whose TPS holds this landmark).
			// donor_view_setup_id is the underlying view whose split sub-view's surface produced
			// this corner; equals recipient for centers/midpoints + self-nail donations, and
			// differs for cross-view nail donations. Added in scheme V2 (symmetric nails).
			w.write( "view_setup_id,timepoint_id,type,source_x,source_y,source_z,target_x,target_y,target_z,donor_view_setup_id" );
			w.newLine();
			return new TpsLandmarksSink( w, path );
		}
		catch ( final IOException ex )
		{
			System.err.println( "[--tpsLandmarksOut] could not open '" + path + "': " + ex
					+ " — continuing without landmark export." );
			return TpsLandmarksSink.NOOP;
		}
	}

	/**
	 * Holds a (possibly-null) {@link BufferedWriter} and the {@link Consumer} that pushes
	 * {@link SplitImgLoaderThinPlateSplineFusion.LandmarkRecord}s into it as CSV rows.
	 *
	 * The {@link #NOOP} singleton represents "no landmark export"; its {@link #visitor}
	 * field is null and {@link #close()} does nothing.
	 */
	static final class TpsLandmarksSink
	{
		static final TpsLandmarksSink NOOP = new TpsLandmarksSink( null, null );

		final BufferedWriter writer;        // null = no export
		final String path;                  // non-null when writer != null, only used in close() messages
		final Consumer< SplitImgLoaderThinPlateSplineFusion.LandmarkRecord > visitor;

		private TpsLandmarksSink( final BufferedWriter writer, final String path )
		{
			this.writer = writer;
			this.path = path;
			this.visitor = ( writer == null ) ? null : rec -> {
				try
				{
					writer.write( String.format( "%d,%d,%s,%s,%s,%s,%s,%s,%s,%d%n",
							rec.underlyingViewId.getViewSetupId(),
							rec.underlyingViewId.getTimePointId(),
							rec.type,
							Double.toString( rec.source[ 0 ] ), Double.toString( rec.source[ 1 ] ), Double.toString( rec.source[ 2 ] ),
							Double.toString( rec.target[ 0 ] ), Double.toString( rec.target[ 1 ] ), Double.toString( rec.target[ 2 ] ),
							rec.donorViewId.getViewSetupId() ) );
				}
				catch ( final IOException ex )
				{
					throw new RuntimeException( "Failed to write landmark CSV row", ex );
				}
			};
		}

		void close()
		{
			if ( writer == null )
				return;
			try { writer.close(); }
			catch ( final IOException ex ) { System.err.println( "[--tpsLandmarksOut] error closing '" + path + "': " + ex ); }
			System.out.println( "[--tpsLandmarksOut] wrote landmarks to '" + path + "'." );
		}
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
