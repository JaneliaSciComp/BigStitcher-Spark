package net.preibisch.bigstitcher.spark;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;

import bdv.ViewerImgLoader;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewRegistrations;
import mpicbg.spim.data.registration.ViewTransform;
import mpicbg.spim.data.registration.ViewTransformAffine;
import mpicbg.spim.data.sequence.Illumination;
import mpicbg.spim.data.sequence.ImgLoader;
import mpicbg.spim.data.sequence.MissingViews;
import mpicbg.spim.data.sequence.MultiResolutionImgLoader;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.Tile;
import mpicbg.spim.data.sequence.TimePoint;
import mpicbg.spim.data.sequence.TimePoints;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.ViewSetup;
import net.imglib2.Dimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.Interval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Pair;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.legacy.io.IOFunctions;
import net.preibisch.mvrecon.fiji.plugin.Split_Views.InterestPointAdding;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.splitting.SplitImgLoader;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.splitting.SplitMultiResolutionImgLoader;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.splitting.SplitViewerImgLoader;
import net.preibisch.mvrecon.fiji.spimdata.intensityadjust.IntensityAdjustments;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsN5;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPointLists;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.pointspreadfunctions.PointSpreadFunctions;
import net.preibisch.mvrecon.fiji.spimdata.stitchingresults.StitchingResults;
import net.preibisch.mvrecon.process.splitting.ConsensusSetCriterion;
import net.preibisch.mvrecon.process.splitting.CrossViewCorrespondenceCriterion;
import net.preibisch.mvrecon.process.splitting.OctTreeSplitCriterion;
import net.preibisch.mvrecon.process.splitting.SplitDistributeEvenly;
import net.preibisch.mvrecon.process.splitting.SplitOctTree;
import net.preibisch.mvrecon.process.splitting.SplitResult;
import net.preibisch.mvrecon.process.splitting.SplitView;
import net.preibisch.mvrecon.process.splitting.SplittingTools;
import scala.Tuple2;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

public class SplitDatasets extends AbstractBasic
{
	private static final long serialVersionUID = -1983886010602093434L;

	// ==================== Split method ====================

	@Option(names = "--splitMethod", description = "splitting method: 'uniform' or 'octtree' (default: uniform)")
	private String splitMethod = "uniform";

	// ==================== Uniform splitting options ====================

	@Option(names = { "-tis", "--targetImageSize" }, description = "target image size after splitting e.g.: 512,512,256")
	private String targetImageSizeString = null;

	@Option(names = { "-to", "--targetOverlap" }, description = "target overlap after splitting e.g.: 32,32,32")
	private String targetOverlapString = null;

	@Option(names = { "--disableOptimization" }, description = "do not optimize image size and overlap")
	private boolean disableOptimization = false;

	// ==================== Oct-tree splitting options ====================

	@Option(names = "--criterion", description = "oct-tree criterion: 'crossview' or 'consensus' (default: crossview)")
	private String criterionType = "crossview";

	@Option(names = "--labels", description = "interest point labels for oct-tree (comma-separated, required for octtree)")
	private String labelsString = null;

	@Option(names = "--maxCorrespondences", description = "split threshold for oct-tree (default: 20)")
	private int maxCorrespondences = 20;

	@Option(names = "--tolerance", description = "tolerance mode for consensus criterion: NONE, PERCENTAGE, or COUNT (default: PERCENTAGE)")
	private ConsensusSetCriterion.ToleranceMode tolerance = ConsensusSetCriterion.ToleranceMode.PERCENTAGE;

	@Option(names = "--toleranceValue", description = "tolerance value: percentage (e.g. 10.0 for 10%%) or absolute count depending on --tolerance (default: 10.0)")
	private double toleranceValue = 10.0;

	@Option(names = "--minTileSize", description = "min tile size per dimension in voxels for oct-tree, e.g. 128,128,64. "
			+ "Each value must be divisible by the dataset's minStepSize per axis and >= 2 × minStepSize. "
			+ "Mutually exclusive with --minSizeMultiplier. Default (if neither given): 4 × minStepSize per axis. "
			+ "Run the plugin in BigStitcher to figure out minTileSize allowed ranges, which are defined by multiresolution pyramid.")
	private String minTileSizeString = null;

	@Option(names = "--minSizeMultiplier", description = "min tile size as a per-dimension multiplier of the dataset's minStepSize, e.g. 4,4,2. "
			+ "Each value must be >= 2. Mutually exclusive with --minTileSize. Default (if neither given): 4,4,4 (i.e. 4 × minStepSize per axis). "
			+ "Run the plugin in BigStitcher to figure out minStepSize, which is defined by multiresolution pyramid.")
	private String minSizeMultiplierString = null;

	@Option(names = "--minSplitLevels", description = "force minimum split levels for oct-tree (default: 0, use 1 for TPS-compatible)")
	private int minSplitLevels = 0;

	// ==================== Fake interest points options ====================

	@Option(names = { "-fip", "--fakeInterestPoints" }, description = "add fake (corresponding) interest points to overlapping regions of split images/views, NONE for none, IP for self-matching (deprecated), CORR adds points+correspondences (Default: CORR)")
	private InterestPointAdding fakeInterestPoints = InterestPointAdding.CORR;

	@Option(names = { "--fipDensity" }, description = "density of fake interest points; number of points per 100x100x100 px volume (default: 100.0)")
	private double fipDensity = 100.0;

	@Option(names = { "--fipMinNumPoints" }, description = "minimal number of fake interest points per overlap (default: 20)")
	private int fipMinNumPoints = 20;

	@Option(names = { "--fipMaxNumPoints" }, description = "maximal number of fake interest points per overlap (default: 500)")
	private int fipMaxNumPoints = 500;

	@Option(names = { "--fipError" }, description = "artificial error for fake corresponding interest points (default: 0.5)")
	private double fipError = 0.5;

	@Option(names = { "--fipExclusionRadius" }, description = "exclusion radius for fake interest points; to not put them close to existing points, only for --fakeInterestPoints IP (default: 20)")
	private double fipExclusionRadius = 20.0;

	@Option(names = { "--fakeLabel" }, description = "label for fake interest points (default: auto-generated)")
	private String fakeLabel = null;

	// ==================== Output / display options ====================

	@Option(names = { "-xo", "--xmlout" }, description = "path to the output BigStitcher xml, e.g. /home/project-n5.xml or s3://myBucket/dataset.xml (default: overwrite input)")
	private String xmlOutURIString = null;

	@Option(names = { "--assignIlluminations" }, description = "assign old tile id's as illumination id's, this can be great for visualization")
	private boolean assignIlluminations = false;


	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		// ==================== 1. Setup on driver ====================
		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			throw new IllegalArgumentException( "Couldn't load SpimData XML project." );

		final URI xmlOutURI = xmlOutURIString != null ? URITools.toURI( xmlOutURIString ) : xmlURI;
		System.out.println( "xmlout: " + xmlOutURI );

		// Compute min step size from multi-resolution pyramid
		final long[] minStepSize = SplittingTools.findMinStepSize( dataGlobal );

		// Print current image sizes
		final Pair< HashMap< String, Integer >, long[] > imgSizes = SplittingTools.collectImageSizes( dataGlobal );
		System.out.println( "Current image sizes of dataset:" );
		for ( final String size : imgSizes.getA().keySet() )
			IOFunctions.println( imgSizes.getA().get( size ) + "x: " + size );

		// Create splitter on driver (for maxIntervalSpread and description)
		final SplitView splitting = createSplitView( xmlURI, minStepSize, splitMethod,
				targetImageSizeString, targetOverlapString, !disableOptimization,
				labelsString, criterionType, maxCorrespondences, tolerance, toleranceValue,
				minTileSizeString, minSizeMultiplierString, minSplitLevels );
		if ( splitting == null )
			return null;

		// Prepare setup list and parameters
		final List< ViewSetup > oldSetups = new ArrayList<>( dataGlobal.getSequenceDescription().getViewSetups().values() );
		Collections.sort( oldSetups );

		final int maxIntervalSpread = splitting.maxIntervalSpread( oldSetups );
		final String fLabel = fakeLabel != null ? fakeLabel : SplittingTools.defaultFakeLabel();
		final long baseSeed = 23424459;
		final String splittingDescription = splitting.description();

		// check that there is only one illumination if assignIlluminations
		if ( assignIlluminations )
			if ( dataGlobal.getSequenceDescription().getAllIlluminationsOrdered().size() > 1 )
				throw new IllegalArgumentException( "Cannot assign illuminations from tile ids because more than one Illumination exists." );

		// Save the underlying img loader before setting it to null
		final ImgLoader underlyingImgLoader = dataGlobal.getSequenceDescription().getImgLoader();
		dataGlobal.getSequenceDescription().setImgLoader( null ); // not needed during processing

		// ==================== Create Spark context ====================
		final SparkConf conf = new SparkConf().setAppName( "SplitDatasets" );
		if ( localSparkBindAddress )
		{
			conf.set( "spark.driver.bindAddress", "127.0.0.1" );
			conf.set( "spark.driver.host", "localhost" );
			org.apache.spark.util.Utils.setCustomHostname( "localhost" );
		}

		final JavaSparkContext sc = new JavaSparkContext( conf );
		sc.setLogLevel( "ERROR" );

		try
		{
			// ==================== 2. Phase 1: Splitting (via Spark) ====================
			IOFunctions.println( "(" + new Date( System.currentTimeMillis() ) + "): Phase 1 - Computing split intervals..." );
	
			final Map< Integer, ArrayList< Interval > > splitResults = new HashMap<>();
	
			// Capture config for executor-side SplitView creation
			final URI finalXmlURI = xmlURI;
			final long[] finalMinStepSize = minStepSize.clone();
			final String finalSplitMethod = splitMethod;
			final String finalCriterionType = criterionType;
			final String finalLabelsString = labelsString;
			final int finalMaxCorrespondences = maxCorrespondences;
			final ConsensusSetCriterion.ToleranceMode finalTolerance = tolerance;
			final double finalToleranceValue = toleranceValue;
			final String finalMinTileSizeString = minTileSizeString;
			final String finalMinSizeMultiplierString = minSizeMultiplierString;
			final int finalMinSplitLevels = minSplitLevels;
			final String finalTargetImageSizeString = targetImageSizeString;
			final String finalTargetOverlapString = targetOverlapString;
			final boolean finalOptimize = !disableOptimization;
	
			// Build jobs: [setupId, timepointId] — pick the first present timepoint per setup
			final ArrayList< int[] > phase1Jobs = new ArrayList<>();
			for ( final ViewSetup oldSetup : oldSetups )
			{
				final ViewId firstPresent = SplittingTools.findFirstPresentViewId( dataGlobal, oldSetup.getId() );
				phase1Jobs.add( new int[]{ firstPresent.getViewSetupId(), firstPresent.getTimePointId() } );
			}
	
			final JavaRDD< int[] > phase1RDD = sc.parallelize(
					phase1Jobs, Math.min( Spark.maxPartitions, phase1Jobs.size() ) );
	
			final JavaRDD< Tuple2< Integer, SplitResult > > phase1Results = phase1RDD.map( job ->
			{
				final int setupId = job[ 0 ];
				final int tpId = job[ 1 ];
	
				final SplitView localSplitting = createSplitView( finalXmlURI, finalMinStepSize, finalSplitMethod,
						finalTargetImageSizeString, finalTargetOverlapString, finalOptimize,
						finalLabelsString, finalCriterionType, finalMaxCorrespondences, finalTolerance, finalToleranceValue,
						finalMinTileSizeString, finalMinSizeMultiplierString, finalMinSplitLevels );
	
				final SplitResult result = localSplitting.split( new ViewId( tpId, setupId ) );
	
				if ( result == null )
					throw new RuntimeException( "Splitting failed for ViewSetup " + setupId );
	
				System.out.println( "ViewId " + setupId + ": " + result.numIntervals + " tiles" );
				return new Tuple2<>( setupId, result );
			});
	
			// Collect results on driver
			for ( final Tuple2< Integer, SplitResult > entry : phase1Results.collect() )
			{
				IOFunctions.println( "ViewId " + entry._1() + ": " + entry._2().numIntervals + " tiles" );
				splitResults.put( entry._1(), entry._2().getIntervals() );
			}
	
			IOFunctions.println( "(" + new Date( System.currentTimeMillis() ) + "): Phase 1 complete. " + splitResults.size() + " setups split." );
	
			// ==================== 3. Pre-compute ID ranges on driver ====================
			final Map< Integer, Integer > setupIdStart = new LinkedHashMap<>();
			int cumulativeId = 0;
			for ( final ViewSetup setup : oldSetups )
			{
				setupIdStart.put( setup.getId(), cumulativeId );
				cumulativeId += splitResults.get( setup.getId() ).size();
			}
	
			IOFunctions.println( "Total new setups: " + cumulativeId );
	
			// ==================== 4. Phase 2: Post-processing via Spark ====================
			IOFunctions.println( "(" + new Date( System.currentTimeMillis() ) + "): Phase 2 - Processing interest points via Spark..." );

			// Serialize job list for Phase 2: one job per setup
			// Each job is: [oldSetupId, idRangeStart]
			final ArrayList< int[] > phase2Jobs = new ArrayList<>();
			for ( final ViewSetup oldSetup : oldSetups )
				phase2Jobs.add( new int[]{ oldSetup.getId(), setupIdStart.get( oldSetup.getId() ) } );

			System.out.println( "Num jobs: " + phase2Jobs.size() );

			// Serialize intervals for broadcast
			final Map< Integer, long[][][] > serializedIntervals = new HashMap<>();
			for ( final Map.Entry< Integer, ArrayList< Interval > > entry : splitResults.entrySet() )
			{
				final ArrayList< Interval > intervals = entry.getValue();
				final long[][][] ser = new long[ intervals.size() ][][];
				for ( int i = 0; i < intervals.size(); i++ )
					ser[ i ] = Spark.serializeInterval( intervals.get( i ) );
				serializedIntervals.put( entry.getKey(), ser );
			}

			// Capture final variables for lambda serialization
			final int finalMaxIntervalSpread = maxIntervalSpread;
			final boolean finalAssignIlluminations = assignIlluminations;
			final InterestPointAdding finalIpAdding = fakeInterestPoints;
			final String finalFakeLabel = fLabel;
			final double finalFipDensity = fipDensity;
			final int finalFipMinNumPoints = fipMinNumPoints;
			final int finalFipMaxNumPoints = fipMaxNumPoints;
			final double finalFipError = fipError;
			final double finalFipExclusionRadius = fipExclusionRadius;
			final String finalSplittingDescription = splittingDescription;

			// Broadcast the serialized intervals (shared across all executors)
			final Map< Integer, long[][][] > broadcastIntervals = serializedIntervals;

			final JavaRDD< int[] > phase2RDD = sc.parallelize(
					phase2Jobs, Math.min( Spark.maxPartitions, phase2Jobs.size() ) );

			final JavaRDD< int[] > phase2Results = phase2RDD.map( job ->
			{
				final int oldSetupId = job[ 0 ];
				final int idRangeStart = job[ 1 ];

				// Load SpimData2 on executor
				final SpimData2 data = Spark.getSparkJobSpimData2( finalXmlURI );
				final ViewSetup oldSetup = data.getSequenceDescription().getViewSetups().get( oldSetupId );

				// Deserialize intervals
				final long[][][] serIntervals = broadcastIntervals.get( oldSetupId );
				final ArrayList< Interval > intervals = new ArrayList<>( serIntervals.length );
				for ( final long[][] si : serIntervals )
					intervals.add( Spark.deserializeInterval( si ) );

				// Create saver that writes interest points on-the-fly.
				// Open one N5Writer on ${baseDir}/interestpoints.n5 per task and reuse it for every save,
				// so we don't pay an open/close cycle per InterestPoints object.
				final URI containerUri = URITools.toURI(
						URITools.appendName( data.getBasePathURI(), InterestPointsN5.baseN5 ) );
				final SplittingTools.InterestPointSaver saver = vipl -> {
					try ( final N5Writer n5Writer = URITools.instantiateN5Writer( StorageFormat.N5, containerUri ) )
					{
						for ( final ViewInterestPointLists v : vipl.values() )
							for ( final InterestPoints ips : v.getHashMap().values() )
							{
								final InterestPointsN5 n5ips = ( InterestPointsN5 ) ips;
								n5ips.saveInterestPoints( false, n5Writer );
								n5ips.saveCorrespondingInterestPoints( false, n5Writer );
							}
					}
					catch ( final Exception e )
					{
						IOFunctions.printErr( "ERROR saving interest points for setup " + oldSetupId + ": " + e );
						e.printStackTrace();
						throw new RuntimeException( "Failed to save interest points for setup " + oldSetupId, e );
					}
				};

				// Call processSetupStatic - does all the IP work - for all timepoints of that setup
				final SplittingTools.SetupSplitResult result = SplittingTools.processSetupStatic(
						oldSetup,
						intervals,
						idRangeStart,
						finalMaxIntervalSpread,
						data.getSequenceDescription().getTimePoints(),
						data.getViewRegistrations(),
						data,
						finalAssignIlluminations,
						finalIpAdding,
						finalFakeLabel,
						finalFipDensity,
						finalFipMinNumPoints,
						finalFipMaxNumPoints,
						finalFipError,
						finalFipExclusionRadius,
						baseSeed + oldSetupId,
						finalSplittingDescription,
						saver );

				System.out.println( "Phase 2 complete for setup " + oldSetupId + ": " + result.newSetupIds.size() + " new setups" );

				// Return just the old setup ID to confirm completion
				return new int[]{ oldSetupId };
			});

			// Trigger execution and collect
			final List< int[] > phase2Completed = phase2Results.collect();
			IOFunctions.println( "(" + new Date( System.currentTimeMillis() ) + "): Phase 2 complete. " + phase2Completed.size() + " setups processed." );

			// ==================== 5. Reconstruct metadata on driver ====================
			IOFunctions.println( "Reconstructing metadata on driver..." );

			final HashMap< Integer, Integer > new2oldSetupId = new HashMap<>();
			final HashMap< Integer, Interval > newSetupId2Interval = new HashMap<>();
			final Map< Integer, ArrayList< Integer > > old2NewSetups = new HashMap<>();
			final ArrayList< ViewSetup > newSetups = new ArrayList<>();
			final Map< ViewId, ViewRegistration > newRegistrations = new HashMap<>();
			final TimePoints timepoints = dataGlobal.getSequenceDescription().getTimePoints();

			for ( final ViewSetup oldSetup : oldSetups )
			{
				final ArrayList< Interval > intervals = splitResults.get( oldSetup.getId() );
				final int idRangeStart = setupIdStart.get( oldSetup.getId() );
				final ArrayList< Integer > newSetupIds = new ArrayList<>();

				final Tile oldTile = oldSetup.getTile();
				int localNewTileId = 0;

				for ( int i = 0; i < intervals.size(); i++ )
				{
					final Interval interval = intervals.get( i );
					final int newId = idRangeStart + i;

					new2oldSetupId.put( newId, oldSetup.getId() );
					newSetupId2Interval.put( newId, interval );
					newSetupIds.add( newId );

					// Create ViewSetup
					final long[] size = new long[ interval.numDimensions() ];
					interval.dimensions( size );
					final Dimensions newDim = new FinalDimensions( size );

					final double[] location = oldTile.getLocation() == null ?
							new double[ interval.numDimensions() ] : oldTile.getLocation().clone();
					for ( int d = 0; d < interval.numDimensions(); d++ )
						location[ d ] += interval.min( d );

					final int newTileId = oldTile.getId() * maxIntervalSpread + localNewTileId++;
					final Tile newTile = new Tile( newTileId, Integer.toString( newTileId ), location );
					final Illumination newIllum = assignIlluminations ?
							new Illumination( oldTile.getId(), "old_tile_" + oldTile.getId() ) :
							oldSetup.getIllumination();
					final ViewSetup newSetup = new ViewSetup( newId, null, newDim, oldSetup.getVoxelSize(),
							newTile, oldSetup.getChannel(), oldSetup.getAngle(), newIllum );
					newSetups.add( newSetup );

					// Create registrations for all timepoints
					for ( final TimePoint t : timepoints.getTimePointsOrdered() )
					{
						final ViewId oldViewId = new ViewId( t.getId(), oldSetup.getId() );
						final ViewRegistration oldVR = dataGlobal.getViewRegistrations().getViewRegistration( oldViewId );
						final ArrayList< ViewTransform > transformList = new ArrayList<>( oldVR.getTransformList() );

						final AffineTransform3D translation = new AffineTransform3D();
						translation.set(
								1.0f, 0.0f, 0.0f, interval.min( 0 ),
								0.0f, 1.0f, 0.0f, interval.min( 1 ),
								0.0f, 0.0f, 1.0f, interval.min( 2 ) );

						final ViewTransformAffine transform = new ViewTransformAffine( SplittingTools.IMAGE_SPLITTING_NAME, translation );
						transformList.add( transform );

						final ViewId newViewId = new ViewId( t.getId(), newSetup.getId() );
						final ViewRegistration newVR = new ViewRegistration( newViewId.getTimePointId(), newViewId.getViewSetupId(), transformList );
						newRegistrations.put( newViewId, newVR );
					}
				}

				old2NewSetups.put( oldSetup.getId(), newSetupIds );
			}

			newSetups.sort( ( a, b ) -> Integer.compare( a.getId(), b.getId() ) );
			IOFunctions.println( "Metadata reconstructed: " + newSetups.size() + " new setups" );

			// ==================== 6. Phase 3: Correspondences via Spark ====================
			IOFunctions.println( "(" + new Date( System.currentTimeMillis() ) + "): Phase 3 - Processing correspondences via Spark..." );

			// Collect old label names for reconstructing InterestPointsN5 objects
			final Set< String > oldLabelNames = new HashSet<>();
			for ( final ViewSetup oldSetup : oldSetups )
			{
				for ( final TimePoint t : timepoints.getTimePointsOrdered() )
				{
					final ViewId oldViewId = new ViewId( t.getId(), oldSetup.getId() );
					final ViewInterestPointLists oldVipl = dataGlobal.getViewInterestPoints().getViewInterestPointLists( oldViewId );
					if ( oldVipl != null )
						oldLabelNames.addAll( oldVipl.getHashMap().keySet() );
				}
			}

			IOFunctions.println( "Interest point labels found: " + oldLabelNames );

			// Build task list: (oldSetupId, timepointId) — one Spark task per (oldSetup, tp),
			// iterating newSetupIds sequentially inside the task to amortize per-task setup cost
			// (SpimData load, newInterestpoints map reconstruction) across all new setups of one old.
			final ArrayList< int[] > phase3Jobs = new ArrayList<>();
			for ( final ViewSetup oldSetup : oldSetups )
				for ( final TimePoint t : timepoints.getTimePointsOrdered() )
					phase3Jobs.add( new int[]{ oldSetup.getId(), t.getId() } );

			IOFunctions.println( "Phase 3 tasks: " + phase3Jobs.size() );

			// Serialize old2NewSetups for broadcast
			final Map< Integer, int[] > serializedOld2New = new HashMap<>();
			for ( final Map.Entry< Integer, ArrayList< Integer > > entry : old2NewSetups.entrySet() )
				serializedOld2New.put( entry.getKey(), entry.getValue().stream().mapToInt( Integer::intValue ).toArray() );

			final String[] oldLabelNamesArray = oldLabelNames.toArray( new String[ 0 ] );
			final InterestPointAdding finalIpAdding3 = fakeInterestPoints;
			final String finalFakeLabel3 = fLabel;

			final JavaRDD< int[] > phase3RDD = sc.parallelize(
					phase3Jobs, Math.min( Spark.maxPartitions, phase3Jobs.size() ) );

			phase3RDD.foreach( job ->
			{
				final int oldSetupId = job[ 0 ];
				final int timepointId = job[ 1 ];

				// Load SpimData2 on executor
				final SpimData2 data = Spark.getSparkJobSpimData2( finalXmlURI );
				final ViewSetup oldSetup = data.getSequenceDescription().getViewSetups().get( oldSetupId );

				// Find the specific TimePoint object
				TimePoint timepoint = null;
				for ( final TimePoint tp : data.getSequenceDescription().getTimePoints().getTimePointsOrdered() )
					if ( tp.getId() == timepointId )
					{
						timepoint = tp;
						break;
					}

				if ( timepoint == null )
					throw new RuntimeException( "TimePoint " + timepointId + " not found" );

				// Deserialize old2NewSetups
				final Map< Integer, List< Integer > > localOld2New = new HashMap<>();
				for ( final Map.Entry< Integer, int[] > entry : serializedOld2New.entrySet() )
				{
					final List< Integer > ids = new ArrayList<>();
					for ( final int id : entry.getValue() )
						ids.add( id );
					localOld2New.put( entry.getKey(), ids );
				}

				// Reconstruct newInterestpoints from disk (lazy-loading InterestPointsN5 objects)
				final URI baseDir = data.getBasePathURI();
				final Map< ViewId, ViewInterestPointLists > newInterestpoints = new HashMap<>();

				for ( final Map.Entry< Integer, List< Integer > > entry : localOld2New.entrySet() )
				{
					for ( final int nSetupId : entry.getValue() )
					{
						for ( final TimePoint tp : data.getSequenceDescription().getTimePoints().getTimePointsOrdered() )
						{
							final ViewId viewId = new ViewId( tp.getId(), nSetupId );
							final ViewInterestPointLists vipl = new ViewInterestPointLists( tp.getId(), nSetupId );

							for ( final String label : oldLabelNamesArray )
							{
								final String newLabel = label + "_split";
								vipl.addInterestPointList( newLabel, InterestPoints.newInstance( baseDir, viewId, newLabel ) );
							}

							if ( finalIpAdding3 != InterestPointAdding.NONE )
								vipl.addInterestPointList( finalFakeLabel3, InterestPoints.newInstance( baseDir, viewId, finalFakeLabel3 ) );

							newInterestpoints.put( viewId, vipl );
						}
					}
				}

				// Open one N5Writer on ${baseDir}/interestpoints.n5 per task and reuse it for every
				// saveCorrespondingInterestPoints call across all newSetups in this (oldSetup, tp).
				final URI corrContainerUri = URITools.toURI(
						URITools.appendName( data.getBasePathURI(), InterestPointsN5.baseN5 ) );
				try ( final N5Writer corrN5Writer = URITools.instantiateN5Writer( StorageFormat.N5, corrContainerUri ) )
				{
					final SplittingTools.CorrespondenceSaver corrSaver = vipl -> {
						for ( final InterestPoints ips : vipl.getHashMap().values() )
						{
							final InterestPointsN5 n5ips = ( InterestPointsN5 ) ips;
							n5ips.saveCorrespondingInterestPoints( false, corrN5Writer );
						}
					};

					// Share one IP-map cache across all newSetup iterations in this task so we don't
					// reload the same target-view IP copies repeatedly. Single-threaded scope → plain HashMap is fine.
					final Map< String, Map< Integer, InterestPoint > > ipMapCache = new HashMap<>();

					// Process correspondences for every newSetup belonging to this (oldSetup, timepoint)
					for ( final int newSetupId : localOld2New.get( oldSetupId ) )
					{
						SplittingTools.processCorrespondingInterestPointsStatic(
								oldSetup, newSetupId, timepoint,
								data, localOld2New, newInterestpoints,
								null, 0,
								corrSaver,
								ipMapCache );
					}
				}
				catch ( final Exception e )
				{
					IOFunctions.printErr( "ERROR saving correspondences for setup " + oldSetupId + " tp " + timepointId + ": " + e );
					e.printStackTrace();
					throw new RuntimeException( "Failed to save correspondences for setup " + oldSetupId + " tp " + timepointId, e );
				}
			});

			IOFunctions.println( "(" + new Date( System.currentTimeMillis() ) + "): Phase 3 complete." );

			// ==================== 7. Assemble and save on driver ====================
			IOFunctions.println( "Assembling final SpimData2..." );

			// Missing views
			final MissingViews oldMissingViews = dataGlobal.getSequenceDescription().getMissingViews();
			final HashSet< ViewId > missingViews = new HashSet<>();

			if ( oldMissingViews != null && oldMissingViews.getMissingViews() != null )
				for ( final ViewId id : oldMissingViews.getMissingViews() )
					for ( final int newSetupId : new2oldSetupId.keySet() )
						if ( new2oldSetupId.get( newSetupId ) == id.getViewSetupId() )
							missingViews.add( new ViewId( id.getTimePointId(), newSetupId ) );

			// Reconstruct interest points on driver (lazy-loading from disk).
			// We must set parameters on each new InterestPoints — the XML writer serializes that field,
			// and a null value blows up in XmlIoViewInterestPoints.viewInterestPointsToXml.
			// Matches the in-process path in SplittingTools.processSetupStatic (copy-from-old for real
			// labels, composed description for the fake label).
			final Map< ViewId, ViewInterestPointLists > driverNewInterestpoints = new HashMap<>();
			final URI baseDir = dataGlobal.getBasePathURI();

			final String fakeParameters =
					( fakeInterestPoints == InterestPointAdding.CORR ? "Fake corresponding points " : "Fake points " ) +
					"for image splitting: " + splittingDescription +
					", pointDensity=" + fipDensity +
					", minPoints=" + fipMinNumPoints +
					", maxPoints=" + fipMaxNumPoints +
					", error=" + fipError +
					( fakeInterestPoints == InterestPointAdding.CORR ? "" : ", excludeRadius=" + fipExclusionRadius );

			for ( final Map.Entry< Integer, ArrayList< Integer > > entry : old2NewSetups.entrySet() )
			{
				for ( final int newSetupId : entry.getValue() )
				{
					final int oldSetupIdForNew = new2oldSetupId.get( newSetupId );
					for ( final TimePoint t : timepoints.getTimePointsOrdered() )
					{
						final ViewId viewId = new ViewId( t.getId(), newSetupId );
						final ViewInterestPointLists vipl = new ViewInterestPointLists( t.getId(), newSetupId );

						final ViewId oldViewId = new ViewId( t.getId(), oldSetupIdForNew );
						final ViewInterestPointLists oldVipl = dataGlobal.getViewInterestPoints().getViewInterestPointLists( oldViewId );

						for ( final String label : oldLabelNames )
						{
							// Only create an entry if the source old view actually had this label
							// (avoids registering a "<label>_split" that points to a non-existent N5 dataset).
							if ( oldVipl == null || !oldVipl.contains( label ) )
								continue;

							final String newLabel = label + "_split";
							final InterestPoints newIps = InterestPoints.newInstance( baseDir, viewId, newLabel );
							final String oldParams = oldVipl.getInterestPointList( label ).getParameters();
							newIps.setParameters( oldParams != null ? oldParams : "" );
							vipl.addInterestPointList( newLabel, newIps );
						}

						if ( fakeInterestPoints != InterestPointAdding.NONE )
						{
							final InterestPoints fakeIps = InterestPoints.newInstance( baseDir, viewId, fLabel );
							fakeIps.setParameters( fakeParameters );
							vipl.addInterestPointList( fLabel, fakeIps );
						}

						driverNewInterestpoints.put( viewId, vipl );
					}
				}
			}

			// Create SequenceDescription
			final SequenceDescription sequenceDescription = new SequenceDescription(
					timepoints, newSetups, null, new MissingViews( missingViews ) );

			// Create SplitImgLoader
			final ImgLoader imgLoader;
			if ( underlyingImgLoader instanceof ViewerImgLoader )
			{
				imgLoader = new SplitViewerImgLoader( (ViewerImgLoader) underlyingImgLoader, new2oldSetupId, newSetupId2Interval, dataGlobal.getSequenceDescription() );
			}
			else if ( underlyingImgLoader instanceof MultiResolutionImgLoader )
			{
				imgLoader = new SplitMultiResolutionImgLoader( (MultiResolutionImgLoader) underlyingImgLoader, new2oldSetupId, newSetupId2Interval, dataGlobal.getSequenceDescription() );
			}
			else
			{
				imgLoader = new SplitImgLoader( underlyingImgLoader, new2oldSetupId, newSetupId2Interval, dataGlobal.getSequenceDescription() );
			}

			sequenceDescription.setImgLoader( imgLoader );

			// Create final SpimData2
			final ViewInterestPoints viewInterestPoints = new ViewInterestPoints( driverNewInterestpoints );
			final ViewRegistrations viewRegistrations = new ViewRegistrations( newRegistrations );
			final PointSpreadFunctions psfs = new PointSpreadFunctions( new HashMap<>() );

			final SpimData2 spimDataNew = new SpimData2(
					dataGlobal.getBasePathURI(),
					sequenceDescription,
					viewRegistrations,
					viewInterestPoints,
					dataGlobal.getBoundingBoxes(),
					psfs,
					new StitchingResults(),
					new IntensityAdjustments() );

			IOFunctions.println( "(" + new Date( System.currentTimeMillis() ) + "): Assembly complete." );

			if ( !dryRun )
			{
				new XmlIoSpimData2().save( spimDataNew, xmlOutURI );
				IOFunctions.println( "Saved: " + xmlOutURI );
			}
		}
		finally
		{
			sc.close();
		}

		return null;
	}

	/**
	 * Create a SplitView from a URI and configuration parameters.
	 * Can be called on both the driver and Spark executors.
	 */
	public static SplitView createSplitView(
			final URI xmlURI,
			final long[] minStepSize,
			final String splitMethod,
			// uniform params
			final String targetImageSizeString,
			final String targetOverlapString,
			final boolean optimize,
			// oct-tree params
			final String labelsString,
			final String criterionType,
			final int maxCorrespondences,
			final ConsensusSetCriterion.ToleranceMode tolerance,
			final double toleranceValue,
			final String minTileSizeString,
			final String minSizeMultiplierString,
			final int minSplitLevels ) throws SpimDataException
	{
		final SpimData2 data = Spark.getSparkJobSpimData2( xmlURI );

		if ( "octtree".equalsIgnoreCase( splitMethod ) )
		{
			if ( labelsString == null )
			{
				IOFunctions.printErr( "ERROR: --labels is required for oct-tree splitting." );
				return null;
			}

			final Set< String > trimmedLabels = new HashSet<>();
			for ( final String l : labelsString.split( "," ) )
				trimmedLabels.add( l.trim() );

			final OctTreeSplitCriterion criterion;
			if ( "consensus".equalsIgnoreCase( criterionType ) )
				criterion = new ConsensusSetCriterion( data, trimmedLabels, maxCorrespondences, tolerance, toleranceValue );
			else
				criterion = new CrossViewCorrespondenceCriterion( data, trimmedLabels, maxCorrespondences );

			final List< ViewId > allViewIds = new ArrayList<>();
			for ( final mpicbg.spim.data.sequence.ViewDescription vd : data.getSequenceDescription().getViewDescriptions().values() )
				if ( vd.isPresent() )
					allViewIds.add( vd );

			final double avgAnisoF = net.preibisch.mvrecon.process.interestpointregistration.TransformationTools.getAverageAnisotropyFactor( data, allViewIds );
			final double[] anisotropy = new double[] { 1, 1, avgAnisoF };
			IOFunctions.println( "Using anisotropy factor: " + Arrays.toString( anisotropy ) );

			final int[] minSizeMultiplier = parseMinSizeMultiplier( minTileSizeString, minSizeMultiplierString, minStepSize );
			if ( minSizeMultiplier == null )
				return null;
			IOFunctions.println( "Using min tile size multiplier per axis: " + Arrays.toString( minSizeMultiplier ) +
					" (min tile size = " + Arrays.toString( minTileSize( minSizeMultiplier, minStepSize ) ) + ")" );

			return new SplitOctTree( minStepSize, minSizeMultiplier, criterion, minSplitLevels, anisotropy );
		}
		else
		{
			if ( targetImageSizeString == null || targetOverlapString == null )
			{
				IOFunctions.printErr( "ERROR: --targetImageSize and --targetOverlap are required for uniform splitting." );
				return null;
			}

			final int[] targetImageSize = Import.csvStringToIntArray( targetImageSizeString );
			final int[] targetOverlap = Import.csvStringToIntArray( targetOverlapString );

			final long[] adjustedSize = new long[ 3 ];
			final long[] adjustedOverlap = new long[ 3 ];
			for ( int d = 0; d < 3; d++ )
			{
				adjustedSize[ d ] = SplitDistributeEvenly.closestLargerLongDivisableBy( targetImageSize[ d ], minStepSize[ d ] );
				adjustedOverlap[ d ] = SplitDistributeEvenly.closestLargerLongDivisableBy( targetOverlap[ d ], minStepSize[ d ] );
			}

			IOFunctions.println( "Adjusted target image size: " + Arrays.toString( adjustedSize ) );
			IOFunctions.println( "Adjusted target overlap: " + Arrays.toString( adjustedOverlap ) );

			for ( int d = 0; d < 3; d++ )
			{
				if ( adjustedOverlap[ d ] > adjustedSize[ d ] )
				{
					IOFunctions.printErr( "overlap cannot be bigger than size." );
					return null;
				}
			}

			return new SplitDistributeEvenly( data, adjustedOverlap, adjustedSize, minStepSize, optimize );
		}
	}

	/**
	 * Resolve the per-dimension minimum-tile-size multiplier from the two mutually
	 * exclusive CLI inputs.
	 *
	 * Exactly one of {@code minTileSizeString} and {@code minSizeMultiplierString} may
	 * be set; if both are null, defaults to 4× minStepSize per axis (matching the
	 * SplitOctTree GUI default). On validation failure, prints an error and returns
	 * null.
	 */
	private static int[] parseMinSizeMultiplier(
			final String minTileSizeString,
			final String minSizeMultiplierString,
			final long[] minStepSize )
	{
		final int n = minStepSize.length;
		final int[] multiplier = new int[ n ];
		final String[] dimNames = { "X", "Y", "Z" };

		if ( minTileSizeString != null && minSizeMultiplierString != null )
		{
			IOFunctions.printErr( "ERROR: --minTileSize and --minSizeMultiplier are mutually exclusive; please set at most one." );
			return null;
		}

		if ( minTileSizeString == null && minSizeMultiplierString == null )
		{
			for ( int d = 0; d < n; d++ )
				multiplier[ d ] = 4;
			return multiplier;
		}

		if ( minSizeMultiplierString != null )
		{
			final int[] mult = Import.csvStringToIntArray( minSizeMultiplierString );
			if ( mult.length != n )
			{
				IOFunctions.printErr( "ERROR: --minSizeMultiplier has " + mult.length +
						" values but dataset has " + n + " dimensions." );
				return null;
			}
			for ( int d = 0; d < n; d++ )
			{
				final String name = d < dimNames.length ? dimNames[ d ] : Integer.toString( d );
				if ( mult[ d ] < SplitOctTree.minSizeMultiplierFloor )
				{
					IOFunctions.printErr( "ERROR: --minSizeMultiplier " + name + " (" + mult[ d ] +
							") must be >= " + SplitOctTree.minSizeMultiplierFloor + "." );
					return null;
				}
				multiplier[ d ] = mult[ d ];
			}
			return multiplier;
		}

		final int[] tileSize = Import.csvStringToIntArray( minTileSizeString );
		if ( tileSize.length != n )
		{
			IOFunctions.printErr( "ERROR: --minTileSize has " + tileSize.length +
					" values but dataset has " + n + " dimensions." );
			return null;
		}

		for ( int d = 0; d < n; d++ )
		{
			final long step = minStepSize[ d ];
			final String name = d < dimNames.length ? dimNames[ d ] : Integer.toString( d );
			if ( tileSize[ d ] % step != 0 )
			{
				IOFunctions.printErr( "ERROR: --minTileSize " + name + " (" + tileSize[ d ] +
						") must be divisible by minStepSize (" + step + ")." );
				return null;
			}
			final long minAllowed = SplitOctTree.minSizeMultiplierFloor * step;
			if ( tileSize[ d ] < minAllowed )
			{
				IOFunctions.printErr( "ERROR: --minTileSize " + name + " (" + tileSize[ d ] +
						") must be >= " + minAllowed + " (" + SplitOctTree.minSizeMultiplierFloor + " × minStepSize)." );
				return null;
			}
			multiplier[ d ] = ( int ) ( tileSize[ d ] / step );
		}
		return multiplier;
	}

	private static long[] minTileSize( final int[] multiplier, final long[] minStepSize )
	{
		final long[] tile = new long[ multiplier.length ];
		for ( int d = 0; d < multiplier.length; d++ )
			tile[ d ] = ( long ) multiplier[ d ] * minStepSize[ d ];
		return tile;
	}

	public static void main( final String... args ) throws SpimDataException
	{
		System.out.println( Arrays.toString( args ) );
		System.exit( new CommandLine( new SplitDatasets() ).execute( args ) );
	}
}
