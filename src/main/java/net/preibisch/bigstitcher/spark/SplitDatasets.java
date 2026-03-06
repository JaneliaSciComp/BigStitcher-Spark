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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import ij.ImageJ;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.generic.sequence.BasicViewDescription;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewRegistrations;
import mpicbg.spim.data.registration.ViewTransformAffine;
import mpicbg.spim.data.registration.ViewTransform;
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
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.legacy.io.IOFunctions;
import net.preibisch.mvrecon.fiji.plugin.Split_Views.InterestPointAdding;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.explorer.SelectedViewDescriptionListener;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPointLists;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.intensityadjust.IntensityAdjustments;
import net.preibisch.mvrecon.fiji.spimdata.pointspreadfunctions.PointSpreadFunctions;
import net.preibisch.mvrecon.fiji.spimdata.stitchingresults.StitchingResults;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.splitting.SplitImgLoader;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.splitting.SplitMultiResolutionImgLoader;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.splitting.SplitViewerImgLoader;
import net.preibisch.mvrecon.process.splitting.SplitDistributeEvenly;
import net.preibisch.mvrecon.process.splitting.SplitInterval;
import net.preibisch.mvrecon.process.splitting.SplitOctTree;
import net.preibisch.mvrecon.process.splitting.SplittingTools;
import net.preibisch.mvrecon.process.splitting.CrossViewCorrespondenceCriterion;
import net.preibisch.mvrecon.process.splitting.ConsensusSetCriterion;
import net.preibisch.mvrecon.process.splitting.OctTreeSplitCriterion;
import net.preibisch.stitcher.gui.StitchingExplorer;
import bdv.ViewerImgLoader;
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

	@Option(names = "--minSizeMultiplier", description = "min tile size multiplier for oct-tree (default: 4)")
	private int minSizeMultiplier = 4;

	@Option(names = "--enableMerge", description = "enable block re-merging for oct-tree (default: true)")
	private boolean enableMerge = true;

	@Option(names = "--minSplitLevels", description = "force minimum split levels for oct-tree (default: 0)")
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

	@Option(names = { "--displayResult" }, description = "display the result, do not save (you can still click save in the GUI that will pop up")
	private boolean displayResult = false;


	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		// ==================== 1. Setup on driver ====================
		final SpimData2 dataGlobal = displayResult ?
				this.loadSpimData2( Runtime.getRuntime().availableProcessors() ) : this.loadSpimData2();

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

		// Create SplitInterval based on method
		final SplitInterval splitting = createSplitInterval( dataGlobal, minStepSize );
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

		// ==================== 2. Phase 1: Splitting ====================
		IOFunctions.println( "(" + new Date( System.currentTimeMillis() ) + "): Phase 1 - Computing split intervals..." );

		final Map< Integer, ArrayList< Interval > > splitResults = new HashMap<>();

		// Uniform splitting is fast — run on driver.
		// Oct-tree could benefit from Spark but also runs fast enough on driver
		// (the expensive part is interest point processing in Phase 2).
		for ( final ViewSetup oldSetup : oldSetups )
		{
			final Interval input = new FinalInterval( oldSetup.getSize() );
			final ArrayList< Interval > intervals = splitting.split( input );

			if ( intervals == null )
			{
				IOFunctions.printErr( "ERROR: Splitting failed for ViewSetup " + oldSetup.getId() );
				return null;
			}

			IOFunctions.println( "ViewId " + oldSetup.getId() + " with interval " + Util.printInterval( input ) +
					": " + intervals.size() + " tiles" );

			splitResults.put( oldSetup.getId(), intervals );
		}

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

		final SparkConf conf = new SparkConf().setAppName( "SplitDatasets" );
		if ( localSparkBindAddress )
			conf.set( "spark.driver.bindAddress", "127.0.0.1" );

		final JavaSparkContext sc = new JavaSparkContext( conf );
		sc.setLogLevel( "ERROR" );

		try
		{
			// Serialize job list for Phase 2: one job per setup
			// Each job is: [oldSetupId, idRangeStart]
			final ArrayList< int[] > phase2Jobs = new ArrayList<>();
			for ( final ViewSetup oldSetup : oldSetups )
				phase2Jobs.add( new int[]{ oldSetup.getId(), setupIdStart.get( oldSetup.getId() ) } );

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
			final URI finalXmlURI = xmlURI;
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

				// Create saver that writes interest points on-the-fly
				final SplittingTools.InterestPointSaver saver = vipl -> {
					for ( final ViewInterestPointLists v : vipl.values() )
						for ( final InterestPoints ips : v.getHashMap().values() )
						{
							ips.saveInterestPoints( false );
							ips.saveCorrespondingInterestPoints( false );
						}
				};

				// Call processSetupStatic - does all the IP work
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

			// Build task list: (oldSetupId, newSetupId, timepointId)
			final ArrayList< int[] > phase3Jobs = new ArrayList<>();
			for ( final ViewSetup oldSetup : oldSetups )
				for ( final int newSetupId : old2NewSetups.get( oldSetup.getId() ) )
					for ( final TimePoint t : timepoints.getTimePointsOrdered() )
						phase3Jobs.add( new int[]{ oldSetup.getId(), newSetupId, t.getId() } );

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
				final int newSetupId = job[ 1 ];
				final int timepointId = job[ 2 ];

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

				// Create correspondence saver
				final SplittingTools.CorrespondenceSaver corrSaver = vipl -> {
					for ( final InterestPoints ips : vipl.getHashMap().values() )
						ips.saveCorrespondingInterestPoints( false );
				};

				// Process correspondences for this specific (oldSetup, newSetupId, timepoint)
				SplittingTools.processCorrespondingInterestPointsStatic(
						oldSetup, newSetupId, timepoint,
						data, localOld2New, newInterestpoints,
						null, 0,
						corrSaver );
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

			// Reconstruct interest points on driver (lazy-loading from disk)
			final Map< ViewId, ViewInterestPointLists > driverNewInterestpoints = new HashMap<>();
			final URI baseDir = dataGlobal.getBasePathURI();

			for ( final Map.Entry< Integer, ArrayList< Integer > > entry : old2NewSetups.entrySet() )
			{
				for ( final int newSetupId : entry.getValue() )
				{
					for ( final TimePoint t : timepoints.getTimePointsOrdered() )
					{
						final ViewId viewId = new ViewId( t.getId(), newSetupId );
						final ViewInterestPointLists vipl = new ViewInterestPointLists( t.getId(), newSetupId );

						for ( final String label : oldLabelNames )
						{
							final String newLabel = label + "_split";
							vipl.addInterestPointList( newLabel, InterestPoints.newInstance( baseDir, viewId, newLabel ) );
						}

						if ( fakeInterestPoints != InterestPointAdding.NONE )
							vipl.addInterestPointList( fLabel, InterestPoints.newInstance( baseDir, viewId, fLabel ) );

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

			// Save or display
			if ( displayResult )
			{
				new ImageJ();

				final StitchingExplorer< SpimData2 > explorer = new StitchingExplorer<>( spimDataNew, xmlOutURI, new XmlIoSpimData2() );
				explorer.getFrame().toFront();

				explorer.addListener( new SelectedViewDescriptionListener< SpimData2 >()
				{
					@Override
					public void updateContent( SpimData2 data ) {}

					@Override
					public void selectedViewDescriptions( List< List< BasicViewDescription< ? > > > viewDescriptions ) {}

					@Override
					public void save() {}

					@Override
					public void quit()
					{
						System.out.println( "quitting GUI." );
						System.exit( 0 );
					}
				} );

				try { Thread.sleep( Long.MAX_VALUE ); }
				catch ( final InterruptedException e ) { System.err.println( "Thread woken up: " + e ); }
			}
			else if ( !dryRun )
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
	 * Create the appropriate SplitInterval based on the --splitMethod option.
	 */
	protected SplitInterval createSplitInterval( final SpimData2 data, final long[] minStepSize )
	{
		if ( "octtree".equalsIgnoreCase( splitMethod ) )
		{
			return createOctTreeSplitter( data, minStepSize );
		}
		else
		{
			return createUniformSplitter( minStepSize );
		}
	}

	/**
	 * Create a SplitDistributeEvenly for uniform splitting.
	 */
	protected SplitInterval createUniformSplitter( final long[] minStepSize )
	{
		if ( targetImageSizeString == null || targetOverlapString == null )
		{
			IOFunctions.printErr( "ERROR: --targetImageSize and --targetOverlap are required for uniform splitting." );
			return null;
		}

		final int[] targetImageSize = Import.csvStringToIntArray( targetImageSizeString );
		final int[] targetOverlap = Import.csvStringToIntArray( targetOverlapString );

		// Adjust to be divisible by minStepSize
		final long[] adjustedSize = new long[ 3 ];
		final long[] adjustedOverlap = new long[ 3 ];
		for ( int d = 0; d < 3; d++ )
		{
			adjustedSize[ d ] = SplitDistributeEvenly.closestLargerLongDivisableBy( targetImageSize[ d ], minStepSize[ d ] );
			adjustedOverlap[ d ] = SplitDistributeEvenly.closestLargerLongDivisableBy( targetOverlap[ d ], minStepSize[ d ] );
		}

		System.out.println( "Target image sizes and overlaps need be adjusted to be divisible by " + Arrays.toString( minStepSize ) );
		System.out.println( "Adjusted target image size: " + Arrays.toString( adjustedSize ) );
		System.out.println( "Adjusted target overlap: " + Arrays.toString( adjustedOverlap ) );

		for ( int d = 0; d < 3; d++ )
		{
			if ( adjustedOverlap[ d ] > adjustedSize[ d ] )
			{
				System.out.println( "overlap cannot be bigger than size." );
				return null;
			}
		}

		return new SplitDistributeEvenly( adjustedOverlap, adjustedSize, minStepSize, !disableOptimization );
	}

	/**
	 * Create a SplitOctTree for oct-tree splitting.
	 */
	protected SplitInterval createOctTreeSplitter( final SpimData2 data, final long[] minStepSize )
	{
		if ( labelsString == null )
		{
			IOFunctions.printErr( "ERROR: --labels is required for oct-tree splitting." );
			return null;
		}

		final Set< String > labels = new HashSet<>( Arrays.asList( labelsString.split( "," ) ) );
		for ( String label : labels )
			label = label.trim();

		// Trim labels properly
		final Set< String > trimmedLabels = new HashSet<>();
		for ( final String label : labels )
			trimmedLabels.add( label.trim() );

		// Create criterion
		final OctTreeSplitCriterion criterion;
		if ( "consensus".equalsIgnoreCase( criterionType ) )
		{
			criterion = new ConsensusSetCriterion(
					data, trimmedLabels, maxCorrespondences,
					ConsensusSetCriterion.TOLERANCE_NONE, 0 );
		}
		else
		{
			criterion = new CrossViewCorrespondenceCriterion( data, trimmedLabels, maxCorrespondences );
		}

		return new SplitOctTree( minStepSize, minSizeMultiplier, criterion, enableMerge, minSplitLevels, SplitOctTree.MERGE_SAME_AS_SPLIT );
	}

	public static void main( final String... args ) throws SpimDataException
	{
		System.out.println( Arrays.toString( args ) );
		System.exit( new CommandLine( new SplitDatasets() ).execute( args ) );
	}
}
