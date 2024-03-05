package net.preibisch.bigstitcher.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import mpicbg.models.Model;
import mpicbg.spim.data.SpimData;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.util.Pair;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractInterestPointRegistration;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.legacy.io.IOFunctions;
import net.preibisch.legacy.mpicbg.PointMatchGeneric;
import net.preibisch.mvrecon.Threads;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.AdvancedRegistrationParameters;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.InterestPointOverlapType;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.OverlapType;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.RegistrationType;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoints;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.MatcherPairwise;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.MatcherPairwiseTools;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.PairwiseResult;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.AllToAll;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.AllToAllRange;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.IndividualTimepoints;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.PairwiseSetup;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.ReferenceTimepoint;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.overlap.AllAgainstAllOverlap;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.overlap.OverlapDetection;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.overlap.SimpleBoundingBoxOverlap;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.range.TimepointRange;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.fastrgldm.FRGLDMPairwise;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.fastrgldm.FRGLDMParameters;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.geometrichashing.GeometricHashingPairwise;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.geometrichashing.GeometricHashingParameters;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.ransac.RANSACParameters;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.rgldm.RGLDMPairwise;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.rgldm.RGLDMParameters;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import scala.Tuple2;

public class SparkGeometricDescriptorRegistration extends AbstractInterestPointRegistration
{
	private static final long serialVersionUID = 6114598951078086239L;

	public enum Method { FAST_ROTATION, FAST_TRANSLATION, PRECISE_TRANSLATION };

	// TODO: support grouping tiles, channels
	final boolean groupTiles = false;
	final boolean groupIllums = false;
	final boolean groupChannels = false;
	final boolean groupTimePoints = false;

	@Option(names = { "-m", "--method" }, required = true, description = "the matching method; FAST_ROTATION, FAST_TRANSLATION or PRECISE_TRANSLATION")
	protected Method registrationMethod = null;

	@Option(names = { "-s", "--significance" }, description = "how much better the first match between two descriptors has to be compareed to the second best one (default: 3.0)")
	protected Double significance = 3.0;

	@Option(names = { "-r", "--redundancy" }, description = "the redundancy of the local descriptor (default: 1)")
	protected Integer redundancy = 1;

	@Option(names = { "-n", "--numNeighbors" }, description = "the number of neighoring points used to build the local descriptor, only supported by PRECISE_TRANSLATION (default: 3)")
	protected Integer numNeighbors = 3;

	@Option(names = { "--clearCorrespondences" }, description = "clear existing corresponding interest points for processed ViewIds and label before adding new ones (default: false)")
	private boolean clearCorrespondences = false;

	@Option(names = { "-ipo", "--interestpointOverlap" }, description = "which interest points to use for pairwise registrations, use OVERLAPPING_ONLY  or ALL points (default: ALL)")
	protected InterestPointOverlapType interestpointOverlap = InterestPointOverlapType.ALL;

	@Option(names = { "-vr", "--viewReg" }, description = "which views to register with each other, compare OVERLAPPING_ONLY or ALL_AGAINST_ALL (default: OVERLAPPING_ONLY)")
	protected OverlapType viewReg = OverlapType.OVERLAPPING_ONLY;


	@Option(names = { "-rit", "--ransacIterations" }, description = "number of ransac iterations (default: 10,000)")
	protected Integer ransacIterations = 10000;

	@Option(names = { "-rme", "--ransacMaxEpsilon" }, description = "ransac max error in pixels (default: 5.0)")
	protected Double ransacMaxEpsilon = 5.0;

	@Option(names = { "-rmir", "--ransacMinInlierRatio" }, description = "ransac min inlier ratio (default: 0.1)")
	protected Double ransacMinInlierRatio = 0.1;

	@Option(names = { "-rmif", "--ransacMinInlierFactor" }, description = "ransac min inlier factor, i.e. how many time the minimal number of matches need to found, e.g. affine needs 4 matches, 3x means at least 12 matches required (default: 3.0)")
	protected Double ransacMinInlierFactor = 3.0;

	//@Option(names = { "-p", "--pairsPerSparkJob" }, description = "how many pairs of views are processed per spark job (default: 1)")
	//protected Integer pairsPerSparkJob = 1;

	@Override
	public Void call() throws Exception
	{
		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIdsGlobal = this.loadViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			return null;

		if ( this.numNeighbors != 3 && registrationMethod != Method.PRECISE_TRANSLATION )
			throw new IllegalArgumentException( "Only PRECISE_TRANSLATION method supports numNeighbors != 3." );

		if ( this.referenceTP == null )
			this.referenceTP = viewIdsGlobal.get( 0 ).getTimePointId();
		else
		{
			final HashSet< Integer > timepointToProcess = 
					new HashSet<>( SpimData2.getAllTimePointsSorted( dataGlobal, viewIdsGlobal ).stream().mapToInt( tp -> tp.getId() ).boxed().collect(Collectors.toList()) );

			if ( !timepointToProcess.contains( referenceTP ) )
				throw new IllegalArgumentException( "Specified reference timepoint is not part of the ViewIds that are processed." );
		}

		if ( registrationTP == RegistrationType.TO_REFERENCE_TIMEPOINT )
			System.out.println( "Reference timepoint = " + this.referenceTP );

		// identify groups/subsets
		final Set< Group< ViewId > > groupsGlobal = AdvancedRegistrationParameters.getGroups( dataGlobal, viewIdsGlobal, groupTiles, groupIllums, groupChannels, groupTimePoints );
		final PairwiseSetup< ViewId > setup = pairwiseSetupInstance( this.registrationTP, viewIdsGlobal, groupsGlobal, this.rangeTP, this.referenceTP );
		final OverlapDetection<ViewId> overlapDetection = getOverlapDetection( dataGlobal, this.viewReg );
		identifySubsets( setup, overlapDetection );

		// find out how many pairs there are
		//final int numJobs = (setup.getPairs().size()/pairsPerSparkJob) + (setup.getPairs().size()%pairsPerSparkJob > 0 ? 1 : 0);
		System.out.println( "In total " + setup.getPairs().size() + " pairs of views need to be aligned.");// with " + pairsPerSparkJob + " pair(s) per Spark job, meaning " + numJobs + " jobs." );

		final SparkConf conf = new SparkConf().setAppName("SparkResaveN5");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		System.out.println( "Pairwise model = " + createModelInstance().getClass().getSimpleName() );

		// clear all correspondences if wanted
		final HashMap< ViewId, String > labelMapGlobal = new HashMap<>();
		viewIdsGlobal.forEach( viewId -> labelMapGlobal.put( viewId, label ));

		if ( clearCorrespondences )
		{
			System.out.println( "Clearing correspondences ... ");
			MatcherPairwiseTools.clearCorrespondences( viewIdsGlobal, dataGlobal.getViewInterestPoints().getViewInterestPoints(), labelMapGlobal );
		}

		final String xmlPath = this.xmlPath;
		final String label = this.label;
		final InterestPointOverlapType interestPointOverlapType = this.interestpointOverlap;
		//final InterestpointGroupingType groupingType = InterestpointGroupingType.DO_NOT_GROUP;

		final int ransacIterations = this.ransacIterations;
		final double ransacMaxEpsilon = this.ransacMaxEpsilon;
		final double ransacMinInlierRatio = this.ransacMinInlierRatio;
		final double ransacMinInlierFactor = this.ransacMinInlierFactor;
		final Method registrationMethod = this.registrationMethod;
		final double ratioOfDistance = this.significance;
		final int redundancy = this.redundancy;
		final int numNeighbors = this.numNeighbors;

		final JavaRDD<int[][]> rdd = sc.parallelize( Spark.serializeViewIdPairsForRDD( setup.getPairs() ) );

		final JavaPairRDD< ArrayList< PointMatchGeneric< InterestPoint > >, int[][] > rddResults = rdd.mapToPair( serializedPair ->
		{
			final SpimData2 data = Spark.getSparkJobSpimData2( "", xmlPath );
			final Pair<ViewId, ViewId> pair = Spark.derserializeViewIdPairsForRDD( serializedPair );

			//System.out.println( Group.pvid( pair.getA() ) + " <=> " + Group.pvid( pair.getB() ) );

			final ArrayList< ViewId > views = new ArrayList<>();
			views.add( pair.getA() );
			views.add( pair.getB() );

			final HashMap< ViewId, String > labelMap = new HashMap<>();
			labelMap.put( pair.getA(), label);
			labelMap.put( pair.getB(), label);

			// load & transform all interest points
			final Map< ViewId, List< InterestPoint > > interestpoints =
					TransformationTools.getAllTransformedInterestPoints(
						views,
						data.getViewRegistrations().getViewRegistrations(),
						data.getViewInterestPoints().getViewInterestPoints(),
						labelMap );

			// only keep those interestpoints that currently overlap with a view to register against
			if ( interestPointOverlapType == InterestPointOverlapType.OVERLAPPING_ONLY )
			{
				final Set< Group< ViewId > > groups = new HashSet<>();

				//if ( groupingType == InterestpointGroupingType.ADD_ALL )
				//	for ( final Subset< ViewId > subset : subsets )
				//		groups.addAll( subset.getGroups() );

				TransformationTools.filterForOverlappingInterestPoints( interestpoints, groups, data.getViewRegistrations().getViewRegistrations(), data.getSequenceDescription().getViewDescriptions() );

				System.out.println( Group.pvid( pair.getA() ) + " <=> " + Group.pvid( pair.getB() ) + ": Remaining interest points for alignment: " );
				for ( final Entry< ViewId, List< InterestPoint > > element: interestpoints.entrySet() )
					System.out.println( Group.pvid( pair.getA() ) + " <=> " + Group.pvid( pair.getB() ) + ": " + element.getKey() + ": " + element.getValue().size() );
			}

			final ExecutorService service = Threads.createFixedExecutorService( 1 );

			final RANSACParameters rp = new RANSACParameters( (float)ransacMaxEpsilon, (float)ransacMinInlierRatio, (float)ransacMinInlierFactor, ransacIterations );
			final Model< ? > model = createModelInstance();

			MatcherPairwise< InterestPoint > matcher;

			if ( registrationMethod == Method.FAST_ROTATION )
			{
				final GeometricHashingParameters gp = new GeometricHashingParameters(
						model,
						GeometricHashingParameters.differenceThreshold,
						(float)ratioOfDistance,
						(int)redundancy );

				matcher = new GeometricHashingPairwise<>( rp, gp );
			}
			else if ( registrationMethod == Method.FAST_TRANSLATION )
			{
				final FRGLDMParameters fp = new FRGLDMParameters(model, (float)ratioOfDistance, redundancy);
				matcher = new FRGLDMPairwise<>( rp, fp );
			}
			else
			{
				final RGLDMParameters dp = new RGLDMParameters(model, RGLDMParameters.differenceThreshold, (float)ratioOfDistance, numNeighbors, redundancy);
				matcher = new RGLDMPairwise<>( rp, dp );
			}

			// compute all pairwise matchings
			final PairwiseResult<InterestPoint> result =
					MatcherPairwiseTools.computePairs( 
							new ArrayList<>( Arrays.asList( pair ) ),
							interestpoints,
							matcher,
							service ).get( 0 ).getB();

			service.shutdown();

			return new Tuple2<>( new ArrayList<>( result.getInliers() ), serializedPair );
		});

		rddResults.cache();
		rddResults.count();
		final List<Tuple2<ArrayList<PointMatchGeneric<InterestPoint>>, int[][]>> results = rddResults.collect();

		// add the corresponding detections and output result
		if ( clearCorrespondences )
			System.out.println( "Adding corresponding interest points ...");
		else
			System.out.println( "Adding corresponding interest points (be sure to use --clearCorrespondences if you run multiple times, you are not using it right now) ...");

		for ( final Tuple2<ArrayList<PointMatchGeneric<InterestPoint>>, int[][]> tuple : results )
		{
			final Pair<ViewId, ViewId> pair = Spark.derserializeViewIdPairsForRDD( tuple._2() );
			
			final ViewId vA = pair.getA();
			final ViewId vB = pair.getB();

			final InterestPoints listA = dataGlobal.getViewInterestPoints().getViewInterestPoints().get( vA ).getInterestPointList( labelMapGlobal.get( vA ) );
			final InterestPoints listB = dataGlobal.getViewInterestPoints().getViewInterestPoints().get( vB ).getInterestPointList( labelMapGlobal.get( vB ) );

			MatcherPairwiseTools.addCorrespondences( tuple._1(), vA, vB, labelMapGlobal.get( vA ), labelMapGlobal.get( vB ), listA, listB );
		}

		if (!dryRun)
		{
			System.out.println( "Saving corresponding interest points ...");
	
			for ( final ViewId v : viewIdsGlobal )
				dataGlobal.getViewInterestPoints().getViewInterestPoints().get( v ).getInterestPointList( labelMapGlobal.get( v ) ).saveCorrespondingInterestPoints( true );
		}

		sc.close();

		System.out.println( "Done.");

		return null;
	}

	// TODO: move to multiview-reconstruction (AdvancedRegistrationParameters)
	public static PairwiseSetup< ViewId > pairwiseSetupInstance(
			final RegistrationType registrationType,
			final List< ViewId > views,
			final Set< Group< ViewId > > groups,
			final int rangeTP,
			final int referenceTP)
	{
		if ( registrationType == RegistrationType.TIMEPOINTS_INDIVIDUALLY )
			return new IndividualTimepoints( views, groups );
		else if ( registrationType == RegistrationType.ALL_TO_ALL )
			return new AllToAll<>( views, groups );
		else if ( registrationType == RegistrationType.ALL_TO_ALL_WITH_RANGE )
			return new AllToAllRange< ViewId, TimepointRange< ViewId > >( views, groups, new TimepointRange<>( rangeTP ) );
		else
			return new ReferenceTimepoint( views, groups, referenceTP );
	}


	// TODO: move to multiview-reconstruction (Interest_Point_Registration)
	public static void identifySubsets( final PairwiseSetup< ViewId > setup, final OverlapDetection< ViewId > overlapDetection )
	{
		IOFunctions.println( "Defined pairs, removed " + setup.definePairs().size() + " redundant view pairs." );
		IOFunctions.println( "Removed " + setup.removeNonOverlappingPairs( overlapDetection ).size() + " pairs because they do not overlap (Strategy='" + overlapDetection.getClass().getSimpleName() + "')" );
		setup.reorderPairs();
		setup.detectSubsets();
		setup.sortSubsets();
		IOFunctions.println( "Identified " + setup.getSubsets().size() + " subsets " );
	}

	// TODO: move to multiview-reconstruction (BasicRegistrationParameters)
	public static OverlapDetection< ViewId > getOverlapDetection( final SpimData spimData, final OverlapType overlapType )
	{
		if ( overlapType == OverlapType.ALL_AGAINST_ALL )
			return new AllAgainstAllOverlap<>( 3 );
		else
			return new SimpleBoundingBoxOverlap<>( spimData );
	}

	public static void main(final String... args)
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new SparkGeometricDescriptorRegistration()).execute(args));
	}

}
