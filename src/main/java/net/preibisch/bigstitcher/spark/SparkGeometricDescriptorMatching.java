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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import mpicbg.models.Model;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractRegistration;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.legacy.io.IOFunctions;
import net.preibisch.legacy.mpicbg.PointMatchGeneric;
import net.preibisch.mvrecon.Threads;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.AdvancedRegistrationParameters;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.InterestPointOverlapType;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.OverlapType;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoints;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.MatcherPairwise;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.MatcherPairwiseTools;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.PairwiseResult;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.PairwiseSetup;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.Subset;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.GroupedInterestPoint;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.InterestPointGroupingMinDistance;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.overlap.OverlapDetection;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.fastrgldm.FRGLDMPairwise;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.fastrgldm.FRGLDMParameters;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.geometrichashing.GeometricHashingPairwise;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.geometrichashing.GeometricHashingParameters;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.icp.IterativeClosestPointPairwise;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.icp.IterativeClosestPointParameters;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.ransac.RANSACParameters;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.rgldm.RGLDMPairwise;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.rgldm.RGLDMParameters;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import scala.Tuple2;

public class SparkGeometricDescriptorMatching extends AbstractRegistration
{
	private static final long serialVersionUID = 6114598951078086239L;

	public enum Method { FAST_ROTATION, FAST_TRANSLATION, PRECISE_TRANSLATION, ICP };

	@Option(names = { "-l", "--label" }, required = true, description = "label of the interest points used for registration (e.g. beads)")
	protected String label = null;

	@Option(names = { "-m", "--method" }, required = true, description = "the matching method; FAST_ROTATION, FAST_TRANSLATION, PRECISE_TRANSLATION or ICP")
	protected Method registrationMethod = null;

	@Option(names = { "-s", "--significance" }, description = "how much better the first match between two descriptors has to be compareed to the second best one (default: 3.0)")
	protected Double significance = 3.0;

	@Option(names = { "-r", "--redundancy" }, description = "the redundancy of the local descriptor (default: 1)")
	protected Integer redundancy = 1;

	@Option(names = { "-n", "--numNeighbors" }, description = "the number of neighoring points used to build the local descriptor, only supported by PRECISE_TRANSLATION (default: 3)")
	protected Integer numNeighbors = 3;

	@Option(names = { "--clearCorrespondences" }, description = "clear existing corresponding interest points for processed ViewIds and label before adding new ones (default: false)")
	protected boolean clearCorrespondences = false;

	@Option(names = { "-ipfr", "--interestpointsForReg" }, description = "which interest points to use for pairwise registrations, use OVERLAPPING_ONLY or ALL points (default: ALL)")
	protected InterestPointOverlapType interestpointsForReg = InterestPointOverlapType.ALL;

	@Option(names = { "-vr", "--viewReg" }, description = "which views to register with each other, compare OVERLAPPING_ONLY or ALL_AGAINST_ALL (default: OVERLAPPING_ONLY)")
	protected OverlapType viewReg = OverlapType.OVERLAPPING_ONLY;


	@Option(names = { "--interestPointMergeDistance" }, description = "when grouping of views is selected, merge interest points within that radius in px (default: 5.0)")
	protected Double interestPointMergeDistance = 5.0;


	@Option(names = { "--groupIllums" }, description = "group all illumination directions that belong to the same angle/channel/tile/timepoint together as one view, e.g. to stitch illums as one (default: false)")
	protected boolean groupIllums = false;

	@Option(names = { "--groupChannels" }, description = "group all channels that belong to the same angle/illumination/tile/timepoint together as one view, e.g. to stitch channels as one (default: false)")
	protected boolean groupChannels = false;

	@Option(names = { "--groupTiles" }, description = "group all tiles that belong to the same angle/channel/illumination/timepoint together as one view, e.g. to align across angles (default: false)")
	protected boolean groupTiles = false;

	@Option(names = { "--splitTimepoints" }, description = "group all angles/channels/illums/tiles that belong to the same timepoint as one View, e.g. for stabilization across time (default: false)")
	protected boolean splitTimepoints = false;


	@Option(names = { "-rit", "--ransacIterations" }, description = "max number of ransac iterations (default: 10,000 for descriptors, 200 for ICP)")
	protected Integer ransacIterations = null;

	@Option(names = { "-rme", "--ransacMaxError" }, description = "ransac max error in pixels (default: 5.0 for descriptors, 2.5 for ICP)")
	protected Double ransacMaxError = null;

	// TODOL ignored by ICP
	@Option(names = { "-rmir", "--ransacMinInlierRatio" }, description = "ransac min inlier ratio (default: 0.1)")
	protected Double ransacMinInlierRatio = 0.1;

	@Option(names = { "-rmif", "--ransacMinInlierFactor" }, description = "ransac min inlier factor, i.e. how many time the minimal number of matches need to found, e.g. affine needs 4 matches, 3x means at least 12 matches required (default: 3.0)")
	protected Double ransacMinInlierFactor = 3.0;

	
	@Option(names = { "-ime", "--icpMaxError" }, description = "ICP max error in pixels (default: 5.0)")
	protected Double icpMaxError = 5.0;

	@Option(names = { "-iit", "--icpIterations" }, description = "max number of ICP iterations (default: 200)")
	protected Integer icpIterations = 200;

	@Option(names = { "--icpUseRANSAC" }, description = "ICP uses RANSAC at every iteration to filter correspondences (default: false)")
	protected boolean icpUseRANSAC = false;
	
	//@Option(names = { "-p", "--pairsPerSparkJob" }, description = "how many pairs of views are processed per spark job (default: 1)")
	//protected Integer pairsPerSparkJob = 1;

	@Override
	public Void call() throws Exception
	{
		initRegistrationParameters();

		if ( this.numNeighbors != 3 && registrationMethod != Method.PRECISE_TRANSLATION )
		{
			System.out.println( "Only PRECISE_TRANSLATION method supports numNeighbors != 3." );
			return null;
		}

		if ( registrationMethod == Method.ICP && ( redundancy != 1 || significance != 3.0 || numNeighbors != 3 ))
		{
			System.out.println( "ICP does not support parameters redundancy, significance and numNeighbors" );
			return null;
		}

		if ( ransacIterations == null && registrationMethod == Method.ICP )
		{
			ransacIterations = 200;
			ransacMaxError = 2.5;
		}
		else if ( ransacIterations == null )
		{
			ransacIterations = 10000;
			ransacMaxError = 5.0;
		}

		// identify groups/subsets
		final PairwiseSetup< ViewId > setup = setupGroups( viewReg );

		// find out how many pairs there are
		//final int numJobs = (setup.getPairs().size()/pairsPerSparkJob) + (setup.getPairs().size()%pairsPerSparkJob > 0 ? 1 : 0);
		System.out.println( "In total " + setup.getPairs().size() + " pairs of views need to be aligned.");// with " + pairsPerSparkJob + " pair(s) per Spark job, meaning " + numJobs + " jobs." );

		// if we group, we will have less pairs, since certain views are combined into one big view
		//final InterestpointGroupingType groupingType = InterestpointGroupingType.DO_NOT_GROUP; -- this is always ADD_ALL - either group or not (was only necessary in the GUI, because one could group for interest points and/or global opt

		System.out.println( "Pairwise model = " + createModelInstance(transformationModel, regularizationModel, regularizationLambda).getClass().getSimpleName() );

		final HashMap< ViewId, String > labelMapGlobal = new HashMap<>();
		viewIdsGlobal.forEach( viewId ->
		{
			// make sure the label exists for all views that should be processed
			if ( dataGlobal.getViewInterestPoints().getViewInterestPointLists( viewId ).getInterestPointList( label ) == null )
			{
				System.out.println( "Error, label '" + label + "' does for exist for ViewId " + Group.pvid( viewId ) );
				System.exit( 1 );
			}

			labelMapGlobal.put( viewId, label );
		});

		// clear all correspondences if wanted
		if ( clearCorrespondences )
		{
			System.out.println( "Clearing correspondences ... ");
			MatcherPairwiseTools.clearCorrespondences( viewIdsGlobal, dataGlobal.getViewInterestPoints().getViewInterestPoints(), labelMapGlobal );
		}

		final String xmlPath = this.xmlPath;
		final String label = this.label;
		final InterestPointOverlapType interestpointsForReg = this.interestpointsForReg;
		final int ransacIterations = this.ransacIterations;
		final double ransacMaxEpsilon = this.ransacMaxError;
		final double ransacMinInlierRatio = this.ransacMinInlierRatio;
		final double ransacMinInlierFactor = this.ransacMinInlierFactor;
		final double icpMaxError = this.icpMaxError;
		final int icpMaxIterations = this.icpIterations;
		final boolean icpUseRANSAC = this.icpUseRANSAC;
		final Method registrationMethod = this.registrationMethod;
		final double ratioOfDistance = this.significance;
		final int redundancy = this.redundancy;
		final int numNeighbors = this.numNeighbors;
		final double interestPointMergeDistance = this.interestPointMergeDistance;
		final TransformationModel transformationModel = this.transformationModel;
		final RegularizationModel regularizationModel = this.regularizationModel;
		final double lambda = this.regularizationLambda;

		final SparkConf conf = new SparkConf().setAppName("SparkGeometricDescriptorRegistration");
		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final JavaRDD< ArrayList< Tuple2< ArrayList< PointMatchGeneric< InterestPoint > >, int[][] > > > rddResults;

		if ( !groupTiles && !groupIllums && !groupChannels && !splitTimepoints )
		{
			System.out.println( "The following ViewIds will be matched to each other: ");
			setup.getPairs().forEach( pair -> System.out.println( "\t" + Group.pvid( pair.getA() ) + " <=> " + Group.pvid( pair.getB() ) ) );
			System.out.println( "In total: " + setup.getPairs().size() + " pair(s).");

			final JavaRDD<int[][]> rdd = sc.parallelize( Spark.serializeViewIdPairsForRDD( setup.getPairs() ) );

			rddResults = rdd.map( serializedPair ->
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
				if ( interestpointsForReg == InterestPointOverlapType.OVERLAPPING_ONLY )
				{
					final Set< Group< ViewId > > groups = new HashSet<>();

					TransformationTools.filterForOverlappingInterestPoints( interestpoints, groups, data.getViewRegistrations().getViewRegistrations(), data.getSequenceDescription().getViewDescriptions() );

					System.out.println( Group.pvid( pair.getA() ) + " <=> " + Group.pvid( pair.getB() ) + ": Remaining interest points for alignment: " );
					for ( final Entry< ViewId, List< InterestPoint > > element: interestpoints.entrySet() )
						System.out.println( Group.pvid( pair.getA() ) + " <=> " + Group.pvid( pair.getB() ) + ": " + element.getKey() + ": " + element.getValue().size() );
				}

				final ExecutorService service = Threads.createFixedExecutorService( 1 );

				final RANSACParameters rp = new RANSACParameters( (float)ransacMaxEpsilon, (float)ransacMinInlierRatio, (float)ransacMinInlierFactor, ransacIterations );
				final Model< ? > model = createModelInstance(transformationModel, regularizationModel, lambda);

				final MatcherPairwise< InterestPoint > matcher = createMatcherInstance(
						rp,
						registrationMethod,
						model,
						numNeighbors,
						redundancy,
						(float)ratioOfDistance,
						icpMaxError,
						icpMaxIterations,
						icpUseRANSAC);

				// compute all pairwise matchings
				final PairwiseResult<InterestPoint> result =
						MatcherPairwiseTools.computePairs( 
								new ArrayList<>( Arrays.asList( pair ) ),
								interestpoints,
								matcher,
								service ).get( 0 ).getB();

				service.shutdown();

				return new ArrayList<>( Arrays.asList( new Tuple2<>( new ArrayList<>( result.getInliers() ), serializedPair ) ) );
			});
		}
		else
		{
			System.out.println( "grouped" );

			final List<Pair<Group<ViewId>, Group<ViewId>>> groupedPairs =
					setup.getSubsets().stream().map( s -> s.getGroupedPairs() ).flatMap(List::stream).collect( Collectors.toList() );

			System.out.println( "The following groups of ViewIds will be matched to each other: ");
			groupedPairs.forEach( pair -> System.out.println( "\t" + pair.getA() + " <=> " + pair.getB() ) );
			System.out.println( "In total: " + groupedPairs.size() + " pair(s).");

			final JavaRDD<int[][][]> rdd = sc.parallelize( Spark.serializeGroupedViewIdPairsForRDD( groupedPairs ) );

			rddResults = rdd.map( serializedGroupPair ->
			{
				final SpimData2 data = Spark.getSparkJobSpimData2( "", xmlPath );
				final Pair<Group<ViewId>, Group<ViewId>> pair = Spark.deserializeGroupedViewIdPairForRDD( serializedGroupPair );

				final ArrayList< ViewId > views = new ArrayList<>();
				views.addAll( pair.getA().getViews() );
				views.addAll( pair.getB().getViews() );

				final HashMap< ViewId, String > labelMap = new HashMap<>();
				views.forEach( v -> labelMap.put( v, label) );

				// load & transform all interest points
				final Map< ViewId, List< InterestPoint > > interestpoints =
						TransformationTools.getAllTransformedInterestPoints(
							views,
							data.getViewRegistrations().getViewRegistrations(),
							data.getViewInterestPoints().getViewInterestPoints(),
							labelMap );

				// only keep those interestpoints that currently overlap with a view to register against
				if ( interestpointsForReg == InterestPointOverlapType.OVERLAPPING_ONLY )
				{
					final Set< Group< ViewId > > groups = new HashSet<>();

					// this code is to make sure that we are not removing interestpoints for overlapping views that are part of the same group
					// because they will be combined into one big View and they most likely overlap
					groups.add( pair.getA() );
					groups.add( pair.getB() );

					TransformationTools.filterForOverlappingInterestPoints( interestpoints, groups, data.getViewRegistrations().getViewRegistrations(), data.getSequenceDescription().getViewDescriptions() );

					System.out.println( pair.getA() + " <=> " + pair.getB() + ": Remaining interest points for alignment: " );
					for ( final Entry< ViewId, List< InterestPoint > > element: interestpoints.entrySet() )
						System.out.println( pair.getA() + " <=> " + pair.getB() + ": " + element.getKey() + ": " + element.getValue().size() );
				}

				final Map< Group< ViewId >, List< GroupedInterestPoint< ViewId > > > groupedInterestpoints = new HashMap<>();

				final InterestPointGroupingMinDistance< ViewId > ipGrouping 
						= new InterestPointGroupingMinDistance<>( interestPointMergeDistance, interestpoints );

				IOFunctions.println( pair.getA() + " <=> " + pair.getB() + ": Using a maximum radius of " + ipGrouping.getRadius() + " to filter interest points from overlapping views." );

				groupedInterestpoints.put( pair.getA(), ipGrouping.group( pair.getA() ) );
				IOFunctions.println( pair.getA() + " <=> " + pair.getB() + ": Grouping interestpoints for " + pair.getA() + " (" + ipGrouping.countBefore() + " >>> " + ipGrouping.countAfter() + ")" );

				groupedInterestpoints.put( pair.getB(), ipGrouping.group( pair.getB() ) );
				IOFunctions.println( pair.getA() + " <=> " + pair.getB() + ": Grouping interestpoints for " + pair.getB() + " (" + ipGrouping.countBefore() + " >>> " + ipGrouping.countAfter() + ")" );

				final RANSACParameters rp = new RANSACParameters( (float)ransacMaxEpsilon, (float)ransacMinInlierRatio, (float)ransacMinInlierFactor, ransacIterations );
				final Model< ? > model = createModelInstance(transformationModel, regularizationModel, lambda);

				final MatcherPairwise matcher = createMatcherInstance(
						rp,
						registrationMethod,
						model,
						numNeighbors,
						redundancy,
						(float)ratioOfDistance,
						icpMaxError,
						icpMaxIterations,
						icpUseRANSAC);

				final List< Pair< Pair< Group< ViewId >, Group< ViewId > >, PairwiseResult< GroupedInterestPoint< ViewId > > > > resultGroup =
						MatcherPairwiseTools.computePairs( new ArrayList<>( Arrays.asList( pair ) ), groupedInterestpoints, matcher );

				final HashMap< Pair< ViewId, ViewId >, ArrayList<PointMatchGeneric<InterestPoint>> > mapResults = new HashMap<>();

				for ( final PointMatchGeneric<GroupedInterestPoint<ViewId>> pm : resultGroup.get( 0 ).getB().getInliers() )
				{
					GroupedInterestPoint<ViewId> p1 = pm.getPoint1();
					GroupedInterestPoint<ViewId> p2 = pm.getPoint2();

					final ViewId v1 = p1.getV();
					final ViewId v2 = p2.getV();

					final InterestPoint ip1 = new InterestPoint( p1.getId(), p1.getL() );
					final InterestPoint ip2 = new InterestPoint( p2.getId(), p2.getL() );
					final PointMatchGeneric<InterestPoint> pmNew = new PointMatchGeneric<>( ip1, ip2 );

					final ValuePair<ViewId, ViewId> pv = new ValuePair<>( v1, v2 );

					ArrayList<PointMatchGeneric<InterestPoint>> list = mapResults.get( pv );

					if ( list == null )
					{
						list = new ArrayList<>();
						list.add( pmNew );
						mapResults.put(pv, list);
					}
					else
					{
						list.add( pmNew );
					}
				}

				final ArrayList<Tuple2<ArrayList<PointMatchGeneric<InterestPoint>>, int[][]>> resultsLocal = new ArrayList<>();

				System.out.println( pair.getA() + " <=> " + pair.getB() + ": The following correspondences were found per ViewId: ");
				for ( final Entry< Pair< ViewId, ViewId >, ArrayList<PointMatchGeneric<InterestPoint>> > entry : mapResults.entrySet( ))
				{
					if ( entry.getValue().size() < model.getMinNumMatches() )
					{
						System.out.println( "\t" + pair.getA() + " <=> " + pair.getB() + ": " + Group.pvid( entry.getKey().getA() ) + "<->" + Group.pvid( entry.getKey().getB() )  + ": " + entry.getValue().size() + " correspondences (will be omitted as it is less than model.getMinNumMatches())." );
					}
					else
					{
						System.out.println( "\t" + pair.getA() + " <=> " + pair.getB() + ": " + Group.pvid( entry.getKey().getA() ) + "<->" + Group.pvid( entry.getKey().getB() )  + ": " + entry.getValue().size() + " correspondences." );
						resultsLocal.add( new Tuple2<>( new ArrayList<>( entry.getValue() ), Spark.serializeViewIdPairForRDD( entry.getKey() ) ) );
					}
				}

				System.out.println( "\t" + pair.getA() + " <=> " + pair.getB() + ": Remaining per-view correspondences=" + mapResults.size() );

				return resultsLocal;
			});
		}

		rddResults.cache();
		rddResults.count();

		final List<ArrayList<Tuple2<ArrayList<PointMatchGeneric<InterestPoint>>, int[][]>>> results = rddResults.collect();

		// add the corresponding detections and output result
		if ( clearCorrespondences )
			System.out.println( "Adding corresponding interest points ...");
		else
			System.out.println( "Adding corresponding interest points (be sure to use --clearCorrespondences if you run multiple times, you are not using it right now) ...");

		for ( final ArrayList<Tuple2<ArrayList<PointMatchGeneric<InterestPoint>>, int[][]>> tupleList : results )
			for ( final Tuple2<ArrayList<PointMatchGeneric<InterestPoint>>, int[][]> tuple : tupleList )
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

	public PairwiseSetup< ViewId > setupGroups( final OverlapType viewReg )
	{
		final Set< Group< ViewId > > groupsGlobal = AdvancedRegistrationParameters.getGroups( dataGlobal, viewIdsGlobal, groupTiles, groupIllums, groupChannels, splitTimepoints );
		final PairwiseSetup< ViewId > setup = pairwiseSetupInstance( this.registrationTP, viewIdsGlobal, groupsGlobal, this.rangeTP, this.referenceTP );
		final OverlapDetection<ViewId> overlapDetection = getOverlapDetection( dataGlobal, viewReg );
		identifySubsets( setup, overlapDetection );

		return setup;
	}

	public static MatcherPairwise< InterestPoint > createMatcherInstance(
			final RANSACParameters rp,
			final Method registrationMethod,
			final Model< ? > model,
			final int numNeighbors,
			final int redundancy,
			final float ratioOfDistance,
			final double icpMaxDistance,
			final int icpMaxIterations,
			final boolean icpUseRANSAC )
	{
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
		else if ( registrationMethod == Method.PRECISE_TRANSLATION )
		{
			final RGLDMParameters dp = new RGLDMParameters(model, RGLDMParameters.differenceThreshold, (float)ratioOfDistance, numNeighbors, redundancy);
			matcher = new RGLDMPairwise<>( rp, dp );
		}
		else
		{
			final IterativeClosestPointParameters ip = new IterativeClosestPointParameters(
					model,
					icpMaxDistance,
					icpMaxIterations,
					icpUseRANSAC,
					rp.getMinInlierRatio(),
					rp.getMaxEpsilon(),
					rp.getNumIterations(),
					Math.round( rp.getMinInlierFactor() * model.getMinNumMatches() ) );
			matcher = new IterativeClosestPointPairwise<>( ip );
		}

		return matcher;
	}

	public static void getAllGroupedPairs( final Collection< Subset< ViewId > > subsets )
	{
		subsets.stream().map( s -> s.getGroupedPairs() ).collect( Collectors.toList() );
		final List<Pair<Group<ViewId>, Group<ViewId>>> groupedPairs = new ArrayList<>();
		
		for ( final Subset< ViewId > subset : subsets )
		{
			List<Pair<Group<ViewId>, Group<ViewId>>> g = subset.getGroupedPairs();
		}
	}

	public static void main(final String... args) throws SpimDataException
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new SparkGeometricDescriptorMatching()).execute(args));
	}

}
