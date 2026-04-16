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

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.Subset;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import mpicbg.models.AbstractModel;
import mpicbg.models.Affine3D;
import mpicbg.models.Model;
import mpicbg.models.RigidModel3D;
import mpicbg.models.Tile;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.multithreading.SimpleMultiThreading;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.preibisch.bigstitcher.spark.SparkGeometricDescriptorMatching.Method;
import net.preibisch.bigstitcher.spark.Solver.PreAlign;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractRegistration;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.legacy.io.IOFunctions;
import net.preibisch.legacy.mpicbg.PointMatchGeneric;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.global.GlobalOptimizationParameters.GlobalOptType;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.AdvancedRegistrationParameters;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.InterestPointOverlapType;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.OverlapType;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPointLists;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import net.preibisch.mvrecon.process.interestpointregistration.global.GlobalOpt;
import net.preibisch.mvrecon.process.interestpointregistration.global.GlobalOptIterative;
import net.preibisch.mvrecon.process.interestpointregistration.global.GlobalOptTwoRound;
import net.preibisch.mvrecon.process.interestpointregistration.global.convergence.ConvergenceStrategy;
import net.preibisch.mvrecon.process.interestpointregistration.global.convergence.SimpleIterativeConvergenceStrategy;
import net.preibisch.mvrecon.process.interestpointregistration.global.linkremoval.MaxErrorLinkRemoval;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.strong.InterestPointMatchCreator;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.MatcherPairwise;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.MatcherPairwiseTools;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.MatcherPairwiseTools.MatchingTask;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.PairwiseResult;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.PairwiseSetup;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.GroupedInterestPoint;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.InterestPointGroupingMinDistance;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.overlap.OverlapDetection;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.ransac.RANSACParameters;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.weak.MetaDataWeakLinkFactory;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.overlap.SimpleBoundingBoxOverlap;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import scala.Tuple2;

public class SparkRegisterWithInterestPoints extends AbstractRegistration
{
	private static final long serialVersionUID = -6719789753171652020L;

	// -----------------------------------------------------------------------
	// Interest point label(s)
	// -----------------------------------------------------------------------

	@Option(names = { "-l", "--label" }, required = true, description = "label(s) of the interest points used for registration (e.g. -l beads -l nuclei)")
	protected ArrayList<String> labels = null;

	// -----------------------------------------------------------------------
	// Matching method and descriptor parameters
	// -----------------------------------------------------------------------

	@Option(names = { "-m", "--method" }, required = true, description = "the matching method; FAST_ROTATION, FAST_TRANSLATION, PRECISE_TRANSLATION or ICP")
	protected Method registrationMethod = null;

	@Option(names = { "-s", "--significance" }, description = "how much better the first match between two descriptors has to be compared to the second best one (default: 3.0)")
	protected Double significance = 3.0;

	@Option(names = { "-sr", "--searchRadius" }, description = "Only for PRECISE_TRANSLATION; limits the search range for corresponding points in global coordinate space (default: no limit)")
	protected Double searchRadius = null;

	@Option(names = { "-r", "--redundancy" }, description = "the redundancy of the local descriptor (default: 1)")
	protected Integer redundancy = 1;

	@Option(names = { "-n", "--numNeighbors" }, description = "the number of neighboring points used to build the local descriptor, only supported by PRECISE_TRANSLATION (default: 3)")
	protected Integer numNeighbors = 3;

	@Option(names = { "--clearCorrespondences" }, description = "clear existing corresponding interest points for processed ViewIds and label before adding new ones (default: false)")
	protected boolean clearCorrespondences = false;

	@Option(names = { "--matchAcrossLabels" }, description = "if you specified more than one label, setting this to true will match between label classes (default: false)")
	protected boolean matchAcrossLabels = false;

	@Option(names = { "-ipfr", "--interestpointsForReg" }, description = "which interest points to use for pairwise registrations, use OVERLAPPING_ONLY or ALL points (default: ALL)")
	protected InterestPointOverlapType interestpointsForReg = InterestPointOverlapType.ALL;

	@Option(names = { "-vr", "--viewReg" }, description = "which views to register with each other, compare OVERLAPPING_ONLY or ALL_AGAINST_ALL (default: OVERLAPPING_ONLY)")
	protected OverlapType viewReg = OverlapType.OVERLAPPING_ONLY;

	@Option(names = { "--interestPointMergeDistance" }, description = "when grouping of views is selected, merge interest points within that radius in px (default: 5.0)")
	protected Double interestPointMergeDistance = 5.0;

	// -----------------------------------------------------------------------
	// Grouping
	// -----------------------------------------------------------------------

	@Option(names = { "--groupIllums" }, description = "group all illumination directions that belong to the same angle/channel/tile/timepoint together as one view (default: false)")
	protected boolean groupIllums = false;

	@Option(names = { "--groupChannels" }, description = "group all channels that belong to the same angle/illumination/tile/timepoint together as one view (default: false)")
	protected boolean groupChannels = false;

	@Option(names = { "--groupTiles" }, description = "group all tiles that belong to the same angle/channel/illumination/timepoint together as one view (default: false)")
	protected boolean groupTiles = false;

	@Option(names = { "--splitTimepoints" }, description = "group all angles/channels/illums/tiles that belong to the same timepoint as one View (default: false)")
	protected boolean splitTimepoints = false;

	// -----------------------------------------------------------------------
	// RANSAC parameters
	// -----------------------------------------------------------------------

	@Option(names = { "-rit", "--ransacIterations" }, description = "max number of RANSAC iterations (default: 10,000 for descriptors, 200 for ICP)")
	protected Integer ransacIterations = null;

	@Option(names = { "-rme", "--ransacMaxError" }, description = "RANSAC max error in pixels (default: 5.0 for descriptors, 2.5 for ICP)")
	protected Double ransacMaxError = null;

	@Option(names = { "-rmir", "--ransacMinInlierRatio" }, description = "RANSAC min inlier ratio (default: 0.1)")
	protected Double ransacMinInlierRatio = 0.1;

	@Option(names = { "-rmni", "--ransacMinNumInliers" }, description = "RANSAC minimal number of required inliers (default: 12)")
	protected Integer ransacMinNumInliers = 12;

	@Option(names = { "-rmc", "--ransacMultiConsensus" }, description = "RANSAC perform multi-consensus matching (default: false)")
	protected boolean ransacMultiConsensus = false;

	// -----------------------------------------------------------------------
	// ICP parameters
	// -----------------------------------------------------------------------

	@Option(names = { "-ime", "--icpMaxError" }, description = "ICP max error in pixels (default: 5.0)")
	protected Double icpMaxError = 5.0;

	@Option(names = { "-iit", "--icpIterations" }, description = "max number of ICP iterations (default: 200)")
	protected Integer icpIterations = 200;

	@Option(names = { "--icpUseRANSAC" }, description = "ICP uses RANSAC at every iteration to filter correspondences (default: false)")
	protected boolean icpUseRANSAC = false;

	// -----------------------------------------------------------------------
	// Global optimization parameters
	// -----------------------------------------------------------------------

	@Option(names = { "--globalOptType" }, description = "global optimization method; ONE_ROUND_SIMPLE, ONE_ROUND_ITERATIVE, TWO_ROUND_SIMPLE or TWO_ROUND_ITERATIVE (default: ONE_ROUND_ITERATIVE)")
	protected GlobalOptType globalOptType = GlobalOptType.ONE_ROUND_ITERATIVE;

	@Option(names = { "-pa", "--preAlign" }, description = "whether to pre-align before solving (PREALIGN) or initialize with current transformations (NO_PREALIGN) (default: PREALIGN)")
	protected PreAlign preAlign = PreAlign.PREALIGN;

	@Option(names = { "--relativeThreshold" }, description = "relative error threshold for iterative solvers (default: 3.5)")
	protected double relativeThreshold = 3.5;

	@Option(names = { "--absoluteThreshold" }, description = "absolute error threshold for iterative solver to drop a link in pixels (default: 7.0)")
	protected double absoluteThreshold = 7.0;

	@Option(names = { "--maxError" }, description = "max error for the global optimization (default: 5.0)")
	protected Double maxError = 5.0;

	@Option(names = { "--maxIterations" }, description = "max number of iterations for global optimization (default: 10,000)")
	protected Integer maxIterations = 10000;

	@Option(names = { "--maxPlateauwidth" }, description = "max plateau width for global optimization (default: 200)")
	protected Integer maxPlateauwidth = 200;

	@Option(names = { "--disableFixedViews" }, description = "disable fixing of views during global optimization (see --fixedViews)")
	protected boolean disableFixedViews = false;

	@Option(names = { "-fv", "--fixedViews" }, description = "define a list of fixed view ids (timepoint, viewSetup), e.g. -fv '0,0' -fv '0,1' (default: first view id per subset)")
	protected String[] fixedViews = null;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();
		initRegistrationParameters();

		// Validate descriptor parameters
		if ( this.numNeighbors != 3 && registrationMethod != Method.PRECISE_TRANSLATION )
		{
			System.out.println( "Only PRECISE_TRANSLATION method supports numNeighbors != 3." );
			return null;
		}

		if ( registrationMethod == Method.ICP && ( redundancy != 1 || significance != 3.0 || numNeighbors != 3 ) )
		{
			System.out.println( "ICP does not support parameters redundancy, significance and numNeighbors." );
			return null;
		}

		// Set RANSAC defaults based on method
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

		System.out.println( "Matching method = " + registrationMethod );
		System.out.println( "Pairwise model = " + createModelInstance( transformationModel, regularizationModel, regularizationLambda ).getClass().getSimpleName() );
		System.out.println( "globalOptType = " + globalOptType );
		System.out.println( "preAlign = " + preAlign );

		// Build label map (one entry per label, weight 1.0 for matching phase)
		final HashMap< String, Double > labelWeightMap = new HashMap<>();
		for ( final String label : labels )
			labelWeightMap.put( label, 1.0 );

		System.out.println( "labels & weights: " + labelWeightMap );

		final HashMap< ViewId, HashMap< String, Double > > labelMapGlobal =
				SparkGeometricDescriptorMatching.buildLabelMap( dataGlobal, viewIdsGlobal, labelWeightMap );

		// Clear correspondences if requested
		if ( clearCorrespondences )
		{
			System.out.println( "Clearing correspondences ..." );
			MatcherPairwiseTools.clearCorrespondences( viewIdsGlobal, dataGlobal.getViewInterestPoints().getViewInterestPoints(), labelMapGlobal );
		}

		// Identify groups and pairwise subsets
		final Set< Group< ViewId > > groupsGlobal = AdvancedRegistrationParameters.getGroups(
				dataGlobal, viewIdsGlobal, groupTiles, groupIllums, groupChannels, splitTimepoints );

		final PairwiseSetup< ViewId > setup = pairwiseSetupInstance(
				this.registrationTP, viewIdsGlobal, groupsGlobal, this.rangeTP, this.referenceTP );

		final OverlapDetection< ViewId > overlapDetection = getOverlapDetection( dataGlobal, viewReg );
		identifySubsets( setup, overlapDetection );

		System.out.println( "In total " + setup.getPairs().size() + " pairs of views need to be aligned." );

		// Capture fields as effectively final for Spark lambdas
		final URI xmlURI = this.xmlURI;
		final boolean matchAcrossLabels = this.matchAcrossLabels;
		final InterestPointOverlapType interestpointsForReg = this.interestpointsForReg;
		final int ransacIterations = this.ransacIterations;
		final double ransacMaxEpsilon = this.ransacMaxError;
		final double ransacMinInlierRatio = this.ransacMinInlierRatio;
		final int ransacMinNumInliers = this.ransacMinNumInliers;
		final boolean ransacMultiConsensus = this.ransacMultiConsensus;
		final double icpMaxError = this.icpMaxError;
		final int icpMaxIterations = this.icpIterations;
		final boolean icpUseRANSAC = this.icpUseRANSAC;
		final Method registrationMethod = this.registrationMethod;
		final double ratioOfDistance = this.significance;
		final boolean limitSearchRadius = ( this.searchRadius != null );
		final double searchRadius = ( this.searchRadius == null ) ? 0 : this.searchRadius;
		final int redundancy = this.redundancy;
		final int numNeighbors = this.numNeighbors;
		final double interestPointMergeDistance = this.interestPointMergeDistance;
		final TransformationModel transformationModel = this.transformationModel;
		final RegularizationModel regularizationModel = this.regularizationModel;
		final double lambda = this.regularizationLambda;

		final SparkConf conf = new SparkConf().setAppName( "SparkRegisterWithInterestPoints" );

		if ( localSparkBindAddress )
		{
			conf.set( "spark.driver.bindAddress", "127.0.0.1" );
			conf.set( "spark.driver.host", "localhost" );
			org.apache.spark.util.Utils.setCustomHostname( "localhost" );
		}

		final JavaSparkContext sc = new JavaSparkContext( conf );
		sc.setLogLevel( "ERROR" );

		// -----------------------------------------------------------------------
		// Phase 1: Parallel pairwise matching via Spark
		// -----------------------------------------------------------------------

		final JavaRDD< ArrayList< Tuple2< ArrayList< PointMatchGeneric< InterestPoint > >, MatchingTask< ViewId > > > > rddResults;

		if ( !groupTiles && !groupIllums && !groupChannels && !splitTimepoints )
		{
			System.out.println( "NO grouping." );

			final ArrayList< MatchingTask< ViewId > > tasksList =
					MatcherPairwiseTools.getTasksList( Spark.toViewIds( setup.getPairs() ), labelMapGlobal, matchAcrossLabels );

			System.out.println( "The following ViewIds will be matched to each other:" );
			setup.getPairs().forEach( pair -> System.out.println( "\t" + Group.pvid( pair.getA() ) + " <=> " + Group.pvid( pair.getB() ) ) );
			System.out.println( "In total: " + tasksList.size() + " pair(s) across labels: " + labels );

			final JavaRDD< MatchingTask< ViewId > > rdd = sc.parallelize( tasksList )
					.repartition( Math.min( Spark.maxPartitions, tasksList.size() ) );

			rddResults = rdd.map( task ->
			{
				final SpimData2 data = Spark.getSparkJobSpimData2( xmlURI );
				final ArrayList< ViewId > views = task.viewsAsList();

				final HashMap< ViewId, HashMap< String, Double > > labelMap = new HashMap<>();
				views.forEach( viewId -> labelMap.put( viewId, new HashMap<>() ) );

				labelMapGlobal.get( task.vA ).forEach( ( label, weight ) -> {
					if ( label.equals( task.labelA ) )
						labelMap.get( task.vA ).put( label, weight );
				} );

				labelMapGlobal.get( task.vB ).forEach( ( label, weight ) -> {
					if ( label.equals( task.labelB ) )
						labelMap.get( task.vB ).put( label, weight );
				} );

				final Map< ViewId, HashMap< String, Collection< InterestPoint > > > interestpoints =
						TransformationTools.getAllTransformedInterestPoints(
								views,
								data.getViewRegistrations().getViewRegistrations(),
								data.getViewInterestPoints().getViewInterestPoints(),
								labelMap );

				if ( interestpointsForReg == InterestPointOverlapType.OVERLAPPING_ONLY )
				{
					final Set< Group< ViewId > > groups = new HashSet<>();
					TransformationTools.filterForOverlappingInterestPoints(
							interestpoints, groups,
							data.getViewRegistrations().getViewRegistrations(),
							data.getSequenceDescription().getViewDescriptions() );

					System.out.println( Group.pvid( task.vA ) + " (" + task.labelA + ") <=> " + Group.pvid( task.vB ) + " (" + task.labelB + "): Remaining interest points for alignment:" );
					for ( final Entry< ViewId, HashMap< String, Collection< InterestPoint > > > element : interestpoints.entrySet() )
						for ( final Entry< String, Collection< InterestPoint > > subElement : element.getValue().entrySet() )
							System.out.println( Group.pvid( element.getKey() ) + ", '" + subElement.getKey() + "' : " + subElement.getValue().size() );
				}

				final RANSACParameters rp = new RANSACParameters( (float) ransacMaxEpsilon, (float) ransacMinInlierRatio, ransacMinNumInliers, ransacIterations, ransacMultiConsensus );
				final Model< ? > model = createModelInstance( transformationModel, regularizationModel, lambda );

				final MatcherPairwise< InterestPoint > matcher = SparkGeometricDescriptorMatching.createMatcherInstance(
						rp, registrationMethod, model,
						numNeighbors, redundancy, ratioOfDistance,
						limitSearchRadius, searchRadius,
						icpMaxError, icpMaxIterations, icpUseRANSAC );

				final PairwiseResult< InterestPoint > result =
						MatcherPairwiseTools.getCallables(Collections.singletonList(task), interestpoints, matcher ).get( 0 ).call().getB();

				return new ArrayList<>(Collections.singletonList(new Tuple2<>(new ArrayList<>(result.getInliers()), task)));
			} );
		}
		else
		{
			System.out.println( "grouped" );

			final HashMap< String, Double > map = labelWeightMap;

			final List< Pair< Group< ViewId >, Group< ViewId > > > groupedPairs =
					Spark.toGroupViewIds(
							setup.getSubsets().stream()
									.map(Subset::getGroupedPairs)
									.flatMap( List::stream )
									.collect( Collectors.toList() ) );

			final HashMap< Group< ViewId >, HashMap< String, Double > > groupedMap = new HashMap<>();
			setup.getSubsets().forEach( subset -> subset.getGroups().forEach( group -> groupedMap.put( Spark.toGroupViewIds( group ), map ) ) );

			final ArrayList< MatchingTask< Group< ViewId > > > tasksList =
					MatcherPairwiseTools.getTasksList( groupedPairs, groupedMap, matchAcrossLabels );

			System.out.println( "The following groups of ViewIds will be matched to each other:" );
			groupedPairs.forEach( pair -> System.out.println( "\t" + pair.getA() + " <=> " + pair.getB() ) );
			System.out.println( "In total: " + groupedPairs.size() + " pair(s)." );

			final JavaRDD< MatchingTask< Group< ViewId > > > rdd = sc.parallelize( tasksList )
					.repartition( Math.min( Spark.maxPartitions, tasksList.size() ) );

			rddResults = rdd.map( task ->
			{
				final SpimData2 data = Spark.getSparkJobSpimData2( xmlURI );

				final ArrayList< ViewId > views = new ArrayList<>();
				views.addAll( task.vA.getViews() );
				views.addAll( task.vB.getViews() );

				final HashMap< ViewId, HashMap< String, Double > > labelMap = new HashMap<>();
				views.forEach( viewId -> labelMap.put( viewId, new HashMap<>() ) );

				task.vA.getViews().forEach( viewA -> {
					labelMapGlobal.get( viewA ).forEach( ( label, weight ) -> {
						if ( label.equals( task.labelA ) )
							labelMap.get( viewA ).put( label, weight );
					} );
				} );

				task.vB.getViews().forEach( viewB -> {
					labelMapGlobal.get( viewB ).forEach( ( label, weight ) -> {
						if ( label.equals( task.labelB ) )
							labelMap.get( viewB ).put( label, weight );
					} );
				} );

				final Map< ViewId, HashMap< String, Collection< InterestPoint > > > interestpoints =
						TransformationTools.getAllTransformedInterestPoints(
								views,
								data.getViewRegistrations().getViewRegistrations(),
								data.getViewInterestPoints().getViewInterestPoints(),
								labelMap );

				if ( interestpointsForReg == InterestPointOverlapType.OVERLAPPING_ONLY )
				{
					final Set< Group< ViewId > > groups = new HashSet<>();
					groups.add( task.vA );
					groups.add( task.vB );

					TransformationTools.filterForOverlappingInterestPoints(
							interestpoints, groups,
							data.getViewRegistrations().getViewRegistrations(),
							data.getSequenceDescription().getViewDescriptions() );

					System.out.println( task.vA + " (" + task.labelA + ") <=> " + task.vB + " (" + task.labelB + "): Remaining interest points for alignment:" );
					for ( final Entry< ViewId, HashMap< String, Collection< InterestPoint > > > element : interestpoints.entrySet() )
						for ( final Entry< String, Collection< InterestPoint > > subElement : element.getValue().entrySet() )
							System.out.println( Group.pvid( element.getKey() ) + ", '" + subElement.getKey() + "' : " + subElement.getValue().size() );
				}

				final Map< Group< ViewId >, HashMap< String, Collection< GroupedInterestPoint< ViewId > > > > groupedInterestpoints = new HashMap<>();

				final InterestPointGroupingMinDistance< ViewId > ipGrouping =
						new InterestPointGroupingMinDistance<>( interestPointMergeDistance, interestpoints );

				IOFunctions.println( task.vA + " <=> " + task.vB + ": Using a maximum radius of " + ipGrouping.getRadius() + " to filter interest points from overlapping views." );

				groupedInterestpoints.put( task.vA, ipGrouping.group( task.vA ) );
				IOFunctions.println( task.vA + " <=> " + task.vB + ": Grouping interestpoints for " + task.vA + " (" + ipGrouping.countBefore() + " >>> " + ipGrouping.countAfter() + ")" );

				groupedInterestpoints.put( task.vB, ipGrouping.group( task.vB ) );
				IOFunctions.println( task.vA + " <=> " + task.vB + ": Grouping interestpoints for " + task.vB + " (" + ipGrouping.countBefore() + " >>> " + ipGrouping.countAfter() + ")" );

				final RANSACParameters rp = new RANSACParameters( (float) ransacMaxEpsilon, (float) ransacMinInlierRatio, ransacMinNumInliers, ransacIterations, ransacMultiConsensus );
				final Model< ? > model = createModelInstance( transformationModel, regularizationModel, lambda );

				final MatcherPairwise< GroupedInterestPoint< ViewId > > matcher = SparkGeometricDescriptorMatching.createMatcherInstance(
						rp, registrationMethod, model,
						numNeighbors, redundancy, ratioOfDistance,
						limitSearchRadius, searchRadius,
						icpMaxError, icpMaxIterations, icpUseRANSAC );

				final PairwiseResult< GroupedInterestPoint< ViewId > > result =
						MatcherPairwiseTools.getCallables( Arrays.asList( task ), groupedInterestpoints, matcher ).get( 0 ).call().getB();

				final HashMap< Pair< ViewId, ViewId >, ArrayList< PointMatchGeneric< InterestPoint > > > mapResults = new HashMap<>();

				for ( final PointMatchGeneric< GroupedInterestPoint< ViewId > > pm : result.getInliers() )
				{
					final GroupedInterestPoint< ViewId > p1 = pm.getPoint1();
					final GroupedInterestPoint< ViewId > p2 = pm.getPoint2();

					final ViewId v1 = p1.getV();
					final ViewId v2 = p2.getV();

					final InterestPoint ip1 = new InterestPoint( p1.getId(), p1.getL() );
					final InterestPoint ip2 = new InterestPoint( p2.getId(), p2.getL() );
					final PointMatchGeneric< InterestPoint > pmNew = new PointMatchGeneric<>( ip1, ip2 );

					final ValuePair< ViewId, ViewId > pv = new ValuePair<>( v1, v2 );

					ArrayList< PointMatchGeneric< InterestPoint > > list = mapResults.get( pv );

					if ( list == null )
					{
						list = new ArrayList<>();
						list.add( pmNew );
						mapResults.put( pv, list );
					}
					else
					{
						list.add( pmNew );
					}
				}

				final ArrayList< Tuple2< ArrayList< PointMatchGeneric< InterestPoint > >, MatchingTask< ViewId > > > resultsLocal = new ArrayList<>();

				System.out.println( task.vA + " <=> " + task.vB + ": The following correspondences were found per ViewId:" );
				for ( final Entry< Pair< ViewId, ViewId >, ArrayList< PointMatchGeneric< InterestPoint > > > entry : mapResults.entrySet() )
				{
					final Model< ? > modelForCheck = createModelInstance( transformationModel, regularizationModel, lambda );
					if ( entry.getValue().size() < modelForCheck.getMinNumMatches() )
					{
						System.out.println( "\t" + Group.pvid( entry.getKey().getA() ) + "<->" + Group.pvid( entry.getKey().getB() )
								+ ": " + entry.getValue().size() + " correspondences (omitted - less than model.getMinNumMatches())." );
					}
					else
					{
						System.out.println( "\t" + Group.pvid( entry.getKey().getA() ) + "<->" + Group.pvid( entry.getKey().getB() )
								+ ": " + entry.getValue().size() + " correspondences." );
						resultsLocal.add( new Tuple2<>( new ArrayList<>( entry.getValue() ),
								new MatchingTask<>( entry.getKey().getA(), entry.getKey().getB(), task.labelA, task.labelB ) ) );
					}
				}

				return resultsLocal;
			} );
		}

		rddResults.cache();
		rddResults.count();

		final List< ArrayList< Tuple2< ArrayList< PointMatchGeneric< InterestPoint > >, MatchingTask< ViewId > > > > results = rddResults.collect();

		sc.close();

		// -----------------------------------------------------------------------
		// Store correspondences on driver
		// -----------------------------------------------------------------------

		System.out.println( "Adding corresponding interest points ..." );

		for ( final ArrayList< Tuple2< ArrayList< PointMatchGeneric< InterestPoint > >, MatchingTask< ViewId > > > tupleList : results )
			for ( final Tuple2< ArrayList< PointMatchGeneric< InterestPoint > >, MatchingTask< ViewId > > tuple : tupleList )
			{
				final ViewId vA = tuple._2().vA;
				final ViewId vB = tuple._2().vB;
				final String labelA = tuple._2().labelA;
				final String labelB = tuple._2().labelB;

				final InterestPoints listA = dataGlobal.getViewInterestPoints().getViewInterestPoints().get( vA ).getInterestPointList( labelA );
				final InterestPoints listB = dataGlobal.getViewInterestPoints().getViewInterestPoints().get( vB ).getInterestPointList( labelB );

				MatcherPairwiseTools.addCorrespondences( tuple._1(), vA, vB, labelA, labelB, listA, listB );
			}

		if ( !dryRun )
		{
			System.out.println( "Saving corresponding interest points (in parallel) ..." );

			final ArrayList< Pair< ViewId, String > > allIps = new ArrayList<>();
			for ( final ViewId v : viewIdsGlobal )
				for ( final String l : labels )
					allIps.add( new ValuePair<>( v, l ) );

			final Map< ViewId, ViewInterestPointLists > ip = dataGlobal.getViewInterestPoints().getViewInterestPoints();
			allIps.parallelStream().forEach( pair -> ip.get( pair.getA() ).getInterestPointList( pair.getB() ).saveCorrespondingInterestPoints( true ) );
		}

		// -----------------------------------------------------------------------
		// Phase 2: Global optimization (driver, sequential)
		// -----------------------------------------------------------------------

		System.out.println( "Running global optimization ..." );

		// Resolve groups for global opt
		final Collection< Group< ViewId > > groups;

		if ( !groupTiles && !groupIllums && !groupChannels && !splitTimepoints )
			groups = new ArrayList<>();
		else
			groups = AdvancedRegistrationParameters.getGroups( dataGlobal, viewIdsGlobal, groupTiles, groupIllums, groupChannels, splitTimepoints );

		// Resolve fixed views
		final HashSet< ViewId > fixedViewIds;

		if ( disableFixedViews )
		{
			fixedViewIds = new HashSet<>();
		}
		else if ( this.fixedViews == null || this.fixedViews.length == 0 )
		{
			fixedViewIds = Solver.assembleFixedAuto( viewIdsGlobal, dataGlobal.getSequenceDescription(), registrationTP, referenceTP );
		}
		else
		{
			// Parse user-specified fixed views
			final ArrayList< ViewId > parsedFixed = Import.getViewIds( this.fixedViews );
			fixedViewIds = new HashSet<>( Import.getViewIds( dataGlobal, parsedFixed ) );
		}

		System.out.println( "The following ViewIds are used as fixed views:" );
		fixedViewIds.forEach( vid -> System.out.print( Group.pvid( vid ) + ", " ) );
		System.out.println();

		final InterestPointMatchCreator pmc = Solver.setupPointMatchesFromInterestPoints(
				dataGlobal, viewIdsGlobal, labelMapGlobal, groups, fixedViewIds );

		if ( pmc == null )
		{
			System.out.println( "No views are connected, stopping." );
			return null;
		}

		final Model< ? > model = createModelInstance( transformationModel, regularizationModel, regularizationLambda );

		final HashMap< ViewId, Tile > models;

		if ( globalOptType == GlobalOptType.ONE_ROUND_SIMPLE )
		{
			final ConvergenceStrategy cs = new ConvergenceStrategy( maxError, maxIterations, maxPlateauwidth );

			models = (HashMap) GlobalOpt.computeTiles(
					(Model) model,
					preAlign == PreAlign.PREALIGN,
					pmc,
					cs,
					fixedViewIds,
					groups );
		}
		else if ( globalOptType == GlobalOptType.ONE_ROUND_ITERATIVE )
		{
			Collection< Pair< Group< ViewId >, Group< ViewId > > > removedInconsistentPairs = new ArrayList<>();
			models = (HashMap) GlobalOptIterative.computeTiles(
					(Model) model,
					preAlign == PreAlign.PREALIGN,
					pmc,
					new SimpleIterativeConvergenceStrategy( Double.MAX_VALUE, maxIterations, maxPlateauwidth, relativeThreshold, absoluteThreshold ),
					new MaxErrorLinkRemoval(),
					removedInconsistentPairs,
					fixedViewIds,
					groups );
		}
		else
		{
			// TWO_ROUND_SIMPLE or TWO_ROUND_ITERATIVE
			final double relThresh = ( globalOptType == GlobalOptType.TWO_ROUND_SIMPLE ) ? Double.MAX_VALUE : relativeThreshold;
			final double absThresh = ( globalOptType == GlobalOptType.TWO_ROUND_SIMPLE ) ? Double.MAX_VALUE : absoluteThreshold;
			Collection< Pair< Group< ViewId >, Group< ViewId > > > removedInconsistentPairs = new ArrayList<>();

			models = (HashMap) GlobalOptTwoRound.computeTiles(
					(Model & Affine3D) model,
					preAlign == PreAlign.PREALIGN,
					pmc,
					new SimpleIterativeConvergenceStrategy( Double.MAX_VALUE, maxIterations, maxPlateauwidth, relThresh, absThresh ),
					new MaxErrorLinkRemoval(),
					removedInconsistentPairs,
					new MetaDataWeakLinkFactory(
							dataGlobal.getViewRegistrations().getViewRegistrations(),
							new SimpleBoundingBoxOverlap<>(
									dataGlobal.getSequenceDescription().getViewSetups(),
									dataGlobal.getViewRegistrations().getViewRegistrations() ) ),
					new ConvergenceStrategy( Double.MAX_VALUE ),
					fixedViewIds,
					groups );
		}

		if ( models == null || models.isEmpty() )
		{
			System.out.println( "No transformations could be found, stopping." );
			return null;
		}

		SimpleMultiThreading.threadWait( 100 );

		System.out.println( "\nFinal models: " + models.size() );

		for ( final ViewId viewId : viewIdsGlobal )
		{
			System.out.println( Group.pvid( viewId ) );
			final Tile< ? extends AbstractModel< ? > > tile = models.get( viewId );
			final ViewRegistration vr = dataGlobal.getViewRegistrations().getViewRegistration( viewId );

			TransformationTools.storeTransformation( vr, viewId, tile, null, model.getClass().getSimpleName() );

			final String output = Group.pvid( viewId ) + ": " + TransformationTools.printAffine3D( (Affine3D< ? >) tile.getModel() );

			if ( tile.getModel() instanceof RigidModel3D )
				System.out.println( output + ", " + TransformationTools.getRotationAxis( (RigidModel3D) tile.getModel() ) );
			else
				System.out.println( output + ", " + TransformationTools.getScaling( (Affine3D< ? >) tile.getModel() ) );
		}

		if ( !dryRun )
		{
			System.out.println( "Saving resulting XML ..." );
			new XmlIoSpimData2().save( dataGlobal, xmlURI );
		}

		System.out.println( "Done." );
		return null;
	}

	public static void main( final String... args ) throws SpimDataException
	{
		System.out.println( Arrays.toString( args ) );
		System.exit( new CommandLine( new SparkRegisterWithInterestPoints() ).execute( args ) );
	}
}
