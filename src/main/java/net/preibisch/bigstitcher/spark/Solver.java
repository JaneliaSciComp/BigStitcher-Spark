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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import mpicbg.models.AbstractModel;
import mpicbg.models.Affine3D;
import mpicbg.models.Model;
import mpicbg.models.RigidModel3D;
import mpicbg.models.Tile;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Dimensions;
import net.imglib2.multithreading.SimpleMultiThreading;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractRegistration;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.legacy.mpicbg.PointMatchGeneric;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.global.GlobalOptimizationParameters;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.global.GlobalOptimizationParameters.GlobalOptType;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.AdvancedRegistrationParameters;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.RegistrationType;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.CorrespondingInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.stitchingresults.PairwiseStitchingResult;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import net.preibisch.mvrecon.process.interestpointregistration.global.GlobalOpt;
import net.preibisch.mvrecon.process.interestpointregistration.global.GlobalOptIterative;
import net.preibisch.mvrecon.process.interestpointregistration.global.GlobalOptTwoRound;
import net.preibisch.mvrecon.process.interestpointregistration.global.convergence.ConvergenceStrategy;
import net.preibisch.mvrecon.process.interestpointregistration.global.convergence.SimpleIterativeConvergenceStrategy;
import net.preibisch.mvrecon.process.interestpointregistration.global.linkremoval.MaxErrorLinkRemoval;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.PointMatchCreator;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.strong.ImageCorrelationPointMatchCreator;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.strong.InterestPointMatchCreator;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.weak.MetaDataWeakLinkFactory;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.PairwiseResult;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.Subset;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.overlap.SimpleBoundingBoxOverlap;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class Solver extends AbstractRegistration
{
	/*
	-x '/Users/preibischs/Documents/Microscopy/SPIM/HisYFP-SPIM/Spark_test_tp/dataset.xml'
	-l beadsSpark3
	--method ONE_ROUND_SIMPLE
	-rtp ALL_TO_ALL
	-s IP
	--splitTimepoints
	--dryRun
	*/
	private static final long serialVersionUID = 5220898723968914742L;

	public enum SolverSource { IP, STITCHING };

	public ArrayList< ViewId > fixedViewIds;

	//public enum MapbackModel { TRANSLATION, RIGID };
	//public ArrayList< ViewId > mapBackViewIds;
	//public Model<?> mapBackModel;

	@Option(names = { "-s", "--sourcePoints" }, required = true, description = "which source to use for the solve, IP (interest points) or STITCHING")
	protected SolverSource sourcePoints = null;


	@Option(names = { "--groupIllums" }, description = "group all illumination directions that belong to the same angle/channel/tile/timepoint together as one view, e.g. to stitch illums as one (default: false for IP, true for stitching)")
	protected Boolean groupIllums = null;

	@Option(names = { "--groupChannels" }, description = "group all channels that belong to the same angle/illumination/tile/timepoint together as one view, e.g. to stitch channels as one (default: false for IP, true for stitching)")
	protected Boolean groupChannels = null;

	@Option(names = { "--groupTiles" }, description = "group all tiles that belong to the same angle/channel/illumination/timepoint together as one view, e.g. to align across angles (default: false)")
	protected Boolean groupTiles = null;

	@Option(names = { "--splitTimepoints" }, description = "group all angles/channels/illums/tiles that belong to the same timepoint as one View, e.g. for stabilization across time (default: false)")
	protected Boolean splitTimepoints = null;


	@Option(names = { "-l", "--label" }, required = false, description = "label(s) of the interest points used for registration (e.g. -l beads -l nuclei)")
	protected ArrayList<String> labels = null;

	@Option(names = { "-lw", "--labelweights" }, required = false, description = "weights of label(s) of the interest points used for registration (e.g. -l 1.0 -l 0.1, default: 1.0)")
	protected ArrayList<Double> labelweights = null;


	@Option(names = { "--method" }, description = "global optimization method; ONE_ROUND_SIMPLE, ONE_ROUND_ITERATIVE, TWO_ROUND_SIMPLE or TWO_ROUND_ITERATIVE. Two round handles unconnected tiles, iterative handles wrong links (default: ONE_ROUND_SIMPLE)")
	protected GlobalOptType globalOptType = GlobalOptType.ONE_ROUND_SIMPLE;

	@Option(names = { "--relativeThreshold" }, description = "relative error threshold for iterative solvers, how many times worse than the average error a link needs to be (default: 3.5)")
	protected double relativeThreshold = 3.5;

	@Option(names = { "--absoluteThreshold" }, description = "absoluted error threshold for iterative solver to drop a link in pixels (default: 7.0)")
	protected double absoluteThreshold = 7.0;

	@Option(names = { "--maxError" }, description = "max error for the solve (default: 5.0)")
	protected Double maxError = 5.0;

	@Option(names = { "--maxIterations" }, description = "max number of iterations for solve (default: 10,000)")
	protected Integer maxIterations = 10000;

	@Option(names = { "--maxPlateauwidth" }, description = "max plateau witdth for solve (default: 200)")
	protected Integer maxPlateauwidth = 200;

	@Option(names = { "--disableFixedViews" }, description = "disable fixing of views (see --fixedViews)")
	protected boolean disableFixedViews = false;

	@Option(names = { "-fv", "--fixedViews" }, description = "define a list of (or a single) fixed view ids (time point, view setup), e.g. -fv '0,0' -fv '0,1' (default: first view id)")
	protected String[] fixedViews = null;

	//@Option(names = { "--enableMapbackViews" }, description = "enable mapping back of views (see --mapbackViews and --mapbackModel), requires --disableFixedViews.")
	//protected boolean enableMapbackViews = false;

	//@Option(names = { "--mapbackViews" }, description = "define a view id (time point, view setup) onto which the registration result is mapped back onto, it needs to be one per independent registration subset (e.g. timepoint) (only works if no views are fixed), e.g. --mapbackView '0,0' (default: first view id)")
	//protected String[] mapbackViews = null;

	//@Option(names = { "--mapbackModel" }, description = "which transformation model to use for mapback if it is activated; TRANSLATION or RIGID (default: RIGID)")
	//protected MapbackModel mapbackModelEntry = MapbackModel.RIGID;

	@Override
	public Void call() throws Exception
	{
		initRegistrationParameters();

		if ( !this.setupParameters( dataGlobal, viewIdsGlobal ) )
			return null;

		final HashMap< ViewId, HashMap< String, Double > > labelMapGlobal;

		// setup specific things for Interestpoints or Stitching as a source
		if ( sourcePoints == SolverSource.IP )
		{
			if ( labels == null || labels.size() == 0 )
			{
				System.out.println( "No labels specified. Stopping." );
				return null;
			}

			if ( labelweights == null || labelweights.size() == 0 )
			{
				labelweights = new ArrayList<>();
				labels.forEach( label -> labelweights.add( 1.0 ));
			}

			if ( labelweights.size() != labels.size() )
			{
				System.out.println( "You need to specify as many weights as labels, or do not specify weights at all" );
				return null;
			}

			final HashMap< String, Double > map = new HashMap<>();

			for ( int i = 0; i < labels.size(); ++i )
				map.put( labels.get( i ), labelweights.get( i ) ); // weights are used for the solve

			System.out.println( "labels & weights: " + map);

			labelMapGlobal = SparkGeometricDescriptorMatching.buildLabelMap( dataGlobal, viewIdsGlobal, map );

			if ( groupIllums == null )
				groupIllums = false;

			if ( groupChannels == null )
				groupChannels = false;

			if ( groupTiles == null )
				groupTiles = false;

			if ( splitTimepoints == null )
				splitTimepoints = false;
		}
		else
		{
			System.out.println( "Using stitching results as source for solve." );

			labelMapGlobal = null;

			if ( groupIllums == null )
				groupIllums = true;

			if ( groupChannels == null )
				groupChannels = true;

			if ( groupTiles == null )
				groupTiles = false;

			if ( splitTimepoints == null )
				splitTimepoints = false;
		}

		System.out.println("The following grouping/splitting modes are set: ");
		System.out.println("groupIllums: " + groupIllums);
		System.out.println("groupChannels: " + groupChannels);
		System.out.println("groupTiles: " + groupTiles);
		System.out.println("splitTimepoints: " + splitTimepoints);

		// assemble fixed views
		final HashSet< ViewId > fixedViewIds;
		
		if ( this.disableFixedViews )
		{
			fixedViewIds = new HashSet<>();
		}
		else
		{
			if ( this.fixedViewIds == null || this.fixedViewIds.size() == 0 )
				fixedViewIds = assembleFixedAuto( viewIdsGlobal, dataGlobal.getSequenceDescription(), registrationTP, referenceTP ); // only TIMEPOINTS_INDIVIDUALLY and TO_REFERENCE_TIMEPOINT matter
			else
				fixedViewIds = new HashSet<>( this.fixedViewIds );
		}

		System.out.println("The following ViewIds are used as fixed views: ");
		fixedViewIds.forEach( vid -> System.out.print( Group.pvid( vid ) + ", ") );
		System.out.println();

		//Set< ViewId > fixedViewIds = assembleFixed( setup.getSubsets(), this.fixedViewIds, dataGlobal.getSequenceDescription());
		/*
		// setup mapback and fixed views
		final FixMapBackParameters fmbp = new FixMapBackParameters();
		fmbp.fixedViews = this.disableFixedViews ? new HashSet<>()
				: assembleFixed(setup.getSubsets(), this.fixedViewIds, dataGlobal.getSequenceDescription());
		fmbp.model = this.mapBackModel;
		fmbp.mapBackViews = this.enableMapbackViews
				? assembleMapBack(setup.getSubsets(), this.mapBackViewIds, dataGlobal.getSequenceDescription())
				: new HashMap<>();
		*/

		// run global optimization
		final Collection< Group< ViewId > > groups;

		if ( !groupTiles && !groupIllums && !groupChannels && !splitTimepoints )
			groups = new ArrayList<>();
		else // for grouping all we need here is the set of groups
			groups = AdvancedRegistrationParameters.getGroups( dataGlobal, viewIdsGlobal, groupTiles, groupIllums, groupChannels, splitTimepoints );

		final PointMatchCreator pmc;

		if ( sourcePoints == SolverSource.IP )
			pmc = setupPointMatchesFromInterestPoints(dataGlobal, viewIdsGlobal, labelMapGlobal, groups, fixedViewIds );//new InterestPointMatchCreator( pairs );
		else
			pmc = setupPointMatchesStitching(dataGlobal, viewIdsGlobal);

		if ( pmc == null )
		{
			System.out.println( "No views are connected, stopping." );
			return null;
		}

		final GlobalOptimizationParameters globalOptParameters = new GlobalOptimizationParameters(relativeThreshold, absoluteThreshold, globalOptType, false );
		final Collection< Pair< Group< ViewId >, Group< ViewId > > > removedInconsistentPairs = new ArrayList<>();
		final HashMap<ViewId, Tile > models;
		final Model<?> model = createModelInstance(transformationModel, regularizationModel, regularizationLambda);

		if ( globalOptParameters.method == GlobalOptType.ONE_ROUND_SIMPLE )
		{
			final ConvergenceStrategy cs = new ConvergenceStrategy( maxError, maxIterations, maxPlateauwidth );

			models = (HashMap)GlobalOpt.computeTiles(
							(Model)(Object)model,
							pmc,
							cs,
							fixedViewIds,
							groups );
		}
		else if ( globalOptParameters.method == GlobalOptType.ONE_ROUND_ITERATIVE )
		{
			models = (HashMap)GlobalOptIterative.computeTiles(
							(Model)(Object)model,
							pmc,
							new SimpleIterativeConvergenceStrategy( Double.MAX_VALUE, globalOptParameters.relativeThreshold, globalOptParameters.absoluteThreshold ),
							new MaxErrorLinkRemoval(),
							removedInconsistentPairs,
							fixedViewIds,
							groups );
		}
		else //if ( globalOptParameters.method == GlobalOptType.TWO_ROUND_SIMPLE || globalOptParameters.method == GlobalOptType.TWO_ROUND_ITERATIVE )
		{
			if ( globalOptParameters.method == GlobalOptType.TWO_ROUND_SIMPLE )
				globalOptParameters.relativeThreshold = globalOptParameters.absoluteThreshold  = Double.MAX_VALUE;

			models = (HashMap)GlobalOptTwoRound.computeTiles(
					(Model & Affine3D)(Object)model,
					pmc,
					new SimpleIterativeConvergenceStrategy( Double.MAX_VALUE, globalOptParameters.relativeThreshold, globalOptParameters.absoluteThreshold ), // if it's simple, both will be Double.MAX
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

		// update models in ViewRegistration
		if ( models == null || models.keySet().size() == 0 )
		{
			System.out.println( "No transformations could be found, stopping." );
			return null;
		}

		SimpleMultiThreading.threadWait( 100 );

		System.out.println( "\nFinal models: ");
		for ( final ViewId viewId : viewIdsGlobal )
		{
			final Tile< ? extends AbstractModel< ? > > tile = models.get( viewId );
			final ViewRegistration vr = dataGlobal.getViewRegistrations().getViewRegistration( viewId );

			TransformationTools.storeTransformation( vr, viewId, tile, null /*mapback*/, model.getClass().getSimpleName() );

			// TODO: We assume it is Affine3D here
			String output = Group.pvid( viewId ) + ": " + TransformationTools.printAffine3D( (Affine3D<?>)tile.getModel() );

			if ( tile.getModel() instanceof RigidModel3D )
				System.out.println( output + ", " + TransformationTools.getRotationAxis( (RigidModel3D)tile.getModel() ) );
			else
				System.out.println( output + ", " + TransformationTools.getScaling( (Affine3D<?>)tile.getModel() ) );
		}

		
		// get all timepoints
		//final List< TimePoint > timepointToProcess = SpimData2.getAllTimePointsSorted( dataGlobal, viewIdsGlobal );

		/*
		// identify groups/subsets
		final Set< Group< ViewId > > groups = arp.getGroups( dataGlobal, viewIdsGlobal, groupTiles, groupIllums, groupChannels );
		final PairwiseSetup< ViewId > setup = arp.pairwiseSetupInstance( this.registrationTP, viewIdsGlobal, groups );
		final OverlapDetection<ViewId> overlapDetection = GeometricDescriptorSpark.getOverlapDetection( dataGlobal, this.viewReg );
		GeometricDescriptorSpark.identifySubsets( setup, overlapDetection );


		// get the grouping parameters
		final GroupParameters gp = new GroupParameters();
		gp.grouping = InterestpointGroupingType.DO_NOT_GROUP;
		*/

		if (!dryRun)
		{
			System.out.println( "Saving resulting XML ... ");
			new XmlIoSpimData2().save( dataGlobal, xmlURI );
		}

		System.out.println( "Done.");
		return null;
	}

	public static ImageCorrelationPointMatchCreator setupPointMatchesStitching(
			final SpimData2 dataGlobal,
			final ArrayList< ViewId > viewIdsGlobal )
	{
		Collection< PairwiseStitchingResult< ViewId > > results = dataGlobal.getStitchingResults().getPairwiseResults().values();

		// filter bad hashes here
		final int numLinksBefore = results.size();
		results = results.stream().filter( psr -> 
		{
			final ViewId firstVidA = psr.pair().getA().getViews().iterator().next();
			final ViewId firstVidB = psr.pair().getB().getViews().iterator().next();
			final ViewRegistration vrA = dataGlobal.getViewRegistrations().getViewRegistration( firstVidA );
			final ViewRegistration vrB = dataGlobal.getViewRegistrations().getViewRegistration( firstVidB );
			final double hash = PairwiseStitchingResult.calculateHash( vrA, vrB );
			return psr.getHash() == hash;
		}).collect( Collectors.toList() );
		final int numLinksAfter = results.size();

		if (numLinksAfter != numLinksBefore)
		{
			System.out.println("Removed " + ( numLinksBefore - numLinksAfter ) + " of " + numLinksBefore + 
					" pairwise results because the underlying view registrations have changed.");
			System.out.println("Did you try to re-run the global optimization after aligning the dataset?");
			System.out.println("In that case, you can remove the latest transformation and try again.");
		}

		if (numLinksAfter < 1)
		{
			System.out.println( new Date(System.currentTimeMillis()) + ": no links remaining, stopping.");
			return null;
		}

		return new ImageCorrelationPointMatchCreator(results);
	}

	public static InterestPointMatchCreator setupPointMatchesFromInterestPoints(
			final SpimData2 dataGlobal,
			final ArrayList< ViewId > viewIdsGlobal,
			final Map< ViewId, ? extends Map< String, Double > > labelMap,
			final Collection< Group< ViewId > > groups,
			final HashSet< ViewId > fixedViewIds )
	{
		// load all interest points and correspondences
		System.out.println( "Loading all relevant interest points (in parallel) ... ");

		final ArrayList< Pair< ViewId, String > > ipsToLoad = new ArrayList<>();
		viewIdsGlobal.forEach( viewId -> labelMap.get( viewId ).forEach( (label, weight) -> ipsToLoad.add( new ValuePair<>( viewId, label ) ) ));

		ipsToLoad.parallelStream().forEach( info ->
		{
			dataGlobal.getViewInterestPoints().getViewInterestPointLists( info.getA() ).getInterestPointList( info.getB() ).getInterestPointsCopy();
			dataGlobal.getViewInterestPoints().getViewInterestPointLists( info.getA() ).getInterestPointList( info.getB() ).getCorrespondingInterestPointsCopy();
		});

		// extract all corresponding interest points for given ViewId's and label
		System.out.println( "Setting up all corresponding interest points ... ");

		final ArrayList< Pair< Pair< ViewId, ViewId >, PairwiseResult< ? > > > pairs = new ArrayList<>();

		for ( int i = 0; i < viewIdsGlobal.size() - 1; ++i )
A:			for ( int j = i+1; j < viewIdsGlobal.size(); ++j )
			{
				// order doesn't matter, saved symmetrically
				final ViewId vA = viewIdsGlobal.get( i );
				final ViewId vB = viewIdsGlobal.get( j );

				// both are fixed, no need to connect them
				if ( fixedViewIds.contains( vA ) && fixedViewIds.contains( vB ) )
				{
					System.out.println( "Not assigning " + Group.pvid( vA ) + " <> " + Group.pvid( vB ) + " becauase they are both fixed." );
					continue;
				}

				// both are part of the same group, no need to connect them
				for ( final Group< ViewId > group : groups )
					if ( group.contains( vA ) && group.contains( vB ) )
					{
						System.out.println( "Not assigning " + Group.pvid( vA ) + " <> " + Group.pvid( vB ) + " becauase they are part of the same group." );
						continue A;
					}

				final ViewRegistration vRegA = dataGlobal.getViewRegistrations().getViewRegistration( vA );
				final ViewRegistration vRegB = dataGlobal.getViewRegistrations().getViewRegistration( vB );

				vRegA.updateModel(); vRegB.updateModel();
				final AffineTransform3D mA = vRegA.getModel();
				final AffineTransform3D mB = vRegB.getModel();

				// iterate over all pairs of labels
				for ( final String labelA : labelMap.get( vA ).keySet() )
					for ( final String labelB : labelMap.get( vB ).keySet() )
					{
						final PairwiseResult< ? > pairResult = new PairwiseResult<>( false );
						final ArrayList inliers = new ArrayList<>();
		
						// set labels (required by InterestPointMatchCreator to assign weights)
						pairResult.setLabelA( labelA );
						pairResult.setLabelB( labelB );
		
						final List<CorrespondingInterestPoints> cpA = dataGlobal.getViewInterestPoints().getViewInterestPointLists( vA ).getInterestPointList( labelA ).getCorrespondingInterestPointsCopy();
						//List<CorrespondingInterestPoints> cpB = dataGlobal.getViewInterestPoints().getViewInterestPointLists( vB ).getInterestPointList( label ).getCorrespondingInterestPointsCopy();
		
						final List<InterestPoint> ipListA = dataGlobal.getViewInterestPoints().getViewInterestPointLists( vA ).getInterestPointList( labelA ).getInterestPointsCopy();
						final List<InterestPoint> ipListB = dataGlobal.getViewInterestPoints().getViewInterestPointLists( vB ).getInterestPointList( labelB ).getInterestPointsCopy();
		
						for ( final CorrespondingInterestPoints p : cpA )
						{
							if ( p.getCorrespodingLabel().equals( labelB ) && p.getCorrespondingViewId().equals( vB ) )
							{
								InterestPoint ipA = ipListA.get( p.getDetectionId() );
								InterestPoint ipB = ipListB.get( p.getCorrespondingDetectionId() );
		
								// we need to copy the array because it might not be bijective
								// (some points in one list might correspond with the same point in the other list)
								// which leads to the SpimData model being applied twice
								ipA = new InterestPoint( ipA.getId(), ipA.getL().clone() );
								ipB = new InterestPoint( ipB.getId(), ipB.getL().clone() );
		
								// transform the points
								mA.apply( ipA.getL(), ipA.getL() );
								mA.apply( ipA.getW(), ipA.getW() );
								mB.apply( ipB.getL(), ipB.getL() );
								mB.apply( ipB.getW(), ipB.getW() );
		
								inliers.add( new PointMatchGeneric<>( ipA, ipB ) );
							}
						}
		
						// set inliers
						if ( inliers.size() > 0 )
						{
							System.out.println( Group.pvid( vA ) + " <-> " + Group.pvid( vB ) + ": " + inliers.size() + " correspondences added." );
							pairResult.setInliers( inliers, 0.0 );
							pairs.add( new ValuePair<>( new ValuePair<>( vA, vB), pairResult)) ;
						}
					}
			}

		System.out.println( "Total number of pairs of views that are connected: " + pairs.size() );

		if ( pairs.size() > 0 )
			return new InterestPointMatchCreator( pairs, labelMap );
		else
			return null;
	}

	public static HashSet< ViewId > assembleFixedAuto(
			final ArrayList< ViewId > allViewIds,
			final SequenceDescription sd,
			final RegistrationType registrationTP,
			final int referenceTP )
	{
		final HashSet< ViewId > fixed = new HashSet<>();

		Collections.sort( allViewIds );

		if ( registrationTP == RegistrationType.TO_REFERENCE_TIMEPOINT )
		{
			for ( final ViewId viewId : allViewIds )
			{
				if ( viewId.getTimePointId() == referenceTP )
				{
					fixed.add( viewId );
					break;
				}
			}
		}
		else if ( registrationTP == RegistrationType.TIMEPOINTS_INDIVIDUALLY )
		{
			// it is sorted by timpoint
			fixed.add( allViewIds.get( 0 ) );
			int currentTp = allViewIds.get( 0 ).getTimePointId();

			for ( final ViewId viewId : allViewIds )
			{
				// next tp
				if ( viewId.getTimePointId() != currentTp )
				{
					fixed.add( viewId );
					currentTp = viewId.getTimePointId();
				}
			}
		}
		else
		{
			fixed.add( allViewIds.get( 0 ) ); // always the first view is fixed
		}

		return fixed;
	}

	public static HashMap< Subset< ViewId >, Pair< ViewId, Dimensions > > assembleMapBack(
			final ArrayList< Subset< ViewId > > subsets,
			final ArrayList< ViewId > mapBackViewIds,
			final SequenceDescription sd )
	{
		final HashMap< Subset< ViewId >, Pair< ViewId, Dimensions > > map = new HashMap<>();

		for ( final Subset< ViewId > subset : subsets )
		{
			ViewId mapBackView = null;

			// see if we have view specified for 
			if ( mapBackViewIds != null && mapBackViewIds.size() > 0 )
			{
				for ( final ViewId mbv : mapBackViewIds )
					if ( subset.contains( mbv ) )
					{
						mapBackView = mbv;
						break;
					}
			}

			// if none was found, use the first one
			if ( mapBackView == null )
			{
				mapBackView = Subset.getViewsSorted( subset.getViews() ).get( 0 );
			}

			final Dimensions mapBackViewDims = sd.getViewDescription( mapBackView ).getViewSetup().getSize();
			map.put( subset, new ValuePair< ViewId, Dimensions >( mapBackView, mapBackViewDims ) );
		}

		System.out.println("The following ViewIds are used for mapback: ");
		map.values().forEach( vid -> System.out.print( Group.pvid( vid.getA() ) + ", ") );
		System.out.println();

		return map;
	}

	public boolean setupParameters( final SpimData2 dataGlobal, final ArrayList< ViewId > viewIdsGlobal )
	{
		//if ( !disableFixedViews && enableMapbackViews )
		//	throw new IllegalArgumentException("You cannot use '--enableMapbackViews' without '--disableFixedViews'.");

		// fixed views and mapping back to original view
		if ( disableFixedViews )
		{
			/*
			if ( enableMapbackViews )
			{
				if ( mapbackViews == null || mapbackViews.length == 0 )
				{
					System.out.println( "First ViewId(s) will be used as mapback view for each respective registration subset (e.g. timepoint) ... ");

					this.mapBackViewIds = null;
				}
				else
				{
					// load mapback view
					System.out.println( "Parsing mapback ViewIds ... ");
	
					final ArrayList<ViewId> parsedViews = Import.getViewIds( mapbackViews ); // all views
					this.mapBackViewIds = Import.getViewIds( dataGlobal, parsedViews );
					System.out.println( "Warning: only " + mapBackViewIds.size() + " of " + parsedViews.size() + " that you specified for mapback views exist and are present.");
	
					if ( this.mapBackViewIds == null || this.mapBackViewIds.size() == 0 )
						throw new IllegalArgumentException( "Mapback views couldn't be parsed. Please provide valid mapsback views." );
	
					System.out.println("The following ViewIds are used for mapback: ");
					fixedViewIds.forEach( vid -> System.out.print( Group.pvid( vid ) + ", ") );
					System.out.println();
				}

				// load mapback model
				this.mapBackModel = (mapbackModelEntry == MapbackModel.TRANSLATION) ? new TranslationModel3D() : new RigidModel3D();

				System.out.println( "Mapback model=" + mapBackModel.getClass().getSimpleName() );
			}
			else
			{
				System.out.println( "No views are fixed and no mapping back is selected, i.e. the Views will more or less float in space (might be fine if desired)." );

				this.mapBackViewIds = null;
			}
			*/
			this.fixedViewIds = null;
		}
		else
		{
			// set/load fixed views
			if ( fixedViews == null || fixedViews.length == 0 )
			{
				System.out.println( "First ViewId(s) will be used as fixed for each respective registration subset (e.g. timepoint) ... ");

				this.fixedViewIds = null;
			}
			else
			{
				System.out.println( "Parsing fixed ViewIds ... ");

				final ArrayList<ViewId> parsedViews = Import.getViewIds( fixedViews ); // all views
				this.fixedViewIds = Import.getViewIds( dataGlobal, parsedViews );

				if ( parsedViews.size() != fixedViewIds.size() )
					System.out.println( "Warning: only " + fixedViewIds.size() + " of " + parsedViews.size() + " that you specified as fixed views exist and are present.");

				if ( this.fixedViewIds == null || this.fixedViewIds.size() == 0 )
					throw new IllegalArgumentException( "Fixed views couldn't be parsed. Please provide a valid fixed view." );

				System.out.println("The following ViewIds are fixed: ");
				fixedViewIds.forEach( vid -> System.out.print( Group.pvid( vid ) + ", ") );
				System.out.println();
			}

			//sthis.mapBackViewIds = null;
		}

		return true;
	}

	public static void main(final String... args) throws SpimDataException
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new Solver()).execute(args));
	}

}
