package net.preibisch.bigstitcher.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import mpicbg.models.AbstractModel;
import mpicbg.models.Affine3D;
import mpicbg.models.Model;
import mpicbg.models.RigidModel3D;
import mpicbg.models.Tile;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Dimensions;
import net.imglib2.multithreading.SimpleMultiThreading;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractInterestPointRegistration;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.legacy.mpicbg.PointMatchGeneric;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.global.GlobalOptimizationParameters;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.global.GlobalOptimizationParameters.GlobalOptType;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.RegistrationType;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.CorrespondingInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import net.preibisch.mvrecon.process.interestpointregistration.global.GlobalOpt;
import net.preibisch.mvrecon.process.interestpointregistration.global.GlobalOptIterative;
import net.preibisch.mvrecon.process.interestpointregistration.global.GlobalOptTwoRound;
import net.preibisch.mvrecon.process.interestpointregistration.global.convergence.ConvergenceStrategy;
import net.preibisch.mvrecon.process.interestpointregistration.global.convergence.SimpleIterativeConvergenceStrategy;
import net.preibisch.mvrecon.process.interestpointregistration.global.linkremoval.MaxErrorLinkRemoval;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.PointMatchCreator;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.strong.InterestPointMatchCreator;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.weak.MetaDataWeakLinkFactory;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.PairwiseResult;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.Subset;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.overlap.SimpleBoundingBoxOverlap;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class Solver extends AbstractInterestPointRegistration
{
	private static final long serialVersionUID = 5220898723968914742L;

	// TODO: support grouping tiles, channels
	final boolean groupTiles = false;
	final boolean groupIllums = false;
	final boolean groupChannels = false;
	final boolean groupTimePoints = false;

	//public enum MapbackModel { TRANSLATION, RIGID };

	public ArrayList< ViewId > fixedViewIds;
	//public ArrayList< ViewId > mapBackViewIds;
	//public Model<?> mapBackModel;

	@Option(names = { "--method" }, description = "global optimization method; ONE_ROUND_SIMPLE, ONE_ROUND_ITERATIVE, TWO_ROUND_SIMPLE or TWO_ROUND_ITERATIVE. Two round handles unconnected tiles, iterative handles wrong links (default: ONE_ROUND_SIMPLE)")
	protected GlobalOptType globalOptType = GlobalOptType.ONE_ROUND_SIMPLE;

	@Option(names = { "--relativeThreshold" }, description = "relative error threshold for iterative solvers, how many times worse than the average error a link needs to be (default: 3.5)")
	protected double relativeThreshold;

	@Option(names = { "--absoluteThreshold" }, description = "absoluted error threshold for iterative solver to drop a link in pixels (default: 7.0)")
	protected double absoluteThreshold;

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
		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIdsGlobal = this.loadViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			return null;

		if ( !this.setupParameters( dataGlobal, viewIdsGlobal ) )
			return null;

		if ( this.referenceTP == null )
			this.referenceTP = viewIdsGlobal.get( 0 ).getTimePointId();	
		else
		{
			final HashSet< Integer > timepointToProcess = 
					new HashSet<>( SpimData2.getAllTimePointsSorted( dataGlobal, viewIdsGlobal ).stream().mapToInt( tp -> tp.getId() ).boxed().collect(Collectors.toList()) );

			if ( !timepointToProcess.contains( referenceTP ) )
				throw new IllegalArgumentException( "Specified reference timepoint is not part of the ViewIds that are processed." );
		}

		// assemble fixed views
		final HashSet< ViewId > fixedViewIds;
		
		if ( this.disableFixedViews )
		{
			fixedViewIds = new HashSet<>();
		}
		else
		{
			if ( this.fixedViewIds == null || this.fixedViewIds.size() == 0 )
				fixedViewIds = assembleFixed( viewIdsGlobal, dataGlobal.getSequenceDescription(), registrationTP, referenceTP ); // only TIMEPOINTS_INDIVIDUALLY and TO_REFERENCE_TIMEPOINT matter
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

		// load all interest points and correspondences
		System.out.println( "Loading all relevant interest points ... ");
		for ( final ViewId viewId : viewIdsGlobal )
		{
			dataGlobal.getViewInterestPoints().getViewInterestPointLists( viewId ).getInterestPointList( label ).getInterestPointsCopy();
			dataGlobal.getViewInterestPoints().getViewInterestPointLists( viewId ).getInterestPointList( label ).getCorrespondingInterestPointsCopy();
		}

		// extract all corresponding interest points for given ViewId's and label
		System.out.println( "Setting up all corresponding interest points ... ");

		final ArrayList< Pair< Pair< ViewId, ViewId >, PairwiseResult< ? > > > pairs = new ArrayList<>();

		for ( int i = 0; i < viewIdsGlobal.size() - 1; ++i )
			for ( int j = i+1; j < viewIdsGlobal.size(); ++j )
			{
				// order doesn't matter, saved symmetrically
				final ViewId vA = viewIdsGlobal.get( i );
				final ViewId vB = viewIdsGlobal.get( j );

				final ViewRegistration vRegA = dataGlobal.getViewRegistrations().getViewRegistration( vA );
				final ViewRegistration vRegB = dataGlobal.getViewRegistrations().getViewRegistration( vB );

				vRegA.updateModel(); vRegB.updateModel();
				AffineTransform3D mA = vRegA.getModel();
				AffineTransform3D mB = vRegB.getModel();

				PairwiseResult< ? > pairResult = new PairwiseResult<>( false );
				List inliers = new ArrayList<>();

				List<CorrespondingInterestPoints> cpA = dataGlobal.getViewInterestPoints().getViewInterestPointLists( vA ).getInterestPointList( label ).getCorrespondingInterestPointsCopy();
				//List<CorrespondingInterestPoints> cpB = dataGlobal.getViewInterestPoints().getViewInterestPointLists( vB ).getInterestPointList( label ).getCorrespondingInterestPointsCopy();

				final List<InterestPoint> ipListA = dataGlobal.getViewInterestPoints().getViewInterestPointLists( vA ).getInterestPointList( label ).getInterestPointsCopy();
				final List<InterestPoint> ipListB = dataGlobal.getViewInterestPoints().getViewInterestPointLists( vB ).getInterestPointList( label ).getInterestPointsCopy();

				for ( final CorrespondingInterestPoints p : cpA )
				{
					if ( p.getCorrespodingLabel().equals( label ) && p.getCorrespondingViewId().equals( vB ) )
					{
						final InterestPoint ipA = ipListA.get( p.getDetectionId() );
						final InterestPoint ipB = ipListB.get( p.getCorrespondingDetectionId() );

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

		// run global optimization
		final ArrayList< Group< ViewId > > groups = new ArrayList<>();
		final PointMatchCreator pmc = new InterestPointMatchCreator( pairs );

		final GlobalOptimizationParameters globalOptParameters = new GlobalOptimizationParameters(relativeThreshold, absoluteThreshold, globalOptType, false );
		final Collection< Pair< Group< ViewId >, Group< ViewId > > > removedInconsistentPairs = new ArrayList<>();
		final HashMap<ViewId, Tile > models;
		final Model<?> model = createModelInstance();

		if ( globalOptParameters.method == GlobalOptType.ONE_ROUND_SIMPLE )
		{
			final ConvergenceStrategy cs = new ConvergenceStrategy( maxError, maxIterations, maxPlateauwidth );

			models = GlobalOpt.computeTiles(
							(Model)model,
							pmc,
							cs,
							fixedViewIds,
							groups );
		}
		else if ( globalOptParameters.method == GlobalOptType.ONE_ROUND_ITERATIVE )
		{
			models = GlobalOptIterative.computeTiles(
							(Model)model,
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

			models = GlobalOptTwoRound.computeTiles(
					(Model & Affine3D)model,
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
			new XmlIoSpimData2( null ).save( dataGlobal, xmlPath );

		System.out.println( "Done.");
		return null;
	}

	public static HashSet< ViewId > assembleFixed(
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

	public static HashSet< ViewId > assembleFixed(
			final ArrayList< Subset< ViewId > > subsets,
			final ArrayList< ViewId > fixedViewIds,
			final SequenceDescription sd )
	{
		final HashSet< ViewId > fixed = new HashSet<>();

		if ( fixedViewIds == null || fixedViewIds.size() == 0 )
			for ( final Subset< ViewId > subset : subsets )
				fixed.add( Subset.getViewsSorted( subset.getViews() ).get( 0 ) ); // always the first view is fixed
		else
			fixed.addAll( fixedViewIds ); // user-defined fixed views

		System.out.println("The following ViewIds are used as fixed views: ");
		fixed.forEach( vid -> System.out.print( Group.pvid( vid ) + ", ") );
		System.out.println();

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

	public static void main(final String... args)
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new Solver()).execute(args));
	}

}
