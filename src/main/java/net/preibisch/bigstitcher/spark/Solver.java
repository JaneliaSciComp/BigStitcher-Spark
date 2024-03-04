package net.preibisch.bigstitcher.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mpicbg.models.AbstractModel;
import mpicbg.models.Affine3D;
import mpicbg.models.Model;
import mpicbg.models.RigidModel3D;
import mpicbg.models.Tile;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.TimePoint;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Dimensions;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractGlobalOpt;
import net.preibisch.legacy.io.IOFunctions;
import net.preibisch.legacy.mpicbg.PointMatchGeneric;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.CorrespondingInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import net.preibisch.mvrecon.process.interestpointregistration.global.GlobalOpt;
import net.preibisch.mvrecon.process.interestpointregistration.global.convergence.ConvergenceStrategy;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.PointMatchCreator;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.strong.InterestPointMatchCreator;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.PairwiseResult;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.Subset;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;

public class Solver extends AbstractGlobalOpt
{
	private static final long serialVersionUID = 5220898723968914742L;

	// TODO: support grouping tiles, channels
	final boolean groupTiles = false;
	final boolean groupIllums = false;
	final boolean groupChannels = false;
	final boolean groupTimePoints = false;

	final double maxError = 5.0;

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

		// TODO: assemble fixed views
		// TODO: we need subsets for that 
		Set< ViewId > fixedViewIds = new HashSet<>();
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

		//models = (HashMap< ViewId, Tile< ? extends AbstractModel< ? > > >)(Object)GlobalOpt.compute( pairwiseMatching.getMatchingModel().getModel(), pmc, cs, fixedViews, groups );

		HashMap<ViewId, Tile > models;

		//if ( globalOptParameters.method == GlobalOptType.ONE_ROUND_SIMPLE )
		{
			final ConvergenceStrategy cs = new ConvergenceStrategy( maxError );

			models = GlobalOpt.computeTiles(
							(Model)this.model,
							pmc,
							cs,
							fixedViewIds,
							groups );
		}

		// TODO: update models in ViewRegistration
		if ( models == null || models.keySet().size() == 0 )
		{
			System.out.println( "No transformations could be found, stopping." );
			return null;
		}

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

		new XmlIoSpimData2( null ).save( dataGlobal, xmlPath );

		System.out.println( "Done.");
		return null;
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

	public static void main(final String... args)
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new Solver()).execute(args));
	}

}
