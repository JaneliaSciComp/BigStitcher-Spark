package net.preibisch.bigstitcher.spark.abstractcmdline;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import mpicbg.models.AffineModel3D;
import mpicbg.models.IdentityModel;
import mpicbg.models.InterpolatedAffineModel3D;
import mpicbg.models.Model;
import mpicbg.models.RigidModel3D;
import mpicbg.models.TranslationModel3D;
import mpicbg.spim.data.SpimData;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.SparkGeometricDescriptorRegistration.Method;
import net.preibisch.legacy.io.IOFunctions;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.AdvancedRegistrationParameters;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.OverlapType;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.RegistrationType;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
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
import picocli.CommandLine.Option;

public abstract class AbstractInterestPointRegistration extends AbstractSelectableViews
{
	private static final long serialVersionUID = 6435121614117716228L;

	public enum TransformationModel { TRANSLATION, RIGID, AFFINE };
	public enum RegularizationModel { NONE, IDENTITY, TRANSLATION, RIGID, AFFINE };

	@Option(names = { "-l", "--label" }, required = true, description = "label of the interest points used for registration (e.g. beads)")
	protected String label = null;

	@Option(names = { "-rtp", "--registrationTP" }, description = "time series registration type; TIMEPOINTS_INDIVIDUALLY (i.e. no registration across time), TO_REFERENCE_TIMEPOINT, ALL_TO_ALL or ALL_TO_ALL_WITH_RANGE (default: TIMEPOINTS_INDIVIDUALLY)")
	protected RegistrationType registrationTP = RegistrationType.TIMEPOINTS_INDIVIDUALLY;

	@Option(names = { "--referenceTP" }, description = "the reference timepoint if timepointAlign == REFERENCE (default: first timepoint)")
	protected Integer referenceTP = null;

	@Option(names = { "--rangeTP" }, description = "the range of timepoints if timepointAlign == ALL_TO_ALL_RANGE (default: 5)")
	protected Integer rangeTP = 5;

	
	@Option(names = { "--groupIllums" }, description = "group all illumination directions that belong to the same angle/channel/tile/timepoint together as one view, e.g. to stitch illums as one (default: false)")
	protected boolean groupIllums = false;

	@Option(names = { "--groupChannels" }, description = "group all channels that belong to the same angle/illumination/tile/timepoint together as one view, e.g. to stitch channels as one (default: false)")
	protected boolean groupChannels = false;

	@Option(names = { "--groupTiles" }, description = "group all tiles that belong to the same angle/channel/illumination/timepoint together as one view, e.g. to align across angles (default: false)")
	protected boolean groupTiles = false;

	@Option(names = { "--splitTimepoints" }, description = "group all angles/channels/illums/tiles that belong to the same timepoint as one View, e.g. for stabilization across time (default: false)")
	protected boolean splitTimepoints = false;

	
	@Option(names = { "-tm", "--transformationModel" }, description = "which transformation model to use; TRANSLATION, RIGID or AFFINE (default: AFFINE)")
	protected TransformationModel transformationModel = TransformationModel.AFFINE;

	@Option(names = { "-rm", "--regularizationModel" }, description = "which regularization model to use; NONE, IDENTITY, TRANSLATION, RIGID or AFFINE (default: RIGID)")
	protected RegularizationModel regularizationModel = RegularizationModel.RIGID;

	@Option(names = { "--lambda" }, description = "lamdba to use for regularization model (default: 0.1)")
	protected Double lambda = 0.1;

	protected SpimData2 dataGlobal;
	protected ArrayList< ViewId > viewIdsGlobal;

	public void initRegistrationParameters() throws SpimDataException
	{
		this.dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			throw new IllegalArgumentException( "Couldn't load SpimData XMl project." );

		this.viewIdsGlobal = this.loadViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			throw new IllegalArgumentException( "No ViewIds found." );

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
	}

	public PairwiseSetup< ViewId > setupGroups( final OverlapType viewReg )
	{
		final Set< Group< ViewId > > groupsGlobal = AdvancedRegistrationParameters.getGroups( dataGlobal, viewIdsGlobal, groupTiles, groupIllums, groupChannels, splitTimepoints );
		final PairwiseSetup< ViewId > setup = pairwiseSetupInstance( this.registrationTP, viewIdsGlobal, groupsGlobal, this.rangeTP, this.referenceTP );
		final OverlapDetection<ViewId> overlapDetection = getOverlapDetection( dataGlobal, viewReg );
		identifySubsets( setup, overlapDetection );

		return setup;
	}

	public Model< ? > createModelInstance()
	{
		// parse model
		final Model< ? > tm, rm;

		if ( transformationModel == TransformationModel.TRANSLATION )
			tm = new TranslationModel3D();
		else if ( transformationModel == TransformationModel.RIGID )
			tm = new RigidModel3D();
		else
			tm = new AffineModel3D();

		// parse regularizer
		if ( regularizationModel == RegularizationModel.NONE )
			rm = null;
		else if ( regularizationModel == RegularizationModel.IDENTITY )
			rm = new IdentityModel();
		else if ( regularizationModel == RegularizationModel.TRANSLATION )
			rm = new TranslationModel3D();
		else if ( regularizationModel == RegularizationModel.RIGID )
			rm = new RigidModel3D();
		else
			rm = new AffineModel3D();

		final Model< ? > model;

		if ( rm == null )
			return tm;
		else
			return new InterpolatedAffineModel3D( tm, rm, lambda );
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

}
