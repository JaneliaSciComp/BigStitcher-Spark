package net.preibisch.bigstitcher.spark.abstractcmdline;

import java.util.ArrayList;
import java.util.Arrays;

import mpicbg.models.AffineModel3D;
import mpicbg.models.IdentityModel;
import mpicbg.models.InterpolatedAffineModel3D;
import mpicbg.models.Model;
import mpicbg.models.RigidModel3D;
import mpicbg.models.TranslationModel3D;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.InterestPointOverlapType;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.OverlapType;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.RegistrationType;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine.Option;

public abstract class AbstractInterestPointRegistration extends AbstractSelectableViews
{
	private static final long serialVersionUID = 6435121614117716228L;

	public enum TransformationModel { TRANSLATION, RIGID, AFFINE };
	public enum RegularizationModel { NONE, IDENTITY, TRANSLATION, RIGID, AFFINE };
	public enum MapbackModel { TRANSLATION, RIGID };

	public ArrayList< ViewId > fixedViewIds, mapBackViewIds;
	public Model<?> mapBackModel, model;

	@Option(names = { "-l", "--label" }, required = true, description = "label of the interest points used for registration (e.g. beads)")
	protected String label = null;

	@Option(names = { "-ipo", "--interestpointOverlap" }, description = "which interest points to use for pairwise registrations, use OVERLAPPING_ONLY  or ALL points (default: ALL)")
	protected InterestPointOverlapType interestpointOverlap = InterestPointOverlapType.ALL;

	@Option(names = { "-vr", "--viewReg" }, description = "which views to register with each other, compare OVERLAPPING_ONLY or ALL_AGAINST_ALL (default: OVERLAPPING_ONLY)")
	protected OverlapType viewReg = OverlapType.OVERLAPPING_ONLY;

	@Option(names = { "-rtp", "--registrationTP" }, description = "time series registration type; TIMEPOINTS_INDIVIDUALLY (i.e. no registration across time), TO_REFERENCE_TIMEPOINT, ALL_TO_ALL or ALL_TO_ALL_WITH_RANGE (default: TIMEPOINTS_INDIVIDUALLY)")
	protected RegistrationType registrationTP = RegistrationType.TIMEPOINTS_INDIVIDUALLY;

	@Option(names = { "--groupTP" }, description = "enable grouping of all views of each timepoint during timeseries registration, only works when any timeseries registration (-rtp) is active, (default: false)")
	protected Boolean groupTP = false;

	@Option(names = { "--referenceTP" }, description = "the reference timepoint if timepointAlign == REFERENCE (default: 0)")
	protected Integer referenceTP = 0;

	@Option(names = { "--rangeTP" }, description = "the range of timepoints if timepointAlign == ALL_TO_ALL_RANGE (default: 5)")
	protected Integer rangeTP = 5;


	@Option(names = { "-tm", "--transformationModel" }, description = "which transformation model to use; TRANSLATION, RIGID or AFFINE (default: AFFINE)")
	protected TransformationModel transformationModel = TransformationModel.AFFINE;

	@Option(names = { "-rm", "--regularizationModel" }, description = "which regularization model to use; NONE, IDENTITY, TRANSLATION, RIGID or AFFINE (default: RIGID)")
	protected RegularizationModel regularizationModel = RegularizationModel.AFFINE;

	@Option(names = { "--lambda" }, description = "lamdba to use for regularization model (default: 0.1)")
	protected Double lambda = 0.1;


	@Option(names = { "--disableFixedViews" }, description = "disable fixing of views (see --fixedViews)")
	protected boolean disableFixedViews = false;

	@Option(names = { "-fv", "--fixedViews" }, description = "define a list of (or a single) fixed view ids (time point, view setup), e.g. -fv '0,0' -fv '0,1' (default: first view id)")
	protected String[] fixedViews = null;

	@Option(names = { "--enableMapbackViews" }, description = "enable mapping back of views (see --mapbackViews and --mapbackModel), requires --disableFixedViews.")
	protected boolean enableMapbackViews = false;

	@Option(names = { "--mapbackViews" }, description = "define a view id (time point, view setup) onto which the registration result is mapped back onto, it needs to be one per independent registration subset (e.g. timepoint) (only works if no views are fixed), e.g. --mapbackView '0,0' (default: first view id)")
	protected String[] mapbackViews = null;

	@Option(names = { "--mapbackModel" }, description = "which transformation model to use for mapback if it is activated; TRANSLATION or RIGID (default: RIGID)")
	protected MapbackModel mapbackModelEntry = MapbackModel.RIGID;

	public boolean setupParameters( final SpimData2 dataGlobal, final ArrayList< ViewId > viewIdsGlobal )
	{
		if ( !disableFixedViews && enableMapbackViews )
			throw new IllegalArgumentException("You cannot use '--enableMapbackViews' without '--disableFixedViews'.");

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

		if ( rm == null )
		{
			model = tm;
			System.out.println( "Final model = " + model.getClass().getSimpleName() );
		}
		else
		{
			model = new InterpolatedAffineModel3D( tm, rm, lambda );
			System.out.println( "Final model = " + model.getClass().getSimpleName() + ", " + tm.getClass().getSimpleName() + " regularized with " + rm.getClass().getSimpleName() + " (lambda=" + lambda + ")" );
		}

		// fixed views and mapping back to original view
		if ( disableFixedViews )
		{
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
				System.out.println( "Warning: only " + fixedViewIds.size() + " of " + parsedViews.size() + " that you specified as fixed views exist and are present.");

				if ( this.fixedViewIds == null || this.fixedViewIds.size() == 0 )
					throw new IllegalArgumentException( "Fixed views couldn't be parsed. Please provide a valid fixed view." );

				System.out.println("The following ViewIds are fixed: ");
				fixedViewIds.forEach( vid -> System.out.print( Group.pvid( vid ) + ", ") );
				System.out.println();
			}

			this.mapBackViewIds = null;
		}

		return true;
	}

}
