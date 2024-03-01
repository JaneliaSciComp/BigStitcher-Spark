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
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine.Option;

public abstract class AbstractInterestPointRegistration extends AbstractSelectableViews
{
	private static final long serialVersionUID = 6435121614117716228L;

	public enum TimepointAlign { INDIVIDUAL, REFERENCE, ALL_TO_ALL, ALL_TO_ALL_RANGE };
	public enum ViewAlign { OVERLAPPING, ALL_TO_ALL };
	public enum InterestPointUsage { OVERLAPPING, ALL };
	
	public enum TransformationModel { TRANSLATION, RIGID, AFFINE };
	public enum RegularizationModel { NONE, IDENTITY, TRANSLATION, RIGID, AFFINE };
	public enum MapbackModel { TRANSLATION, RIGID };

	// TODO: support grouping tiles, channels

	@Option(names = { "-l", "--label" }, required = true, description = "label of the interest points used for registration (e.g. beads)")
	private String label = null;

	@Option(names = { "-ipu", "--interestpointUsage" }, description = "which interest points to use for pairwise registrations, use only OVERLAPPING points (only makes sense in combination with ViewAlign.OVERLAPPING) or ALL points (default: ALL)")
	private InterestPointUsage interestpointUsage = InterestPointUsage.ALL;

	@Option(names = { "-vr", "--viewReg" }, description = "which views to register with each other, compare OVERLAPPING or ALL_TO_ALL (default: OVERLAPPING)")
	private ViewAlign viewAlign = ViewAlign.OVERLAPPING;

	@Option(names = { "-rtp", "--registrationTP" }, description = "time series registration type; INDIVIDUAL (i.e. no registration across time), REFERENCE, ALL_TO_ALL or ALL_TO_ALL_RANGE (default: INDIVIDUAL)")
	private TimepointAlign registrationTP = TimepointAlign.INDIVIDUAL;

	@Option(names = { "--groupTP" }, description = "enable grouping of all views of each timepoint during timeseries registration, only works when any timeseries registration (-rtp) is active, (default: false)")
	private Boolean groupTP = false;

	@Option(names = { "--referenceTP" }, description = "the reference timepoint if timepointAlign == REFERENCE (default: 0)")
	private Integer referenceTP = 0;

	@Option(names = { "--rangeTP" }, description = "the range of timepoints if timepointAlign == ALL_TO_ALL_RANGE (default: 5)")
	private Integer rangeTP = 5;


	@Option(names = { "-tm", "--transformationModel" }, description = "which transformation model to use; TRANSLATION, RIGID or AFFINE (default: AFFINE)")
	private TransformationModel transformationModel = TransformationModel.AFFINE;

	@Option(names = { "-rm", "--regularizationModel" }, description = "which regularization model to use; NONE, IDENTITY, TRANSLATION, RIGID or AFFINE (default: RIGID)")
	private RegularizationModel regularizationModel = RegularizationModel.AFFINE;

	@Option(names = { "--lambda" }, description = "lamdba to use for regularization model (default: 0.1)")
	private Double lambda = 0.1;


	@Option(names = { "--noFixedViews" }, description = "disable fixing of views (see --fixedViews)")
	protected boolean noFixedViews = false;

	@Option(names = { "-fv", "--fixedViews" }, description = "define a list of (or a single) fixed view ids (time point, view setup), e.g. -fv '0,0' -fv '0,1' (default: first view id)")
	protected String[] fixedViews = null;

	@Option(names = { "--mapbackView" }, description = "define a view id (time point, view setup) onto which the registration result is mapped back onto (only works if no views are fixed), e.g. --mapbackView '0,0' (default: null)")
	protected String mapbackView = null;

	@Option(names = { "--mapbackModel" }, description = "which transformation model to use for mapback if it is activated; TRANSLATION or RIGID (default: RIGID)")
	private MapbackModel mapbackModelEntry = MapbackModel.RIGID;

	public ArrayList< ViewId > fixedViewIds;
	public ViewId mapBackViewId;
	public Model<?> mapBackModel, model;

	public boolean setupParameters( final SpimData2 dataGlobal, final ArrayList< ViewId > viewIdsGlobal )
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
		if ( noFixedViews )
		{
			if ( mapbackView != null )
			{
				// load mapback view
				mapBackViewId = Import.getViewIds(dataGlobal, new ArrayList<>( Arrays.asList( Import.getViewId( mapbackView ) ) ) ).get( 0 );

				// load mapback model
				mapBackModel = (mapbackModelEntry == MapbackModel.TRANSLATION) ? new TranslationModel3D() : new RigidModel3D();

				System.out.println( "Mapback viewid = " + Group.pvid( mapBackViewId ) + " type=" + mapBackModel.getClass().getSimpleName() );
			}
			else
			{
				System.out.println( "No views are fixed and no mapping back is selected, i.e. the Views will more or less float in space (might be fine if desired)." );
			}
		}
		else
		{
			// set/load fixed views
			if ( fixedViews == null )
			{
				System.out.println( "Setting first ViewId as fixed ... ");

				fixedViewIds = new ArrayList<>( Arrays.asList( viewIdsGlobal.get( 0 ) ) );
			}
			else
			{
				System.out.println( "Parsing fixed ViewIds ... ");

				final ArrayList<ViewId> parsedViews = Import.getViewIds( vi ); // all views
				this.fixedViewIds = Import.getViewIds( dataGlobal, parsedViews );
				System.out.println( "Warning: only " + fixedViewIds.size() + " of " + parsedViews.size() + " that you specified as fixed views exist and are present.");

				if ( this.fixedViewIds == null || this.fixedViewIds.size() == 0 )
					throw new IllegalArgumentException( "Fixed views couldn't be parsed. Please provide a valid fixed view." );

				System.out.println("The following ViewIds are fixed: ");
				fixedViewIds.forEach( vid -> System.out.print( Group.pvid( mapBackViewId ) + ", ") );
				System.out.println();
			}
		}

		return true;
	}

}
