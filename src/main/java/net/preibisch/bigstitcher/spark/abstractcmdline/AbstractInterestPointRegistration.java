package net.preibisch.bigstitcher.spark.abstractcmdline;

import mpicbg.models.AffineModel3D;
import mpicbg.models.IdentityModel;
import mpicbg.models.InterpolatedAffineModel3D;
import mpicbg.models.Model;
import mpicbg.models.RigidModel3D;
import mpicbg.models.TranslationModel3D;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.RegistrationType;
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

	@Option(names = { "-tm", "--transformationModel" }, description = "which transformation model to use; TRANSLATION, RIGID or AFFINE (default: AFFINE)")
	protected TransformationModel transformationModel = TransformationModel.AFFINE;

	@Option(names = { "-rm", "--regularizationModel" }, description = "which regularization model to use; NONE, IDENTITY, TRANSLATION, RIGID or AFFINE (default: RIGID)")
	protected RegularizationModel regularizationModel = RegularizationModel.RIGID;

	@Option(names = { "--lambda" }, description = "lamdba to use for regularization model (default: 0.1)")
	protected Double lambda = 0.1;

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

}
