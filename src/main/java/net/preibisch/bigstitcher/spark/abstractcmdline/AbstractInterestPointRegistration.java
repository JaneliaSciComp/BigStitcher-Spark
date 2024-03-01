package net.preibisch.bigstitcher.spark.abstractcmdline;

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
	private MapbackModel mapbackModel = MapbackModel.RIGID;

	/*
	@Override
	public Void call() throws Exception
	{
		final SpimData2 dataGlobal = Spark.getSparkJobSpimData2("", xmlPath);

		Import.validateInputParameters(vi, angleIds, channelIds, illuminationIds, tileIds, timepointIds);

		// select views to process
		ArrayList< ViewId > viewIdsGlobal =
				Import.createViewIds(
						dataGlobal, vi, angleIds, channelIds, illuminationIds, tileIds, timepointIds);

		if ( viewIdsGlobal.size() == 0 )
		{
			throw new IllegalArgumentException( "No views to fuse." );
		}
		else
		{
			System.out.println( "For the following ViewIds interest point-based registratiojn will be performed: ");
			for ( final ViewId v : viewIdsGlobal )
				System.out.print( "[" + v.getTimePointId() + "," + v.getViewSetupId() + "] " );
			System.out.println();
		}

		return null;
	}

	public static void main(final String... args)
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new AbstractInterestPointRegistration()).execute(args));
	}
	*/
}
