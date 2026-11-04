package net.preibisch.bigstitcher.spark;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import org.slf4j.LoggerFactory;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityThresholds;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.process.fusion.intensity.Coefficients;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrectionPipeline;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrectionPipeline.Parameters;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrectionPipeline.RegularizationModel;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrectionPipeline.TransformationModel;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrection;
import net.preibisch.mvrecon.process.fusion.intensity.ViewPairCoefficientMatches;
import net.preibisch.mvrecon.process.fusion.intensity.ViewPairCoefficientMatchesIO;
import picocli.CommandLine;
import util.URITools;

public class IntensitySolver extends AbstractSelectableViews {

	@CommandLine.Option(names = { "--numCoefficients" }, description = "number of coefficients per dimension (default: whatever match-intensities used, read from the matches)")
	private String numCoefficientsString = null;

	@CommandLine.Option(names = { "--matchesPath" }, required = true, description = "path (URI) for loading pairwise intensity matches, e.g., file:/home/fused.n5/intensity/ or e.g. s3://myBucket/data.zarr/intensity/")
	private String matchesPathURIString = null;

	@CommandLine.Option(names = { "--maxIterations" }, description = "max number of iterations for solve (default: 1000)")
	private Integer maxIterations = 1000;

	@CommandLine.Option(names = { "-o", "--intensityN5Path" }, required = true, description = "N5/ZARR/HDF5 base path for saving coefficients (e.g. s3://myBucket/coefficients.n5)")
	private String outputPathURIString = null;

	@CommandLine.Option(names = {"-s", "--intensityN5Storage"}, description = "output storage type, can be used to override guessed format (default: guess from n5Path file/directory-ending)")
	private StorageFormat storageType = null;

	@CommandLine.Option(names = { "--intensityN5Group" }, description = "group under which coefficient datasets are stored (default: \"\")")
	private String outputGroup = "";

	@CommandLine.Option(names = { "--intensityN5Dataset" }, description = "dataset name for each coefficient dataset (default: \"intensity\"). The coefficients for view(s,t) are stored in dataset \"{-n5Group}/setup{s}/timepoint{t}/{n5Dataset}\"")
	private String outputDataset = "intensity";

	@CommandLine.Option(names = { "-tm", "--transformationModel" }, description = "which 1D transformation model to use for intensity solving; AFFINE, TRANSLATION, SCALE or IDENTITY (default: AFFINE)")
	private TransformationModel transformationModel = TransformationModel.AFFINE;

	@CommandLine.Option(names = { "-rm1", "--regularizationModel1" }, description = "first regularization model for the transformation model; NONE, AFFINE, TRANSLATION, SCALE or IDENTITY (default: TRANSLATION)")
	private RegularizationModel regularizationModel1 = RegularizationModel.TRANSLATION;

	@CommandLine.Option(names = { "--lambda1" }, description = "lambda [0..1] for the first regularization model (default: 0.01)")
	private double lambda1 = 0.01;

	@CommandLine.Option(names = { "-rm2", "--regularizationModel2" }, description = "second regularization model for the transformation model; NONE, AFFINE, TRANSLATION, SCALE or IDENTITY (default: IDENTITY)")
	private RegularizationModel regularizationModel2 = RegularizationModel.IDENTITY;

	@CommandLine.Option(names = { "--lambda2" }, description = "lambda [0..1] for the second regularization model (default: 0.01)")
	private double lambda2 = 0.01;

	@CommandLine.Option(names = { "--minSignal" }, description = "ignore a pair's match when its matched intensities reach less than "
			+ "this far above either view's threshold (needs match-intensities --minThreshold LI/OTSU). In views without specimen the "
			+ "threshold splits background noise and the few counts above it would set the gain, e.g. 50 (default: 0, off)")
	private double minSignal = 0;

	@CommandLine.Option(names = { "--backgroundWeight" }, description = "add the two views' background levels (backgrounds.txt from "
			+ "match-intensities --minThreshold LI/OTSU) as one more match of this weight to every pair. The matches are otherwise all "
			+ "foreground, so with -tm AFFINE where the background lands is extrapolation; this keeps overlapping backgrounds level, "
			+ "e.g. 2 (default: 0, off)")
	private double backgroundWeight = 0;

	@CommandLine.Option(names = { "--gainField" }, description = "solve per-view gains plus one log-linear ramp fixed in every view's frame "
			+ "(spotlight's gain field) instead of --transformationModel; best with match-intensities --method MEDIAN (default: false)")
	private boolean gainField = false;

	@CommandLine.Option(names = { "--gainFieldLambda" }, description = "ridge on the log gains of the --gainField solve; it fixes the "
			+ "global level and damps views with few overlaps (default: 0.001)")
	private double gainFieldLambda = 0.001;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		// build the model first so an invalid lambda fails before any container is created
		final Parameters p = new Parameters();
		p.model = transformationModel;
		p.regularization1 = regularizationModel1;
		p.lambda1 = lambda1;
		p.regularization2 = regularizationModel2;
		p.lambda2 = lambda2;
		p.solverIterations = maxIterations;
		p.minSignal = minSignal;
		p.backgroundWeight = backgroundWeight;
		p.gainField = gainField;
		p.gainFieldLambda = gainFieldLambda;
		p.createModel();

		final URI outPathURI = URITools.toURI( outputPathURIString );
		System.out.println( "Writing coefficients to: " + outPathURI );

		if ( storageType == null )
		{
			if ( outputPathURIString.toLowerCase().endsWith( ".zarr" ) )
				storageType = StorageFormat.ZARR;
			else if ( outputPathURIString.toLowerCase().endsWith( ".n5" ) )
				storageType = StorageFormat.N5;
			else if ( outputPathURIString.toLowerCase().endsWith( ".h5" ) || outPathURI.toString().toLowerCase().endsWith( ".hdf5" ) )
				storageType = StorageFormat.HDF5;
			else
			{
				System.out.println( "Unable to guess format from URI '" + outPathURI + "', please specify using '-s'");
				return null;
			}

			System.out.println( "Guessed format " + storageType + " will be used to open URI '" + outPathURI + "', you can override it using '-s'");
		}
		else
		{
			System.out.println( "Format " + storageType + " will be used to open " + outPathURI );
		}

		final N5Writer n5Writer = N5Util.createN5Writer( outPathURI, storageType );


		final SpimData2 spimData = this.loadSpimData2();
		if ( spimData == null )
			throw new IllegalArgumentException( "Couldn't load SpimData XMl project." );

		final List< ViewId > views = loadViewIds( spimData );
		if ( views == null || views.isEmpty() )
			throw new IllegalArgumentException( "No ViewIds found." );

		final URI matchesURI = URITools.toURI( matchesPathURIString );

		final String matchingDone = IntensityThresholds.readDone( matchesURI );

		if ( matchingDone == null )
			throw new IllegalArgumentException( "No 'matching-done.txt' in " + matchesURI + ": match-intensities did not "
					+ "finish (it crashed, was killed, or predates this check). Any match files present are from an "
					+ "earlier run and solving them would silently give coefficients for the wrong parameters. "
					+ "Re-run match-intensities." );

		System.out.println( "Matches were computed with: " + matchingDone );

		final ViewPairCoefficientMatchesIO matchesIO = new ViewPairCoefficientMatchesIO( matchesURI );
		final int[] matchesCoefficientsSize = matchesIO.readCoefficientsSize();

		// the matches record what they were computed with, so default to that. Solving 1,1,1
		// matches as an 8,8,8 field leaves 511 of 512 sub-tiles per view unconstrained, and
		// fusion then interpolates that across the tile -- so a mismatch is an error, not a warning.
		final int[] coefficientsSize;

		if ( numCoefficientsString == null )
		{
			coefficientsSize = matchesCoefficientsSize;
			System.out.println( "numCoefficients (from the matches): " + Arrays.toString( coefficientsSize ) );
		}
		else
		{
			coefficientsSize = Import.csvStringToIntArray( numCoefficientsString );

			if ( !Arrays.equals( matchesCoefficientsSize, coefficientsSize ) )
				throw new IllegalArgumentException( "--numCoefficients " + Arrays.toString( coefficientsSize )
						+ " does not match the " + Arrays.toString( matchesCoefficientsSize ) + " the matches in " + matchesURI
						+ " were computed with. Omit --numCoefficients to use the stored value." );
		}

		// one directory listing, not a probe per possible pair (millions of opens for thousands of views)
		final List< ViewPairCoefficientMatches > pairwiseMatches = matchesIO.readAll( views );

		System.out.println( "Connected pairs: " + pairwiseMatches.size() );
		System.out.println( "Intensity solving model: " + transformationModel
				+ ", regularized by " + regularizationModel1 + " (lambda1=" + lambda1 + ")"
				+ " and " + regularizationModel2 + " (lambda2=" + lambda2 + ")" );
		final Map< ViewId, Double > thresholds = IntensityThresholds.read( matchesURI );
		// specific messages for what the matching did not write
		if ( minSignal > 0 && thresholds == null )
			throw new IllegalArgumentException( "--minSignal needs per-view thresholds (thresholds.txt in " + matchesURI
					+ "), i.e. match-intensities --minThreshold LI or OTSU." );

		final Map< ViewId, Double > backgrounds = backgroundWeight > 0 ? IntensityThresholds.readBackgrounds( matchesURI ) : null;
		if ( backgroundWeight > 0 && backgrounds == null )
			throw new IllegalArgumentException( "--backgroundWeight needs per-view backgrounds (backgrounds.txt in " + matchesURI
					+ "), written by match-intensities --minThreshold LI or OTSU; re-run it if the matches predate that." );

		if ( minSignal > 0 )
			System.out.println( "Ignoring matches less than " + minSignal + " above either view's threshold." );
		if ( backgroundWeight > 0 )
			System.out.println( "Adding each pair's background levels as a match of weight " + backgroundWeight + "." );

		// the matches were unbleached with this factor; fusion and the preview need it with the coefficients
		p.numCoefficients = coefficientsSize;
		p.bleachFactor = Parameters.bleachFactor( matchingDone );
		if ( p.bleachFactor > 0 )
			System.out.println( "Matches were unbleached with factor " + p.bleachFactor + ", storing it with the coefficients." );

		System.out.println( "Running solve... " );

		final Map< ViewId, Coefficients > coefficients = IntensityCorrectionPipeline.solve( spimData, pairwiseMatches, thresholds, backgrounds, p );

		if ( thresholds != null )
		{
			final long missing = coefficients.keySet().stream().filter( v -> !thresholds.containsKey( v ) ).count();
			if ( missing > 0 )
				System.out.println( "WARNING: " + missing + " of " + coefficients.size() + " views have no entry in thresholds.txt; "
						+ "those will be corrected everywhere even with --intensityMaskBelowThreshold." );
			System.out.println( "Stored per-view thresholds for " + ( coefficients.size() - missing ) + " views with the coefficients." );
		}

		IntensityCorrection.writeCoefficients( n5Writer, outputGroup, outputDataset, coefficients );

		System.out.println( "Done.");

		return null;
	}

	public static void main(final String... args) throws SpimDataException
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new IntensitySolver()).execute(args));
	}
}
