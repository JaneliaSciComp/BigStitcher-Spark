package net.preibisch.bigstitcher.spark;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.RealInterval;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrectionPipeline;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrectionPipeline.Method;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrectionPipeline.Parameters;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrectionPipeline.RegularizationModel;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrectionPipeline.Threshold;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrectionPipeline.TransformationModel;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityThresholds;
import net.preibisch.mvrecon.process.fusion.intensity.ViewPairCoefficientMatchesIO;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import scala.Tuple2;
import util.URITools;

/**
 * Pairwise intensity matching of overlapping views, distributed: one Spark task per view for the LI / OTSU
 * threshold, one per pair for the matching, the matches written to {@code --outputPath} for
 * {@link IntensitySolver}. The steps themselves are multiview-reconstruction's {@link IntensityCorrectionPipeline},
 * which the GUI runs in memory.
 */
public class SparkIntensityMatching extends AbstractSelectableViews
{
	@Option(names = { "--numCoefficients" }, description = "number of coefficients per dimension (default: 8,8,8)")
	private String numCoefficientsString = "8,8,8";

	@Option(names = { "--renderScale" }, description = "at which scale to sample images (default: 0.25, which meaning using 4x downsampled images)")
	private double renderScale = 0.25;

	@Option(names = { "-o", "--outputPath" }, required = true, description = "path (URI) for saving pairwise intensity matches, e.g., file:/home/fused.n5/intensity/ or e.g. s3://myBucket/data.zarr/intensity/")
	private String outputPathURIString = null;

	@Option(names = { "--minThreshold" }, description = "min threshold for intensities to consider for matching, anything below this value will be discarded; either a fixed value, or LI or OTSU to compute it per view (at --renderScale) and use max(thresholdView1, thresholdView2) for each pair (default: 1)")
	private String minThresholdString = "1";

	@Option(names = { "--maxThreshold" }, description = "max threshold for intensities to consider for matching, anything above this value will be discarded (default: none)")
	private double maxIntensityThreshold = Double.NaN;

	@Option(names = { "--minNumCandidates" }, description = "minimum number of (non-discarded) overlapping pixels required to match overlapping coefficient regions (default: 1000)")
	private int minNumCandidates = 1000;

	@Option(names = {"--method"}, defaultValue = "RANSAC", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Method to match intensities between overlapping views: RANSAC, HISTOGRAM or MEDIAN (the ratio of the medians "
					+ "of the voxels both views see, a pure gain per pair; for a SCALE solve or solve-intensities --gainField)")
	private Method intensityMatchingMethod;

	@Option(names = { "--numBins" }, description = "number of quantile samples (bins) taken from each intensity distribution for histogram matching (default: 100, only for HISTOGRAM method)")
	private int numBins = 100;

	@Option(names = { "--bleachFactor" }, description = "fraction of intensity lost each time a voxel was imaged before: a voxel "
			+ "covered by n earlier views (same timepoint, earlier = lower setup id) is matched as v / (1 - bleachFactor)^n (default: 0 = no bleaching)")
	private double bleachFactor = 0;

	@Option(names = { "-tm", "--transformationModel" }, description = "which 1D transformation model to use for intensity matching; AFFINE, TRANSLATION, SCALE or IDENTITY (default: AFFINE)")
	private TransformationModel transformationModel = TransformationModel.AFFINE;

	@Option(names = { "-rm1", "--regularizationModel1" }, description = "first regularization model for the transformation model; NONE, AFFINE, TRANSLATION, SCALE or IDENTITY (default: TRANSLATION)")
	private RegularizationModel regularizationModel1 = RegularizationModel.TRANSLATION;

	@Option(names = { "--lambda1" }, description = "lambda [0..1] for the first regularization model (default: 0.01)")
	private double lambda1 = 0.01;

	@Option(names = { "-rm2", "--regularizationModel2" }, description = "second regularization model for the transformation model; NONE, AFFINE, TRANSLATION, SCALE or IDENTITY (default: IDENTITY)")
	private RegularizationModel regularizationModel2 = RegularizationModel.IDENTITY;

	@Option(names = { "--lambda2" }, description = "lambda [0..1] for the second regularization model (default: 0.01)")
	private double lambda2 = 0.01;

	@CommandLine.Option(names = { "--numIterations" }, description = "number of RANSAC iterations (default: 1000)")
	private int iterations = 1000;

	@CommandLine.Option(names = { "--maxEpsilon" }, description = "maximal allowed transfer error (default: 5.1, only for RANSAC method)")
	private double maxEpsilon = 0.02 * 255;

	@CommandLine.Option(names = { "--minInlierRatio" }, description = "minimal ratio of of inliers to number of candidates (default: 0.1, only for RANSAC method)")
	private double minInlierRatio = 0.1;

	@CommandLine.Option(names = { "--minNumInliers" }, description = "minimally required absolute number of inliers (default: 10, only for RANSAC method)")
	private int minNumInliers = 10;

	@CommandLine.Option(names = { "--maxTrust" }, description = "reject candidates with a cost larger than maxTrust * median cost (default: 3, only for RANSAC method)")
	private double maxTrust = 3.0;

	/** the command-line options as the pipeline's parameters */
	Parameters parameters()
	{
		final Parameters p = new Parameters().threshold( minThresholdString );
		p.method = intensityMatchingMethod;
		p.numCoefficients = Import.csvStringToIntArray( numCoefficientsString );
		p.renderScale = renderScale;
		p.maxIntensity = maxIntensityThreshold;
		p.minNumCandidates = minNumCandidates;
		p.numBins = numBins;
		p.bleachFactor = bleachFactor;
		p.model = transformationModel;
		p.regularization1 = regularizationModel1;
		p.lambda1 = lambda1;
		p.regularization2 = regularizationModel2;
		p.lambda2 = lambda2;
		p.ransacIterations = iterations;
		p.maxEpsilon = maxEpsilon;
		p.minInlierRatio = minInlierRatio;
		p.minNumInliers = minNumInliers;
		p.maxTrust = maxTrust;
		return p;
	}

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		// fails on the driver (e.g. an invalid lambda), not in every Spark task
		final Parameters p = parameters();
		p.validate();

		final SpimData2 dataGlobal = this.loadSpimData2();
		if ( dataGlobal == null )
			throw new IllegalArgumentException( "Couldn't load SpimData XMl project." );

		final List< ViewId > viewIds = loadViewIds( dataGlobal ).stream()
				.map( v -> new ViewId( v.getTimePointId(), v.getViewSetupId() ) ) // serializable
				.collect( Collectors.toList() );
		if ( viewIds.isEmpty() )
			throw new IllegalArgumentException( "No ViewIds found." );

		final java.net.URI xmlURI = this.xmlURI;
		final java.net.URI outputURI = URITools.toURI( outputPathURIString );

		System.out.println( "Intensity matching: " + p );
		new ViewPairCoefficientMatchesIO( outputURI ).writeCoefficientsSize( p.numCoefficients );

		final SparkConf conf = new SparkConf().setAppName( "SparkIntensityMatching" );
		if ( localSparkBindAddress )
		{
			conf.set( "spark.driver.bindAddress", "127.0.0.1" );
			conf.set( "spark.driver.host", "localhost" );
			org.apache.spark.util.Utils.setCustomHostname( "localhost" );
		}
		final JavaSparkContext sc = new JavaSparkContext( conf );
		sc.setLogLevel( "ERROR" );

		// on the driver: with Spark every task would load the XML, too slow for hundreds of ViewSetups
		System.out.println( "(" + new Date( System.currentTimeMillis() ) + "): computing bounds ... " );
		final Map< ViewId, RealInterval > viewBounds = IntensityCorrectionPipeline.bounds( dataGlobal, viewIds );
		final List< ViewId[] > pairs = IntensityCorrectionPipeline.overlappingPairs( viewIds, viewBounds );
		System.out.println( "(" + new Date( System.currentTimeMillis() ) + "): " + pairs.size() + " overlapping pairs" );

		final Map< ViewId, List< ViewId > > bleachers = IntensityCorrectionPipeline.bleachers( viewIds, viewBounds, p.bleachFactor );
		if ( p.bleachFactor > 0 )
			System.out.println( "Unbleaching with factor " + p.bleachFactor + "; views with earlier overlapping views: "
					+ bleachers.values().stream().filter( b -> !b.isEmpty() ).count() );

		// one threshold per view, each pair is then gated at the higher of the two
		final Map< ViewId, Double > viewThresholds;
		if ( p.threshold == Threshold.FIXED )
		{
			viewThresholds = null;
		}
		else
		{
			final List< ViewId > viewsToThreshold = pairs.stream().flatMap( Stream::of ).distinct().collect( Collectors.toList() );
			System.out.println( "(" + new Date( System.currentTimeMillis() ) + "): computing " + p.threshold
					+ " threshold for " + viewsToThreshold.size() + " views ... " );

			final JavaRDD< ViewId > viewRDD = sc.parallelize( viewsToThreshold, Math.min( Spark.maxPartitions, viewsToThreshold.size() ) );
			final Map< ViewId, double[] > levels = new HashMap<>( viewRDD.mapToPair( viewId -> new Tuple2<>( viewId,
					IntensityCorrectionPipeline.viewLevels( Spark.getSparkJobSpimData2( xmlURI ).getSequenceDescription().getImgLoader(), viewId, p ) ) ).collectAsMap() );

			viewThresholds = new HashMap<>();
			final Map< ViewId, Double > viewBackgrounds = new HashMap<>();
			levels.forEach( ( viewId, l ) -> {
				viewThresholds.put( viewId, l[ 0 ] );
				viewBackgrounds.put( viewId, l[ 1 ] );
			} );

			final double[] sorted = viewThresholds.values().stream().mapToDouble( Double::doubleValue ).sorted().toArray();
			System.out.println( String.format( "Per-view thresholds: min %.2f, median %.2f, max %.2f", sorted[ 0 ], sorted[ sorted.length / 2 ], sorted[ sorted.length - 1 ] ) );
			IntensityThresholds.write( outputURI, viewThresholds );
			IntensityThresholds.writeBackgrounds( outputURI, viewBackgrounds );
		}

		System.out.println( "(" + new Date( System.currentTimeMillis() ) + "): running ... " );
		final JavaRDD< ViewId[] > pairRDD = sc.parallelize( pairs, Math.max( 1, Math.min( Spark.maxPartitions, pairs.size() ) ) );
		pairRDD.foreach( pair -> {
			System.out.println( "(" + new Date( System.currentTimeMillis() ) + "): " + pair[ 0 ].getViewSetupId() + "<>" + pair[ 1 ].getViewSetupId() );
			new ViewPairCoefficientMatchesIO( outputURI ).write(
					IntensityCorrectionPipeline.matchPair( Spark.getSparkJobSpimData2( xmlURI ), pair[ 0 ], pair[ 1 ], viewThresholds, bleachers, p ) );
		} );

		// only now: a crashed job must NOT leave a directory that looks complete
		IntensityThresholds.writeDone( outputURI, p.toString() );

		sc.close();
		System.out.println( "(" + new Date( System.currentTimeMillis() ) + "): Done." );
		return null;
	}

	public static void main( final String... args ) throws SpimDataException
	{
		System.out.println( Arrays.toString( args ) );
		System.exit( new CommandLine( new SparkIntensityMatching() ).execute( args ) );
	}
}
