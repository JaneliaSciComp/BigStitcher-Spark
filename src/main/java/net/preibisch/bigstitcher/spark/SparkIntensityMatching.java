package net.preibisch.bigstitcher.spark;

import static net.imglib2.util.Intervals.intersect;
import static net.imglib2.util.Intervals.isEmpty;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.RealInterval;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrection;
import net.preibisch.mvrecon.process.fusion.intensity.ViewPairCoefficientMatches;
import net.preibisch.mvrecon.process.fusion.intensity.ViewPairCoefficientMatchesIO;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import scala.Tuple2;
import util.URITools;

public class SparkIntensityMatching extends AbstractSelectableViews
{
	public enum IntensityMatchingMethod
	{
		RANSAC, HISTOGRAM
	}

	@Option(names = { "--numCoefficients" }, description = "number of coefficients per dimension (default: 8,8,8)")
	private String numCoefficientsString = "8,8,8";

	@Option(names = { "--renderScale" }, description = "at which scale to sample images (default: 0.25, which meaning using 4x downsampled images)")
	private double renderScale = 0.25;

	@Option(names = { "-o", "--outputPath" }, required = true, description = "path (URI) for saving pairwise intensity matches, e.g., file:/home/fused.n5/intensity/ or e.g. s3://myBucket/data.zarr/intensity/")
	private String outputPathURIString = null;

	@Option(names = { "--minThreshold" }, description = "min threshold for intensities to consider for matching, anything below this value will be discarded (default: 1)")
	private double minIntensityThreshold = 1;

	@Option(names = { "--maxThreshold" }, description = "max threshold for intensities to consider for matching, anything above this value will be discarded (default: none)")
	private double maxIntensityThreshold = Double.NaN;

	@Option(names = { "--minNumCandidates" }, description = "minimum number of (non-discarded) overlapping pixels required to match overlapping coefficient regions (default: 1000)")
	private int minNumCandidates = 1000;

	@Option(names = {"--method"}, defaultValue = "RANSAC", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Method to match intensities between overlapping views: RANSAC or HISTOGRAM")
	private IntensityMatchingMethod intensityMatchingMethod;

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

	private SpimData2 dataGlobal;

	private List< ViewId > viewIdsGlobal;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		this.dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			throw new IllegalArgumentException( "Couldn't load SpimData XMl project." );

		this.viewIdsGlobal = serializable( loadViewIds( dataGlobal ) );

		if ( viewIdsGlobal == null || viewIdsGlobal.isEmpty() )
			throw new IllegalArgumentException( "No ViewIds found." );


		// Global variables that need to be serialized for Spark as each job needs access to them
		final URI xmlURI = this.xmlURI;
		final double renderScale = this.renderScale;
		final int[] coefficientsSize = Import.csvStringToIntArray( numCoefficientsString );
		final URI outputURI = URITools.toURI( outputPathURIString );
		final double minIntensityThreshold = this.minIntensityThreshold;
		final double maxIntensityThreshold = this.maxIntensityThreshold;
		final int minNumCandidates = this.minNumCandidates;
		final int iterations = this.iterations;
		final double maxEpsilon = this.maxEpsilon;
		final double minInlierRatio = this.minInlierRatio;
		final int minNumInliers = this.minNumInliers;
		final double maxTrust = this.maxTrust;
		final IntensityMatchingMethod method = this.intensityMatchingMethod;

		new ViewPairCoefficientMatchesIO( outputURI ).writeCoefficientsSize( coefficientsSize );

		final SparkConf conf = new SparkConf().setAppName("SparkIntensityMatching");

		if ( localSparkBindAddress )
			conf.set("spark.driver.bindAddress", "127.0.0.1");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		System.out.println( "(" + new Date( System.currentTimeMillis() ) + "): computing bounds ... " );

		// doing this with Spark is too slow, since it requires to load the XML many times, which takes long for 415 ViewSetups
		/*
		final JavaRDD< ViewId > viewIdRDD = sc.parallelize( viewIdsGlobal, Math.min( Spark.maxPartitions, viewIdsGlobal.size() ) );
		final JavaPairRDD< ViewId, RealInterval > viewBoundsRDD = viewIdRDD.mapToPair( viewId -> {
			final SpimData2 dataLocal = Spark.getSparkJobSpimData2( xmlURI );
			final RealInterval bounds = IntensityCorrection.getBounds( dataLocal, viewId );
			return new Tuple2<>( viewId, IntensityCorrection.SerializableRealInterval.serializable( bounds ) );
		} );
		final Map< ViewId, RealInterval > viewBounds = viewBoundsRDD.collectAsMap();
		*/

		final Map< ViewId, RealInterval > viewBounds = new HashMap<ViewId, RealInterval>();

		viewIdsGlobal.forEach( viewId -> {
			final RealInterval bounds = IntensityCorrection.getBounds( dataGlobal, viewId );
			viewBounds.put(viewId, IntensityCorrection.SerializableRealInterval.serializable( bounds ) );
		} );

		System.out.println( "(" + new Date( System.currentTimeMillis() ) + "): computing view ids to match ... " );

		final List< Tuple2< ViewId, ViewId > > viewIdPairsToMatch = new ArrayList<>();
		final int numViewIds = viewIdsGlobal.size();
		for ( int i = 0; i < numViewIds; i++ )
		{
			final ViewId view0 = viewIdsGlobal.get( i );
			final RealInterval bounds0 = viewBounds.get( view0 );
			for ( int j = i + 1; j < numViewIds; j++ )
			{
				final ViewId view1 = viewIdsGlobal.get( j );
				final RealInterval bounds1 = viewBounds.get( view1 );
				if ( !isEmpty( intersect( bounds0, bounds1 ) ) )
				{
					viewIdPairsToMatch.add( new Tuple2<>( view0, view1 ) );
				}
			}
		}

		System.out.println( "(" + new Date( System.currentTimeMillis() ) + "): running ... " );

		final JavaRDD< Tuple2< ViewId, ViewId > > viewPairRDD = sc.parallelize( viewIdPairsToMatch, Math.min( Spark.maxPartitions, viewIdPairsToMatch.size() ) );
		viewPairRDD.foreach( views -> {
			final SpimData2 dataLocal = Spark.getSparkJobSpimData2( xmlURI );
			System.out.println( "(" + new Date( System.currentTimeMillis() ) + "): " + views._1().getViewSetupId() + "<>" + views._2().getViewSetupId() );

			final ViewPairCoefficientMatches matches;
			if ( method == IntensityMatchingMethod.RANSAC )
			{
				matches = IntensityCorrection.matchRansac( dataLocal, views._1(), views._2(), renderScale, coefficientsSize,
						minIntensityThreshold, maxIntensityThreshold, minNumCandidates, iterations, maxEpsilon, minInlierRatio, minNumInliers, maxTrust );
			}
			else // method == IntensityMatchingMethod.HISTOGRAM
			{
				matches = IntensityCorrection.matchHistograms( dataLocal, views._1(), views._2(), renderScale, coefficientsSize,
						minIntensityThreshold, maxIntensityThreshold, minNumCandidates );
			}
			final ViewPairCoefficientMatchesIO matchWriter = new ViewPairCoefficientMatchesIO(outputURI);
			matchWriter.write( matches );
		} );

		sc.close();

		System.out.println( "(" + new Date( System.currentTimeMillis() ) + "): Done.");

		return null;
	}

	static List< ViewId > serializable( final List< ? extends ViewId > list )
	{
		return list
				.stream()
				.map( v -> new ViewId( v.getTimePointId(), v.getViewSetupId() ) )
				.collect( Collectors.toList() );
	}

	public static void main(final String... args) throws SpimDataException
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new SparkIntensityMatching()).execute(args));
	}

}
