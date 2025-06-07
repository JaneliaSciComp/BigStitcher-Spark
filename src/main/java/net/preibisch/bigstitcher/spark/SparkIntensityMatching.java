package net.preibisch.bigstitcher.spark;

import static net.imglib2.util.Intervals.intersect;
import static net.imglib2.util.Intervals.isEmpty;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
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
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrection;
import net.preibisch.mvrecon.process.fusion.intensity.ViewPairCoefficientMatches;
import net.preibisch.mvrecon.process.fusion.intensity.ViewPairCoefficientMatchesIO;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import scala.Tuple2;

public class SparkIntensityMatching extends AbstractSelectableViews
{
	@Option(names = { "-c", "--numCoefficients" }, description = "... (default: 3.0)")
	protected Integer numCoefficients = 8;

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
		final double renderScale = 0.25; // TODO command line argument
		final int[] coefficientsSize = { 8, 8, 8 }; // TODO command line argument
		final URI outputURI = URI.create( "file:/Users/pietzsch/Desktop/matches_spark/" );  // TODO command line argument, URI, or path within dataset?

		final SparkConf conf = new SparkConf().setAppName("SparkIntensityMatching");

		if ( localSparkBindAddress )
			conf.set("spark.driver.bindAddress", "127.0.0.1");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final JavaRDD< ViewId > viewIdRDD = sc.parallelize( viewIdsGlobal, Math.min( Spark.maxPartitions, viewIdsGlobal.size() ) );
		final JavaPairRDD< ViewId, RealInterval > viewBoundsRDD = viewIdRDD.mapToPair( viewId -> {
			final SpimData2 dataLocal = Spark.getSparkJobSpimData2( xmlURI );
			final RealInterval bounds = IntensityCorrection.getBounds( dataLocal, viewId );
			return new Tuple2<>( viewId, IntensityCorrection.SerializableRealInterval.serializable( bounds ) );
		} );
		final Map< ViewId, RealInterval > viewBounds = viewBoundsRDD.collectAsMap();

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

		final JavaRDD< Tuple2< ViewId, ViewId > > viewPairRDD = sc.parallelize( viewIdPairsToMatch, Math.min( Spark.maxPartitions, viewIdPairsToMatch.size() ) );
		final JavaRDD< ViewPairCoefficientMatches > matchesRDD = viewPairRDD.map( views -> {
			final SpimData2 dataLocal = Spark.getSparkJobSpimData2( xmlURI );
			final ViewPairCoefficientMatches matches = IntensityCorrection.match( dataLocal, views._1(), views._2(), renderScale, coefficientsSize );
			final ViewPairCoefficientMatchesIO matchWriter = new ViewPairCoefficientMatchesIO(outputURI);
			matchWriter.write( matches );
			return matches;
		} );

		final List< ViewPairCoefficientMatches > pairwiseMatches = matchesRDD.collect();

		// save text files (multi-threaded)
//		matchesRDD.foreach( matches -> {
//			final ViewPairCoefficientMatchesIO matchWriter = new ViewPairCoefficientMatchesIO(outputURI);
//			matchWriter.write( matches );
//		} );

		sc.close();

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
