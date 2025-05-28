package net.preibisch.bigstitcher.spark;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class SparkIntensityMatching extends AbstractSelectableViews
{
	@Option(names = { "-c", "--numCoefficients" }, description = "... (default: 3.0)")
	protected Integer numCoefficients = 8;

	protected SpimData2 dataGlobal;
	protected ArrayList< ViewId > viewIdsGlobal;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		this.dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			throw new IllegalArgumentException( "Couldn't load SpimData XMl project." );

		this.viewIdsGlobal = this.loadViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			throw new IllegalArgumentException( "No ViewIds found." );

		final SparkConf conf = new SparkConf().setAppName("SparkIntensityMatching");

		if ( localSparkBindAddress )
			conf.set("spark.driver.bindAddress", "127.0.0.1");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final URI xmlURI = this.xmlURI;

		
		//final List<double[]> results = rddResults.collect();

		// save text files (multi-threaded)
		
		sc.close();

		return null;
	}

	public static void main(final String... args) throws SpimDataException
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new SparkIntensityMatching()).execute(args));
	}

}
