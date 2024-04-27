package net.preibisch.bigstitcher.spark.cloud;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.opendal.Entry;
import org.apache.opendal.Operator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import bdv.ViewerImgLoader;
import ij.ImageJ;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.generic.sequence.BasicImgLoader;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.multithreading.SimpleMultiThreading;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.explorer.ViewSetupExplorer;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsN5;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPointLists;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * How to read/write: https://opendal.apache.org/docs/quickstart
 * How to use S3: https://opendal.apache.org/docs/services/s3
 * GitHub: https://github.com/apache/opendal
 * Supported architectures: https://repo1.maven.org/maven2/org/apache/opendal/opendal-java/0.45.1/
 */
public class TestDAL implements Callable<Void>
{
	@Option(names = "--repartition", description = "specify number of Spark partitions (note to set spark.dynamicAllocation.enabled=false on AWS)")
	private Integer repartition = null;

	@Option(names = "--localSparkBindAddress", description = "specify Spark bind address as localhost")
	private boolean localSparkBindAddress = false;

	public static void fileSystem()
	{
		final Map<String, String> conf = new HashMap<>();
		conf.put("root", "/tmp");

		final Operator op = Operator.of("fs", conf );
		op.write( "hello2.txt", "hi" ).join();
		byte[] b = op.read( "hello2.txt" ).join();
		System.out.println( new String(b, StandardCharsets.UTF_8));
		
		op.close();
		
	}

	public static void awsS3() throws SpimDataException
	{
		final Map<String, String> builder = new HashMap<>();
		builder.put("root", "Stitching");
		builder.put("bucket", "janelia-bigstitcher-spark" );
		builder.put("region", "us-east-1");
		builder.put("endpoint", "https://s3.amazonaws.com");

		final Operator op = Operator.of("s3", builder );

		byte[] decodedBytes = op.read( "dataset.xml" ).join();
		System.out.println( new String(decodedBytes, StandardCharsets.UTF_8).substring( 0, 200 ) );

		System.out.println( "\ndataset.xml exists = " + CloudUtil.exists(op, "dataset.xml"));
		System.out.println( "dataset_2.xml exists = " + CloudUtil.exists(op, "dataset_2.xml"));

		System.out.println( "\nList of '" + builder.get( "root" ) + "'" );
		List<Entry> entries = op.list( "" ).join();
		entries.forEach( e -> System.out.println( e.getPath() + "\t" + e.metadata.getContentLength() ) );

		op.close();

		//SpimData2 data = Spark.getSparkJobSpimData2( "s3://janelia-bigstitcher-spark/Stitching/dataset.xml" );
		//System.out.println( data.getSequenceDescription().getViewSetupsOrdered().size() );
	}

	public static void testLoadInterestPoints() throws SpimDataException
	{
		final SpimData2 data = Spark.getSparkJobSpimData2( "s3://janelia-bigstitcher-spark/Stitching/dataset.xml" );

		System.out.println( "num viewsetups: " + data.getSequenceDescription().getViewSetupsOrdered().size() );

		final Map<ViewId, ViewInterestPointLists> ips = data.getViewInterestPoints().getViewInterestPoints();
		final ViewInterestPointLists ipl = ips.values().iterator().next();
		final InterestPoints ip = ipl.getHashMap().values().iterator().next();
		
		System.out.println("base dir: " + ip.getBaseDir() );
		System.out.println("base dir modified: " + InterestPointsN5.assembleURI( ip.getBaseDir(), InterestPointsN5.baseN5 ) );

		List<InterestPoint> ipList = ip.getInterestPointsCopy();

		System.out.println( "Loaded " + ipList.size() + " interest points.");

		System.out.println( "Saving s3://janelia-bigstitcher-spark/Stitching/dataset-save.xml ...");

		Spark.saveSpimData2( data, "s3://janelia-bigstitcher-spark/Stitching/dataset-save.xml" );

		System.out.println( "Done.");
	}

	public static void testBigStitcherGUI() throws SpimDataException
	{
		new ImageJ();

		final String xml = "s3://janelia-bigstitcher-spark/Stitching/dataset.xml";

		final SpimData2 data = Spark.getSparkJobSpimData2( xml );

		final BasicImgLoader imgLoader = data.getSequenceDescription().getImgLoader();
		if (imgLoader instanceof ViewerImgLoader)
			((ViewerImgLoader) imgLoader).setNumFetcherThreads(-1);

		final ViewSetupExplorer< SpimData2 > explorer = new ViewSetupExplorer<>( data, xml, new XmlIoSpimData2("") );

		explorer.getFrame().toFront();
	}

	public static void testS3Write( String fn )
	{
		final Map<String, String> builder = new HashMap<>();
		builder.put("root", "spark-logs");
		builder.put("bucket", "bigstitcher-spark-test" );
		builder.put("region", "us-east-1");
		builder.put("endpoint", "https://s3.amazonaws.com");

		final Operator op = Operator.of("s3", builder );
		op.write( fn + System.currentTimeMillis() + ".txt", "This is just a test" ).join();
		op.close();
	}

	@Override
	public Void call() throws SpimDataException
	{
		System.out.println( "Starting AWS test  @ " + new Date( System.currentTimeMillis() ) );

		//testS3Write( "test_" );

		System.out.println( "Starting AWS-Spark test @ " + new Date( System.currentTimeMillis() ) );

		final SparkConf conf = new SparkConf().setAppName("TestDAL");

		if (localSparkBindAddress)
			conf.set("spark.driver.bindAddress", "127.0.0.1");

		System.out.println( conf.get( "spark.master" ) );

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final ArrayList< long[] > input = new ArrayList<>();
		for ( int i = 0; i < 10; ++i )
			input.add( new long[] { i } );

		// EMR will try to optimize the partitions based on estimating what it'll take to process 
		// the data along with the cluster configuration.  In this case with 1000 items, it will
		// estimate that it will take 2 minutes to process each item.  So it will try to process
		// 1000 items in 2 minutes. For that reason it created 313 executors and made each 
		// executor process about 3 items. To override this behavior, you can set the number
		// of partitions to the number of executor by repartitioning the RDD.
		// I added the repartition for this purpose.

		JavaRDD<long[]> rdd = sc.parallelize( input ).repartition( 10 );

		System.out.println("RDD Number of Partitions: " + rdd.partitions().size());

		final JavaRDD< HashMap<String,Long> > result =  rdd.map( i ->
		{
			String executorId = SparkEnv.get().executorId();

			System.out.println("Executor ID: " + executorId);
			System.out.println("Processing: " + i[0] + " @ " + new Date( System.currentTimeMillis() ) );

			//testS3Write( "worker_"+ i[0] );
			try { Thread.sleep( 20 * 1000 ); }
			catch (InterruptedException e) { e.printStackTrace(); }

			System.out.println( "Done: " + i[0] + " @ " + new Date( System.currentTimeMillis() ) );

			HashMap<String,Long> map = new HashMap<>();
			map.put(executorId, i[0]);
			return map;
		});

		result.cache();
		result.count();
		List<HashMap<String, Long>> r = result.collect();

		Integer mapSize = r.size();
		System.out.println("Map size: " + mapSize);
		System.out.println("Map content: ");
		System.out.println("------------------");

		for (final HashMap<String, Long> i : r)
			System.out.println(i);

		sc.close();

		System.out.println( "Done ... ");

		
		//fileSystem();
		//awsS3();
		//testLoadInterestPoints();
		//testBigStitcherGUI();

		return null;
	}

	public static void main( String[] args )
	{
		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new TestDAL()).execute(args));
	}
}
