package net.preibisch.bigstitcher.spark.cloud;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;

import mpicbg.spim.data.SpimDataException;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

public class TestCloudFunctions implements Callable<Void>
{
	@Option(names = "--repartition", description = "specify number of Spark partitions (note to set spark.dynamicAllocation.enabled=false on AWS), if set to 0 as many partitions as jobs will be created.")
	private Integer repartition = null;

	@Option(names = "--localSparkBindAddress", description = "specify Spark bind address as localhost")
	private boolean localSparkBindAddress = false;

	@Option(names = "--testBucketWriting", description = "location for testing s3/gc reading/writing, e.g. s3://mybucket/ or gc://mybucket/")
	private String testBucketWriting = null;

	@Override
	public Void call() throws SpimDataException, IOException
	{
		System.out.println( "Starting Cloud processing, reading & writing test  @ " + new Date( System.currentTimeMillis() ) );

		//Operating system name
		System.out.println("Your OS name -> " + System.getProperty("os.name"));

		//Operating system version
		System.out.println("Your OS version -> " + System.getProperty("os.version"));

		//Operating system architecture
		System.out.println("Your OS Architecture -> " + System.getProperty("os.arch"));

		// Test reading
		System.out.println( "Test reading XML from Janelia ..." );

		final String readBucket = "s3://janelia-bigstitcher-spark/";
		final String readDataset = "Stitching/dataset.xml";

		System.out.println( "Trying to get KeyValueAccess for " + URI.create( readBucket ) );
		URITools.s3Region = "us-east-1";
		final KeyValueAccess kva = URITools.getKeyValueAccess( URI.create( readBucket ) );

		System.out.print( "Does " + readDataset + " exist? " );
		System.out.println( kva.exists( readBucket + readDataset ) );

		if ( kva.exists( readBucket + readDataset ) )
		{
			final BufferedReader reader = URITools.openFileReadCloudReader( kva, URI.create( readBucket + readDataset ) );
			System.out.println( reader.lines().collect(Collectors.joining("\n") ).substring(0, 500) + " ... [cut off] " );
			reader.close();	
		}

		if ( testBucketWriting != null )
		{
			if ( !testBucketWriting.endsWith( "/") )
				testBucketWriting = testBucketWriting + "/";

			URITools.s3Region = null;

			System.out.println( "Trying to get KeyValueAccess for " + URI.create( testBucketWriting ) );
			final KeyValueAccess kvaWrite = URITools.getKeyValueAccess( URI.create( testBucketWriting ) );

			final long timeStamp = System.currentTimeMillis();
			final String testFile = "dataset-test" + timeStamp + ".txt";

			System.out.println( "Writing random file: " + URI.create( testBucketWriting + testFile ) );
			final PrintWriter writer = URITools.openFileWriteCloudWriter( kvaWrite, URI.create( testBucketWriting + testFile ) );
			writer.println( "test " + new Date( System.currentTimeMillis() ) );
			writer.close();

			System.out.println( "Creating N5 container " + timeStamp + ".n5" );
			final N5Writer n5writer = URITools.instantiateN5Writer( StorageFormat.N5, URI.create( testBucketWriting + timeStamp +".n5" ) );

			n5writer.createDataset( "test",
					new long[] { 128, 128, 128 },
					new int[] { 64,64,32},
					DataType.FLOAT32,
					new GzipCompression( 1 ) );

			n5writer.close();

			System.out.println( "Done with test bucket writing ..." );
		}
		else
		{
			System.out.println( "testBucketWriting is null, not testing writing..." );
		}

		System.out.println( "Starting SPARK test @ " + new Date( System.currentTimeMillis() ) );

		final SparkConf conf = new SparkConf().setAppName("TestDAL");

		if (localSparkBindAddress)
			conf.set("spark.driver.bindAddress", "127.0.0.1");

		System.out.println( conf.get( "spark.master" ) );

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final ArrayList< long[] > input = new ArrayList<>();
		for ( int i = 0; i < 1000; ++i )
			input.add( new long[] { i } );

		// EMR will try to optimize the partitions based on estimating what it'll take to process 
		// the data along with the cluster configuration.  In this case with 1000 items, it will
		// estimate that it will take 2 minutes to process each item.  So it will try to process
		// 1000 items in 2 minutes. For that reason it created 313 executors and made each 
		// executor process about 3 items. To override this behavior, you can set the number
		// of partitions to the number of executor by repartitioning the RDD.
		// I added the repartition for this purpose.

		JavaRDD<long[]> rdd = sc.parallelize( input );

		if ( repartition != null )
		{
			if ( repartition > 0 )
				rdd = rdd.repartition( repartition );
			else if ( repartition == 0 )
				rdd = rdd.repartition( input.size() );
		}

		System.out.println("RDD Number of Partitions: " + rdd.partitions().size());

		final JavaRDD< HashMap<String,Long> > result =  rdd.map( i ->
		{
			String executorId = SparkEnv.get().executorId();

			System.out.println("Executor ID: " + executorId);
			System.out.println("Processing: " + i[0] + " @ " + new Date( System.currentTimeMillis() ) );

			try { Thread.sleep( 2 * 1000 ); }
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

		System.out.println( "Done @ " + new Date( System.currentTimeMillis() ) );

		return null;
	}

	public static void main( String[] args )
	{
		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new TestCloudFunctions()).execute(args));
	}
}