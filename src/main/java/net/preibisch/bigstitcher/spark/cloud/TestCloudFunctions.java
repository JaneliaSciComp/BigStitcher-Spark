package net.preibisch.bigstitcher.spark.cloud;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.*;
import org.janelia.saalfeldlab.n5.universe.N5Factory;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
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
public class TestCloudFunctions implements Callable<Void>
{
	@Option(names = "--repartition", description = "specify number of Spark partitions (note to set spark.dynamicAllocation.enabled=false on AWS), if set to 0 as many partitions as jobs will be created.")
	private Integer repartition = null;

	@Option(names = "--localSparkBindAddress", description = "specify Spark bind address as localhost")
	private boolean localSparkBindAddress = false;

	@Option(names = "--testAWSBucketAccess", description = "location for testing s3 reading/writing")
	private String testAWSBucketAccess = null;

	@Override
	public Void call() throws SpimDataException, IOException
	{
		System.out.println( "Starting AWS test  @ " + new Date( System.currentTimeMillis() ) );

		//Operating system name
		System.out.println("Your OS name -> " + System.getProperty("os.name"));

		//Operating system version
		System.out.println("Your OS version -> " + System.getProperty("os.version"));

		//Operating system architecture
		System.out.println("Your OS Architecture -> " + System.getProperty("os.arch"));

		if ( testAWSBucketAccess != null )
		{
			final String s3Region = new DefaultAwsRegionProviderChain().getRegion();
//			final KeyValueAccess kva = CloudUtil.getKeyValueAccessForBucket( "s3://aind-scratch-data/" );
//			http://s3-us-west-2.amazonaws.com/aind-scratch-data
//			final N5Reader n5r = new N5Factory().s3UseCredentials().s3Region(s3Region).openReader( N5Factory.StorageFormat.N5,  "s3://aind-scratch-data/" );
			final N5Reader n5r = new N5Factory().s3UseCredentials().openReader( N5Factory.StorageFormat.N5,  "https://s3-us-west-2.amazonaws.com/aind-scratch-data" );
			final KeyValueAccess kva = ((GsonKeyValueN5Reader)n5r).getKeyValueAccess();
	
			System.out.println( kva.exists( "/gabor.kovacs/Stitching/dataset.xml" ) );
//			CloudUtil.copy(kva, "/gabor.kovacs/Stitching/dataset.xml", "/gabor.kovacs/Stitching/dataset-2.xml" );
//
//			final BufferedReader reader = CloudUtil.openFileReadCloud(kva, "/gabor.kovacs/Stitching/dataset.xml" );
//			System.out.println( reader.lines().collect(Collectors.joining("\n") ).substring(0, 200) + " ... " );
//			reader.close();
	
			if ( kva.exists( "dataset-test.txt" ) )
				kva.delete( "dataset-test.txt" );
	
			final PrintWriter writer = CloudUtil.openFileWriteCloud( kva, "gabor.kovacs/dataset-test.txt" );
			writer.println( "test " + new Date( System.currentTimeMillis() ) );
			writer.close();
	
			//System.exit( 0 );
	
			System.out.println( "Creating N5 container @ " + new Date( System.currentTimeMillis() ) );
//			System.out.println( "Using s3Region " + s3Region );
			N5Writer w = new N5Factory().s3Region(s3Region).s3UseCredentials().createWriter( "s3://aind-scratch-data/gabor.kovacs/testcontainer_"+ System.currentTimeMillis() +".n5" );
//			N5Writer w = new N5Factory().createWriter( "https://s3-us-west-2.amazonaws.com/aind-scratch-data/gabor.kovacs/testcontainer_"+ System.currentTimeMillis() +".n5" );
//			N5Writer w = new N5Factory().openWriter( N5Factory.StorageFormat.ZARR, kva, "gabor.kovacs/testcontainer_"+ System.currentTimeMillis() +".n5" );
			w.createDataset( "test",
					new long[] { 128, 128, 128 },
					new int[] { 64,64,32},
					DataType.FLOAT32,
					new GzipCompression( 1 ) );
	
			//System.exit( 0 );
		}

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

			//testS3Write( "worker_"+ i[0] );
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
