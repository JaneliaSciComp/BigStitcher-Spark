package net.preibisch.bigstitcher.spark.cloud;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import mpicbg.spim.data.SpimDataException;

public class TestDAL
{

	public static void main( String[] args ) throws SpimDataException
	{
		System.out.println( "Starting AWS-Spark test  @ " + new Date( System.currentTimeMillis() ) );

		final SparkConf conf = new SparkConf().setAppName("TestDAL");

		System.out.println( conf.get( "spark.master" ) + " @ " + new Date( System.currentTimeMillis() ) );

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final ArrayList< long[] > input = new ArrayList<>();
		for ( int i = 0; i < 1000; ++i )
			input.add( new long[] { i } );

		final JavaRDD<long[]> rdd = sc.parallelize( input );


		final JavaRDD< int[] > result =  rdd.map( i ->
		{
			System.out.println( "Processing: " + i[0] + " @ " + new Date( System.currentTimeMillis() ) );

			try
			{
				Thread.sleep( 1000 );
			}
			catch ( final InterruptedException e )
			{
				System.err.println( "Sleep - thread woken up: " + e );
			}

			System.out.println( "Done: " + i[0] + " @ " + new Date( System.currentTimeMillis() ) );

			return new int[] { (int)i[0] + 17 };
		});

		result.cache();
		result.count();

		List<int[]> r = result.collect();

		for ( final int[] i : r )
			System.out.println( i[0] + " @ " + new Date( System.currentTimeMillis() ) );

		sc.close();

		System.out.println( "Done ...  @ " + new Date( System.currentTimeMillis() ) );

		System.exit( 0 );
	}
}
