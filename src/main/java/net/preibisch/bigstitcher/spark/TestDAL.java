package net.preibisch.bigstitcher.spark;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.opendal.Operator;

/**
 * How to read/write: https://opendal.apache.org/docs/quickstart
 * How to use S3: https://opendal.apache.org/docs/services/s3
 * 
 */
public class TestDAL
{
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

	public static void awsS3()
	{
		final Map<String, String> builder = new HashMap<>();
		builder.put("root", "/Stitching");
		builder.put("bucket", "janelia-bigstitcher-spark" );
		builder.put("region", "us-east-1");
		builder.put("endpoint", "https://s3.amazonaws.com");

		final Operator op = Operator.of("s3", builder );

		byte[] b = op.read( "dataset.xml" ).join();
		System.out.println( new String(b, StandardCharsets.UTF_8));

		op.close();

	}

	public static void main( String[] args )
	{
		fileSystem();
		awsS3();
	}
}
