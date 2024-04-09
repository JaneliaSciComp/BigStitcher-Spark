package net.preibisch.bigstitcher.spark.cloud;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.opendal.Operator;

import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;

public class CloudUtil
{
	public static String aws_region= "us-east-1";
	public static String aws_endpoint = "https://s3.amazonaws.com";

	public static boolean exists( final Operator op, final String path )
	{
		try
		{
			op.read( path ).join();
			return true;
		}
		catch (Exception e )
		{
			return false;
		}
	}

	public static Pair< Map<String, String>, String > parseAWSS3Details( final String xmlPath )
	{
		System.out.println( "Parsing XML path for aws-s3 for '" + xmlPath + "':" );

		final File f = new File( xmlPath );
		String parent = f.getParent().replace( "//", "/" ); // new File cuts // already, but just to make sure
		parent = parent.substring(4, parent.length() );

		final String bucket, root;

		if (parent.contains( "/" ) )
		{
			// there is an extra path
			bucket = parent.substring(0,parent.indexOf( "/" ) );
			root = parent.substring(parent.indexOf( "/" ) + 1, parent.length() );
		}
		else
		{
			bucket = parent;
			root = "/";
		}

		final String xmlFile = f.getName();

		System.out.println( "bucket: '" + bucket + "'" );
		System.out.println( "root dir: '" + root + "'" );
		System.out.println( "xmlFile: '" + xmlFile + "'" );
		System.out.println( "region: '" + aws_region + "'" );
		System.out.println( "endpoint: '" + aws_endpoint + "'" );

		final Map<String, String> builder = new HashMap<>();

		builder.put("bucket", bucket );
		builder.put("root", root );
		builder.put("region", aws_region);
		builder.put("endpoint", aws_endpoint);

		return new ValuePair<>( builder, xmlFile );
	}
}
