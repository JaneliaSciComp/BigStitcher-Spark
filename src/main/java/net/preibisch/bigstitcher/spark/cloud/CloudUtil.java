package net.preibisch.bigstitcher.spark.cloud;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;

import org.janelia.saalfeldlab.n5.GsonKeyValueN5Reader;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.universe.N5Factory;
import org.janelia.saalfeldlab.n5.universe.N5Factory.StorageFormat;

public class CloudUtil
{
	//public static String aws_region= "us-east-1";
	//public static String aws_endpoint = "https://s3.amazonaws.com";

	public static KeyValueAccess getKeyValueAccessForBucket( String bucketUri )
	{
		final N5Reader n5r = new N5Factory().openReader( StorageFormat.N5, bucketUri );
		final KeyValueAccess kva = ((GsonKeyValueN5Reader)n5r).getKeyValueAccess();

		return kva;
	}

	public static KeyValueAccess getKeyValueAccessForBucket( ParsedBucket pb )
	{
		final N5Reader n5r = new N5Factory().openReader( StorageFormat.N5, pb.protocol + pb.bucket );
		final KeyValueAccess kva = ((GsonKeyValueN5Reader)n5r).getKeyValueAccess();

		return kva;
	}

	public static class ParsedBucket
	{
		public String protocol;
		public String bucket;
		public String rootDir;
		public String file;
	}

	public static ParsedBucket parseCloudLink( final String uri )
	{
		System.out.println( "Parsing link path for '" + uri + "':" );

		final ParsedBucket pb = new ParsedBucket();

		final File f = new File( uri );
		String parent = f.getParent().replace( "//", "/" ); // new File cuts // already, but just to make sure
		parent = parent.replace(":/", "://" );
		pb.protocol = parent.substring( 0, parent.indexOf( "://" ) + 3 );
		parent = parent.substring( parent.indexOf( "://" ) + 3, parent.length() );

		if (parent.contains( "/" ) )
		{
			// there is an extra path
			pb.bucket = parent.substring(0,parent.indexOf( "/" ) );
			pb.rootDir = parent.substring(parent.indexOf( "/" ) + 1, parent.length() );
		}
		else
		{
			pb.bucket = parent;
			pb.rootDir = "/";
		}

		pb.file = f.getName();

		System.out.println( "protocol: '" + pb.protocol + "'" );
		System.out.println( "bucket: '" + pb.bucket + "'" );
		System.out.println( "root dir: '" + pb.rootDir + "'" );
		System.out.println( "xmlFile: '" + pb.file + "'" );

		return pb;
	}

	public static BufferedReader openFileReadCloud( final KeyValueAccess kva, final String file ) throws IOException
	{
		final InputStream is = kva.lockForReading( file ).newInputStream();
		return new BufferedReader(new InputStreamReader(is));
	}

	public static PrintWriter openFileWriteCloud( final KeyValueAccess kva, final String file ) throws IOException
	{
		final OutputStream os = kva.lockForWriting( file ).newOutputStream();
		return new PrintWriter( os );
	}

	public static void copy( final KeyValueAccess kva, final String src, final String dst ) throws IOException
	{
		final InputStream is = kva.lockForReading( src ).newInputStream();
		final OutputStream os = kva.lockForWriting( dst ).newOutputStream();

		final byte[] buffer = new byte[32768];
		int len;
		while ((len = is.read(buffer)) != -1)
			os.write(buffer, 0, len);

		is.close();
		os.close();
	}
}
