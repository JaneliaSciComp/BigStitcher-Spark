package net.preibisch.bigstitcher.spark;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import picocli.CommandLine;

public class TestSparkResave
{
	public static enum CloudProvider { S3, GC };

	public static String copyCloudDataset( final boolean deleteAfter, final CloudProvider cp )
	{
		try
		{
			final Path tmp = Files.createTempDirectory(null);

			if ( deleteAfter )
				tmp.toFile().deleteOnExit();

			final String xml = tmp.toString() + File.separator + "dataset.xml";
			final String cloud = (cp.equals(CloudProvider.S3)) ?
					"s3://janelia-bigstitcher-spark/Stitching/dataset.xml"
					: "gs://janelia-spark-test/I2K-test/dataset.xml";

			System.out.println( "Resaving " + cloud + " to " + xml );

			final String[] args = new String[] {
					"-x",
					cloud,
					"-xo",
					xml
			};

			new CommandLine(new SparkResaveN5()).execute(args);

			return xml;
		}
		catch (IOException e)
		{
			e.printStackTrace();
			return null;
		}
	}

	public static final void main(final String... args)
	{
		copyCloudDataset( true, CloudProvider.S3 );
	}
}
