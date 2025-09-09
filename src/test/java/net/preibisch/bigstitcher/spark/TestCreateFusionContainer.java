package net.preibisch.bigstitcher.spark;

import java.io.File;

import org.apache.hadoop.fs.Path;

import net.preibisch.bigstitcher.spark.TestSparkResave.CloudProvider;
import picocli.CommandLine;

public class TestCreateFusionContainer
{
	public static String createFusionContainer( final String xml )
	{
		try
		{
			final Path parent = new Path( xml ).getParent();
			final String omeZarrPath = parent.toString() + File.separator + "fused.ome.zarr";

			System.out.println( "Creating fusion container in '" + omeZarrPath + "' for '" + xml + "'.");

			final String[] args = new String[] {
					"-x",
					xml,
					"-d",
					"UINT8",
					"--preserveAnisotropy",
					"--multiRes",
					"-o",
					omeZarrPath
			};

			new CommandLine(new CreateFusionContainer()).execute(args);

			return omeZarrPath;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return null;
		}
	}

	public static final void main(final String... args)
	{
		final String xml = TestSparkResave.copyCloudDataset( false, CloudProvider.S3 );
		//final String xml = "/var/folders/b0/sgyw0d9918vfjl96c5dlc3fw0000gp/T/2347377857383995071/dataset.xml";
		createFusionContainer( xml );
	}
}
