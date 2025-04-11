package net.preibisch.bigstitcher.spark;

import net.preibisch.bigstitcher.spark.TestSparkResave.CloudProvider;
import picocli.CommandLine;

public class TestSparkAffineFusion
{
	public static void affineFusion( final String omeZarrPath )
	{
		try
		{
			System.out.println( "Fusing '" + omeZarrPath + "' ...");

			final String[] args = new String[] {
					"-o",
					omeZarrPath
			};

			new CommandLine(new SparkAffineFusion()).execute(args);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static final void main(final String... args)
	{
		final String xml = TestSparkResave.copyCloudDataset( true, CloudProvider.S3 );
		//final String xml = "/var/folders/b0/sgyw0d9918vfjl96c5dlc3fw0000gp/T/2347377857383995071/dataset.xml";

		final String omeZarr = TestCreateFusionContainer.createFusionContainer( xml );
		//final String omeZarr = "/var/folders/b0/sgyw0d9918vfjl96c5dlc3fw0000gp/T/2347377857383995071/fused.ome.zarr";

		affineFusion( omeZarr );
	}
}
