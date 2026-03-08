/*-
 * #%L
 * Spark-based parallel BigStitcher project.
 * %%
 * Copyright (C) 2021 - 2026 Developers.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */
package net.preibisch.bigstitcher.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.imglib2.Interval;
import net.preibisch.mvrecon.SimulateUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.tests.TestBoundingBox;
import net.preibisch.mvrecon.tests.TestInterestPointDetection;
import net.preibisch.mvrecon.tests.TestRegistration;
import picocli.CommandLine;
import util.URITools;

/**
 * Tests Spark-based fusion container creation using different storage formats.
 *
 * This test creates a simulated dataset, runs interest point detection and registration,
 * then tests CreateFusionContainer for various output formats:
 * - Zarr v2 (no sharding)
 * - Zarr v3 with sharding
 * - N5
 * - HDF5 (commented out - currently not working)
 */
public class TestSparkFusion
{
	@TempDir
	Path tempDir;

	private SpimData2 spimData;
	private Interval boundingBox;
	private File xmlFile;

	@BeforeEach
	public void setUp() throws Exception
	{
		// Set Spark master URL for local execution
		System.setProperty( "spark.master", "local[*]" );

		// 1. Create simulated dataset (3 views, 200 beads)
		spimData = SimulateUtil.setUp();

		// 2. Run interest point detection
		TestInterestPointDetection.testDoG( spimData, "beads" );

		// 3. Run registration
		TestRegistration.testRegistration( spimData, "beads", false );

		// 4. Compute bounding box
		boundingBox = TestBoundingBox.testBoundingBox( spimData, false );

		// 5. Save as XML
		xmlFile = tempDir.resolve( "dataset.xml" ).toFile();
		new XmlIoSpimData2().save( spimData, xmlFile.toURI() );

		System.out.println( "Saved test dataset to: " + xmlFile.getAbsolutePath() );
		System.out.println( "Bounding box: " + boundingBox );
	}

	@Test
	public void testCreateContainerZarrV2() throws Exception
	{
		final File outputPath = tempDir.resolve( "fused_v2.zarr" ).toFile();

		System.out.println( "\n=== Testing Zarr v2 (no sharding) ===" );

		// Use manual downsampling steps since --multiRes with small data creates only
		// a single level which causes issues with OME-ZARR metadata creation.
		final String[] args = new String[] {
			"-x", xmlFile.getAbsolutePath(),
			"-o", outputPath.getAbsolutePath(),
			"-s", "ZARR2",
			"-d", "UINT16",
			"--preserveAnisotropy",
			"--blockSize", "32,32,32",
			"-ds", "1,1,1",
			"-ds", "2,2,2"
		};

		final int exitCode = new CommandLine( new CreateFusionContainer() ).execute( args );
		assertEquals( 0, exitCode, "CreateFusionContainer should succeed for Zarr v2" );

		// Verify container was created
		assertTrue( outputPath.exists(), "Zarr v2 container should exist" );

		// Verify we can read it back
		try ( N5Reader reader = URITools.instantiateN5Reader( StorageFormat.ZARR2, outputPath.toURI() ) )
		{
			// Check metadata
			final String fusionFormat = reader.getAttribute( "/", "Bigstitcher-Spark/FusionFormat", String.class );
			assertNotNull( fusionFormat, "FusionFormat should be stored" );
			assertEquals( "OME-ZARR", fusionFormat, "FusionFormat should be OME-ZARR" );

			// Check that sharding is disabled
			final Boolean useSharding = reader.getAttribute( "/", "Bigstitcher-Spark/UseSharding", Boolean.class );
			assertEquals( false, useSharding, "Zarr v2 should not use sharding" );

			// Check resolution levels exist
			assertTrue( reader.exists( "/0" ), "Level 0 should exist" );

			System.out.println( "Zarr v2 container created successfully" );
		}
	}

	@Test
	public void testCreateContainerZarrV3WithSharding() throws Exception
	{
		final File outputPath = tempDir.resolve( "fused_v3.zarr" ).toFile();

		System.out.println( "\n=== Testing Zarr v3 with sharding ===" );

		// Use manual downsampling steps since --multiRes with small data creates only
		// a single level which causes issues with OME-ZARR metadata creation.
		final String[] args = new String[] {
			"-x", xmlFile.getAbsolutePath(),
			"-o", outputPath.getAbsolutePath(),
			"-s", "ZARR",
			"-d", "UINT16",
			"--preserveAnisotropy",
			"--blockSize", "32,32,32",
			"--useSharding",
			"--shardSizeFactor", "2,2,2",
			"-ds", "1,1,1",
			"-ds", "2,2,2"
		};

		final int exitCode = new CommandLine( new CreateFusionContainer() ).execute( args );
		assertEquals( 0, exitCode, "CreateFusionContainer should succeed for Zarr v3 with sharding" );

		// Verify container was created
		assertTrue( outputPath.exists(), "Zarr v3 container should exist" );

		// Verify we can read it back
		try ( N5Reader reader = URITools.instantiateN5Reader( StorageFormat.ZARR, outputPath.toURI() ) )
		{
			// Check metadata
			final String fusionFormat = reader.getAttribute( "/", "Bigstitcher-Spark/FusionFormat", String.class );
			assertNotNull( fusionFormat, "FusionFormat should be stored" );
			assertEquals( "OME-ZARR", fusionFormat, "FusionFormat should be OME-ZARR" );

			// Check that sharding is enabled
			final Boolean useSharding = reader.getAttribute( "/", "Bigstitcher-Spark/UseSharding", Boolean.class );
			assertEquals( true, useSharding, "Zarr v3 should use sharding" );

			// Check shard size
			final int[] shardSize = reader.getAttribute( "/", "Bigstitcher-Spark/ShardSize", int[].class );
			assertNotNull( shardSize, "ShardSize should be stored" );
			assertEquals( 64, shardSize[0], "Shard size X should be 64 (32 * 2)" );
			assertEquals( 64, shardSize[1], "Shard size Y should be 64 (32 * 2)" );
			assertEquals( 64, shardSize[2], "Shard size Z should be 64 (32 * 2)" );

			// Check resolution levels exist
			assertTrue( reader.exists( "/0" ), "Level 0 should exist" );

			System.out.println( "Zarr v3 with sharding container created successfully" );
		}
	}

	@Test
	public void testCreateContainerN5() throws Exception
	{
		final File outputPath = tempDir.resolve( "fused.n5" ).toFile();

		System.out.println( "\n=== Testing N5 ===" );

		final String[] args = new String[] {
			"-x", xmlFile.getAbsolutePath(),
			"-o", outputPath.getAbsolutePath(),
			"-s", "N5",
			"-d", "UINT16",
			"--preserveAnisotropy",
			"--multiRes",
			"--blockSize", "32,32,32"
		};

		final int exitCode = new CommandLine( new CreateFusionContainer() ).execute( args );
		assertEquals( 0, exitCode, "CreateFusionContainer should succeed for N5" );

		// Verify container was created
		assertTrue( outputPath.exists(), "N5 container should exist" );

		// Verify we can read it back
		try ( N5Reader reader = URITools.instantiateN5Reader( StorageFormat.N5, outputPath.toURI() ) )
		{
			// Check metadata
			final String fusionFormat = reader.getAttribute( "/", "Bigstitcher-Spark/FusionFormat", String.class );
			assertNotNull( fusionFormat, "FusionFormat should be stored" );
			assertEquals( "N5", fusionFormat, "FusionFormat should be N5" );

			// N5 doesn't support sharding, but flag should still be stored
			final Boolean useSharding = reader.getAttribute( "/", "Bigstitcher-Spark/UseSharding", Boolean.class );
			assertEquals( false, useSharding, "N5 should not use sharding" );

			// Check resolution levels exist (N5 uses different path structure)
			assertTrue( reader.exists( "ch0tp0/s0" ), "Level 0 should exist" );

			System.out.println( "N5 container created successfully" );
		}
	}

	/**
	 * Tests full fusion pipeline: CreateFusionContainer + SparkFusion.
	 * Verifies that pixel intensities at both resolution levels are non-zero.
	 */
	@Test
	public void testFusionWithMultiResPyramid() throws Exception
	{
		final File outputPath = tempDir.resolve( "fused_full.zarr" ).toFile();

		System.out.println( "\n=== Testing full fusion with multi-resolution pyramid ===" );

		// 1. Create the fusion container with 2 levels: 1,1,1 and 2,2,2
		// Use ZARR2 to avoid sharding (which requires special handling in SparkFusion)
		final String[] createArgs = new String[] {
			"-x", xmlFile.getAbsolutePath(),
			"-o", outputPath.getAbsolutePath(),
			"-s", "ZARR2",
			"-d", "UINT16",
			"--preserveAnisotropy",
			"--blockSize", "32,32,32",
			"-ds", "1,1,1",
			"-ds", "2,2,2"
		};

		int exitCode = new CommandLine( new CreateFusionContainer() ).execute( createArgs );
		assertEquals( 0, exitCode, "CreateFusionContainer should succeed" );

		// 2. Run SparkFusion to populate the container (reads XML and config from container metadata)
		final String[] fusionArgs = new String[] {
			"-o", outputPath.getAbsolutePath(),
			"-s", "ZARR2",
			"--localSparkBindAddress"
		};

		exitCode = new CommandLine( new SparkFusion() ).execute( fusionArgs );
		assertEquals( 0, exitCode, "SparkFusion should succeed" );

		// 3. Verify pixel intensities at both levels
		try ( N5Reader reader = URITools.instantiateN5Reader( StorageFormat.ZARR2, outputPath.toURI() ) )
		{
			// Check level 0 (full resolution)
			assertTrue( reader.exists( "/0" ), "Level 0 should exist" );
			final RandomAccessibleInterval< UnsignedShortType > imgS0 = N5Utils.open( reader, "/0" );
			final long[] dimsS0 = imgS0.dimensionsAsLongArray();
			System.out.println( "Level 0 dimensions: " + java.util.Arrays.toString( dimsS0 ) );

			// Check level 1 (2x2x2 downsampled)
			assertTrue( reader.exists( "/1" ), "Level 1 should exist" );
			final RandomAccessibleInterval< UnsignedShortType > imgS1 = N5Utils.open( reader, "/1" );
			final long[] dimsS1 = imgS1.dimensionsAsLongArray();
			System.out.println( "Level 1 dimensions: " + java.util.Arrays.toString( dimsS1 ) );

			// OME-ZARR is 5D [x, y, z, c, t]
			// Verify specific pixel values at coordinates where fused beads exist
			// Note: OME-ZARR is zero-min, coordinates chosen from regions with fused bead data
			final int valS0 = imgS0.getAt( 64, 32, 64, 0, 0 ).get();
			final int valS1 = imgS1.getAt( 32, 16, 32, 0, 0 ).get();

			// Assert specific expected values from fusion
			assertEquals( 44, valS0, "Level 0 at (64,32,64) should be 44" );
			assertEquals( 86, valS1, "Level 1 at (32,16,32) should be 86" );

			System.out.println( "✓ Full fusion with multi-resolution pyramid test passed" );
		}
	}

	/*
	 * HDF5 test is commented out because HDF5 support is currently not working.
	 * Uncomment when HDF5 support is fixed.
	 */
	// @Test
	// public void testCreateContainerHDF5() throws Exception
	// {
	// 	final File outputPath = tempDir.resolve( "fused.h5" ).toFile();
	//
	// 	System.out.println( "\n=== Testing HDF5 ===" );
	//
	// 	final String[] args = new String[] {
	// 		"-x", xmlFile.getAbsolutePath(),
	// 		"-o", outputPath.getAbsolutePath(),
	// 		"-s", "HDF5",
	// 		"-d", "UINT16",
	// 		"--preserveAnisotropy",
	// 		"--multiRes",
	// 		"--blockSize", "32,32,32"
	// 	};
	//
	// 	final int exitCode = new CommandLine( new CreateFusionContainer() ).execute( args );
	// 	assertEquals( 0, exitCode, "CreateFusionContainer should succeed for HDF5" );
	//
	// 	// Verify container was created
	// 	assertTrue( outputPath.exists(), "HDF5 container should exist" );
	//
	// 	// Verify we can read it back
	// 	try ( N5Reader reader = URITools.instantiateN5Reader( StorageFormat.HDF5, outputPath.toURI() ) )
	// 	{
	// 		// Check metadata
	// 		final String fusionFormat = reader.getAttribute( "/", "Bigstitcher-Spark/FusionFormat", String.class );
	// 		assertNotNull( fusionFormat, "FusionFormat should be stored" );
	// 		assertEquals( "HDF5", fusionFormat, "FusionFormat should be HDF5" );
	//
	// 		System.out.println( "HDF5 container created successfully" );
	// 	}
	// }

	/*
	ERROR MESSAGE:

	Setting up container and metadata in 'file:/var/folders/b0/sgyw0d9918vfjl96c5dlc3fw0000gp/T/junit-3707500257499232434/fused_v2.zarr' ... 
	Creating 5D OME-ZARR metadata for 'file:/var/folders/b0/sgyw0d9918vfjl96c5dlc3fw0000gp/T/junit-3707500257499232434/fused_v2.zarr' ... 
	Resolution of level 0: (1.0, 1.0, 1.0) micrometer
	Zarr v2 container created successfully
	[ERROR] Tests run: 4, Failures: 1, Errors: 0, Skipped: 0, Time elapsed: 2.787 s <<< FAILURE! -- in net.preibisch.bigstitcher.spark.TestSparkFusion
	[ERROR] net.preibisch.bigstitcher.spark.TestSparkFusion.testFusionWithMultiResPyramid -- Time elapsed: 0.671 s <<< FAILURE!
	java.lang.AssertionError: assertion failed: Expected hostname or IPv6 IP enclosed in [] but got fe80:0:0:0:34f6:9dff:fe9c:d86a%13
		at scala.Predef$.assert(Predef.scala:223)
		at org.apache.spark.util.Utils$.checkHost(Utils.scala:1121)
		at org.apache.spark.executor.Executor.<init>(Executor.scala:89)
		at org.apache.spark.scheduler.local.LocalEndpoint.<init>(LocalSchedulerBackend.scala:64)
		at org.apache.spark.scheduler.local.LocalSchedulerBackend.start(LocalSchedulerBackend.scala:132)
		at org.apache.spark.scheduler.TaskSchedulerImpl.start(TaskSchedulerImpl.scala:222)
		at org.apache.spark.SparkContext.<init>(SparkContext.scala:595)
		at org.apache.spark.api.java.JavaSparkContext.<init>(JavaSparkContext.scala:58)
		at net.preibisch.bigstitcher.spark.SparkFusion.call(SparkFusion.java:489)
		at net.preibisch.bigstitcher.spark.SparkFusion.call(SparkFusion.java:95)
		at picocli.CommandLine.executeUserObject(CommandLine.java:2045)
		at picocli.CommandLine.access$1500(CommandLine.java:148)
		at picocli.CommandLine$RunLast.executeUserObjectOfLastSubcommandWithSameParent(CommandLine.java:2465)
		at picocli.CommandLine$RunLast.handle(CommandLine.java:2457)
		at picocli.CommandLine$RunLast.handle(CommandLine.java:2419)
		at picocli.CommandLine$AbstractParseResultHandler.execute(CommandLine.java:2277)
		at picocli.CommandLine$RunLast.execute(CommandLine.java:2421)
		at picocli.CommandLine.execute(CommandLine.java:2174)
		at net.preibisch.bigstitcher.spark.TestSparkFusion.testFusionWithMultiResPyramid(TestSparkFusion.java:272)
		at java.lang.reflect.Method.invoke(Method.java:498)
		at java.util.ArrayList.forEach(ArrayList.java:1259)
		at java.util.ArrayList.forEach(ArrayList.java:1259)

	[INFO] 
	[INFO] Results:
	[INFO] 
	[ERROR] Failures: 
	[ERROR]   TestSparkFusion.testFusionWithMultiResPyramid:272 assertion failed: Expected hostname or IPv6 IP enclosed in [] but got fe80:0:0:0:34f6:9dff:fe9c:d86a%13
	[ERROR]   TestSparkInterestPointDetection.testSparkDoGDetection:92 assertion failed: Expected hostname or IPv6 IP enclosed in [] but got fe80:0:0:0:34f6:9dff:fe9c:d86a%13
	[INFO] 
	[ERROR] Tests run: 6, Failures: 2, Errors: 0, Skipped: 0
	[INFO] 
	[INFO] ------------------------------------------------------------------------
	[INFO] BUILD FAILURE
	[INFO] ------------------------------------------------------------------------
	[INFO] Total time:  23.004 s
	[INFO] Finished at: 2026-03-06T17:22:44+01:00
	[INFO] ------------------------------------------------------------------------
	[ERROR] Failed to execute goal org.apache.maven.plugins:maven-surefire-plugin:3.2.5:test (default-test) on project BigStitcher-Spark: There are test failures.
	[ERROR] 
	[ERROR] Please refer to /Users/preibischs/workspace/BigStitcher-Spark/target/surefire-reports for the individual test results.
	[ERROR] Please refer to dump files (if any exist) [date].dump, [date]-jvmRun[N].dump and [date].dumpstream.
	[ERROR] -> [Help 1]
	[ERROR] 
	[ERROR] To see the full stack trace of the errors, re-run Maven with the -e switch.
	[ERROR] Re-run Maven using the -X switch to enable full debug logging.
	[ERROR] 
	[ERROR] For more information about the errors and possible solutions, please read the following articles:
	[ERROR] [Help 1] http://cwiki.apache.org/confluence/display/MAVEN/MojoFailureException
	*/

	/**
	 * Main method for manual testing outside of JUnit.
	 */
	public static void main( final String[] args ) throws Exception
	{
		final TestSparkFusion test = new TestSparkFusion();
		test.tempDir = Files.createTempDirectory( "spark-fusion-test" );
		test.tempDir.toFile().deleteOnExit();

		test.setUp();

		System.out.println( "\n========================================" );
		System.out.println( "Running Zarr v2 test..." );
		test.testCreateContainerZarrV2();

		System.out.println( "\n========================================" );
		System.out.println( "Running Zarr v3 with sharding test..." );
		test.testCreateContainerZarrV3WithSharding();

		System.out.println( "\n========================================" );
		System.out.println( "Running N5 test..." );
		test.testCreateContainerN5();

		System.out.println( "\n========================================" );
		System.out.println( "Running fusion multiresolution test..." );
		test.testFusionWithMultiResPyramid();

		System.out.println( "\n========================================" );
		System.out.println( "All tests passed!" );
	}
}
