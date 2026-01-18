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
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
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
		System.out.println( "All tests passed!" );
	}
}
