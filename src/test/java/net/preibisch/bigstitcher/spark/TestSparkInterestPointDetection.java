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

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.preibisch.mvrecon.SimulateUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.tests.TestInterestPointDetection;
import picocli.CommandLine;

/**
 * Tests Spark-based interest point detection using the same simulated dataset
 * and assertions as the multiview-reconstruction TestInterestPointDetection test.
 *
 * This ensures that distributed Spark execution produces identical results
 * to the single-threaded implementation.
 */
public class TestSparkInterestPointDetection
{
	@TempDir
	Path tempDir;

	@Test
	public void testSparkDoGDetection() throws Exception
	{
		// Set Spark master URL for local execution
		System.setProperty( "spark.master", "local[*]" );

		// 1. Create simulated dataset (3 views, 200 beads)
		final SpimData2 spimData = SimulateUtil.setUp();

		// 2. Save as XML (use URI to ensure proper basepath)
		final File xmlFile = tempDir.resolve( "dataset.xml" ).toFile();
		new XmlIoSpimData2().save( spimData, xmlFile.toURI() );

		System.out.println( "Saved test dataset to: " + xmlFile.getAbsolutePath() );

		// 3. Run distributed interest point detection using standard test parameters
		// Both multiview-reconstruction and Spark use the same global min/max intensity
		// (matching the GUI option "use same min/max for all views")
		final String[] args = new String[] {
			"-x", xmlFile.getAbsolutePath(),
			"-l", TestInterestPointDetection.STANDARD_LABEL,
			"-s", String.valueOf( TestInterestPointDetection.STANDARD_SIGMA ),
			"-t", String.valueOf( TestInterestPointDetection.STANDARD_THRESHOLD ),
			"-i0", String.valueOf( TestInterestPointDetection.GLOBAL_MIN_INTENSITY ),
			"-i1", String.valueOf( TestInterestPointDetection.GLOBAL_MAX_INTENSITY ),
			"-dsxy", String.valueOf( TestInterestPointDetection.STANDARD_DOWNSAMPLE_XY ),
			"-dsz", String.valueOf( TestInterestPointDetection.STANDARD_DOWNSAMPLE_Z ),
			"--localSparkBindAddress"  // Required for local Spark execution
		};

		System.out.println( "Running SparkInterestPointDetection with args: " );
		for ( String arg : args )
			System.out.println( "  " + arg );

		final int exitCode = new CommandLine( new SparkInterestPointDetection() ).execute( args );

		if ( exitCode != 0 )
			throw new RuntimeException( "SparkInterestPointDetection failed with exit code: " + exitCode );

		// 4. Reload and verify using shared assertions from multiview-reconstruction
		final SpimData2 result = new XmlIoSpimData2().load( xmlFile.toURI() );

		System.out.println( "Verifying results using shared assertions..." );

		// Spark uses virtual downsampling (LazyDownsample2x) for efficiency, while non-Spark uses
		// Downsample.simple2x. Point counts are identical, positions may differ very slightly.
		TestInterestPointDetection.assertDoGResults( result, TestInterestPointDetection.STANDARD_LABEL );

		System.out.println( "All assertions passed!" );
	}

	/**
	 * Test that saves/loads XML and runs DoG directly (without Spark) to isolate
	 * whether the difference comes from XML serialization or Spark processing.
	 */
	@Test
	public void testDoGAfterXmlReload() throws Exception
	{
		// 1. Create simulated dataset (3 views, 200 beads)
		final SpimData2 spimData = SimulateUtil.setUp();

		// 2. Save as XML
		final File xmlFile = tempDir.resolve( "dataset-reload.xml" ).toFile();
		new XmlIoSpimData2().save( spimData, xmlFile.toURI() );

		System.out.println( "Saved test dataset to: " + xmlFile.getAbsolutePath() );

		// 3. Reload XML
		final SpimData2 reloaded = new XmlIoSpimData2().load( xmlFile.toURI() );

		// 4. Run DoG detection directly using testDoG from multiview-reconstruction
		System.out.println( "Running DoG detection on reloaded XML (no Spark)..." );
		TestInterestPointDetection.testDoG( reloaded, TestInterestPointDetection.STANDARD_LABEL );

		// 5. Verify using shared assertions
		System.out.println( "Verifying results..." );
		TestInterestPointDetection.assertDoGResults( reloaded, TestInterestPointDetection.STANDARD_LABEL );

		System.out.println( "All assertions passed!" );
	}

	/**
	 * Main method for manual testing outside of JUnit.
	 */
	public static void main( final String[] args ) throws Exception
	{
		final TestSparkInterestPointDetection test = new TestSparkInterestPointDetection();
		test.tempDir = Files.createTempDirectory( "spark-test" );
		test.tempDir.toFile().deleteOnExit();
		test.testSparkDoGDetection();
	}
}
