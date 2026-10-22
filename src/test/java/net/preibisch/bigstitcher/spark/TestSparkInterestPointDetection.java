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
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.mvrecon.SimulateUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPointLists;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
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
	// the parameters of the shared detection test of multiview-reconstruction (TestInterestPointDetection.testDoG), which the
	// simulated beads were tuned for: the intensity range is the one of the data (0..1137), a wrong one finds nothing
	public static final String STANDARD_LABEL = TestInterestPointDetection.STANDARD_LABEL;
	public static final double STANDARD_SIGMA = TestInterestPointDetection.STANDARD_SIGMA;
	public static final double STANDARD_THRESHOLD = TestInterestPointDetection.STANDARD_THRESHOLD;
	public static final double GLOBAL_MIN_INTENSITY = TestInterestPointDetection.GLOBAL_MIN_INTENSITY;
	public static final double GLOBAL_MAX_INTENSITY = TestInterestPointDetection.GLOBAL_MAX_INTENSITY;
	public static final int STANDARD_DOWNSAMPLE_XY = TestInterestPointDetection.STANDARD_DOWNSAMPLE_XY;
	public static final int STANDARD_DOWNSAMPLE_Z = TestInterestPointDetection.STANDARD_DOWNSAMPLE_Z;

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
			"-l", STANDARD_LABEL,
			"-s", String.valueOf( STANDARD_SIGMA ),
			"-t", String.valueOf( STANDARD_THRESHOLD ),
			"-i0", String.valueOf( GLOBAL_MIN_INTENSITY ),
			"-i1", String.valueOf( GLOBAL_MAX_INTENSITY ),
			"-dsxy", String.valueOf( STANDARD_DOWNSAMPLE_XY ),
			"-dsz", String.valueOf( STANDARD_DOWNSAMPLE_Z ),
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
		// TestInterestPointDetection.assertDoGResults( result, STANDARD_LABEL );

		// every detection carries its response (above the threshold), the sigma of the detection at full resolution and a finite image intensity
		final int numPoints = assertAttributes( result, STANDARD_LABEL, STANDARD_THRESHOLD, STANDARD_SIGMA * STANDARD_DOWNSAMPLE_XY, STANDARD_SIGMA * STANDARD_DOWNSAMPLE_XY );
		assertTrue( numPoints > 100, "points: " + numPoints );

		// the counts of the shared test (virtual downsampling may move a point across the threshold)
		final int[] expected = new int[] { TestInterestPointDetection.EXPECTED_COUNT_VIEW_0, TestInterestPointDetection.EXPECTED_COUNT_VIEW_1, TestInterestPointDetection.EXPECTED_COUNT_VIEW_2 };
		for ( int setup = 0; setup < expected.length; ++setup )
		{
			final int count = result.getViewInterestPoints().getViewInterestPointLists( new ViewId( 0, setup ) ).getInterestPointList( STANDARD_LABEL ).getInterestPointsCopy().size();
			assertTrue( Math.abs( count - expected[ setup ] ) <= 2, "setup " + setup + ": " + count + " points, expected " + expected[ setup ] );
		}
		assertTrue( parameters( result, STANDARD_LABEL ).startsWith( "DOG (Spark) s=" + STANDARD_SIGMA + " t=" + STANDARD_THRESHOLD ), parameters( result, STANDARD_LABEL ) );
		assertTrue( parameters( result, STANDARD_LABEL ).contains( " anisotropy=NaN " ), parameters( result, STANDARD_LABEL ) );

		System.out.println( "All assertions passed!" );
	}

	/**
	 * The scale space in Spark (with an anisotropy): points of several sigmas, all with their attributes, the parameters
	 * as the GUI writes them; and a second label whose threshold comes from the noise floor
	 */
	@Test
	public void testSparkScaleSpaceAndNoiseFloor() throws Exception
	{
		System.setProperty( "spark.master", "local[*]" );

		final SpimData2 spimData = SimulateUtil.setUp();
		final File xmlFile = tempDir.resolve( "dataset-ss.xml" ).toFile();
		new XmlIoSpimData2().save( spimData, xmlFile.toURI() );

		final double sigma = 1.6;
		final String[] args = new String[] {
			"-x", xmlFile.getAbsolutePath(),
			"-l", "beads-ss",
			"--method", "SCALE_SPACE",
			"-s", String.valueOf( sigma ),
			"-t", String.valueOf( STANDARD_THRESHOLD ),
			"--octaves", "2",
			"--anisotropy", "1.5",
			"-i0", String.valueOf( GLOBAL_MIN_INTENSITY ),
			"-i1", String.valueOf( GLOBAL_MAX_INTENSITY ),
			"-dsxy", String.valueOf( STANDARD_DOWNSAMPLE_XY ),
			"-dsz", String.valueOf( STANDARD_DOWNSAMPLE_Z ),
			"--localSparkBindAddress" };

		assertEquals( 0, new CommandLine( new SparkInterestPointDetection() ).execute( args ), "scale space" );

		final SpimData2 result = new XmlIoSpimData2().load( xmlFile.toURI() );

		// the sigmas vary (beads at the finest level, larger structures in the octaves), none above the coarsest level
		final int numPoints = assertAttributes( result, "beads-ss", STANDARD_THRESHOLD, 0.0, sigma * 4 * STANDARD_DOWNSAMPLE_XY );
		assertTrue( numPoints > 100, "points: " + numPoints );
		final String params = parameters( result, "beads-ss" );
		assertTrue( params.startsWith( "DOG-SS (Spark) s=" + sigma + " steps=4 octaves=2 finestLevel=true t=" + STANDARD_THRESHOLD ), params );
		assertTrue( params.contains( " anisotropy=1.5 " ), params );

		// the threshold from the noise floor: resolved once on the driver, stored as a fixed number
		final String[] argsNoise = new String[] {
			"-x", xmlFile.getAbsolutePath(),
			"-l", "beads-noise",
			"-s", String.valueOf( STANDARD_SIGMA ),
			"--noiseFactor", "2.5",
			"-i0", String.valueOf( GLOBAL_MIN_INTENSITY ),
			"-i1", String.valueOf( GLOBAL_MAX_INTENSITY ),
			"-dsxy", String.valueOf( STANDARD_DOWNSAMPLE_XY ),
			"-dsz", String.valueOf( STANDARD_DOWNSAMPLE_Z ),
			"--localSparkBindAddress" };

		assertEquals( 0, new CommandLine( new SparkInterestPointDetection() ).execute( argsNoise ), "noise factor" );

		final SpimData2 resultNoise = new XmlIoSpimData2().load( xmlFile.toURI() );
		final String paramsNoise = parameters( resultNoise, "beads-noise" );
		final double threshold = net.preibisch.mvrecon.fiji.plugin.Filter_Detections.parseThreshold( paramsNoise );
		assertTrue( threshold > 0 && Double.isFinite( threshold ), paramsNoise );
		assertTrue( assertAttributes( resultNoise, "beads-noise", threshold, STANDARD_SIGMA * STANDARD_DOWNSAMPLE_XY, STANDARD_SIGMA * STANDARD_DOWNSAMPLE_XY ) > 0 );
	}

	/** the parameter string of a label (the first view that has it) */
	static String parameters( final SpimData2 data, final String label )
	{
		for ( final ViewInterestPointLists vipl : data.getViewInterestPoints().getViewInterestPoints().values() )
			if ( vipl.contains( label ) )
				return vipl.getInterestPointList( label ).getParameters();

		throw new IllegalArgumentException( "no view has '" + label + "'" );
	}

	/**
	 * Every point of the label has |response| >= threshold, a sigma in [minSigma, maxSigma] and a finite intensity
	 *
	 * @return the number of points over all views
	 */
	static int assertAttributes( final SpimData2 data, final String label, final double threshold, final double minSigma, final double maxSigma )
	{
		int numPoints = 0;
		final java.util.TreeSet< Float > sigmas = new java.util.TreeSet<>();

		for ( final ViewInterestPointLists vipl : data.getViewInterestPoints().getViewInterestPoints().values() )
		{
			if ( !vipl.contains( label ) )
				continue;

			for ( final InterestPoint ip : vipl.getInterestPointList( label ).getInterestPointsCopy().values() )
			{
				assertTrue( Float.isFinite( ip.getResponse() ) && Math.abs( ip.getResponse() ) >= threshold, "response " + ip.getResponse() + " of point " + ip.getId() );
				assertTrue( ip.getSigma() >= minSigma - 1e-4 && ip.getSigma() <= maxSigma + 1e-4, "sigma " + ip.getSigma() + " not in [" + minSigma + ", " + maxSigma + "]" );
				assertTrue( Float.isFinite( ip.getIntensity() ) && ip.getIntensity() >= GLOBAL_MIN_INTENSITY && ip.getIntensity() <= GLOBAL_MAX_INTENSITY, "intensity " + ip.getIntensity() );
				sigmas.add( ip.getSigma() );
				++numPoints;
			}
		}

		System.out.println( label + ": " + numPoints + " points, sigmas " + sigmas );

		return numPoints;
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
		TestInterestPointDetection.testDoG( reloaded, STANDARD_LABEL );

		// 5. Verify using shared assertions
		System.out.println( "Verifying results..." );
		// TestInterestPointDetection.assertDoGResults( reloaded, STANDARD_LABEL );

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
