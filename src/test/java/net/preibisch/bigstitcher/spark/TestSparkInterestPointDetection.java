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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewTransformAffine;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.realtransform.AffineTransform3D;
import net.preibisch.mvrecon.SimulateUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPointLists;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.CorrespondingInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsZarrStore;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsZarrStore.Key;
import net.preibisch.mvrecon.tests.TestInterestPointDetection;
import net.preibisch.mvrecon.tests.TestRegistration;
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
		// the tail the GUI writes too (read back by StoredDetectionParameters for a re-detection in the GUI)
		assertTrue( parameters( result, STANDARD_LABEL ).endsWith( " localization=QUADRATIC groupTiles=false groupIllums=false thresholdType=fixed" ), parameters( result, STANDARD_LABEL ) );

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
		assertTrue( params.contains( " localization=QUADRATIC " ) && params.endsWith( " thresholdType=fixed" ), params );

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
		assertTrue( paramsNoise.endsWith( " thresholdType=noisefloor noiseFactor=2.5" ), paramsNoise );
		assertTrue( assertAttributes( resultNoise, "beads-noise", threshold, STANDARD_SIGMA * STANDARD_DOWNSAMPLE_XY, STANDARD_SIGMA * STANDARD_DOWNSAMPLE_XY ) > 0 );
	}

	/**
	 * A detection onto a label that exists (here in one of three views) replaces that view's entry, drops the correspondences of
	 * the other views that pointed at the old points (as the GUI's re-detection does) and leaves the other views alone; the
	 * scratch N5 of the blocks is gone after every run
	 */
	@Test
	public void testRedetectionReplacesLabel() throws Exception
	{
		System.setProperty( "spark.master", "local[*]" );

		final SpimData2 spimData = SimulateUtil.setUp();
		final File xmlFile = tempDir.resolve( "dataset-redetect.xml" ).toFile();
		new XmlIoSpimData2().save( spimData, xmlFile.toURI() );

		final ViewId v0 = new ViewId( 0, 0 ), v1 = new ViewId( 0, 1 ), v2 = new ViewId( 0, 2 );

		// all views, then a registration for the correspondences
		assertEquals( 0, new CommandLine( new SparkInterestPointDetection() ).execute( detectArgs( xmlFile, STANDARD_THRESHOLD ) ), "first detection" );
		assertFalse( Files.exists( tempDir.resolve( "interestpoints.n5" ) ), "the scratch N5 of the blocks is removed" );

		final SpimData2 registered = new XmlIoSpimData2().load( xmlFile.toURI() );
		TestRegistration.testRegistration( registered, STANDARD_LABEL, false );
		new XmlIoSpimData2().save( registered, xmlFile.toURI() );

		final SpimData2 before = new XmlIoSpimData2().load( xmlFile.toURI() );
		assertTrue( corrsTo( before, v1, v0 ) > 0 && corrsTo( before, v2, v0 ) > 0 && corrsTo( before, v1, v2 ) > 0, "fixture: correspondences between all views" );
		final int points1 = count( before, v1 ), points2 = count( before, v2 );

		// view 0 again, with another threshold
		final String[] args = detectArgs( xmlFile, 2 * STANDARD_THRESHOLD );
		final String[] argsView0 = java.util.Arrays.copyOf( args, args.length + 2 );
		argsView0[ args.length ] = "-vi";
		argsView0[ args.length + 1 ] = "0,0";
		assertEquals( 0, new CommandLine( new SparkInterestPointDetection() ).execute( argsView0 ), "re-detection of view 0" );
		assertFalse( Files.exists( tempDir.resolve( "interestpoints.n5" ) ), "the scratch N5 of the blocks is removed" );

		final SpimData2 after = new XmlIoSpimData2().load( xmlFile.toURI() );
		final InterestPointsZarrStore store = InterestPointsZarrStore.get( tempDir.toUri() );

		assertTrue( parametersOf( after, v0 ).contains( " t=" + ( 2 * STANDARD_THRESHOLD ) + " " ), parametersOf( after, v0 ) );
		assertTrue( parametersOf( after, v1 ).contains( " t=" + STANDARD_THRESHOLD + " " ), parametersOf( after, v1 ) );
		assertTrue( count( after, v0 ) > 0, "view 0 has points again" );
		assertEquals( points1, count( after, v1 ), "view 1 untouched" );
		assertEquals( points2, count( after, v2 ), "view 2 untouched" );
		for ( final ViewId v : new ViewId[] { v0, v1, v2 } )
			assertTrue( store.hasEntry( Key.of( v, STANDARD_LABEL ) ), "entry of " + v + " in the store" );

		assertEquals( 0, after.getViewInterestPoints().getViewInterestPointLists( v0 ).getInterestPointList( STANDARD_LABEL ).getCorrespondingInterestPointsCopy().size(), "the new points have no correspondences" );
		assertEquals( 0, corrsTo( after, v1, v0 ), "the links of view 1 into the re-detected view are gone" );
		assertEquals( 0, corrsTo( after, v2, v0 ), "the links of view 2 into the re-detected view are gone" );
		assertTrue( corrsTo( after, v1, v2 ) > 0, "the links between the other views stay" );
	}

	/**
	 * --overlappingOnly: the points of a view lie in the regions other views cover by the current registrations, fewer than in
	 * the whole view, no duplicates where the regions of two partners overlap each other; --onlyCompareOverlapTiles gives the
	 * same here (the three setups are three angles, i.e. three volumes)
	 */
	@Test
	public void testOverlappingOnly() throws Exception
	{
		System.setProperty( "spark.master", "local[*]" );

		// view 1 shifted by 64 px in x and view 2 by 64 px in y: view 0 overlaps view 1 in x >= 64 and view 2 in y >= 64 (full resolution)
		final SpimData2 spimData = SimulateUtil.setUp();
		shift( spimData, new ViewId( 0, 1 ), 64, 0 );
		shift( spimData, new ViewId( 0, 2 ), 0, 64 );
		final File xmlFile = tempDir.resolve( "dataset-overlap.xml" ).toFile();
		new XmlIoSpimData2().save( spimData, xmlFile.toURI() );

		final ViewId v0 = new ViewId( 0, 0 );
		int previous = -1;

		for ( final boolean onlyTiles : new boolean[] { false, true } )
		{
			final String[] args = detectArgs( xmlFile, STANDARD_THRESHOLD );
			final String[] argsOverlap = Arrays.copyOf( args, args.length + ( onlyTiles ? 2 : 1 ) );
			argsOverlap[ args.length ] = "--overlappingOnly";
			if ( onlyTiles )
				argsOverlap[ args.length + 1 ] = "--onlyCompareOverlapTiles";

			assertEquals( 0, new CommandLine( new SparkInterestPointDetection() ).execute( argsOverlap ), "overlappingOnly, onlyCompareOverlapTiles=" + onlyTiles );

			final SpimData2 result = new XmlIoSpimData2().load( xmlFile.toURI() );
			final List< InterestPoint > points = new ArrayList<>( result.getViewInterestPoints().getViewInterestPointLists( v0 ).getInterestPointList( STANDARD_LABEL ).getInterestPointsCopy().values() );

			assertTrue( points.size() > 10 && points.size() < TestInterestPointDetection.EXPECTED_COUNT_VIEW_0 - 2, "fewer points than in the whole view (" + TestInterestPointDetection.EXPECTED_COUNT_VIEW_0 + "): " + points.size() );
			for ( final InterestPoint p : points )
				assertTrue( p.getL()[ 0 ] >= 64 - 2 || p.getL()[ 1 ] >= 64 - 2, "outside the overlaps: " + Arrays.toString( p.getL() ) );
			for ( int i = 0; i < points.size(); ++i )
				for ( int j = i + 1; j < points.size(); ++j )
					assertTrue( distance( points.get( i ), points.get( j ) ) > 0.4, "duplicate " + Arrays.toString( points.get( i ).getL() ) );
			assertTrue( parametersOf( result, v0 ).contains( " overlappingOnly=true " ), parametersOf( result, v0 ) );

			if ( previous >= 0 )
				assertEquals( previous, points.size(), "the same partners with --onlyCompareOverlapTiles (three angles = three volumes)" );
			previous = points.size();
		}
	}

	/** shifts a view in world coordinates (= full-resolution pixels of the simulated dataset) */
	static void shift( final SpimData2 data, final ViewId view, final double dx, final double dy )
	{
		final AffineTransform3D t = new AffineTransform3D();
		t.translate( dx, dy, 0 );

		final ViewRegistration vr = data.getViewRegistrations().getViewRegistration( view );
		vr.preconcatenateTransform( new ViewTransformAffine( "shift", t ) );
		vr.updateModel();
	}

	static double distance( final InterestPoint a, final InterestPoint b )
	{
		double sum = 0;
		for ( int d = 0; d < 3; ++d )
			sum += ( a.getL()[ d ] - b.getL()[ d ] ) * ( a.getL()[ d ] - b.getL()[ d ] );
		return Math.sqrt( sum );
	}

	/** the arguments of the standard DoG detection with the given threshold */
	static String[] detectArgs( final File xmlFile, final double threshold )
	{
		return new String[] {
			"-x", xmlFile.getAbsolutePath(),
			"-l", STANDARD_LABEL,
			"-s", String.valueOf( STANDARD_SIGMA ),
			"-t", String.valueOf( threshold ),
			"-i0", String.valueOf( GLOBAL_MIN_INTENSITY ),
			"-i1", String.valueOf( GLOBAL_MAX_INTENSITY ),
			"-dsxy", String.valueOf( STANDARD_DOWNSAMPLE_XY ),
			"-dsz", String.valueOf( STANDARD_DOWNSAMPLE_Z ),
			"--localSparkBindAddress" };
	}

	/** the number of points of the standard label in a view */
	static int count( final SpimData2 data, final ViewId view )
	{
		return data.getViewInterestPoints().getViewInterestPointLists( view ).getInterestPointList( STANDARD_LABEL ).getInterestPointsCopy().size();
	}

	/** the parameter string of the standard label in a view */
	static String parametersOf( final SpimData2 data, final ViewId view )
	{
		return data.getViewInterestPoints().getViewInterestPointLists( view ).getInterestPointList( STANDARD_LABEL ).getParameters();
	}

	/** the correspondences of the standard label of one view that point into another view */
	static int corrsTo( final SpimData2 data, final ViewId from, final ViewId to )
	{
		int n = 0;

		for ( final CorrespondingInterestPoints c : data.getViewInterestPoints().getViewInterestPointLists( from ).getInterestPointList( STANDARD_LABEL ).getCorrespondingInterestPointsCopy() )
			if ( c.getCorrespondingViewId().equals( to ) )
				++n;

		return n;
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
