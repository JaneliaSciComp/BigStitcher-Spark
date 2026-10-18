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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.mvrecon.SimulateUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.CorrespondingInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPointLists;
import net.preibisch.mvrecon.tests.TestInterestPointDetection;
import net.preibisch.mvrecon.tests.TestRegistration;
import picocli.CommandLine;

/**
 * View-selection behaviour of {@code clear-interestpoints} and {@code clear-registrations}.
 *
 * Fixture: the simulated 3-view dataset (views (0,0), (0,1), (0,2)) with DoG detections and the
 * correspondences produced by a registration, saved to a temp dir as dataset.xml + interestpoints.n5.
 */
public class TestClearInterestPoints
{
	static final String LABEL = "beads";
	static final ViewId V0 = new ViewId( 0, 0 );
	static final ViewId V1 = new ViewId( 0, 1 );
	static final ViewId V2 = new ViewId( 0, 2 );
	static final ViewId[] ALL = { V0, V1, V2 };

	@TempDir
	Path tempDir;

	String xmlPath;
	SpimData2 before;

	@BeforeEach
	void buildFixture() throws Exception
	{
		final SpimData2 sd = SimulateUtil.setUp();
		sd.setBasePathURI( tempDir.toUri() );
		TestInterestPointDetection.testDoG( sd, LABEL );

		final URI xml = tempDir.resolve( "dataset.xml" ).toUri();
		new XmlIoSpimData2().save( sd, xml );

		final SpimData2 loaded = new XmlIoSpimData2().load( xml );
		TestRegistration.testRegistration( loaded, LABEL, false ); // creates correspondences
		new XmlIoSpimData2().save( loaded, xml );

		xmlPath = tempDir.resolve( "dataset.xml" ).toString();
		before = load();

		// fixture sanity: detections everywhere, correspondences between every pair we rely on
		for ( final ViewId v : ALL )
			assertTrue( ipCount( before, v ) > 0, "fixture: detections in " + v );
		assertTrue( corrsTo( before, V1, V0 ) > 0, "fixture: (0,1) -> (0,0) correspondences" );
		assertTrue( corrsTo( before, V2, V0 ) > 0, "fixture: (0,2) -> (0,0) correspondences" );
		assertTrue( corrsTo( before, V1, V2 ) > 0, "fixture: (0,1) -> (0,2) correspondences" );
		assertTrue( Files.isDirectory( tempDir.resolve( "interestpoints.n5" ) ), "fixture: container exists" );
	}

	// ------------------------------------------------------------------ helpers

	SpimData2 load() throws Exception
	{
		return new XmlIoSpimData2().load( tempDir.resolve( "dataset.xml" ).toUri() );
	}

	int runClear( final String... modeArgs )
	{
		final String[] args = new String[ modeArgs.length + 2 ];
		args[ 0 ] = "-x";
		args[ 1 ] = xmlPath;
		System.arraycopy( modeArgs, 0, args, 2, modeArgs.length );
		return new CommandLine( new ClearInterestPoints() ).execute( args );
	}

	static boolean hasEntry( final SpimData2 d, final ViewId v )
	{
		return d.getViewInterestPoints().getViewInterestPoints().containsKey( v );
	}

	static boolean hasLabel( final SpimData2 d, final ViewId v, final String label )
	{
		final ViewInterestPointLists l = d.getViewInterestPoints().getViewInterestPoints().get( v );
		return l != null && l.contains( label );
	}

	static InterestPoints list( final SpimData2 d, final ViewId v )
	{
		return d.getViewInterestPoints().getViewInterestPoints().get( v ).getInterestPointList( LABEL );
	}

	static int ipCount( final SpimData2 d, final ViewId v )
	{
		return list( d, v ).getInterestPointsCopy().size();
	}

	static int corrCount( final SpimData2 d, final ViewId v )
	{
		return list( d, v ).getCorrespondingInterestPointsCopy().size();
	}

	static int corrsTo( final SpimData2 d, final ViewId from, final ViewId to )
	{
		int n = 0;
		for ( final CorrespondingInterestPoints c : list( d, from ).getCorrespondingInterestPointsCopy() )
			if ( c.getCorrespondingViewId().equals( to ) )
				++n;
		return n;
	}

	static String group( final ViewId v )
	{
		return "tpId_" + v.getTimePointId() + "_viewSetupId_" + v.getViewSetupId();
	}

	N5Reader n5()
	{
		return new N5FSReader( tempDir.resolve( "interestpoints.n5" ).toString() );
	}

	// ------------------------------------------------------------------ tests

	@Test
	public void removeLabelOnSubset() throws Exception
	{
		assertEquals( 0, runClear( "--clearMode", "REMOVE_LABEL", "--label", LABEL, "-vi", "0,0", "--silent" ) );
		final SpimData2 after = load();

		assertFalse( hasLabel( after, V0, LABEL ), "(0,0) lost the label" );
		assertTrue( hasLabel( after, V1, LABEL ) );
		assertTrue( hasLabel( after, V2, LABEL ) );
		assertEquals( ipCount( before, V1 ), ipCount( after, V1 ), "(0,1) detections untouched" );
		assertEquals( ipCount( before, V2 ), ipCount( after, V2 ), "(0,2) detections untouched" );

		try ( final N5Reader n5 = n5() )
		{
			assertFalse( n5.exists( group( V0 ) + "/" + LABEL ), "N5 label group of (0,0) removed" );
			assertTrue( n5.exists( group( V1 ) + "/" + LABEL + "/interestpoints" ), "N5 data of (0,1) kept" );
		}

		assertEquals( 0, corrsTo( after, V1, V0 ), "no dangling links (0,1) -> (0,0)" );
		assertEquals( 0, corrsTo( after, V2, V0 ), "no dangling links (0,2) -> (0,0)" );
		assertEquals( corrsTo( before, V1, V2 ), corrsTo( after, V1, V2 ), "(0,1) <-> (0,2) links untouched" );
		assertEquals( corrsTo( before, V2, V1 ), corrsTo( after, V2, V1 ), "(0,2) <-> (0,1) links untouched" );
	}

	@Test
	public void angleIdSelectsSameAsVi() throws Exception
	{
		// angle id 0 == view setup 0 in the simulated dataset
		assertEquals( 0, runClear( "--clearMode", "REMOVE_LABEL", "--label", LABEL, "--angleId", "0", "--silent" ) );
		final SpimData2 after = load();

		assertFalse( hasLabel( after, V0, LABEL ) );
		assertTrue( hasLabel( after, V1, LABEL ) );
		assertTrue( hasLabel( after, V2, LABEL ) );
		assertEquals( 0, corrsTo( after, V1, V0 ) );
		assertEquals( corrsTo( before, V1, V2 ), corrsTo( after, V1, V2 ) );
	}

	@Test
	public void clearEverythingOnSubset() throws Exception
	{
		assertEquals( 0, runClear( "--clearMode", "CLEAR_EVERYTHING", "-vi", "0,0", "--silent" ) );
		final SpimData2 after = load();

		assertFalse( hasEntry( after, V0 ), "(0,0) entry removed from XML map" );
		assertTrue( hasEntry( after, V1 ) );
		assertTrue( hasEntry( after, V2 ) );
		assertTrue( Files.isDirectory( tempDir.resolve( "interestpoints.n5" ) ), "container itself kept" );

		try ( final N5Reader n5 = n5() )
		{
			assertFalse( n5.exists( group( V0 ) ), "N5 view group of (0,0) removed" );
			assertTrue( n5.exists( group( V1 ) ) );
			assertTrue( n5.exists( group( V2 ) ) );
		}

		assertEquals( ipCount( before, V1 ), ipCount( after, V1 ) );
		assertEquals( 0, corrsTo( after, V1, V0 ) );
		assertEquals( 0, corrsTo( after, V2, V0 ) );
		assertEquals( corrsTo( before, V1, V2 ), corrsTo( after, V1, V2 ) );
		assertEquals( corrsTo( before, V2, V1 ), corrsTo( after, V2, V1 ) );
	}

	@Test
	public void clearAllCorrespondencesOnSubset() throws Exception
	{
		assertEquals( 0, runClear( "--clearMode", "CLEAR_ALL_CORRESPONDENCES", "-vi", "0,0", "--silent" ) );
		final SpimData2 after = load();

		for ( final ViewId v : ALL )
			assertEquals( ipCount( before, v ), ipCount( after, v ), "detections untouched in " + v );

		assertEquals( 0, corrCount( after, V0 ), "(0,0) has no correspondences left" );
		assertEquals( 0, corrsTo( after, V1, V0 ) );
		assertEquals( 0, corrsTo( after, V2, V0 ) );
		assertEquals( corrsTo( before, V1, V2 ), corrsTo( after, V1, V2 ), "(0,1) -> (0,2) untouched" );
		assertTrue( corrsTo( after, V1, V2 ) > 0 );
	}

	@Test
	public void clearEverythingNoFlagsRegression() throws Exception
	{
		// no --silent: exercises the listing path too
		assertEquals( 0, runClear( "--clearMode", "CLEAR_EVERYTHING" ) );
		final SpimData2 after = load();

		assertTrue( after.getViewInterestPoints().getViewInterestPoints().isEmpty(), "IP map empty" );
		assertFalse( Files.exists( tempDir.resolve( "interestpoints.n5" ) ), "whole container removed" );
	}

	@Test
	public void addLabelOnSubset() throws Exception
	{
		assertEquals( 0, runClear( "--clearMode", "ADD_LABEL", "--label", "empty", "-vi", "0,1", "--silent" ) );
		final SpimData2 after = load();

		assertTrue( hasLabel( after, V1, "empty" ) );
		assertFalse( hasLabel( after, V0, "empty" ) );
		assertFalse( hasLabel( after, V2, "empty" ) );
		assertEquals( 0, after.getViewInterestPoints().getViewInterestPoints().get( V1 ).getInterestPointList( "empty" ).getInterestPointsCopy().size() );

		try ( final N5Reader n5 = n5() )
		{
			assertTrue( n5.exists( group( V1 ) + "/empty/interestpoints" ) );
			assertFalse( n5.exists( group( V0 ) + "/empty" ) );
		}
	}

	@Test
	public void fixModeIgnoresSelection() throws Exception
	{
		assertEquals( 0, runClear( "--clearMode", "FIX_INTERESTPOINTS", "-vi", "0,0" ) );
		final SpimData2 after = load();

		for ( final ViewId v : ALL )
		{
			assertTrue( hasLabel( after, v, LABEL ) );
			assertEquals( ipCount( before, v ), ipCount( after, v ) );
			assertEquals( corrCount( before, v ), corrCount( after, v ) );
		}
	}

	@Test
	public void clearRegistrationsOnSubset() throws Exception
	{
		final int n0 = before.getViewRegistrations().getViewRegistration( V0 ).getTransformList().size();
		final int n1 = before.getViewRegistrations().getViewRegistration( V1 ).getTransformList().size();
		final int n2 = before.getViewRegistrations().getViewRegistration( V2 ).getTransformList().size();
		assertTrue( n0 > 0, "fixture: (0,0) has at least one transform" );

		assertEquals( 0, new CommandLine( new ClearRegistrations() ).execute( "-x", xmlPath, "--remove", "1", "-vi", "0,0" ) );
		final SpimData2 after = load();

		assertEquals( n0 - 1, after.getViewRegistrations().getViewRegistration( V0 ).getTransformList().size(), "(0,0) lost one transform" );
		assertEquals( n1, after.getViewRegistrations().getViewRegistration( V1 ).getTransformList().size(), "(0,1) untouched" );
		assertEquals( n2, after.getViewRegistrations().getViewRegistration( V2 ).getTransformList().size(), "(0,2) untouched" );
	}
}
