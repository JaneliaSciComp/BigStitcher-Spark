/*-
 * #%L
 * Spark-based parallel BigStitcher project.
 * %%
 * Copyright (C) 2021 - 2024 Developers.
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
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.mvrecon.SimulateUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointFilter;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPointLists;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.VirtualFilteredInterestPoints;
import net.preibisch.mvrecon.tests.TestInterestPointDetection;
import picocli.CommandLine;

/**
 * filter-interestpoints on a detection of the simulated beads: filtered labels (ranges, a count limit, several entries), a
 * plain copy, the XML round trip, and the refusals
 */
public class TestFilterInterestPoints
{
	static final String LABEL = "beads";

	@TempDir
	Path tempDir;

	String xmlPath;

	@BeforeEach
	void detect() throws Exception
	{
		final SpimData2 sd = SimulateUtil.setUp();
		sd.setBasePathURI( tempDir.toUri() );
		TestInterestPointDetection.testDoG( sd, LABEL );

		final URI xml = tempDir.resolve( "dataset.xml" ).toUri();
		new XmlIoSpimData2().save( sd, xml );
		xmlPath = tempDir.resolve( "dataset.xml" ).toString();
	}

	SpimData2 load() throws Exception
	{
		return new XmlIoSpimData2().load( tempDir.resolve( "dataset.xml" ).toUri() );
	}

	int run( final String... filterArgs )
	{
		final String[] args = new String[ filterArgs.length + 2 ];
		args[ 0 ] = "-x";
		args[ 1 ] = xmlPath;
		System.arraycopy( filterArgs, 0, args, 2, filterArgs.length );
		return new CommandLine( new FilterInterestPoints() ).execute( args );
	}

	static int count( final SpimData2 data, final ViewId viewId, final String label )
	{
		return data.getViewInterestPoints().getViewInterestPointLists( viewId ).getInterestPointList( label ).getInterestPointsCopy().size();
	}

	@Test
	public void testFilteredLabels() throws Exception
	{
		final SpimData2 before = load();
		final List< ViewId > views = SpimData2.getAllViewIdsSorted( before, before.getSequenceDescription().getViewSetupsOrdered(), before.getSequenceDescription().getTimePoints().getTimePointsOrdered() );
		assertTrue( views.size() >= 2 );

		// the median response of the first view: a range that keeps some, not all
		final List< InterestPoint > points = new java.util.ArrayList<>( before.getViewInterestPoints().getViewInterestPointLists( views.get( 0 ) ).getInterestPointList( LABEL ).getInterestPointsCopy().values() );
		points.sort( ( a, b ) -> Float.compare( Math.abs( a.getResponse() ), Math.abs( b.getResponse() ) ) );
		final double cut = Math.abs( points.get( points.size() / 2 ).getResponse() );
		final String cutString = InterestPointFilter.plain( InterestPointFilter.fromString( "response=" + cut + ".." ).responseMin );

		// a response range plus a count limit by intensity: one filtered label, named by the suffix
		assertEquals( 0, run( "-l", LABEL, "--response", cutString, "--keepPoints", "20", "--keepBy", "intensity" ) );

		final InterestPointFilter filter = InterestPointFilter.fromString( "response=" + cutString + "..;limit=intensity:highest:20" );
		final String name = LABEL + filter.suffix();
		SpimData2 after = load();

		for ( final ViewId viewId : views )
		{
			final ViewInterestPointLists vipl = after.getViewInterestPoints().getViewInterestPointLists( viewId );
			assertTrue( vipl.contains( name ), name + " in " + viewId );
			final InterestPoints ips = vipl.getInterestPointList( name );
			assertTrue( ips instanceof VirtualFilteredInterestPoints );
			assertEquals( LABEL, ( (VirtualFilteredInterestPoints)ips ).getBaseLabel() );
			assertEquals( filter, ( (VirtualFilteredInterestPoints)ips ).getFilter() );

			// what the filter keeps of the base, at most 20
			final List< InterestPoint > base = new java.util.ArrayList<>( vipl.getInterestPointList( LABEL ).getInterestPointsCopy().values() );
			final int expected = filter.filter( base ).size();
			assertEquals( expected, count( after, viewId, name ) );
			assertTrue( expected <= 20 && expected <= base.size() );
			assertEquals( count( before, viewId, LABEL ), base.size(), "the base is untouched" );
		}

		assertEquals( 1, after.getActionHistory().getRecords().stream().filter( r -> "filter-interestpoints".equals( r.getActionId() ) ).count() );

		// several entries: one label per entry, the smaller cut keeps at least as many
		assertEquals( 0, run( "-l", LABEL, "--response", cutString + "," + InterestPointFilter.plain( cut / 2 ) ) );
		after = load();
		final String nameA = LABEL + InterestPointFilter.fromString( "response=" + cutString + ".." ).suffix();
		final String nameB = LABEL + InterestPointFilter.fromString( "response=" + InterestPointFilter.plain( cut / 2 ) + ".." ).suffix();
		for ( final ViewId viewId : views )
		{
			assertTrue( after.getViewInterestPoints().getViewInterestPointLists( viewId ).contains( nameA ) && after.getViewInterestPoints().getViewInterestPointLists( viewId ).contains( nameB ) );
			assertTrue( count( after, viewId, nameB ) >= count( after, viewId, nameA ) );
			assertTrue( count( after, viewId, nameA ) < count( after, viewId, LABEL ) );
		}

		// a plain copy under a given name, the canonical range grammar of the action history, one view only
		assertEquals( 0, run( "-l", LABEL, "--response", cutString + "..", "--newLabel", "strong", "--copy", "-vi", views.get( 0 ).getTimePointId() + "," + views.get( 0 ).getViewSetupId() ) );
		after = load();
		final InterestPoints copy = after.getViewInterestPoints().getViewInterestPointLists( views.get( 0 ) ).getInterestPointList( "strong" );
		assertFalse( copy instanceof VirtualFilteredInterestPoints );
		assertEquals( count( after, views.get( 0 ), nameA ), copy.getInterestPointsCopy().size() );
		assertFalse( after.getViewInterestPoints().getViewInterestPointLists( views.get( 1 ) ).contains( "strong" ) );

		// nothing to filter by, a malformed entry, a cycle: nothing is created (the command returns without an error code, as the other utilities do)
		final int labelsBefore = after.getViewInterestPoints().getViewInterestPointLists( views.get( 0 ) ).getHashMap().size();
		assertEquals( 0, run( "-l", LABEL ) );
		assertEquals( 0, run( "-l", LABEL, "--response", "abc" ) );
		assertEquals( 0, run( "-l", nameA, "--response", cutString, "--newLabel", LABEL ) );
		assertEquals( 0, run( "-l", LABEL, "--keepPoints", "0" ) );
		assertEquals( labelsBefore, load().getViewInterestPoints().getViewInterestPointLists( views.get( 0 ) ).getHashMap().size() );

		// a dry run creates nothing either
		assertEquals( 0, run( "-l", LABEL, "--sigma", "0-100", "--dryRun" ) );
		assertEquals( labelsBefore, load().getViewInterestPoints().getViewInterestPointLists( views.get( 0 ) ).getHashMap().size() );
	}

	@Test
	public void testFilters()
	{
		assertEquals( "0.004", FilterInterestPoints.dialogEntries( "0.004..", InterestPointFilter.Axis.RESPONSE ) );
		assertEquals( "0.004-0.01,0-5", FilterInterestPoints.dialogEntries( " 0.004-0.01, ..5 ", InterestPointFilter.Axis.RESPONSE ) );
		assertEquals( "", FilterInterestPoints.dialogEntries( null, InterestPointFilter.Axis.RESPONSE ) );

		final List< InterestPointFilter > filters = FilterInterestPoints.filters( "0.004,0.008", null, "1.5-3", 100, "response", "median" );
		assertEquals( 2, filters.size() );
		assertEquals( "response=0.004..;sigma=1.5..3;limit=response:median:100", filters.get( 0 ).toString() );
		assertEquals( "response=0.008..;sigma=1.5..3;limit=response:median:100", filters.get( 1 ).toString() );

		final List< InterestPointFilter > limitOnly = FilterInterestPoints.filters( null, null, null, 7, "Intensity", "HIGHEST" );
		assertEquals( 1, limitOnly.size() );
		assertEquals( "limit=intensity:highest:7", limitOnly.get( 0 ).toString() );

		assertTrue( FilterInterestPoints.filters( null, null, null, null, "response", "highest" ).isEmpty() );
	}
}
