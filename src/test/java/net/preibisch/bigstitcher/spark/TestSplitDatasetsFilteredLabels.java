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

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.mvrecon.SimulateUtil;
import net.preibisch.mvrecon.fiji.plugin.Filter_Detections;
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
 * split-images with a filtered label: the split views carry the plain label's points and a filtered label of the split base
 * (not a plain label over a marker), which loads and filters as before
 */
public class TestSplitDatasetsFilteredLabels
{
	@TempDir
	Path tempDir;

	@Test
	public void filteredLabelsStayFiltered() throws Exception
	{
		System.setProperty( "spark.master", "local[*]" );

		final SpimData2 sd = SimulateUtil.setUp();
		sd.setBasePathURI( tempDir.toUri() );
		TestInterestPointDetection.testDoG( sd, "beads" );

		final List< ViewId > views = new ArrayList<>( sd.getSequenceDescription().getViewDescriptions().keySet() );
		final InterestPointFilter filter = InterestPointFilter.fromString( "limit=intensity:highest:20" );
		Filter_Detections.create( sd, views, "beads", "beads_top20", filter, false );

		final URI xml = tempDir.resolve( "dataset.xml" ).toUri();
		new XmlIoSpimData2().save( sd, xml );

		final String out = tempDir.resolve( "split.xml" ).toString();
		final int exit = new CommandLine( new SplitDatasets() ).execute(
				"-x", tempDir.resolve( "dataset.xml" ).toString(), "-xo", out,
				"--splitMethod", "uniform", "-tis", "64,64,50", "-to", "16,16,16", "--disableOptimization",
				"-fip", "NONE", "--localSparkBindAddress" );
		assertEquals( 0, exit, "split-images" );

		final SpimData2 split = new XmlIoSpimData2().load( new File( out ).toURI() );
		assertTrue( split.getSequenceDescription().getViewSetupsOrdered().size() > views.size(), "views were split" );

		int plain = 0, filtered = 0, points = 0;

		for ( final ViewInterestPointLists vipl : split.getViewInterestPoints().getViewInterestPoints().values() )
		{
			if ( !vipl.contains( "beads_split" ) )
				continue;

			final InterestPoints base = vipl.getInterestPointList( "beads_split" );
			assertFalse( base instanceof VirtualFilteredInterestPoints );
			final List< InterestPoint > basePoints = new ArrayList<>( base.getInterestPointsCopy().values() );
			points += basePoints.size();
			++plain;

			assertTrue( vipl.contains( "beads_top20_split" ), "the filtered label was split too" );
			final InterestPoints ips = vipl.getInterestPointList( "beads_top20_split" );
			assertTrue( ips instanceof VirtualFilteredInterestPoints, "still a filtered label" );
			final VirtualFilteredInterestPoints virtual = (VirtualFilteredInterestPoints)ips;
			assertEquals( "beads_split", virtual.getBaseLabel() );
			assertEquals( filter, virtual.getFilter() );
			assertEquals( filter.filter( basePoints ).size(), virtual.getInterestPointsCopy().size() );
			assertTrue( virtual.getInterestPointsCopy().size() <= 20 );
			assertTrue( virtual.getParameters().contains( "beads_split" ), virtual.getParameters() );
			++filtered;
		}

		assertTrue( plain > views.size() && filtered == plain, plain + " / " + filtered );
		assertTrue( points > 0 );
	}
}
