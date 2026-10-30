/*-
 * #%L
 * Spark-based parallel BigStitcher project.
 * %%
 * Copyright (C) 2021 - 2025 Developers.
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.mvrecon.SimulateUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.CorrespondingInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoints;
import net.preibisch.mvrecon.tests.TestInterestPointArrays;
import net.preibisch.mvrecon.tests.TestInterestPointDetection;
import picocli.CommandLine;

/**
 * {@code match-interestpoints -m EMBEDDINGS} end to end on the simulated beads: FAST_ROTATION finds the true
 * correspondences first, the test plants one random unit vector per corresponding group of points (and one of its own
 * per unmatched point) as the per-point array {@code descriptor} of the store, exactly as pcmatch writes it, and the
 * embeddings matcher has to find the planted pairs again from the store alone (every point within the radius of
 * every other, so the vectors decide). Without {@code --searchRadius} the command refuses and changes nothing.
 */
public class TestEmbeddings
{
	@TempDir
	Path tempDir;

	static final String LABEL = "beads";
	static final int COLUMNS = 16;

	@Test
	public void matchesPlantedDescriptors() throws Exception
	{
		System.setProperty( "spark.master", "local[*]" );

		final File xmlFile = tempDir.resolve( "dataset.xml" ).toFile();
		final SpimData2 simulated = SimulateUtil.setUp();
		new XmlIoSpimData2().save( simulated, xmlFile.toURI() );
		TestInterestPointDetection.testDoG( simulated, LABEL );
		new XmlIoSpimData2().save( simulated, xmlFile.toURI() );
		final String xml = xmlFile.getAbsolutePath();

		// the true pairs, found the classic way
		run( new SparkGeometricDescriptorMatching(), "-x", xml, "-l", LABEL, "-m", "FAST_ROTATION", "--localSparkBindAddress" );

		final SpimData2 data = new XmlIoSpimData2().load( xmlFile.toURI() );
		final URI base = data.getBasePathURI();
		final List< ViewId > views = new ArrayList<>();
		for ( final ViewId v : data.getViewInterestPoints().getViewInterestPoints().keySet() )
			views.add( new ViewId( v.getTimePointId(), v.getViewSetupId() ) );
		views.sort( ( a, b ) -> Integer.compare( a.getViewSetupId(), b.getViewSetupId() ) );
		assertEquals( 3, views.size() );

		// one vector per connected group of corresponding points: union-find over (view, id)
		final Map< String, String > parent = new HashMap<>();
		final Map< ViewId, Map< Integer, InterestPoint > > points = new HashMap<>();
		final TreeMap< String, Integer > planted = new TreeMap<>();
		for ( final ViewId v : views )
		{
			points.put( v, data.getViewInterestPoints().getViewInterestPointLists( v ).getInterestPointList( LABEL ).getInterestPointsCopy() );
			for ( final int id : points.get( v ).keySet() )
				parent.put( node( v, id ), node( v, id ) );
		}
		for ( final ViewId v : views )
		{
			final InterestPoints list = data.getViewInterestPoints().getViewInterestPointLists( v ).getInterestPointList( LABEL );
			for ( final CorrespondingInterestPoints c : list.getCorrespondingInterestPointsCopy() )
			{
				union( parent, node( v, c.getDetectionId() ), node( c.getCorrespondingViewId(), c.getCorrespondingDetectionId() ) );
				if ( v.getViewSetupId() < c.getCorrespondingViewId().getViewSetupId() )
					planted.merge( pair( v, c.getCorrespondingViewId() ), 1, Integer::sum );
			}
		}
		assertEquals( 3, planted.size(), "FAST_ROTATION should link all three view pairs: " + planted );
		planted.values().forEach( n -> assertTrue( n >= 20, "too few true pairs to test with: " + planted ) );

		final Random random = new Random( 1 );
		final Map< String, float[] > vectorOf = new HashMap<>();
		for ( final ViewId v : views )
		{
			final Map< Integer, InterestPoint > local = points.get( v );
			final int[] ids = new int[ local.size() ];
			final float[] values = new float[ COLUMNS * local.size() ];
			int r = 0;
			for ( final InterestPoint point : local.values() )
			{
				ids[ r ] = point.getId();
				final float[] vector = vectorOf.computeIfAbsent( find( parent, node( v, point.getId() ) ), k -> unit( random ) );
				System.arraycopy( vector, 0, values, COLUMNS * r, COLUMNS );
				++r;
			}
			TestInterestPointArrays.writeArray( base, LABEL, v.getTimePointId(), "descriptor", v.getViewSetupId(), ids, values, COLUMNS, "planted-by-the-test" );
		}

		// without a radius the command refuses: the classic correspondences stay
		run( new SparkGeometricDescriptorMatching(), "-x", xml, "-l", LABEL, "-m", "EMBEDDINGS", "--clearCorrespondences", "--localSparkBindAddress" );
		assertEquals( planted, correspondences( xmlFile, LABEL ) );

		// from the store alone: a radius larger than the volume, so that only the vectors tell the partners apart
		run( new SparkGeometricDescriptorMatching(), "-x", xml, "-l", LABEL, "-m", "EMBEDDINGS", "--searchRadius", "100000", "-tm", "RIGID", "-rm", "NONE",
				"--clearCorrespondences", "--localSparkBindAddress" );

		final TreeMap< String, Integer > found = correspondences( xmlFile, LABEL );
		assertEquals( planted.keySet(), found.keySet(), "pairs: planted " + planted + ", found " + found );
		for ( final String pair : planted.keySet() )
		{
			final int expected = planted.get( pair ), actual = found.get( pair );
			assertTrue( actual >= 0.9 * expected && actual <= expected + 3, pair + ": planted " + expected + " pairs, found " + actual );
		}
	}

	/** The stored correspondences per view pair (lower setup first), counted once per pair. */
	static TreeMap< String, Integer > correspondences( final File xmlFile, final String label ) throws Exception
	{
		final SpimData2 data = new XmlIoSpimData2().load( xmlFile.toURI() );
		final TreeMap< String, Integer > counts = new TreeMap<>();
		for ( final ViewId v : data.getViewInterestPoints().getViewInterestPoints().keySet() )
			for ( final CorrespondingInterestPoints c : data.getViewInterestPoints().getViewInterestPointLists( v ).getInterestPointList( label ).getCorrespondingInterestPointsCopy() )
				if ( v.getViewSetupId() < c.getCorrespondingViewId().getViewSetupId() )
					counts.merge( pair( v, c.getCorrespondingViewId() ), 1, Integer::sum );
		return counts;
	}

	static float[] unit( final Random random )
	{
		final float[] v = new float[ COLUMNS ];
		double norm = 0;
		for ( int d = 0; d < COLUMNS; ++d )
		{
			v[ d ] = (float) random.nextGaussian();
			norm += v[ d ] * v[ d ];
		}
		for ( int d = 0; d < COLUMNS; ++d )
			v[ d ] /= (float) Math.sqrt( norm );
		return v;
	}

	static String node( final ViewId v, final int id ) { return v.getTimePointId() + "," + v.getViewSetupId() + ":" + id; }

	static String pair( final ViewId a, final ViewId b ) { return a.getViewSetupId() + "-" + b.getViewSetupId(); }

	static String find( final Map< String, String > parent, final String node )
	{
		String root = node;
		while ( !parent.get( root ).equals( root ) )
			root = parent.get( root );
		return root;
	}

	static void union( final Map< String, String > parent, final String a, final String b )
	{
		parent.put( find( parent, a ), find( parent, b ) );
	}

	private static void run( final Object cmd, final String... args )
	{
		final int exit = new CommandLine( cmd ).execute( args );
		if ( exit != 0 )
			throw new RuntimeException( cmd.getClass().getSimpleName() + " failed with exit code " + exit );
	}
}
