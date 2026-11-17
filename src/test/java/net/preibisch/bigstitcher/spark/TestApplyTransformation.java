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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewTransform;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.realtransform.AffineTransform3D;
import net.preibisch.mvrecon.SimulateUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import picocli.CommandLine;

/**
 * {@code apply-transformation} on the simulated 3-view dataset: every transform kind lands as the right matrix at
 * the right place of the selected views' transform lists, with the dialog's rotation convention; the other views,
 * refused calls and dry runs change nothing.
 */
public class TestApplyTransformation
{
	static final ViewId V0 = new ViewId( 0, 0 );
	static final ViewId V1 = new ViewId( 0, 1 );
	static final ViewId V2 = new ViewId( 0, 2 );

	@TempDir
	Path tempDir;
	URI xml;
	SpimData2 before;

	@BeforeEach
	void buildFixture() throws Exception
	{
		final SpimData2 sd = SimulateUtil.setUp();
		sd.setBasePathURI( tempDir.toUri() );
		xml = tempDir.resolve( "dataset.xml" ).toUri();
		new XmlIoSpimData2().save( sd, xml );
		before = load();
	}

	SpimData2 load() throws Exception
	{
		return new XmlIoSpimData2().load( xml );
	}

	int run( final String... args )
	{
		final String[] all = new String[ args.length + 2 ];
		all[ 0 ] = "-x";
		all[ 1 ] = tempDir.resolve( "dataset.xml" ).toString();
		System.arraycopy( args, 0, all, 2, args.length );
		return new CommandLine( new ApplyTransformation() ).execute( all );
	}

	static List< ViewTransform > transforms( final SpimData2 d, final ViewId v )
	{
		return d.getViewRegistrations().getViewRegistration( v ).getTransformList();
	}

	static double[] matrix( final ViewTransform t )
	{
		return t.asAffine3D().getRowPackedCopy();
	}

	void assertUnchanged( final SpimData2 after, final ViewId... views )
	{
		for ( final ViewId v : views )
		{
			final List< ViewTransform > a = transforms( before, v ), b = transforms( after, v );
			assertEquals( a.size(), b.size(), v + " unchanged" );
			for ( int i = 0; i < a.size(); ++i )
				assertArrayEquals( matrix( a.get( i ) ), matrix( b.get( i ) ), 1e-12, v + " transform " + i + " unchanged" );
		}
	}

	@Test
	public void rotationHasTheDialogsConvention() throws Exception
	{
		assertEquals( 0, run( "--rotate", "y,33", "-vi", "0,1" ) );
		final SpimData2 after = load();
		assertUnchanged( after, V0, V2 );
		final List< ViewTransform > t = transforms( after, V1 );
		assertEquals( transforms( before, V1 ).size() + 1, t.size() );
		// applied last = first in the list; the numbers BigStitcher wrote for "Rotation around y-axis by 33.0 degrees"
		assertEquals( "Manually defined transformation (Rotation around y-axis by 33.0 degrees)", t.get( 0 ).getName() );
		final double[] m = matrix( t.get( 0 ) );
		assertEquals( 0.838670567945424, m[ 0 ], 1e-12 );
		assertEquals( 0.5446390350150271, m[ 2 ], 1e-12 );
		assertEquals( -0.5446390350150271, m[ 8 ], 1e-12 );
		assertEquals( 0.838670567945424, m[ 10 ], 1e-12 );
		assertEquals( 1.0, m[ 5 ], 0 );
		assertEquals( 0.0, m[ 3 ], 0 );
		// the tail is the old chain
		for ( int i = 1; i < t.size(); ++i )
			assertArrayEquals( matrix( transforms( before, V1 ).get( i - 1 ) ), matrix( t.get( i ) ), 1e-12 );

		// x and z: a rotation by 90 degrees maps the axes as the right-hand rule says
		final double[] rx = ApplyTransformation.rotation( 0, 90 ).getRowPackedCopy();
		assertEquals( -1.0, rx[ 6 ], 1e-12 ); // y' = -z
		assertEquals( 1.0, rx[ 9 ], 1e-12 );  // z' = y
		final double[] rz = ApplyTransformation.rotation( 2, 90 ).getRowPackedCopy();
		assertEquals( -1.0, rz[ 1 ], 1e-12 ); // x' = -y
		assertEquals( 1.0, rz[ 4 ], 1e-12 );  // y' = x
	}

	@Test
	public void mirrorTranslateScaleAffine() throws Exception
	{
		assertEquals( 0, run( "--mirror", "z", "--angleId", "1" ) );
		SpimData2 after = load();
		assertUnchanged( after, V0, V2 );
		assertEquals( "Mirror z", transforms( after, V1 ).get( 0 ).getName() );
		assertArrayEquals( new double[] { 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, -1, 0 }, matrix( transforms( after, V1 ).get( 0 ) ), 0 );

		assertEquals( 0, run( "--translate", "-1635,-184,-524", "--name", "start", "-vi", "0,1" ) );
		after = load();
		assertEquals( "start", transforms( after, V1 ).get( 0 ).getName() );
		assertArrayEquals( new double[] { 1, 0, 0, -1635, 0, 1, 0, -184, 0, 0, 1, -524 }, matrix( transforms( after, V1 ).get( 0 ) ), 0 );
		assertEquals( "Mirror z", transforms( after, V1 ).get( 1 ).getName(), "the mirror moved down the list" );

		assertEquals( 0, run( "--scale", "1,2,0.5", "-vi", "0,0" ) );
		after = load();
		assertArrayEquals( new double[] { 1, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0.5, 0 }, matrix( transforms( after, V0 ).get( 0 ) ), 0 );

		final String a = "0.9628,0.0006,-0.0491,68.8046,0.0367,0.9987,0.0668,-86.4739,0.0308,0.0011,1.0377,-60.5159";
		assertEquals( 0, run( "--affine", a, "-vi", "0,2" ) );
		after = load();
		assertArrayEquals( new double[] { 0.9628, 0.0006, -0.0491, 68.8046, 0.0367, 0.9987, 0.0668, -86.4739, 0.0308, 0.0011, 1.0377, -60.5159 },
				matrix( transforms( after, V2 ).get( 0 ) ), 1e-12 );

		// without a selection every view gets it
		final int n0 = transforms( after, V0 ).size(), n1 = transforms( after, V1 ).size(), n2 = transforms( after, V2 ).size();
		assertEquals( 0, run( "--translate", "1,2,3" ) );
		after = load();
		assertEquals( n0 + 1, transforms( after, V0 ).size() );
		assertEquals( n1 + 1, transforms( after, V1 ).size() );
		assertEquals( n2 + 1, transforms( after, V2 ).size() );
	}

	@Test
	public void applyFirstGoesToTheEndOfTheChain() throws Exception
	{
		assertEquals( 0, run( "--translate", "0.5,0,0", "--applyFirst", "-vi", "0,1" ) );
		final SpimData2 after = load();
		final List< ViewTransform > t = transforms( after, V1 );
		assertEquals( transforms( before, V1 ).size() + 1, t.size() );
		assertEquals( 0.5, matrix( t.get( t.size() - 1 ) )[ 3 ], 0, "the new transform is applied first, i.e. last in the list" );
		for ( int i = 0; i < t.size() - 1; ++i )
			assertArrayEquals( matrix( transforms( before, V1 ).get( i ) ), matrix( t.get( i ) ), 1e-12 );
		// the full model is old o translation
		final ViewRegistration vr = after.getViewRegistrations().getViewRegistration( V1 );
		final AffineTransform3D expected = before.getViewRegistrations().getViewRegistration( V1 ).getModel().copy();
		expected.concatenate( t.get( t.size() - 1 ).asAffine3D() );
		assertArrayEquals( expected.getRowPackedCopy(), vr.getModel().getRowPackedCopy(), 1e-9 );
	}

	@Test
	public void centerOnView() throws Exception
	{
		// move (0,1) away first, then centre it on (0,2)
		assertEquals( 0, run( "--translate", "100,-50,30", "-vi", "0,1" ) );
		assertEquals( 0, run( "--centerOnView", "0,2", "-vi", "0,1" ) );
		final SpimData2 after = load();
		assertUnchanged( after, V0, V2 );
		final double[] c1 = ApplyTransformation.transformedCenter( after, V1 );
		final double[] c2 = ApplyTransformation.transformedCenter( after, V2 );
		assertArrayEquals( c2, c1, 1e-6, "centres coincide" );
		assertEquals( transforms( before, V1 ).size() + 2, transforms( after, V1 ).size() );
	}

	@Test
	public void refusalsAndDryRunChangeNothing() throws Exception
	{
		assertEquals( 0, run( "-vi", "0,1" ) );                                   // no transform
		assertUnchanged( load(), V0, V1, V2 );
		assertEquals( 0, run( "--rotate", "y,10", "--mirror", "z", "-vi", "0,1" ) ); // two transforms
		assertUnchanged( load(), V0, V1, V2 );
		assertEquals( 0, run( "--rotate", "w,10", "-vi", "0,1" ) );                 // bad axis
		assertUnchanged( load(), V0, V1, V2 );
		assertEquals( 0, run( "--affine", "1,2,3", "-vi", "0,1" ) );                // too few values
		assertUnchanged( load(), V0, V1, V2 );
		assertEquals( 2, run( "--shear", "xz,1.5", "-vi", "0,1" ) );                // no shear (future work): unknown option
		assertUnchanged( load(), V0, V1, V2 );
		assertEquals( 0, run( "--centerOnView", "0,7", "-vi", "0,1" ) );            // no such view
		assertUnchanged( load(), V0, V1, V2 );
		assertEquals( 0, run( "--centerOnView", "0,2", "--applyFirst", "-vi", "0,1" ) );
		assertUnchanged( load(), V0, V1, V2 );
		assertEquals( 0, run( "--rotate", "y,10", "-vi", "0,1", "--dryRun" ) );
		assertUnchanged( load(), V0, V1, V2 );
	}
}
