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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.awt.image.BufferedImage;
import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.imageio.ImageIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import mpicbg.models.AffineModel3D;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewTransform;
import mpicbg.spim.data.registration.ViewTransformAffine;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.RealInterval;
import net.imglib2.realtransform.AffineTransform3D;
import net.preibisch.bigstitcher.spark.render.Camera;
import net.preibisch.bigstitcher.spark.render.Camera.Plane;
import net.preibisch.bigstitcher.spark.render.HeadlessBdvRenderer;
import net.preibisch.bigstitcher.spark.render.ScreenshotManifest;
import net.preibisch.mvrecon.SimulateUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.simulation.SimulateMultiViewDataset;
import picocli.CommandLine;

/**
 * {@code screenshot} on the simulated 3-view dataset. The simulated views are one bead cloud rotated about x by 0, 45
 * and 90 degrees (UnsignedShortType, bead peaks about 1000), so the fixture registers setups 1 and 2 with the inverse
 * rotation: then aligned beads render white in a pair image and a shifted registration shows as magenta/green doubles.
 */
public class TestScreenshot
{
	static final ViewId V0 = new ViewId( 0, 0 ), V1 = new ViewId( 0, 1 ), V2 = new ViewId( 0, 2 );
	static final RealInterval VOLUME = new FinalRealInterval( new double[] { 0, 0, 0 }, new double[] { 127, 127, 49 } );

	@TempDir
	Path tempDir;
	URI xml;
	String xmlPath;
	SpimData2 before;

	@BeforeEach
	void buildFixture() throws Exception
	{
		final SpimData2 sd = SimulateUtil.setUp();
		sd.setBasePathURI( tempDir.toUri() );
		final int[] angles = { 0, 45, 90 };
		for ( int s = 1; s < 3; ++s )
		{
			final AffineModel3D inverse = SimulateMultiViewDataset.axisRotation( new FinalInterval( 128, 128, 50 ), 0, angles[ s ] ).createInverse();
			final AffineTransform3D t = new AffineTransform3D();
			t.set( inverse.getMatrix( null ) );
			final ViewRegistration vr = sd.getViewRegistrations().getViewRegistration( new ViewId( 0, s ) );
			vr.preconcatenateTransform( new ViewTransformAffine( "align", t ) );
			vr.updateModel();
		}
		xml = tempDir.resolve( "dataset.xml" ).toUri();
		xmlPath = tempDir.resolve( "dataset.xml" ).toString();
		new XmlIoSpimData2().save( sd, xml );
		before = load();
	}

	SpimData2 load() throws Exception
	{
		return new XmlIoSpimData2().load( xml );
	}

	File out( final String name )
	{
		return tempDir.resolve( name ).toFile();
	}

	int run( final File dir, final String... args )
	{
		final List< String > all = new ArrayList<>( Arrays.asList( "-x", xmlPath, "-o", dir.getAbsolutePath(), "--min", "0", "--max", "500" ) );
		if ( !Arrays.asList( args ).contains( "--width" ) )
			all.addAll( Arrays.asList( "--width", "254" ) );
		all.addAll( Arrays.asList( args ) );
		return new CommandLine( new Screenshot() ).execute( all.toArray( new String[ 0 ] ) );
	}

	int shift( final String... args )
	{
		final List< String > all = new ArrayList<>( Arrays.asList( "-x", xmlPath ) );
		all.addAll( Arrays.asList( args ) );
		return new CommandLine( new ApplyTransformation() ).execute( all.toArray( new String[ 0 ] ) );
	}

	static String[] pngs( final File dir )
	{
		final String[] f = dir.isDirectory() ? dir.list( ( d, n ) -> n.endsWith( ".png" ) ) : new String[ 0 ];
		Arrays.sort( f );
		return f;
	}

	static long nonBlack( final BufferedImage img )
	{
		long n = 0;
		for ( int y = 0; y < img.getHeight(); ++y )
			for ( int x = 0; x < img.getWidth(); ++x )
				if ( ( img.getRGB( x, y ) & 0xffffff ) != 0 )
					++n;
		return n;
	}

	/** the shift dx (in pixels) that maximises the correlation of the red channel with the green channel shifted by dx along x */
	static int peakShift( final BufferedImage img, final int maxShift )
	{
		final int w = img.getWidth(), h = img.getHeight();
		final int[] px = img.getRGB( 0, 0, w, h, null, 0, w );
		double best = -1;
		int bestDx = 0;
		for ( int dx = -maxShift; dx <= maxShift; ++dx )
		{
			double s = 0;
			for ( int y = 0; y < h; ++y )
				for ( int x = 0; x < w; ++x )
				{
					final int x2 = x + dx;
					if ( x2 < 0 || x2 >= w )
						continue;
					s += ( ( px[ y * w + x ] >> 16 ) & 0xff ) * (double) ( ( px[ y * w + x2 ] >> 8 ) & 0xff );
				}
			if ( s > best )
			{
				best = s;
				bestDx = dx;
			}
		}
		return bestDx;
	}

	void assertXmlUnchanged() throws Exception
	{
		final SpimData2 after = load();
		for ( final ViewId v : new ViewId[] { V0, V1, V2 } )
		{
			final List< ViewTransform > a = before.getViewRegistrations().getViewRegistration( v ).getTransformList();
			final List< ViewTransform > b = after.getViewRegistrations().getViewRegistration( v ).getTransformList();
			assertEquals( a.size(), b.size(), v + " unchanged" );
			for ( int i = 0; i < a.size(); ++i )
				assertArrayEquals( a.get( i ).asAffine3D().getRowPackedCopy(), b.get( i ).asAffine3D().getRowPackedCopy(), 1e-12 );
		}
	}

	@Test
	public void overviewSlicesInAllPlanes() throws Exception
	{
		final File dir = out( "a" );
		assertEquals( 0, run( dir, "--overview", "--planes", "xy,xz,yz", "--render", "slice", "--width", "256" ) );
		assertArrayEquals( new String[] { "overview_xy.png", "overview_xz.png", "overview_yz.png" }, pngs( dir ) );
		for ( final String f : pngs( dir ) )
		{
			final BufferedImage img = ImageIO.read( new File( dir, f ) );
			assertEquals( 256, img.getWidth(), f );
			assertTrue( img.getHeight() >= 16 && img.getHeight() <= 4096, f );
			assertFalse( img.getColorModel().hasAlpha(), f + " is opaque" );
			assertTrue( nonBlack( img ) > 100, f + " shows something" );
		}
		assertXmlUnchanged();
	}

	@Test
	public void misregistrationShowsAsDoubles() throws Exception
	{
		final File aligned = out( "aligned" );
		assertEquals( 0, run( aligned, "--pairs", "0,2", "--planes", "xy", "--margin", "0", "--json" ) );
		assertArrayEquals( new String[] { "pair_tp0_s0_s2_xy.png" }, pngs( aligned ) );
		final BufferedImage a = ImageIO.read( new File( aligned, "pair_tp0_s0_s2_xy.png" ) );
		assertTrue( nonBlack( a ) > 100 );
		final int dxAligned = peakShift( a, 80 );
		assertTrue( Math.abs( dxAligned ) <= 1, "aligned beads coincide, peak at " + dxAligned );

		final ScreenshotManifest m = ScreenshotManifest.load( new File( aligned, "screenshots.json" ) );
		assertEquals( 1, m.images.size() );
		final double scale = m.images.get( 0 ).scale;
		assertTrue( scale > 0 );
		assertEquals( "pair", m.images.get( 0 ).kind );
		assertArrayEquals( new int[] { 0, 2 }, m.images.get( 0 ).setups );
		assertEquals( "#ff00ff", m.images.get( 0 ).colors.get( "0" ) );
		assertEquals( "#00ff00", m.images.get( 0 ).colors.get( "2" ) );

		assertEquals( 0, shift( "--translate", "20,0,0", "-vi", "0,2" ) );
		final File shifted = out( "shifted" );
		assertEquals( 0, run( shifted, "--pairs", "0,2", "--planes", "xy", "--margin", "0", "--json" ) );
		final BufferedImage b = ImageIO.read( new File( shifted, "pair_tp0_s0_s2_xy.png" ) );
		final int dxShifted = peakShift( b, 80 );
		// the intersection box shrinks with the shift, so the scale of this image differs from the aligned one
		final double shiftedScale = ScreenshotManifest.load( new File( shifted, "screenshots.json" ) ).images.get( 0 ).scale;
		assertTrue( shiftedScale > scale, "a smaller box is drawn at a larger scale" );
		assertEquals( 20 * shiftedScale, dxShifted, 2, "the green view moved by 20 global px = " + 20 * shiftedScale + " image px, peak at " + dxShifted );
	}

	@Test
	public void singleViewsAreGrey() throws Exception
	{
		final File dir = out( "s" );
		assertEquals( 0, run( dir, "--single", "--planes", "xy" ) );
		assertArrayEquals( new String[] { "view_tp0_s0_xy.png", "view_tp0_s1_xy.png", "view_tp0_s2_xy.png" }, pngs( dir ) );
		for ( final String f : pngs( dir ) )
		{
			final BufferedImage img = ImageIO.read( new File( dir, f ) );
			assertTrue( nonBlack( img ) > 100, f );
			for ( int y = 0; y < img.getHeight(); y += 3 )
				for ( int x = 0; x < img.getWidth(); x += 3 )
				{
					final int rgb = img.getRGB( x, y );
					final int r = ( rgb >> 16 ) & 0xff, g = ( rgb >> 8 ) & 0xff, bl = rgb & 0xff;
					assertTrue( r == g && g == bl, f + " is grey at " + x + "," + y );
				}
		}
	}

	@Test
	public void overlappingPairs() throws Exception
	{
		final File all = out( "all" );
		assertEquals( 0, run( all, "--pairs", "overlapping", "--planes", "xy", "--render", "slice" ) );
		assertArrayEquals( new String[] { "pair_tp0_s0_s1_xy.png", "pair_tp0_s0_s2_xy.png", "pair_tp0_s1_s2_xy.png" }, pngs( all ) );

		assertEquals( 0, shift( "--translate", "10000,0,0", "-vi", "0,2" ) );
		final File one = out( "one" );
		assertEquals( 0, run( one, "--pairs", "overlapping", "--planes", "xy", "--render", "slice" ) );
		assertArrayEquals( new String[] { "pair_tp0_s0_s1_xy.png" }, pngs( one ) );
	}

	@Test
	public void manifestListsEveryFile() throws Exception
	{
		final File dir = out( "m" );
		assertEquals( 0, run( dir, "--overview", "--single", "--planes", "xy,xz", "--json", "--palette", "fixed" ) );
		final File json = new File( dir, "screenshots.json" );
		assertTrue( json.isFile() );
		final ScreenshotManifest m = ScreenshotManifest.load( json );
		assertEquals( 8, m.images.size(), "2 overview + 6 single" );
		assertEquals( 8, pngs( dir ).length );
		for ( final ScreenshotManifest.Entry e : m.images )
		{
			assertTrue( new File( dir, e.file ).isFile(), e.file );
			assertEquals( 12, e.viewerTransform.length );
			assertTrue( e.scale > 0 );
			assertEquals( "mip", e.render );
			assertNotNull( e.depth );
			assertTrue( e.frames >= 1 );
			for ( final int s : e.setups )
			{
				assertNotNull( e.colors.get( Integer.toString( s ) ) );
				assertArrayEquals( new double[] { 0, 500 }, e.ranges.get( Integer.toString( s ) ), 0 );
			}
		}
		// the fixed palette: magenta, green, cyan for the three views of the overview
		final ScreenshotManifest.Entry ov = m.images.stream().filter( e -> e.kind.equals( "overview" ) ).findFirst().get();
		assertEquals( "#ff00ff", ov.colors.get( "0" ) );
		assertEquals( "#00ff00", ov.colors.get( "1" ) );
		assertEquals( "#00ffff", ov.colors.get( "2" ) );
	}

	@Test
	public void refusalsWriteNothing() throws Exception
	{
		final File dir = out( "r" );
		final String[][] bad = {
				{},                                                        // nothing to render
				{ "--overview", "--planes", "xw" },
				{ "--pairs", "0;1" },
				{ "--pairs", "0,0" },
				{ "--pairs", "0,9" },                                      // not a selected view
				{ "--overview", "--render", "foo" },
				{ "--overview", "--width", "4" },
				{ "--overview", "--auto" },                                // together with the common --min/--max
				{ "--overview", "--range", "0=5:1" },
				{ "--overview", "--maxScale", "0" },
				{ "--overview", "--palette", "red,green" },                // two colours for three views
				{ "--overview", "--transform", tempDir.resolve( "missing.json" ).toString() },
				{ "--single", "--timepointId", "7" },
		};
		for ( final String[] args : bad )
		{
			assertEquals( 0, run( dir, args ), Arrays.toString( args ) );
			assertFalse( dir.exists(), Arrays.toString( args ) + " wrote nothing" );
		}
		assertEquals( 2, run( dir, "--overview", "--nope" ), "unknown option" );
		assertFalse( dir.exists() );
		assertEquals( 0, run( dir, "--overview", "--dryRun" ) );
		assertFalse( dir.exists(), "dry run writes nothing" );
		assertXmlUnchanged();
	}

	@Test
	public void explicitTransforms() throws Exception
	{
		final AffineTransform3D t0 = Camera.viewerTransform( VOLUME, Plane.XY, 128, 128, 0.1 );
		final AffineTransform3D t1 = Camera.viewerTransformWithScale( VOLUME, Plane.XZ, 128, 128, 2 * Camera.scale( t0 ) );
		final File json = out( "transforms.json" );
		ScreenshotManifest.saveTransforms( Arrays.asList( t0, t1 ), json );
		assertEquals( 2, ScreenshotManifest.loadTransforms( json ).size() );
		assertArrayEquals( t1.getRowPackedCopy(), ScreenshotManifest.loadTransforms( json ).get( 1 ).getRowPackedCopy(), 1e-12 );

		final File dir = out( "t" );
		assertEquals( 0, run( dir, "--transform", json.getAbsolutePath(), "--width", "128", "--height", "128", "--render", "slice", "--json" ) );
		assertArrayEquals( new String[] { "transform0.png", "transform1.png" }, pngs( dir ) );
		for ( final String f : pngs( dir ) )
		{
			final BufferedImage img = ImageIO.read( new File( dir, f ) );
			assertEquals( 128, img.getWidth() );
			assertEquals( 128, img.getHeight() );
			assertTrue( nonBlack( img ) > 50, f );
		}
		final ScreenshotManifest m = ScreenshotManifest.load( new File( dir, "screenshots.json" ) );
		assertEquals( "transform", m.images.get( 0 ).kind );
		assertNull( m.images.get( 0 ).plane );
		assertArrayEquals( t0.getRowPackedCopy(), m.images.get( 0 ).viewerTransform, 1e-12 );
	}

	@Test
	public void rangeInsideBox() throws Exception
	{
		final SpimData2 d = load();
		try ( final HeadlessBdvRenderer r = new HeadlessBdvRenderer( d, 2, null ) )
		{
			r.setTimepointIndex( 0 );
			final double[] whole = r.estimateDisplayRange( 0, 0, 0.001, 0.999, 1_000_000 );
			final double[] inBox = r.estimateDisplayRange( 0, 0, 0.001, 0.999, 1_000_000, new FinalRealInterval( new double[] { 0, 0, 0 }, new double[] { 63, 63, 24 } ) );
			assertNotNull( whole );
			assertNotNull( inBox );
			assertTrue( whole[ 1 ] > whole[ 0 ] && whole[ 1 ] > 100, "beads reach the upper percentile: " + Arrays.toString( whole ) );
			assertTrue( inBox[ 1 ] > inBox[ 0 ] && inBox[ 1 ] > 100, Arrays.toString( inBox ) );
			assertNull( r.estimateDisplayRange( 0, 0, 0.001, 0.999, 1_000_000, new FinalRealInterval( new double[] { 1000, 1000, 1000 }, new double[] { 1100, 1100, 1100 } ) ), "a box outside the view" );
		}
	}

	@Test
	public void cameraMaths()
	{
		final RealInterval box = VOLUME;
		final double[] c = Camera.centre( box );
		for ( final Plane p : Plane.values() )
		{
			final int w = 256, h = Camera.defaultHeight( box, p, w );
			final AffineTransform3D t = Camera.viewerTransform( box, p, w, h, 0.1 );
			final double[] out = new double[ 3 ];
			t.apply( c, out );
			assertArrayEquals( new double[] { w / 2.0, h / 2.0, 0 }, out, 1e-9, "centre of " + p );
			final double s = Camera.scale( t );
			// all corners inside the margin
			final double[] in = new double[ 3 ];
			for ( int i = 0; i < 8; ++i )
			{
				for ( int d = 0; d < 3; ++d )
					in[ d ] = ( ( i >> d ) & 1 ) == 0 ? box.realMin( d ) : box.realMax( d );
				t.apply( in, out );
				assertTrue( out[ 0 ] >= -1e-9 && out[ 0 ] <= w + 1e-9 && out[ 1 ] >= -1e-9 && out[ 1 ] <= h + 1e-9, p + " corner " + i + " inside: " + Arrays.toString( out ) );
			}
			// screen-z range is centred on 0
			final double[] zr = Camera.screenZRange( box, t );
			assertEquals( -zr[ 0 ], zr[ 1 ], 1e-9, p + " z range centred" );
			// axis directions
			final double[] q = c.clone();
			if ( p == Plane.XY )
			{
				q[ 0 ] += 10; t.apply( q, out ); assertEquals( w / 2.0 + 10 * s, out[ 0 ], 1e-9 );
				q[ 0 ] -= 10; q[ 2 ] += 10; t.apply( q, out ); assertEquals( 10 * s, out[ 2 ], 1e-9 );
			}
			else if ( p == Plane.XZ )
			{
				q[ 2 ] += 10; t.apply( q, out ); assertEquals( h / 2.0 + 10 * s, out[ 1 ], 1e-9, "z goes down" );
				q[ 2 ] -= 10; q[ 1 ] += 10; t.apply( q, out ); assertEquals( -10 * s, out[ 2 ], 1e-9, "screen z = -y" );
			}
			else
			{
				q[ 1 ] += 10; t.apply( q, out ); assertEquals( w / 2.0 + 10 * s, out[ 0 ], 1e-9, "y goes right" );
				q[ 1 ] -= 10; q[ 0 ] += 10; t.apply( q, out ); assertEquals( 10 * s, out[ 2 ], 1e-9, "screen z = +x" );
			}
		}
		assertEquals( 256, Camera.defaultHeight( box, Plane.XY, 256 ) );
		assertEquals( 99, Camera.defaultHeight( box, Plane.XZ, 256 ) );
		assertEquals( 99, Camera.defaultHeight( box, Plane.YZ, 256 ) );
		assertEquals( 16, Camera.defaultHeight( new FinalRealInterval( new double[] { 0, 0, 0 }, new double[] { 1000, 1, 1 } ), Plane.XY, 256 ) );
		assertEquals( 1 / 0.149, Camera.mipStepGlobal( 201, 0.149, null ), 1e-9, "one screen pixel dominates" );
		assertEquals( 2498 / 256.0, Camera.mipStepGlobal( 2498, 0.149, null ), 1e-9, "the frame cap dominates" );
		assertEquals( 1.0, Camera.mipStepGlobal( 49, 2, null ), 1e-9 );
		assertEquals( 1 / 0.2, Camera.mipStepGlobal( 10, 0.2, null ), 1e-9 );
		assertEquals( 3.5, Camera.mipStepGlobal( 49, 2, 3.5 ), 1e-9 );
		assertEquals( 3.19, Camera.mipStepGlobal( 275, 6, null, 3.19 ), 1e-9, "one voxel along the axis dominates" );

		// fitting: a wide box fills the width, a thin strip is capped at --maxScale and gets a narrow image
		final Camera.Fit wide = Camera.fit( box, Plane.XY, 1024, null, 0.1, 100 );
		assertEquals( 1024, wide.width() );
		assertEquals( 1024, wide.height(), "square box, square image" );
		assertEquals( 1024 / ( 127 * 1.2 ), wide.scale(), 1e-9 );
		final Camera.Fit capped = Camera.fit( box, Plane.XY, 1024, null, 0.1, 4 );
		assertEquals( 4, capped.scale(), 1e-9, "the default cap binds for a 127 px box at width 1024" );
		assertEquals( (int) Math.ceil( 127 * 1.2 * 4 ), capped.width() );
		assertEquals( (int) Math.ceil( 127 * 1.2 * 4 ), capped.height() );
		final RealInterval strip = new FinalRealInterval( new double[] { 0, 0, 0 }, new double[] { 50, 512, 275 } );
		final Camera.Fit thin = Camera.fit( strip, Plane.XY, 1024, null, 0.1, 4 );
		assertEquals( 4, thin.scale(), 1e-9, "capped" );
		assertEquals( (int) Math.ceil( 50 * 1.2 * 4 ), thin.width(), "shrunk to the strip" );
		assertEquals( (int) Math.ceil( 512 * 1.2 * 4 ), thin.height() );
		final Camera.Fit given = Camera.fit( strip, Plane.XY, 1024, 600, 0.1, 100 );
		assertEquals( 1024, given.width() );
		assertEquals( 600, given.height(), "the given height is kept" );
		assertEquals( 600 / ( 512 * 1.2 ), given.scale(), 1e-9 );
		final double[] out = new double[ 3 ];
		thin.t().apply( Camera.centre( strip ), out );
		assertArrayEquals( new double[] { thin.width() / 2.0, thin.height() / 2.0, 0 }, out, 1e-9 );

		// voxel extent along screen z: anisotropic calibration of 1,1,3.19 seen along z, y and x
		final AffineTransform3D cal = new AffineTransform3D();
		cal.set( 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 3.19, 0 );
		assertEquals( 3.19, Camera.voxelExtentAlongScreenZ( Camera.viewerTransform( box, Plane.XY, 256, 256, 0 ), cal ), 1e-9 );
		assertEquals( 1.0, Camera.voxelExtentAlongScreenZ( Camera.viewerTransform( box, Plane.XZ, 256, 256, 0 ), cal ), 1e-9 );
		assertEquals( 1.0, Camera.voxelExtentAlongScreenZ( Camera.viewerTransform( box, Plane.YZ, 256, 256, 0 ), cal ), 1e-9 );
	}
}
