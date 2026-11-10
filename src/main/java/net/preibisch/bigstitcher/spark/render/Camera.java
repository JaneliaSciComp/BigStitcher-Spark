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
package net.preibisch.bigstitcher.spark.render;

import bdv.util.Affine3DHelpers;
import net.imglib2.RealInterval;
import net.imglib2.realtransform.AffineTransform3D;

/**
 * Camera maths for headless BigDataViewer renderings, without any BDV state. BigDataViewer's convention: the
 * viewer transform maps global coordinates to screen pixels, x to the right, y down, z into the screen, and the
 * rendered plane is screen z = 0.
 */
public final class Camera
{
	private Camera() {}

	/** the viewing axis of the global frame; the image plane is spanned by the other two axes */
	public enum Plane
	{
		/** looking along z: x to the right, y down (BigDataViewer's default view) */
		XY,
		/** looking along y: x to the right, z down */
		XZ,
		/** looking along x: y to the right, z down */
		YZ;

		public static Plane parse( final String s )
		{
			switch ( s.trim().toLowerCase() )
			{
			case "xy": return XY;
			case "xz": return XZ;
			case "yz": return YZ;
			default: throw new IllegalArgumentException( "unknown plane '" + s + "', use xy, xz or yz" );
			}
		}

		public String lower()
		{
			return name().toLowerCase();
		}
	}

	public static final int MIN_HEIGHT = 16;
	public static final int MAX_HEIGHT = 4096;
	/** a maximum projection renders at most this many frames plus one */
	public static final int MAX_MIP_FRAMES = 256;

	/**
	 * The rotation that takes the two in-plane global axes to screen x and y and the viewing axis to screen z.
	 * All three are proper rotations (determinant +1), so the images equal what BigDataViewer shows after
	 * rotating the view with the mouse, without a mirror.
	 */
	public static AffineTransform3D rotation( final Plane p )
	{
		final AffineTransform3D r = new AffineTransform3D();
		switch ( p )
		{
		case XY:
			break;
		case XZ: // x -> x, z -> y (down), screen z = -y
			r.set( 1, 0, 0, 0, 0, 0, 1, 0, 0, -1, 0, 0 );
			break;
		case YZ: // y -> x, z -> y (down), screen z = +x
			r.set( 0, 1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0 );
			break;
		}
		return r;
	}

	public static double[] centre( final RealInterval box )
	{
		final double[] c = new double[ 3 ];
		for ( int d = 0; d < 3; ++d )
			c[ d ] = ( box.realMin( d ) + box.realMax( d ) ) / 2.0;
		return c;
	}

	/** extents of the box after the plane's rotation: along screen x, screen y and screen z */
	public static double[] extents( final RealInterval box, final Plane p )
	{
		final RealInterval rb = rotation( p ).estimateBounds( box );
		return new double[] { rb.realMax( 0 ) - rb.realMin( 0 ), rb.realMax( 1 ) - rb.realMin( 1 ), rb.realMax( 2 ) - rb.realMin( 2 ) };
	}

	/** the image height that keeps the aspect of the box in the plane for the given width, clamped to 16..4096 */
	public static int defaultHeight( final RealInterval box, final Plane p, final int width )
	{
		final double[] e = extents( box, p );
		final long h = Math.round( width * Math.max( 1, e[ 1 ] ) / Math.max( 1, e[ 0 ] ) );
		return (int) Math.min( MAX_HEIGHT, Math.max( MIN_HEIGHT, h ) );
	}

	/**
	 * Global to screen: the box centre lands at (width/2, height/2, 0) and the box, enlarged by {@code margin} times
	 * its extent on every side, fits into width x height (the smaller of the two scales wins).
	 */
	public static AffineTransform3D viewerTransform( final RealInterval box, final Plane p, final int width, final int height, final double margin )
	{
		final double[] e = extents( box, p );
		final double s = Math.min(
				width / ( Math.max( 1, e[ 0 ] ) * ( 1 + 2 * margin ) ),
				height / ( Math.max( 1, e[ 1 ] ) * ( 1 + 2 * margin ) ) );
		return viewerTransformWithScale( box, p, width, height, s );
	}

	/** as above with a given scale (screen pixels per global pixel) instead of fitting the box */
	public static AffineTransform3D viewerTransformWithScale( final RealInterval box, final Plane p, final int width, final int height, final double s )
	{
		final double[] c = centre( box );
		final AffineTransform3D t = new AffineTransform3D();
		t.translate( -c[ 0 ], -c[ 1 ], -c[ 2 ] );   // move the box centre to the origin
		t.preConcatenate( rotation( p ) );           // rotate into the plane
		final AffineTransform3D sc = new AffineTransform3D();
		sc.scale( s );
		t.preConcatenate( sc );                       // global px -> screen px
		final AffineTransform3D tr = new AffineTransform3D();
		tr.translate( width / 2.0, height / 2.0, 0 );
		t.preConcatenate( tr );                       // centre of the image
		return t;
	}

	/** a fitted camera: the viewer transform, the image size and the scale (screen pixels per global pixel) */
	public record Fit( AffineTransform3D t, int width, int height, double scale ) {}

	/**
	 * Fits the box (enlarged by {@code margin} times its extent per side) into an image of at most {@code width} x
	 * {@code height} pixels (height null: as tall as the box's aspect needs, 16..4096), magnifying it by at most
	 * {@code maxScale} screen pixels per global pixel. When the cap binds, the image shrinks to the box instead of
	 * leaving a huge black border, unless the height was given.
	 */
	public static Fit fit( final RealInterval box, final Plane p, final int width, final Integer height, final double margin, final double maxScale )
	{
		final double[] e = extents( box, p );
		final double ex = Math.max( 1, e[ 0 ] ) * ( 1 + 2 * margin );
		final double ey = Math.max( 1, e[ 1 ] ) * ( 1 + 2 * margin );
		final double sFit = Math.min( width / ex, ( height != null ? height : MAX_HEIGHT ) / ey );
		final double s = Math.min( sFit, maxScale );
		final int w = s < sFit ? (int) Math.min( width, Math.max( MIN_HEIGHT, Math.ceil( ex * s ) ) ) : width;
		final int h = height != null ? height : (int) Math.min( MAX_HEIGHT, Math.max( MIN_HEIGHT, Math.ceil( ey * s ) ) );
		return new Fit( viewerTransformWithScale( box, p, w, h, s ), w, h, s );
	}

	/** screen pixels per global pixel of a viewer transform */
	public static double scale( final AffineTransform3D t )
	{
		return Affine3DHelpers.extractScale( t, 0 );
	}

	/**
	 * The extent of one voxel of a source along the viewing axis (screen z) in global pixels: the longest
	 * screen-z component of the three voxel edges, {@code model} being the source's pixel-to-global transform.
	 * A maximum projection need not step finer than the smallest of these over the sources it shows.
	 */
	public static double voxelExtentAlongScreenZ( final AffineTransform3D viewer, final AffineTransform3D model )
	{
		final AffineTransform3D c = viewer.copy();
		c.concatenate( model ); // source pixel -> screen
		double max = 0;
		for ( int d = 0; d < 3; ++d )
			max = Math.max( max, Math.abs( c.get( 2, d ) ) );
		return max / scale( viewer );
	}

	/** the screen-z range the 8 corners of the box cover under t, as [min, max] */
	public static double[] screenZRange( final RealInterval box, final AffineTransform3D t )
	{
		double min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY;
		final double[] in = new double[ 3 ], out = new double[ 3 ];
		for ( int i = 0; i < 8; ++i )
		{
			for ( int d = 0; d < 3; ++d )
				in[ d ] = ( ( i >> d ) & 1 ) == 0 ? box.realMin( d ) : box.realMax( d );
			t.apply( in, out );
			min = Math.min( min, out[ 2 ] );
			max = Math.max( max, out[ 2 ] );
		}
		return new double[] { min, max };
	}

	/**
	 * The step of a maximum projection along the viewing axis, in global pixels: never finer than one voxel along
	 * that axis ({@code voxelAlongAxis}, at least 1), never finer than one screen pixel (1 / scale), and such that a
	 * depth of {@code depthGlobal} takes at most {@link #MAX_MIP_FRAMES} steps; {@code userStep} overrides.
	 */
	public static double mipStepGlobal( final double depthGlobal, final double scale, final Double userStep, final double voxelAlongAxis )
	{
		if ( userStep != null )
			return userStep;
		return Math.max( Math.max( Math.max( 1.0, voxelAlongAxis ), 1.0 / scale ), depthGlobal / MAX_MIP_FRAMES );
	}

	/** {@link #mipStepGlobal(double, double, Double, double)} for isotropic one-pixel voxels */
	public static double mipStepGlobal( final double depthGlobal, final double scale, final Double userStep )
	{
		return mipStepGlobal( depthGlobal, scale, userStep, 1.0 );
	}
}
