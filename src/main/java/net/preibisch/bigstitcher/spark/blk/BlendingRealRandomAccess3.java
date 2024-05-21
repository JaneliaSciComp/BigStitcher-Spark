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
package net.preibisch.bigstitcher.spark.blk;

import ij.ImageJ;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RealPoint;
import net.imglib2.RealRandomAccess;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.real.FloatType;

public class BlendingRealRandomAccess3 extends RealPoint implements RealRandomAccess< FloatType >
{
	final Interval interval;
	final int[] min, dimMinus1;
	final float[] l, border, blending, tmp;
	final FloatType v;

	static class SmallLookup
	{
		// [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ]
		//   0                             1
		//   n = 10
		//   d = (double) i / n

		private static final int n = 30;

		// static lookup table for the blending function
		private static final float[] lookUp = createLookup( n );

		private static float[] createLookup( final int n )
		{
			final float[] lookup = new float[ n + 2 ];
			for ( int i = 0; i <= n; i++ )
			{
				final double d = ( double ) i / n;
				lookup[ i ] = ( float ) ( ( Math.cos( ( 1 - d ) * Math.PI ) + 1 ) / 2 );
			}
			lookup[ n + 1 ] = lookup[ n ];
			return lookup;
		}

		static float fn( final float d )
		{
			final int i = ( int ) ( d * n );
			final float s = ( d * n ) - i;
			return lookUp[ i ] * (1.0f - s) + lookUp[ i + 1 ] * s;
		}
	}

	/**
	 * RealRandomAccess that computes a blending function for a certain {@link Interval}
	 *
	 * @param interval - the interval it is defined on (return zero outside of it)
	 * @param border - how many pixels to skip before starting blending (on each side of each dimension)
	 * @param blending - how many pixels to compute the blending function on (on each side of each dimension)
	 */
	public BlendingRealRandomAccess3(
			final Interval interval,
			final float[] border,
			final float[] blending )
	{
		super( interval.numDimensions() );

		this.interval = interval;
		this.l = new float[ n ];
		this.tmp = new float[ n ];
		this.border = border;
		this.blending = blending;
		this.v = new FloatType();

		this.min = new int[ n ];
		this.dimMinus1 = new int[ n ];

		for ( int d = 0; d < n; ++d )
		{
			this.min[ d ] = (int)interval.min( d );
			this.dimMinus1[ d ] = (int)interval.max( d ) - min[ d ];
		}
	}

	@Override
	public FloatType get()
	{
		v.set( computeWeight( position, min, dimMinus1, border, blending, tmp, n ) );
		return v;
	}

	private static float computeWeight(
			final double[] location,
			final int[] min,
			final int[] dimMinus1,
			final float[] border,
			final float[] blending,
			final float[] tmp, // holds dist, if any of it is zero we can stop
			final int n )
	{
		for ( int d = 0; d < n; ++d )
		{
			// the position in the image relative to the boundaries and the border
			final float l = ( ( float ) location[ d ] - min[ d ] );

			// the distance to the border that is closer
			tmp[ d ] = Math.min( l - border[ d ], dimMinus1[ d ] - l - border[ d ] );

			// if this is smaller or equal to 0, the total result will be 0, independent of the number of dimensions
			if ( tmp[ d ] <= 0 )
				return 0;
		}

		// compute multiplicative distance to the respective borders [0...1]
		float minDistance = 1;

		for ( int d = 0; d < n; ++d )
		{
			final float relDist = tmp[ d ] / blending[ d ];

			// within the range where we blend from 0 to 1
			if ( relDist < 1 )
				minDistance *= SmallLookup.fn( relDist ); //( Math.cos( ( 1 - relDist ) * Math.PI ) + 1 ) / 2;
		}

		return minDistance;
	}

	@Override
	public RealRandomAccess<FloatType> copy()
	{
		final BlendingRealRandomAccess3 r = new BlendingRealRandomAccess3( interval, border, blending );
		r.setPosition( this );
		return r;
	}

	public static void main( String[] args )
	{
		new ImageJ();

		final Img< FloatType > img = ArrayImgs.floats( 1400, 1500, 100 );
		final BlendingRealRandomAccess3 blend = new BlendingRealRandomAccess3(
				new FinalInterval( new long[] {0, 600, 0 }, new long[] {1399, 1200, 99 } ),
				new float[]{ 0, 10, 0 },
				new float[]{ 150, 10, 10 } );

		final long time = System.nanoTime();

		final Cursor< FloatType > c = img.localizingCursor();

		while ( c.hasNext() )
		{
			c.fwd();
			blend.setPosition( c );
			c.get().setReal( blend.get().getRealFloat() );
		}

		System.out.println( (System.nanoTime() - time) / 1000000 );

		ImageJFunctions.show( img );
	}
}
