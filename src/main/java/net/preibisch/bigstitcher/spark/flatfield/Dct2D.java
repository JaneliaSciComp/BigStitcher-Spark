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
package net.preibisch.bigstitcher.spark.flatfield;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jtransforms.dct.DoubleDCT_1D;
import org.jtransforms.dct.DoubleDCT_2D;

/**
 * FFT-based <b>orthonormal</b> 2D DCT-II (forward) and DCT-III (inverse) on
 * {@code float[]} planes, backed by JTransforms.
 * <p>
 * A fast (O(N log N)) replacement for the previous hand-rolled O(N&sup3;)
 * separable transform. JTransforms' <em>scaled</em> DCT is exactly the
 * orthonormal DCT-II/III (verified: the DCT-II of an impulse equals
 * {@code alpha(k1)*alpha(k2)} with {@code alpha(0)=sqrt(1/N)},
 * {@code alpha(k>0)=sqrt(2/N)}, and {@code inverse(forward(x,true),true)==x}).
 * This matches MATLAB's {@code dct2} and the Julia reference
 * {@code dct2_ortho}/{@code idct2_ortho} in
 * {@code flatfield-correction/BigFlatFieldIlluminator.jl/src/basic.jl}, so it is
 * energy-preserving (Parseval) and the DC coefficient of a constant {@code c}
 * plane is {@code c*sqrt(H*W)}.
 * <p>
 * <b>Allocation-free in the hot loop:</b> the transforms write into a
 * caller-provided {@code out} array (which may alias the input), and the internal
 * {@code double[]} JTransforms scratch is cached per {@code (H,W)} plan (as a
 * {@link ThreadLocal}, so it is safe under concurrent use while still avoiding
 * per-call allocation). The {@link DoubleDCT_2D}/{@link DoubleDCT_1D} plan itself
 * is also built once per {@code (H,W)} and reused (like an FFTW plan).
 * <p>
 * Data layout: row-major with the fast axis being the width {@code W}, i.e.
 * {@code data[h*W + w]} ({@code rows=H}, {@code columns=W}). This matches how
 * {@link BasicFlatfield} stores its planes.
 */
public final class Dct2D
{
	private Dct2D() {}

	/** Cached transform plans (JTransforms plan + scratch), keyed by (H,W). */
	private static final Map< Long, Plan > PLANS = new ConcurrentHashMap<>();

	private static final class Plan
	{
		final DoubleDCT_2D dct2; // non-null unless a degenerate (1-wide or 1x1) plane
		final DoubleDCT_1D dct1; // non-null iff exactly one axis has length 1 (and not 1x1)
		final ThreadLocal< double[] > scratch;

		Plan( final int H, final int W )
		{
			final int n = H * W;
			if ( H == 1 && W == 1 )
			{
				dct2 = null;
				dct1 = null; // identity
			}
			else if ( H == 1 || W == 1 )
			{
				dct2 = null;
				dct1 = new DoubleDCT_1D( n );
			}
			else
			{
				dct2 = new DoubleDCT_2D( H, W );
				dct1 = null;
			}
			scratch = ThreadLocal.withInitial( () -> new double[ n ] );
		}

		void forward( final double[] a )
		{
			if ( dct2 != null )
				dct2.forward( a, true );   // scaled == orthonormal DCT-II
			else if ( dct1 != null )
				dct1.forward( a, true );
			// else 1x1: identity
		}

		void inverse( final double[] a )
		{
			if ( dct2 != null )
				dct2.inverse( a, true );   // scaled == orthonormal DCT-III
			else if ( dct1 != null )
				dct1.inverse( a, true );
			// else 1x1: identity
		}
	}

	private static Plan plan( final int H, final int W )
	{
		final long key = ( ( ( long ) H ) << 32 ) | ( W & 0xffffffffL );
		return PLANS.computeIfAbsent( key, k -> new Plan( H, W ) );
	}

	/**
	 * Forward orthonormal 2D DCT-II, written into {@code out}.
	 *
	 * @param x   row-major input plane, length {@code H*W}
	 * @param out destination for the coefficients, length {@code H*W}; may be the
	 *            same array as {@code x} (in-place)
	 * @param H   number of rows (slow axis)
	 * @param W   number of columns (fast axis)
	 * @return    out
	 */
	public static float[] dct2( final float[] x, final float[] out, final int H, final int W )
	{
		final Plan p = plan( H, W );
		final double[] a = p.scratch.get();

		for ( int i = 0; i < a.length; ++i )
			a[ i ] = x[ i ];

		p.forward( a );

		for ( int i = 0; i < a.length; ++i )
			out[ i ] = ( float ) a[ i ];

		return out;
	}

	/**
	 * Inverse orthonormal 2D DCT (DCT-III), the exact inverse of {@link #dct2},
	 * written into {@code out}.
	 *
	 * @param y   row-major coefficient plane, length {@code H*W}
	 * @param out destination for the reconstruction, length {@code H*W}; may be the
	 *            same array as {@code y} (in-place)
	 * @param H   number of rows
	 * @param W   number of columns
	 * @return    out
	 */
	public static float[] idct2( final float[] y, final float[] out, final int H, final int W )
	{
		final Plan p = plan( H, W );
		final double[] a = p.scratch.get();
		for ( int i = 0; i < a.length; ++i )
			a[ i ] = y[ i ];
		p.inverse( a );
		for ( int i = 0; i < a.length; ++i )
			out[ i ] = ( float ) a[ i ];
		return out;
	}
}
