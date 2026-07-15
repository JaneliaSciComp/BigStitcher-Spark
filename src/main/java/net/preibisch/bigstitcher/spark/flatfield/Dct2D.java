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
 * <b>Lazy plan reuse (like FFTW):</b> the {@link DoubleDCT_2D} plan is built once
 * per {@code (H,W)} and cached, then reused across the thousands of transforms in
 * one estimation run — this is where almost all of the speed-up comes from.
 * <p>
 * Data layout: row-major with the fast axis being the width {@code W}, i.e.
 * {@code data[h*W + w]} ({@code rows=H}, {@code columns=W}). This matches how
 * {@link BasicFlatfield} stores its planes.
 */
public final class Dct2D
{
	private Dct2D() {}

	/** Cached transform plans, keyed by (H,W). Built lazily, reused across calls. */
	private static final Map< Long, DoubleDCT_2D > PLANS = new ConcurrentHashMap<>();

	/** Cached 1D plans for degenerate 2D planes where one axis has length 1. */
	private static final Map< Integer, DoubleDCT_1D > PLANS_1D = new ConcurrentHashMap<>();

	private static DoubleDCT_2D plan( final int H, final int W )
	{
		final long key = ( ( ( long ) H ) << 32 ) | ( W & 0xffffffffL );
		return PLANS.computeIfAbsent( key, k -> new DoubleDCT_2D( H, W ) );
	}

	private static DoubleDCT_1D plan1D( final int N )
	{
		return PLANS_1D.computeIfAbsent( N, DoubleDCT_1D::new );
	}

	/**
	 * Forward orthonormal 2D DCT-II.
	 *
	 * @param x row-major plane, length {@code H*W}
	 * @param H number of rows (slow axis)
	 * @param W number of columns (fast axis)
	 * @return orthonormal DCT-II coefficients, new array of length {@code H*W}
	 */
	public static float[] dct2( final float[] x, final int H, final int W )
	{
		final double[] a = new double[ H * W ];
		for ( int i = 0; i < a.length; ++i )
			a[ i ] = x[ i ];

		if ( H == 1 && W == 1 )
		{
			// identity
		}
		else if ( H == 1 || W == 1 )
		{
			plan1D( H * W ).forward( a, true ); // scaled == orthonormal DCT-II
		}
		else
		{
			plan( H, W ).forward( a, true ); // scaled == orthonormal DCT-II
		}

		final float[] out = new float[ H * W ];
		for ( int i = 0; i < out.length; ++i )
			out[ i ] = ( float ) a[ i ];
		return out;
	}

	/**
	 * Inverse orthonormal 2D DCT (DCT-III), the exact inverse of {@link #dct2}.
	 *
	 * @param y row-major coefficient plane, length {@code H*W}
	 * @param H number of rows
	 * @param W number of columns
	 * @return reconstructed plane, new array of length {@code H*W}
	 */
	public static float[] idct2( final float[] y, final int H, final int W )
	{
		final double[] a = new double[ H * W ];
		for ( int i = 0; i < a.length; ++i )
			a[ i ] = y[ i ];

		if ( H == 1 && W == 1 )
		{
			// identity
		}
		else if ( H == 1 || W == 1 )
		{
			plan1D( H * W ).inverse( a, true ); // scaled == orthonormal DCT-III
		}
		else
		{
			plan( H, W ).inverse( a, true ); // scaled == orthonormal DCT-III
		}

		final float[] out = new float[ H * W ];
		for ( int i = 0; i < out.length; ++i )
			out[ i ] = ( float ) a[ i ];
		return out;
	}
}
