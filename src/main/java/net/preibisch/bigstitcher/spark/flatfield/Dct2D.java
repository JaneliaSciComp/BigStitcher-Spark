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

/**
 * Hand-rolled separable <b>orthonormal</b> 2D DCT-II (forward) and DCT-III
 * (inverse) on {@code float[]} planes.
 * <p>
 * This is a faithful port of the Julia reference {@code dct2_ortho} /
 * {@code idct2_ortho} in
 * {@code flatfield-correction/BigFlatFieldIlluminator.jl/src/basic.jl}, which
 * itself matches MATLAB's {@code dct2}. The reference uses FFTW's unnormalized
 * {@code REDFT10} (forward) / {@code REDFT01} (inverse) and applies orthonormal
 * weights explicitly:
 *
 * <pre>
 *   Y_fftw[k1,k2] = 4 * sum_h sum_w x[h,w] cos(pi*(2h+1)*k1/(2m)) cos(pi*(2w+1)*k2/(2n))
 *   W[k1,k2]      = w(k1,m) * w(k2,n),   w(0,N)=1/sqrt(N), w(k&gt;0,N)=sqrt(2/N)
 *   Y_orth        = (W/4) .* Y_fftw
 * </pre>
 *
 * The separable implementation below computes the orthonormal DCT-II directly
 * (rows then columns), which yields exactly {@code Y_orth}; the inverse applies
 * the transpose (DCT-III), so {@code idct2(dct2(x)) == x} up to floating point.
 * <p>
 * Data layout: row-major with the fast axis being the width {@code W}, i.e.
 * {@code data[h*W + w]}. This matches how {@link BasicFlatfield} stores its
 * planes.
 */
public final class Dct2D
{
	private Dct2D() {}

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
		// rows: transform each row (length W) along the fast axis
		final float[] tmp = new float[ H * W ];
		final double[] cosW = cosTable( W );
		for ( int h = 0; h < H; ++h )
			dct1( x, h * W, 1, W, tmp, h * W, 1, cosW );

		// columns: transform each column (length H) along the slow axis (stride W)
		final float[] out = new float[ H * W ];
		final double[] cosH = cosTable( H );
		for ( int w = 0; w < W; ++w )
			dct1( tmp, w, W, H, out, w, W, cosH );

		return out;
	}

	/**
	 * Inverse orthonormal 2D DCT (DCT-III).
	 *
	 * @param y row-major coefficient plane, length {@code H*W}
	 * @param H number of rows
	 * @param W number of columns
	 * @return reconstructed plane, new array of length {@code H*W}
	 */
	public static float[] idct2( final float[] y, final int H, final int W )
	{
		// inverse along columns first (undo the column DCT-II), then rows
		final float[] tmp = new float[ H * W ];
		final double[] cosH = cosTable( H );
		for ( int w = 0; w < W; ++w )
			idct1( y, w, W, H, tmp, w, W, cosH );

		final float[] out = new float[ H * W ];
		final double[] cosW = cosTable( W );
		for ( int h = 0; h < H; ++h )
			idct1( tmp, h * W, 1, W, out, h * W, 1, cosW );

		return out;
	}

	/**
	 * Orthonormal 1D DCT-II of a strided sub-sequence.
	 *
	 * X[k] = alpha(k) * sum_{n=0}^{N-1} x[n] cos(pi*(2n+1)*k/(2N))
	 * with alpha(0) = sqrt(1/N), alpha(k&gt;0) = sqrt(2/N).
	 */
	private static void dct1(
			final float[] in, final int inOff, final int inStride, final int N,
			final float[] out, final int outOff, final int outStride,
			final double[] cos )
	{
		final double norm0 = Math.sqrt( 1.0 / N );
		final double norm = Math.sqrt( 2.0 / N );
		final int period = 4 * N;
		for ( int k = 0; k < N; ++k )
		{
			double sum = 0.0;
			for ( int n = 0; n < N; ++n )
				sum += in[ inOff + n * inStride ] * cos[ ( ( ( 2 * n + 1 ) * k ) % period ) ];
			final double a = ( k == 0 ) ? norm0 : norm;
			out[ outOff + k * outStride ] = ( float ) ( a * sum );
		}
	}

	/**
	 * Orthonormal 1D DCT-III (inverse of {@link #dct1}) of a strided sub-sequence.
	 *
	 * x[n] = sum_{k=0}^{N-1} alpha(k) X[k] cos(pi*(2n+1)*k/(2N))
	 */
	private static void idct1(
			final float[] in, final int inOff, final int inStride, final int N,
			final float[] out, final int outOff, final int outStride,
			final double[] cos )
	{
		final double norm0 = Math.sqrt( 1.0 / N );
		final double norm = Math.sqrt( 2.0 / N );
		final int period = 4 * N;
		for ( int n = 0; n < N; ++n )
		{
			double sum = 0.0;
			for ( int k = 0; k < N; ++k )
			{
				final double a = ( k == 0 ) ? norm0 : norm;
				sum += a * in[ inOff + k * inStride ] * cos[ ( ( ( 2 * n + 1 ) * k ) % period ) ];
			}
			out[ outOff + n * outStride ] = ( float ) sum;
		}
	}

	/**
	 * Precompute cos(pi * i / (2N)) for i in [0, 4N). The DCT argument is
	 * pi*(2n+1)*k/(2N); as a function of the integer index {@code i=(2n+1)*k}
	 * it has period 4N (cos increases its phase by 2*pi when i grows by 4N),
	 * so we index the table with {@code i % (4N)}.
	 */
	private static double[] cosTable( final int N )
	{
		final int period = 4 * N;
		final double[] t = new double[ period ];
		for ( int i = 0; i < period; ++i )
			t[ i ] = Math.cos( Math.PI * i / ( 2.0 * N ) );
		return t;
	}
}
