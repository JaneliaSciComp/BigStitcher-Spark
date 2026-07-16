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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;

public class TestBasicFlatfield
{
	// ─── Dct2D unit tests ──────────────────────────────────────────────────────

	@Test
	public void testDctRoundTrip()
	{
		final int H = 4, W = 4;
		final Random rnd = new Random( 7 );
		final float[] x = new float[ H * W ];
		for ( int i = 0; i < x.length; ++i )
			x[ i ] = rnd.nextFloat() * 10f - 5f;

		final float[] coeffs = new float[ H * W ];
		Dct2D.dct2( x, coeffs, H, W );
		final float[] back = new float[ H * W ];
		Dct2D.idct2( coeffs, back, H, W );

		for ( int i = 0; i < x.length; ++i )
			assertEquals( x[ i ], back[ i ], 1e-4f, "idct2(dct2(x)) must equal x at index " + i );
	}

	@Test
	public void testDctRoundTripWithDegenerateAxis()
	{
		assertDctRoundTrip( 1, 1 );
		assertDctRoundTrip( 1, 7 );
		assertDctRoundTrip( 7, 1 );
	}

	@Test
	public void testDctOrthonormalScaling()
	{
		// Orthonormal DCT-II: the DC coefficient of a constant image equals
		// value * sqrt(H*W) (since alpha(0)=sqrt(1/N) per axis and the constant
		// sum is value*N per axis). For a constant field c on HxW:
		//   X[0,0] = c * sqrt(H*W)
		// and Parseval: sum(coeffs^2) == sum(x^2).
		final int H = 4, W = 4;
		final float c = 3f;
		final float[] x = new float[ H * W ];
		java.util.Arrays.fill( x, c );

		final float[] coeffs = new float[ H * W ];
		Dct2D.dct2( x, coeffs, H, W );

		assertEquals( c * Math.sqrt( H * W ), coeffs[ 0 ], 1e-4f, "DC coefficient of constant field" );
		// all other coefficients ~ 0
		for ( int i = 1; i < coeffs.length; ++i )
			assertEquals( 0f, coeffs[ i ], 1e-4f, "non-DC coefficient " + i + " of constant field" );

		// Parseval / orthonormality: energy preserved
		final Random rnd = new Random( 11 );
		final float[] y = new float[ H * W ];
		for ( int i = 0; i < y.length; ++i )
			y[ i ] = rnd.nextFloat();
		final float[] cy = new float[ H * W ];
		Dct2D.dct2( y, cy, H, W );
		double eIn = 0, eOut = 0;
		for ( int i = 0; i < y.length; ++i ) { eIn += y[ i ] * y[ i ]; eOut += cy[ i ] * cy[ i ]; }
		assertEquals( eIn, eOut, 1e-3, "orthonormal DCT preserves energy (Parseval)" );
	}

	// ─── Synthetic end-to-end estimation test ──────────────────────────────────

	@Test
	public void testSyntheticRecovery()
	{
		final int H = 64, W = 64;
		final int N = 80;
		final Random rnd = new Random( 123 );

		// ground-truth flatfield: smooth radial gradient, mean ~ 1
		final float[] gtFlat = new float[ H * W ];
		double sumF = 0;
		for ( int y = 0; y < H; ++y )
			for ( int x = 0; x < W; ++x )
			{
				final double dx = ( x - W / 2.0 ) / ( W / 2.0 );
				final double dy = ( y - H / 2.0 ) / ( H / 2.0 );
				final double v = 1.0 - 0.4 * ( dx * dx + dy * dy ); // brighter center
				gtFlat[ y * W + x ] = ( float ) v;
				sumF += v;
			}
		final double meanF = sumF / ( H * W );
		for ( int i = 0; i < gtFlat.length; ++i )
			gtFlat[ i ] /= meanF; // normalize to mean 1

		// ground-truth darkfield: a horizontal gradient. This is structurally
		// INDEPENDENT of the radial flatfield shape, so it is identifiable by BaSiC.
		// (A darkfield proportional to (mean(f) - f) would be degenerate with the
		// flatfield scale and is intentionally avoided here.)
		final float[] gtDark = new float[ H * W ];
		for ( int y = 0; y < H; ++y )
			for ( int x = 0; x < W; ++x )
				gtDark[ y * W + x ] = ( float ) ( 10.0 + 25.0 * ( ( double ) x / W ) );

		final BasicFlatfieldParams params = BasicFlatfieldParams.defaults();

		// build frames: base intensity * scale * flatfield + darkfield + noise + sparse foreground
		int frameIndex = 0;
		BasicFlatfield.FramesStack frameStack = BasicFlatfield.createFramesStack( H, W, params.workingSize, params.workingSize, N );
		for ( int k = 0; k < N; ++k )
		{
			final double scale = 40.0 + 80.0 * rnd.nextDouble();
			final float[] frame = new float[ H * W ];
			for ( int p = 0; p < H * W; ++p )
			{
				double val = scale * gtFlat[ p ] + gtDark[ p ];
				val += rnd.nextGaussian(); // read noise
				// sparse bright specks to exercise the sparse-residual term
				if ( rnd.nextDouble() < 0.01 )
					val += 50.0 + 100.0 * rnd.nextDouble();
				frame[ p ] = ( float ) Math.max( val, 0.0 );
			}
			frameStack.setFrame( frameIndex++, ArrayImgs.floats( frame, W, H ) );
		}

		final BasicFlatfieldResult result = BasicFlatfield.estimate( frameStack, params, rnd );

		final float[] estFlat = toArray( result.flatfield, H, W );
		final float[] estDark = toArray( result.darkfield, H, W );

		final double corrFlat = correlation( gtFlat, estFlat );
		final double corrDark = correlation( gtDark, estDark );
		final double meanGtDark = mean( gtDark );
		final double meanEstDark = mean( estDark );

		System.out.println( "Synthetic recovery: corrFlat=" + corrFlat + " corrDark=" + corrDark );

		assertTrue( corrFlat > 0.95, "flatfield correlation should exceed 0.95, was " + corrFlat );
		assertTrue( corrDark > 0.95, "darkfield correlation should exceed 0.95, was " + corrDark );
		assertEquals( meanGtDark, meanEstDark, meanGtDark * 0.5,
				"darkfield should be returned in source intensity units, not normalized units" );

		// flatfield should be roughly mean 1
		double m = 0;
		for ( final float v : estFlat )
			m += v;
		m /= estFlat.length;
		assertEquals( 1.0, m, 0.05, "flatfield mean should be ~1" );
	}

	// ─── helpers ────────────────────────────────────────────────────────────────

	private static void assertDctRoundTrip( final int H, final int W )
	{
		final float[] x = new float[ H * W ];
		for ( int i = 0; i < x.length; ++i )
			x[ i ] = ( float ) ( Math.sin( ( i + 1 ) * 0.37 ) + 0.125 * i );

		final float[] coeffs = new float[ H * W ];
		Dct2D.dct2( x, coeffs, H, W );
		final float[] back = new float[ H * W ];
		Dct2D.idct2( coeffs, back, H, W );

		for ( int i = 0; i < x.length; ++i )
			assertEquals( x[ i ], back[ i ], 1e-4f, H + "x" + W + " DCT round-trip at index " + i );
	}

	private static float[] toArray( final RandomAccessibleInterval< FloatType > img, final int H, final int W )
	{
		final float[] out = new float[ H * W ];
		final var ra = img.randomAccess();
		for ( int y = 0; y < H; ++y )
			for ( int x = 0; x < W; ++x )
			{
				ra.setPosition( x, 0 );
				ra.setPosition( y, 1 );
				out[ y * W + x ] = ra.get().get();
			}
		return out;
	}

	/** Pearson correlation coefficient (mean-normalized). */
	private static double correlation( final float[] a, final float[] b )
	{
		final int n = a.length;
		double ma = 0, mb = 0;
		for ( int i = 0; i < n; ++i ) { ma += a[ i ]; mb += b[ i ]; }
		ma /= n; mb /= n;
		double cov = 0, va = 0, vb = 0;
		for ( int i = 0; i < n; ++i )
		{
			final double da = a[ i ] - ma;
			final double db = b[ i ] - mb;
			cov += da * db;
			va += da * da;
			vb += db * db;
		}
		return cov / Math.sqrt( va * vb );
	}

	private static double mean( final float[] a )
	{
		double s = 0;
		for ( final float v : a )
			s += v;
		return s / a.length;
	}
}
