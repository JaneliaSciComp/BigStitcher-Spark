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

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.AffineTransform2D;
import net.imglib2.realtransform.RealViews;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;

/**
 * BaSiC flatfield / darkfield <b>estimation</b> (Peng et al., <i>Nature
 * Communications</i> 2017). Pure Java, no Spark / CLI.
 * <p>
 * Faithful port of the Julia reference
 * {@code flatfield-correction/BigFlatFieldIlluminator.jl/src/basic.jl}
 * ({@code basic_estimate}). Estimates a shared low-rank illumination model
 * (flatfield x per-frame scale + darkfield) plus a sparse residual, via an
 * inexact Augmented Lagrangian Multiplier (IALM) inner loop with an outer
 * reweighting loop.
 * <p>
 * Data layout convention throughout: a 2D plane of size HxW is a row-major
 * {@code float[H*W]} with the fast axis being W ({@code plane[h*W + w]}). The
 * stack {@code D} is {@code float[N][H*W]} (frame-major). Reductions accumulate
 * in {@code double} for stability; storage is {@code float} to match the
 * reference and halve memory.
 */
public class BasicFlatfield
{
	private static final float EPS = 1e-7f; // ~ eps(Float32) scale used for guards

	/**
	 * Estimate flatfield and darkfield from a stack of 2D frames.
	 *
	 * @param images list of 2D frames (all the same X/Y size); values read via
	 *               {@link RealType#getRealFloat()}
	 * @param params estimation parameters (see {@link BasicFlatfieldParams})
	 * @return the estimated fields (at the original frame size), per-frame scales
	 *         and darkfield baseline
	 */
	public static BasicFlatfieldResult estimate(
			final List< RandomAccessibleInterval< ? extends RealType< ? > > > images,
			final BasicFlatfieldParams params )
	{
		final int N = images.size();
		if ( N == 0 )
			throw new IllegalArgumentException( "BaSiC: empty image list" );

		System.out.printf("Estimate darkfield/flatfield from %d images using %s\n", images.size(), params );
		final RandomAccessibleInterval< ? extends RealType< ? > > first = images.get( 0 );
		if ( first.numDimensions() != 2 )
			throw new IllegalArgumentException( "BaSiC: frames must be 2D, got " + first.numDimensions() + "D" );

		final int H_orig = ( int ) first.dimension( 1 ); // rows (Y)
		final int W_orig = ( int ) first.dimension( 0 ); // cols (X)

		final int ws = params.workingSize;
		final int H = ( ws > 0 ) ? ws : H_orig;
		final int W = ( ws > 0 ) ? ws : W_orig;
		final int HW = H * W;

		System.out.printf( "Image size used for darkfield/flatfield (%d, %d) -> (%d, %d)\n", H_orig, W_orig, H, W );

		// ── Load frames into row-major float planes, optionally resize ────────────
		final float[][] D = new float[ N ][];
		for ( int k = 0; k < N; ++k )
		{
			final RandomAccessibleInterval< ? extends RealType< ? > > frame = images.get( k );
			if ( frame.dimension( 0 ) != W_orig || frame.dimension( 1 ) != H_orig )
				throw new IllegalArgumentException( "BaSiC: all frames must share the same size" );

			final float[] plane = toPlane( frame, W_orig, H_orig );
			D[ k ] = ( ws > 0 && ( H_orig != H || W_orig != W ) )
					? resize( plane, H_orig, W_orig, H, W )
					: plane;
		}

		// ── Normalize so working values hover around 1 ────────────────────────────
		double sum = 0.0;
		for ( int k = 0; k < N; ++k )
			for ( int p = 0; p < HW; ++p )
				sum += D[ k ][ p ];
		final double globalMean = sum / ( ( double ) HW * N );
		if ( globalMean < EPS )
			throw new IllegalArgumentException( "BaSiC: image stack is all-zero" );

		System.out.printf( "Normalize images using global mean: %f\n", globalMean );

		for ( int k = 0; k < N; ++k )
			for ( int p = 0; p < HW; ++p )
				D[ k ][ p ] /= ( float ) globalMean;

		// ── Per-pixel sort along the frame axis (matches MATLAB sort(D,3)) ────────
		// After sorting, D[0][p] is the per-pixel minimum across frames.
		final float[] col = new float[ N ];
		for ( int p = 0; p < HW; ++p )
		{
			for ( int k = 0; k < N; ++k )
				col[ k ] = D[ k ][ p ];
			Arrays.sort( col );
			for ( int k = 0; k < N; ++k )
				D[ k ][ p ] = col[ k ];
		}

		// mean_img[p]: spatial mean over all frames
		final float[] meanImg = new float[ HW ];
		for ( int p = 0; p < HW; ++p )
		{
			double s = 0.0;
			for ( int k = 0; k < N; ++k )
				s += D[ k ][ p ];
			meanImg[ p ] = ( float ) ( s / N );
		}

		// ── Auto-lambda ───────────────────────────────────────────────────────────
		final float[] lambdas = params.deriveLambdas( meanImg, H, W );
		final float lambda = lambdas[ 0 ];
		final float lambdaDarkfield = lambdas[ 1 ];
		System.out.println( "BaSiC auto-params: lambda=" + lambda + " lambdaDarkfield=" + lambdaDarkfield );

		// ── Spectral norm for penalty initialisation ──────────────────────────────
		final float normTwo = ( float ) spectralNorm( D, HW, N );
		final float normD = ( float ) frobeniusNorm( D, HW, N );
		final float muInit = 12.5f / normTwo;
		final float muBar = muInit * 1e7f;
		final float rho = 1.5f;
		final float ent1 = 1f;
		final float ent2 = 10f;

		System.out.printf( "Spectral norm: %f, Frobenius norm: %f\n", normTwo, normD );

		// Upper bound for darkfield: mean of per-pixel minima over frames (D[0] after sort)
		float darkfieldLimit;
		{
			double s = 0.0;
			for ( int p = 0; p < HW; ++p )
				s += D[ 0 ][ p ];
			darkfieldLimit = ( float ) ( s / HW );
		}

		final boolean estimateDarkfield = params.estimateDarkfield;

		// ── Optimisation variables ─────────────────────────────────────────────────
		float[] W_hat = Dct2D.dct2( meanImg, H, W );      // flatfield DCT coefficients
		final float[][] E = new float[ N ][ HW ];         // sparse residual
		final float[] A1_coeff = new float[ N ];          // per-frame illumination scale
		Arrays.fill( A1_coeff, 1f );
		final float[] A_offset = new float[ HW ];         // spatially varying darkfield
		final float[][] W_coeff = new float[ N ][ HW ];   // reweighting coefficients
		for ( int k = 0; k < N; ++k )
			Arrays.fill( W_coeff[ k ], 1f );

		// ── Pre-allocated buffers ──────────────────────────────────────────────────
		float[] f = new float[ HW ];                      // spatial flatfield = max(idct2(W_hat),0)
		final float[][] A1_hat = new float[ N ][ HW ];    // low-rank model prediction
		final float[] R_W = new float[ HW ];              // mean-over-frames W-update residual
		final float[][] R1 = new float[ N ][ HW ];        // D - E: the "clean" low-rank part
		final float[][] Z = new float[ N ][ HW ];         // primal feasibility residual
		final float[][] Y1 = new float[ N ][ HW ];        // Lagrange multiplier
		final float[] ffCurr = new float[ HW ];           // current flatfield for reweight check
		final float[] B1_coeff = new float[ estimateDarkfield ? N : 0 ];
		final float[] B_offset = new float[ estimateDarkfield ? HW : 0 ];
		final float[] A1_offset = new float[ estimateDarkfield ? HW : 0 ];

		final float[] flatfieldPrev = new float[ HW ];
		Arrays.fill( flatfieldPrev, 1f );
		final float[] darkfieldPrev = new float[ HW ]; // zeros

		float B1_offsetFinal = 0f;

		for ( int rw = 1; rw <= params.maxReweightIterations; ++rw )
		{
			// reset Lagrange multiplier and sparse residual together
			for ( int k = 0; k < N; ++k )
			{
				Arrays.fill( Y1[ k ], 0f );
				Arrays.fill( E[ k ], 0f );
			}

			final float B1_offset = almLoop(
					W_hat, E, A1_coeff, A_offset, Y1,
					D, W_coeff, f, A1_hat, R_W, R1, Z,
					B1_coeff, B_offset, A1_offset,
					H, W, N, muInit, muBar, rho, ent1, ent2,
					lambda, lambdaDarkfield, darkfieldLimit,
					estimateDarkfield, params.maxIterations, params.optimizationTol, normD );

			B1_offsetFinal = B1_offset;

			// Add scalar darkfield component accumulated during ALM iterations
			if ( estimateDarkfield )
			{
				// f currently holds max(idct2(W_hat),0) from the last ALM iteration
				idct2Nonneg( W_hat, f, H, W );
				for ( int p = 0; p < HW; ++p )
					A_offset[ p ] += B1_offset * f[ p ];
			}

			// ── Reweighting convergence check (normalized L1) ─────────────────────
			idct2Nonneg( W_hat, f, H, W );
			final float meanA1 = mean( A1_coeff );
			for ( int p = 0; p < HW; ++p )
				ffCurr[ p ] = f[ p ] * meanA1;
			final float ffCurrMean = Math.max( mean( ffCurr ), EPS );
			for ( int p = 0; p < HW; ++p )
				ffCurr[ p ] /= ffCurrMean;

			double madFfNum = 0.0, madFfDen = 0.0;
			for ( int p = 0; p < HW; ++p )
			{
				madFfNum += Math.abs( ffCurr[ p ] - flatfieldPrev[ p ] );
				madFfDen += Math.abs( flatfieldPrev[ p ] );
			}
			final float madFf = ( float ) ( madFfNum / Math.max( madFfDen, EPS ) );

			float madDf = 0f;
			if ( estimateDarkfield )
			{
				double td = 0.0, den = 0.0;
				for ( int p = 0; p < HW; ++p )
				{
					td += Math.abs( A_offset[ p ] - darkfieldPrev[ p ] );
					den += Math.abs( darkfieldPrev[ p ] );
				}
				madDf = ( td < 1e-7 ) ? 0f : ( float ) ( td / Math.max( den, 1e-6 ) );
			}

			System.out.println( "BaSiC reweighting rw=" + rw + " madFf=" + madFf + " madDf=" + madDf );

			if ( Math.max( madFf, madDf ) <= params.reweightTol )
			{
				System.out.println( "BaSiC reweighting converged at rw=" + rw );
				break;
			}

			System.arraycopy( ffCurr, 0, flatfieldPrev, 0, HW );
			if ( estimateDarkfield )
				System.arraycopy( A_offset, 0, darkfieldPrev, 0, HW );

			updateWeights( W_coeff, E, f, A1_coeff, A_offset, params.epsilon, H, W, N );
		}

		// ── Final flatfield: non-negative, normalised to mean = 1 ─────────────────
		final float[] flatfield = new float[ HW ];
		idct2Nonneg( W_hat, flatfield, H, W );
		float flatMean = mean( flatfield );
		if ( flatMean < EPS )
			flatMean = 1f;
		for ( int p = 0; p < HW; ++p )
			flatfield[ p ] /= flatMean;

		// A_offset was estimated on D = images / globalMean. Convert the additive
		// darkfield back to source intensity units because applyCorrection subtracts
		// it from raw pixels.
		final float[] darkfield = A_offset.clone();
		for ( int p = 0; p < HW; ++p )
			darkfield[ p ] *= ( float ) globalMean;

		// ── Resize outputs back to original dimensions if working_size was used ────
		final float[] flatOut = ( H_orig != H || W_orig != W ) ? resize( flatfield, H, W, H_orig, W_orig ) : flatfield;
		final float[] darkOut = ( H_orig != H || W_orig != W ) ? resize( darkfield, H, W, H_orig, W_orig ) : darkfield;

		// wrap as ArrayImg<FloatType> in (X=W, Y=H) imglib2 order
		final ArrayImg< FloatType, FloatArray > flatImg = ArrayImgs.floats( flatOut, W_orig, H_orig );
		final ArrayImg< FloatType, FloatArray > darkImg = ArrayImgs.floats( darkOut, W_orig, H_orig );

		final double[] frameScales = new double[ N ];
		for ( int k = 0; k < N; ++k )
			frameScales[ k ] = A1_coeff[ k ];

		System.out.println("Finished BaSiC flatfield/darkfield estimation");

		return new BasicFlatfieldResult( flatImg, darkImg, frameScales, B1_offsetFinal );
	}

	// ─── Inner ALM loop ──────────────────────────────────────────────────────────

	private static float almLoop(
			final float[] W_hat, final float[][] E, final float[] A1_coeff,
			final float[] A_offset, final float[][] Y1,
			final float[][] D, final float[][] W_coeff, final float[] f,
			final float[][] A1_hat, final float[] R_W, final float[][] R1, final float[][] Z,
			final float[] B1_coeff, final float[] B_offset, final float[] A1_offset,
			final int H, final int W, final int N,
			final float muInit, final float muBar, final float rho, final float ent1, final float ent2,
			final float lambda, final float lambdaDarkfield, final float darkfieldLimit,
			final boolean estimateDarkfield, final int maxIterations, final float optimizationTol,
			final float normD )
	{
		final int HW = H * W;
		float mu = muInit;
		float B1_offset = 0f;

		for ( int iter = 1; iter <= maxIterations; ++iter )
		{
			// f = max(idct2(W_hat), 0)
			idct2Nonneg( W_hat, f, H, W );
			// A1_hat = f * A1_coeff[k] + A_offset
			buildA1Hat( A1_hat, f, A1_coeff, A_offset, HW, N );

			// ── Update W_hat ──────────────────────────────────────────────────────
			// E := D - A1_hat - E + Y1/mu (E used as temporary residual store)
			for ( int k = 0; k < N; ++k )
			{
				final float[] Dk = D[ k ], Ak = A1_hat[ k ], Ek = E[ k ], Yk = Y1[ k ];
				for ( int p = 0; p < HW; ++p )
					Ek[ p ] = Dk[ p ] - Ak[ p ] - Ek[ p ] + Yk[ p ] / mu;
			}
			// R_W := mean over frames of E
			meanOverFrames( E, R_W, HW, N );
			// W_hat += dct2(R_W / ent1); shrink by lambda/(ent1*mu)
			final float[] rwScaled = new float[ HW ];
			for ( int p = 0; p < HW; ++p )
				rwScaled[ p ] = R_W[ p ] / ent1;
			final float[] dctRW = Dct2D.dct2( rwScaled, H, W );
			for ( int p = 0; p < HW; ++p )
				W_hat[ p ] += dctRW[ p ];
			shrink( W_hat, lambda / ( ent1 * mu ) );

			// recompute f and A1_hat
			idct2Nonneg( W_hat, f, H, W );
			buildA1Hat( A1_hat, f, A1_coeff, A_offset, HW, N );

			// ── Update E (sparse residual) ────────────────────────────────────────
			// E := shrink(D - A1_hat + Y1/mu, W_coeff/(ent1*mu))
			final float invEnt1Mu = 1f / ( ent1 * mu );
			for ( int k = 0; k < N; ++k )
			{
				final float[] Dk = D[ k ], Ak = A1_hat[ k ], Ek = E[ k ], Yk = Y1[ k ], Wk = W_coeff[ k ];
				for ( int p = 0; p < HW; ++p )
				{
					final float x = Dk[ p ] - Ak[ p ] + Yk[ p ] / mu;
					final float t = Wk[ p ] * invEnt1Mu;
					Ek[ p ] = shrinkScalar( x, t );
				}
			}

			// ── Update A1_coeff (per-frame illumination scale) ────────────────────
			// R1 := D - E ; global_R1 = mean(R1)
			double globalR1 = 0.0;
			for ( int k = 0; k < N; ++k )
			{
				final float[] Dk = D[ k ], Ek = E[ k ], Rk = R1[ k ];
				double s = 0.0;
				for ( int p = 0; p < HW; ++p )
				{
					final float v = Dk[ p ] - Ek[ p ];
					Rk[ p ] = v;
					s += v;
				}
				globalR1 += s;
			}
			globalR1 /= ( ( double ) HW * N );
			for ( int k = 0; k < N; ++k )
			{
				double s = 0.0;
				final float[] Rk = R1[ k ];
				for ( int p = 0; p < HW; ++p )
					s += Rk[ p ];
				final double meanRk = s / HW;
				A1_coeff[ k ] = ( float ) Math.max( meanRk / globalR1, 0.0 );
			}
			buildA1Hat( A1_hat, f, A1_coeff, A_offset, HW, N );

			// ── Update darkfield ──────────────────────────────────────────────────
			if ( estimateDarkfield )
			{
				B1_offset = updateDarkfield(
						A_offset, B_offset, A1_offset, B1_coeff,
						f, R1, A1_coeff, darkfieldLimit, lambdaDarkfield, ent2, mu, H, W, N );
				buildA1Hat( A1_hat, f, A1_coeff, A_offset, HW, N );
			}

			// ── Lagrange multiplier and penalty update ────────────────────────────
			// Z := D - A1_hat - E ; Y1 += mu*Z ; mu = min(mu*rho, muBar)
			double normZsq = 0.0;
			for ( int k = 0; k < N; ++k )
			{
				final float[] Dk = D[ k ], Ak = A1_hat[ k ], Ek = E[ k ], Zk = Z[ k ], Yk = Y1[ k ];
				for ( int p = 0; p < HW; ++p )
				{
					final float z = Dk[ p ] - Ak[ p ] - Ek[ p ];
					Zk[ p ] = z;
					Yk[ p ] += mu * z;
					normZsq += ( double ) z * z;
				}
			}
			mu = Math.min( mu * rho, muBar );

			if ( Math.sqrt( normZsq ) / normD < optimizationTol )
			{
				System.out.println( "BaSiC ALM converged at iter=" + iter );
				break;
			}
		}

		return B1_offset;
	}

	// ─── Darkfield update ────────────────────────────────────────────────────────

	private static float updateDarkfield(
			final float[] A_offset, final float[] B_offset, final float[] A1_offset, final float[] B1_coeff,
			final float[] f, final float[][] R1, final float[] A1_coeff,
			final float darkfieldLimit, final float lambdaDarkfield, final float ent2, final float mu,
			final int H, final int W, final int N )
	{
		final int HW = H * W;

		// valid[k]: frames with below-average illumination (A1_coeff < 1)
		int nValid = 0;
		final boolean[] valid = new boolean[ N ];
		for ( int k = 0; k < N; ++k )
		{
			valid[ k ] = A1_coeff[ k ] < 1f;
			if ( valid[ k ] )
				++nValid;
		}
		if ( nValid == 0 )
			return 0f;

		final float fMean = mean( f );
		// f_high[p]: pixels >= mean-1e-6 ; f_low[p]: pixels <= mean+1e-6
		int nHigh = 0, nLow = 0;
		final boolean[] fHigh = new boolean[ HW ];
		final boolean[] fLow = new boolean[ HW ];
		for ( int p = 0; p < HW; ++p )
		{
			if ( f[ p ] > fMean - 1e-6f ) { fHigh[ p ] = true; ++nHigh; }
			if ( f[ p ] < fMean + 1e-6f ) { fLow[ p ] = true; ++nLow; }
		}

		// safe_gR1: global mean of R1 (over valid... no: over all, matches Julia mean(R1))
		double gr1 = 0.0;
		for ( int k = 0; k < N; ++k )
		{
			final float[] Rk = R1[ k ];
			for ( int p = 0; p < HW; ++p )
				gr1 += Rk[ p ];
		}
		gr1 /= ( ( double ) HW * N );
		final float safeGR1 = ( Math.abs( gr1 ) < EPS ) ? 1f : ( float ) gr1;

		// B1_coeff[k]: contrast of R1 between high/low regions, normalized by global mean
		Arrays.fill( B1_coeff, 0f );
		if ( nHigh != 0 && nLow != 0 )
		{
			for ( int k = 0; k < N; ++k )
			{
				if ( !valid[ k ] )
					continue;
				final float[] Rk = R1[ k ];
				double sHigh = 0.0, sLow = 0.0;
				for ( int p = 0; p < HW; ++p )
				{
					if ( fHigh[ p ] ) sHigh += Rk[ p ];
					if ( fLow[ p ] ) sLow += Rk[ p ];
				}
				final double meanHigh = sHigh / nHigh;
				final double meanLow = sLow / nLow;
				B1_coeff[ k ] = ( float ) ( ( meanHigh - meanLow ) / safeGR1 );
			}
		}

		// Least-squares scalar darkfield offset (normal equations over valid frames)
		double t1 = 0.0, t2 = 0.0, t3 = 0.0, t4 = 0.0;
		for ( int k = 0; k < N; ++k )
		{
			if ( !valid[ k ] )
				continue;
			final double a = A1_coeff[ k ];
			final double b = B1_coeff[ k ];
			t1 += a * a;
			t2 += a;
			t3 += b;
			t4 += a * b;
		}
		final double kn = nValid;
		final double t5 = t2 * t3 - kn * t4;
		float B1_offset = ( Math.abs( t5 ) < EPS ) ? 0f : ( float ) ( ( t1 * t3 - t2 * t4 ) / t5 );
		final float clampMax = darkfieldLimit / Math.max( fMean, EPS );
		B1_offset = Math.max( 0f, Math.min( B1_offset, clampMax ) );

		// B_offset = B1_offset*(fMean - f)
		for ( int p = 0; p < HW; ++p )
			B_offset[ p ] = B1_offset * fMean - B1_offset * f[ p ];

		// A1_offset = mean over valid frames of R1 - mean(A1_valid)*f ; then mean-subtract
		double meanA1Valid = t2 / kn; // mean of A1_coeff over valid
		for ( int p = 0; p < HW; ++p )
		{
			double s = 0.0;
			for ( int k = 0; k < N; ++k )
				if ( valid[ k ] )
					s += R1[ k ][ p ];
			A1_offset[ p ] = ( float ) ( s / nValid - meanA1Valid * f[ p ] );
		}
		final float a1OffMean = mean( A1_offset );
		for ( int p = 0; p < HW; ++p )
			A1_offset[ p ] -= a1OffMean;

		// A_offset = A1_offset - B_offset
		for ( int p = 0; p < HW; ++p )
			A_offset[ p ] = A1_offset[ p ] - B_offset[ p ];

		// Smooth and sparsify via DCT + image-domain shrink
		final float thr = lambdaDarkfield / ( ent2 * mu );
		final float[] W_off = Dct2D.dct2( A_offset, H, W );
		shrink( W_off, thr );
		final float[] recon = Dct2D.idct2( W_off, H, W );
		System.arraycopy( recon, 0, A_offset, 0, HW );
		shrink( A_offset, thr );
		for ( int p = 0; p < HW; ++p )
			A_offset[ p ] += B_offset[ p ];

		return B1_offset;
	}

	// ─── Reweighting ─────────────────────────────────────────────────────────────

	private static void updateWeights(
			final float[][] W_coeff, final float[][] E,
			final float[] f, final float[] A1_coeff, final float[] A_offset,
			final float epsilon, final int H, final int W, final int N )
	{
		final int HW = H * W;
		final float meanF = mean( f );
		final float meanAoff = mean( A_offset );

		double sumW = 0.0;
		for ( int k = 0; k < N; ++k )
		{
			final float frameMean = meanF * A1_coeff[ k ] + meanAoff;
			final float denom = frameMean + 1e-6f;
			final float[] Ek = E[ k ], Wk = W_coeff[ k ];
			for ( int p = 0; p < HW; ++p )
			{
				final float w = 1f / ( Math.abs( Ek[ p ] / denom ) + epsilon );
				Wk[ p ] = w;
				sumW += w;
			}
		}
		final float scale = ( float ) ( ( double ) HW * N / sumW );
		for ( int k = 0; k < N; ++k )
		{
			final float[] Wk = W_coeff[ k ];
			for ( int p = 0; p < HW; ++p )
				Wk[ p ] *= scale;
		}
	}

	// ─── Numeric helpers ─────────────────────────────────────────────────────────

	/** A1_hat[k] = f * A1_coeff[k] + A_offset */
	private static void buildA1Hat( final float[][] A1_hat, final float[] f, final float[] A1_coeff, final float[] A_offset, final int HW, final int N )
	{
		for ( int k = 0; k < N; ++k )
		{
			final float a = A1_coeff[ k ];
			final float[] Ak = A1_hat[ k ];
			for ( int p = 0; p < HW; ++p )
				Ak[ p ] = f[ p ] * a + A_offset[ p ];
		}
	}

	/** out := mean over frames of stack */
	private static void meanOverFrames( final float[][] stack, final float[] out, final int HW, final int N )
	{
		Arrays.fill( out, 0f );
		for ( int k = 0; k < N; ++k )
		{
			final float[] sk = stack[ k ];
			for ( int p = 0; p < HW; ++p )
				out[ p ] += sk[ p ];
		}
		final float inv = 1f / N;
		for ( int p = 0; p < HW; ++p )
			out[ p ] *= inv;
	}

	/** out := max(idct2(coeffs), 0) */
	private static void idct2Nonneg( final float[] coeffs, final float[] out, final int H, final int W )
	{
		final float[] r = Dct2D.idct2( coeffs, H, W );
		for ( int p = 0; p < out.length; ++p )
			out[ p ] = Math.max( r[ p ], 0f );
	}

	/** In-place soft-threshold: x = sign(x)*max(|x|-t, 0). */
	private static void shrink( final float[] x, final float t )
	{
		for ( int i = 0; i < x.length; ++i )
			x[ i ] = shrinkScalar( x[ i ], t );
	}

	private static float shrinkScalar( final float x, final float t )
	{
		final float a = Math.abs( x ) - t;
		if ( a <= 0f )
			return 0f;
		return ( x >= 0f ) ? a : -a;
	}

	private static float mean( final float[] x )
	{
		if ( x.length == 0 )
			return 0f;
		double s = 0.0;
		for ( final float v : x )
			s += v;
		return ( float ) ( s / x.length );
	}

	private static double frobeniusNorm( final float[][] D, final int HW, final int N )
	{
		double s = 0.0;
		for ( int k = 0; k < N; ++k )
		{
			final float[] Dk = D[ k ];
			for ( int p = 0; p < HW; ++p )
				s += ( double ) Dk[ p ] * Dk[ p ];
		}
		return Math.sqrt( s );
	}

	/**
	 * Largest singular value of the flattened (HW x N) stack D, via power
	 * iteration on the NxN Gram matrix G = D^T D. sigma_max = sqrt(lambda_max(G)).
	 */
	private static double spectralNorm( final float[][] D, final int HW, final int N )
	{
		// G[i][j] = sum_p D[i][p] * D[j][p]  (frame-major: D[k] is a frame plane)
		final double[][] G = new double[ N ][ N ];
		for ( int i = 0; i < N; ++i )
		{
			final float[] Di = D[ i ];
			for ( int j = i; j < N; ++j )
			{
				final float[] Dj = D[ j ];
				double s = 0.0;
				for ( int p = 0; p < HW; ++p )
					s += ( double ) Di[ p ] * Dj[ p ];
				G[ i ][ j ] = s;
				G[ j ][ i ] = s;
			}
		}

		// power iteration for the largest eigenvalue of G
		double[] v = new double[ N ];
		final Random rnd = new Random( 42L );
		for ( int i = 0; i < N; ++i )
			v[ i ] = rnd.nextDouble() + 1e-3;
		normalize( v );

		double eig = 0.0;
		final double[] w = new double[ N ];
		for ( int it = 0; it < 1000; ++it )
		{
			for ( int i = 0; i < N; ++i )
			{
				double s = 0.0;
				final double[] Gi = G[ i ];
				for ( int j = 0; j < N; ++j )
					s += Gi[ j ] * v[ j ];
				w[ i ] = s;
			}
			final double newEig = norm( w );
			if ( newEig < 1e-30 )
				break;
			for ( int i = 0; i < N; ++i )
				v[ i ] = w[ i ] / newEig;
			if ( Math.abs( newEig - eig ) <= 1e-9 * newEig )
			{
				eig = newEig;
				break;
			}
			eig = newEig;
		}

		return Math.sqrt( Math.max( eig, 0.0 ) );
	}

	private static double norm( final double[] v )
	{
		double s = 0.0;
		for ( final double x : v )
			s += x * x;
		return Math.sqrt( s );
	}

	private static void normalize( final double[] v )
	{
		final double n = norm( v );
		if ( n > 0 )
			for ( int i = 0; i < v.length; ++i )
				v[ i ] /= n;
	}

	// ─── I/O + resize helpers ──────────────────────────────────────────────────────

	/** Read a 2D RAI into a row-major float[H*W] plane (fast axis = W = X). */
	private static float[] toPlane( final RandomAccessibleInterval< ? extends RealType< ? > > frame, final int W, final int H )
	{
		final float[] plane = new float[ H * W ];
		final RandomAccess< ? extends RealType< ? > > ra = frame.randomAccess();
		final long minX = frame.min( 0 );
		final long minY = frame.min( 1 );
		for ( int y = 0; y < H; ++y )
		{
			ra.setPosition( minY + y, 1 );
			final int rowOff = y * W;
			for ( int x = 0; x < W; ++x )
			{
				ra.setPosition( minX + x, 0 );
				plane[ rowOff + x ] = ra.get().getRealFloat();
			}
		}
		return plane;
	}

	/**
	 * Bilinear resize of a row-major HxW plane to newH x newW using imglib2's
	 * {@link NLinearInterpolatorFactory}. Coordinate mapping follows the standard
	 * "align pixel centers / scale by size ratio" convention.
	 */
	public static float[] resize( final float[] in, final int H, final int W, final int newH, final int newW )
	{
		if ( H == newH && W == newW )
			return in.clone();

		// wrap as (X=W, Y=H)
		final ArrayImg< FloatType, FloatArray > img = ArrayImgs.floats( in, W, H );

		final double sx = ( double ) W / newW;
		final double sy = ( double ) H / newH;

		final AffineTransform2D t = new AffineTransform2D();
		t.set(
				sx, 0, 0.5 * ( sx - 1.0 ),
				0, sy, 0.5 * ( sy - 1.0 ) );

		final var interp = Views.interpolate( Views.extendBorder( img ), new NLinearInterpolatorFactory< FloatType >() );
		final var transformed = RealViews.affineReal( interp, t );
		final var ra = transformed.realRandomAccess();

		final float[] out = new float[ newH * newW ];
		for ( int y = 0; y < newH; ++y )
		{
			ra.setPosition( y, 1 );
			final int rowOff = y * newW;
			for ( int x = 0; x < newW; ++x )
			{
				ra.setPosition( x, 0 );
				out[ rowOff + x ] = ra.get().getRealFloat();
			}
		}
		return out;
	}
}
