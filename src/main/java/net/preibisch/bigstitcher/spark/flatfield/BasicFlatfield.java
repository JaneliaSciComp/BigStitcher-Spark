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
import java.util.Random;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccess;
import net.imglib2.RealRandomAccessible;
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
 * Port of the Julia reference
 * {@code flatfield-correction/BigFlatFieldIlluminator.jl/src/basic.jl}
 * ({@code basic_estimate}). Estimates a shared low-rank illumination model
 * (flatfield x per-frame scale + darkfield) plus a sparse residual, via an
 * inexact Augmented Lagrangian Multiplier (IALM) inner loop with an outer
 * reweighting loop.
 * <p>
 * Matches the Julia reference: darkfield limit = mean of per-pixel minima,
 * warm-started model state across reweighting iterations, Float32 precision,
 * the non-negativity projection {@code f = max(idct2(W_hat), 0)}, and a
 * (non-anti-aliased) bilinear resize. (MATLAB/Fiji differ in these choices —
 * tighter min(D) limit, per-call re-init, double precision, un-clamped idct.)
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

	public static class FramesStack {
		final int sourceWidth;
		final int sourceHeight;
		final int frameWidth;
		final int frameHeight;
		final int nFrames;
		final int frameSize;
		final float[][] stack;

		private FramesStack(int sourceWidth, int sourceHeight, int workingSize, int nFrames) {
			this.sourceWidth = sourceWidth;
			this.sourceHeight = sourceHeight;
			this.frameWidth = workingSize > 0 && workingSize < sourceWidth ? workingSize : sourceWidth;
			this.frameHeight = workingSize > 0 && workingSize < sourceHeight ? workingSize : sourceHeight;
			this.nFrames = nFrames;
			this.frameSize = frameWidth	* frameHeight;
			stack = new float[nFrames][];
		}

		public void setFrame(int frameIndex, RandomAccessibleInterval< ? extends RealType< ? > > frame) {
			if ( frame.dimension( 0 ) != sourceWidth || frame.dimension( 1 ) != sourceHeight )
				throw new IllegalArgumentException( "all frames must have the same size " +
						frame.dimension( 0 ) + " x " + frame.dimension(1) +
						" != " +
						sourceWidth + " x " + sourceHeight );

			stack[frameIndex] = getFrame(frame, frameWidth, frameHeight);
		}

		public boolean isEmpty() {
			return nFrames == 0;
		}

		public int size()  {
			return nFrames;
		}

		float[] avg( )
		{
			final float[] avg = new float[ frameSize ];
			for ( int p = 0; p < frameSize; ++p )
			{
				double s = 0.0;
				for ( int k = 0; k < nFrames; ++k )
					s += stack[ k ][ p ];
				avg[ p ] = ( float ) ( s / nFrames );
			}
			return avg;
		}

		double mean() {
			return BasicFlatfield.mean( stack );
		}

		void divide(double v) {
			for ( int k = 0; k < nFrames; ++k )
				for ( int p = 0; p < frameSize; ++p )
					stack[ k ][ p ] /= ( float ) v;
		}

		void sort()
		{
			// ── Per-pixel sort along the frame axis (matches MATLAB sort(D,3)) ────────
			final float[] col = new float[ nFrames ];
			for ( int p = 0; p < frameSize; ++p )
			{
				for ( int k = 0; k < nFrames; ++k )
					col[ k ] = stack[ k ][ p ];

				Arrays.sort( col );

				for ( int k = 0; k < nFrames; ++k )
					stack[ k ][ p ] = col[ k ];
			}
		}

		public RandomAccessibleInterval< FloatType > asImage() {
			float[] imgArray = new float[ frameSize * nFrames ];
			for (int k  = 0; k < nFrames; ++k ) {
				for ( int p = 0; p < frameSize; ++p ) {
					imgArray[ frameSize * k + p ] = stack[ k ][ p ];
				}
			}
			return ArrayImgs.floats( imgArray, frameWidth, frameHeight, nFrames );
		}
	}

	public static FramesStack createFramesStack( int imageWidth, int imageHeight, int frameWidth, int frameHeight, int nFrames ) {
		return new FramesStack(imageWidth, imageHeight, frameWidth, nFrames);
	}

	private static class BasicEstimateData
	{
		final int nFrames;
		final int frameSize;
		final boolean estimateDarkfield;

		final float[] W_hat;       // flatfield DCT coefficients
		final float[][] E;         // sparse residual
		final float[][] A1_hat;    // low-rank model prediction

		final float[] A1_coeff;    // per-frame illumination scale
		final float[] A_offset;    // spatially varying darkfield
		final float[][] W_coeff;   // reweighting coefficients
		final float[][] Z;         // primal feasibility residual
		final float[][] Y1;        // Lagrange multiplier
		final float[] f;           // spatial flatfield = idct2(W_hat)
		final float[] R_W;         // mean-over-frames W-update residual
		final float[][] R1;        // D - E: the "clean" low-rank part
		final float[] ffCurr;      // current flatfield for reweight check
		final float[] B1_coeff;
		final float[] B_offset;
		final float[] A1_offset;

		BasicEstimateData(int nFrames, int frameSize, boolean estimateDarkfield ) {
			this.nFrames = nFrames;
			this.frameSize = frameSize;
			this.estimateDarkfield = estimateDarkfield;

			W_hat = new float[ frameSize ];                // flatfield DCT coefficients
			E = new float[ nFrames ][ frameSize ];         // sparse residual
			A1_hat = new float[ nFrames ][ frameSize ];    // low-rank model prediction
			A1_coeff = new float[ nFrames ];               // per-frame illumination scale
			A_offset = new float[ frameSize ];             // spatially varying darkfield
			W_coeff = new float[ nFrames ][ frameSize ];   // reweighting coefficients
			Z = new float[ nFrames ][ frameSize ];         // primal feasibility residual
			Y1 = new float[ nFrames ][ frameSize ];        // Lagrange multiplier
			f = new float[ frameSize ];                    // spatial flatfield = idct2(W_hat)
			R_W = new float[ frameSize ];                  // mean-over-frames W-update residual
			R1 = new float[ nFrames ][ frameSize ];        // D - E: the "clean" low-rank part
			ffCurr = new float[ frameSize ];               // current flatfield for reweight check
			B1_coeff = new float[ estimateDarkfield ? nFrames : 0 ];
			B_offset = new float[ estimateDarkfield ? frameSize : 0 ];
			A1_offset = new float[ estimateDarkfield ? frameSize : 0 ];

			Arrays.fill( A1_coeff, 1f );
			for ( int k = 0; k < nFrames; ++k )
				Arrays.fill( W_coeff[ k ], 1f );

		}
	}
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
			final FramesStack images,
			final BasicFlatfieldParams params,
			final Random rng )
	{
		long start = System.currentTimeMillis();
		final int N = images.nFrames;
		if ( N == 0 )
			throw new IllegalArgumentException( "BaSiC: empty image list" );

		System.out.printf("Estimate darkfield/flatfield from %d images using %s\n", images.nFrames, params );

		final int H_orig = images.sourceHeight;
		final int W_orig = images.sourceWidth;

		final int H = images.frameHeight;
		final int W = images.frameWidth;
		final int HW = images.frameSize;
		final float[][] D = images.stack;

		System.out.printf( "Image size used for darkfield/flatfield (%d, %d) -> (%d, %d)\n", H_orig, W_orig, H, W );

		// ── Normalize so working values hover around 1 ────────────────────────────
		final double globalMean = images.mean();
		if ( globalMean < EPS )
			throw new IllegalArgumentException( "BaSiC: image stack is all-zero" );

		System.out.printf( "Normalize images using global mean: %f\n", globalMean );
		images.divide( globalMean );

		images.sort();

		// mean_img[p]: spatial mean over all frames
		final float[] meanImg = images.avg();

		// ── Auto-lambdaFlatfield ───────────────────────────────────────────────────────────
		final float[] lambdas = params.deriveLambdas( meanImg, H, W );
		final float lambdaFlatfield = lambdas[ 0 ];
		final float lambdaDarkfield = lambdas[ 1 ];
		System.out.println( "BaSiC auto-params: lambdaFlatfield=" + lambdaFlatfield + " lambdaDarkfield=" + lambdaDarkfield );

		// ── Spectral norm for penalty initialisation ──────────────────────────────
		final float normTwo = ( float ) spectralNorm( D, rng, HW );
		final float normD = ( float ) frobeniusNorm( D, HW );
		final float muInit = 12.5f / normTwo;
		final float muBar = muInit * 1e7f;
		final float rho = 1.5f;
		final float ent1 = 1f;
		final float ent2 = 10f;

		System.out.printf( "Spectral norm: %f, Frobenius norm: %f\n", normTwo, normD );

		// Upper bound for darkfield: mean of the per-pixel minima over frames
		// (D[0] after the ascending per-pixel sort), matching the Julia reference
		// mean(D[:,:,1]). MATLAB/Fiji use the tighter B1_uplimit = min(D(:)); the
		// looser Julia bound lets B1_offset grow larger, raising the darkfield DC.
		float darkfieldLimit = mean( D[0] );

		final boolean estimateDarkfield = params.estimateDarkfield;

		// ── Optimisation variables ─────────────────────────────────────────────────
		// The model (W_hat / A_offset / A1_coeff / W_coeff) warm-starts across
		// reweighting iterations, matching the Julia reference; only Y1 and E are
		// re-zeroed each reweighting (see the loop below). (MATLAB/Fiji instead
		// re-initialize the whole model on every inexact_alm_rspca_l1 call.)
		BasicEstimateData basicEstimateData = new BasicEstimateData( images.nFrames, images.frameSize, estimateDarkfield );

		// ── Pre-allocated buffers ──────────────────────────────────────────────────

		final float[] flatfieldPrev = new float[ images.frameSize ];
		ones( flatfieldPrev );
		final float[] darkfieldPrev = new float[ images.frameSize ];
		zeros( darkfieldPrev );

		float B1_offsetFinal = 0f;

		for ( int rw = 1; rw <= params.maxReweightIterations; ++rw )
		{
			// Reset only the Lagrange multiplier and sparse residual each
			// reweighting (Julia: Y1 .= 0; E .= 0); the rest warm-starts.
			for ( int k = 0; k < N; ++k )
			{
				zeros( basicEstimateData.Y1[ k ] );
				zeros( basicEstimateData.E[ k ] );
			}

			final float B1_offset = almLoop(
					images,
					basicEstimateData,
					muInit, muBar, rho, ent1, ent2,
					lambdaFlatfield, lambdaDarkfield, darkfieldLimit,
					params.maxIterations, params.optimizationTol, normD
			);

			B1_offsetFinal = B1_offset;

			// Add scalar darkfield component accumulated during ALM iterations
			// (MATLAB: A_offset += B1_offset * W_idct_hat, un-clamped flatfield)
			if ( basicEstimateData.estimateDarkfield )
			{
				max (Dct2D.idct2( basicEstimateData.W_hat, basicEstimateData.f, H, W ), 0 );
				for ( int p = 0; p < basicEstimateData.frameSize; ++p )
					basicEstimateData.A_offset[ p ] += B1_offset * basicEstimateData.f[ p ];
			}

			// ── Reweighting convergence check (normalized L1) ─────────────────────
			max( Dct2D.idct2( basicEstimateData.W_hat, basicEstimateData.f, H, W ), 0 );

			final float meanA1 = mean( basicEstimateData.A1_coeff );

			for ( int p = 0; p < basicEstimateData.frameSize; ++p )
				basicEstimateData.ffCurr[ p ] = basicEstimateData.f[ p ] * meanA1;

			final float ffCurrMean = Math.max( mean( basicEstimateData.ffCurr ), EPS );
			div( basicEstimateData.ffCurr,  ffCurrMean );

			double madFfNum = 0.0, madFfDen = 0.0;
			for ( int p = 0; p < basicEstimateData.frameSize; ++p )
			{
				madFfNum += Math.abs( basicEstimateData.ffCurr[ p ] - flatfieldPrev[ p ] );
				madFfDen += Math.abs( flatfieldPrev[ p ] );
			}
			final float madFf = ( float ) ( madFfNum / Math.max( madFfDen, EPS ) );

			float madDf = 0f;
			if ( estimateDarkfield )
			{
				double td = 0.0, den = 0.0;
				for ( int p = 0; p < HW; ++p )
				{
					td += Math.abs( basicEstimateData.A_offset[ p ] - darkfieldPrev[ p ] );
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

			System.arraycopy( basicEstimateData.ffCurr, 0, flatfieldPrev, 0, HW );
			if ( estimateDarkfield )
				System.arraycopy( basicEstimateData.A_offset, 0, darkfieldPrev, 0, HW );

			updateWeights( basicEstimateData.W_coeff, basicEstimateData.E, basicEstimateData.f,
					basicEstimateData.A1_coeff, basicEstimateData.A_offset, params.epsilon, H, W, N );
		}

		// ── Final flatfield: max(idct2(W_hat), 0) normalised to mean = 1 (Julia) ──
		final float[] flatfield = new float[ HW ];
		max ( Dct2D.idct2( basicEstimateData.W_hat, flatfield, H, W ), 0 );

		float flatMean = mean( flatfield );
		if ( flatMean < EPS )
			flatMean = 1f;

		div(flatfield, flatMean);

		// A_offset was estimated on D = images / globalMean. Convert the additive
		// darkfield back to source intensity units because applyCorrection subtracts
		// it from raw pixels.
		final float[] darkfield = basicEstimateData.A_offset.clone();
		mul( darkfield, (float) globalMean );

		// ── Resize outputs back to original dimensions if working_size was used ────
		final float[] flatOut = H_orig != H || W_orig != W
				? resize( flatfield, H, W, H_orig, W_orig )
				: flatfield;
		final float[] darkOut = H_orig != H || W_orig != W
				? resize( darkfield, H, W, H_orig, W_orig )
				: darkfield;

		// wrap as ArrayImg<FloatType> in (X=W, Y=H) imglib2 order
		final ArrayImg< FloatType, FloatArray > flatImg = ArrayImgs.floats( flatOut, W_orig, H_orig );
		final ArrayImg< FloatType, FloatArray > darkImg = ArrayImgs.floats( darkOut, W_orig, H_orig );

		final double[] frameScales = new double[ N ];
		for ( int k = 0; k < N; ++k )
			frameScales[ k ] = basicEstimateData.A1_coeff[ k ];

		System.out.printf("Finished BaSiC flatfield/darkfield estimation in %f secs\n", (System.currentTimeMillis() - start) / 1000.0);

		return new BasicFlatfieldResult( flatImg, darkImg, frameScales, B1_offsetFinal );
	}

	// ─── Inner ALM loop ──────────────────────────────────────────────────────────

	private static float almLoop(
			final FramesStack framesStack,
			final BasicEstimateData basicEstimateData,
			final float muInit, final float muBar, final float rho, final float ent1, final float ent2,
			final float lambdaFlatfield, final float lambdaDarkfield, final float darkfieldLimit,
			final int maxIterations, final float optimizationTol, final float normD )
	{
		// local aliases into the frame stack / working buffers
		final int H = framesStack.frameHeight;
		final int W = framesStack.frameWidth;
		final int HW = framesStack.frameSize;
		final int N = framesStack.nFrames;
		final float[][] D = framesStack.stack;
		final float[] W_hat = basicEstimateData.W_hat;
		final float[] f = basicEstimateData.f;
		final float[] R_W = basicEstimateData.R_W;
		final float[] A1_coeff = basicEstimateData.A1_coeff;
		final float[] A_offset = basicEstimateData.A_offset;
		final float[][] E = basicEstimateData.E;
		final float[][] A1_hat = basicEstimateData.A1_hat;
		final float[][] Y1 = basicEstimateData.Y1;
		final float[][] R1 = basicEstimateData.R1;
		final float[][] Z = basicEstimateData.Z;
		final float[][] W_coeff = basicEstimateData.W_coeff;
		final float[] B_offset = basicEstimateData.B_offset;
		final float[] A1_offset = basicEstimateData.A1_offset;
		final float[] B1_coeff = basicEstimateData.B1_coeff;
		final boolean estimateDarkfield = basicEstimateData.estimateDarkfield;

		float mu = muInit;
		float B1_offset = 0f;

		for ( int iter = 1; iter <= maxIterations; ++iter )
		{
			// f = max(idct2(W_hat), 0)  (Julia non-negativity projection)
			max( Dct2D.idct2( W_hat, f, H, W ), 0 );
			// A1_hat = f * A1_coeff[k] + A_offset
			buildA1Hat( basicEstimateData );

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
			// (scale and transform R_W in place; R_W is refilled by meanOverFrames each iter)
			div( R_W, ent1);

			Dct2D.dct2( R_W, R_W, H, W );

			add( W_hat, R_W);

			shrink( W_hat, lambdaFlatfield / ( ent1 * mu ) );

			// recompute f = max(idct2(W_hat), 0) and A1_hat
			Dct2D.idct2( W_hat, f, H, W );

			max( f, 0 );

			buildA1Hat( basicEstimateData );

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
			buildA1Hat( basicEstimateData );

			// ── Update darkfield ──────────────────────────────────────────────────
			if ( estimateDarkfield )
			{
				B1_offset = updateDarkfield(
						A_offset, B_offset, A1_offset, B1_coeff,
						f, R1, A1_coeff, darkfieldLimit, lambdaDarkfield, ent2, mu, H, W, N );
				buildA1Hat( basicEstimateData );
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
		double gr1 = mean( R1 );
		final float safeGR1 = ( Math.abs( gr1 ) < EPS ) ? 1f : ( float ) gr1;

		// B1_coeff[k]: contrast of R1 between high/low regions, normalized by global mean
		zeros( B1_coeff );
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

		// Smooth and sparsify via DCT + image-domain shrink.
		// Reuse A1_offset as the DCT-coefficient scratch (it is no longer needed here).
		final float thr = lambdaDarkfield / ( ent2 * mu );

		final float[] wOff = A1_offset;
		shrink( Dct2D.dct2( A_offset, wOff, H, W ), thr );
		shrink( Dct2D.idct2( wOff, A_offset, H, W ), thr );

		add( A_offset, B_offset );

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

	/**
	 * A1_hat[h,w,k] = f[h,w] * A1_coeff[k] + A_offset[h,w]:
	 * low-rank model (rank-1 flatfield scaled per frame, plus additive darkfield)
	 */
	private static void buildA1Hat( final BasicEstimateData basicEstimateData )
	{
		int N = basicEstimateData.nFrames;
		int HW = basicEstimateData.frameSize;
		final float[][] A1_hat = basicEstimateData.A1_hat;
		final float[] f = basicEstimateData.f;
		final float[] A1_coeff = basicEstimateData.A1_coeff;
		final float[] A_offset = basicEstimateData.A_offset;
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

	private static void add( float[] a, float[] b )
	{
		for  ( int p = 0; p < a.length; ++p )
			a[p] += b[p];
	}

	private static void div( float[] a, float b )
	{
		for  ( int p = 0; p < a.length; ++p )
			a[p] /= b;
	}

	private static void mul( float[] a, float b )
	{
		for  ( int p = 0; p < a.length; ++p )
			a[p] *= b;
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

	private static void max( final float[] a, final float b )
	{
		for ( int p = 0; p < a.length; ++p )
		{
			if ( a[ p ] < b )
				a[ p ] = b;
		}
	}

	private static void zeros( final float[] x )
	{
		Arrays.fill( x, 0f );
	}

	private static void ones( final float[] x )
	{
		Arrays.fill( x, 1f );
	}

	private static double mean( float[][] stack )
	{
		double sum = 0.0;
		long n = 0;
		for ( int k = 0; k < stack.length; ++k )
			for ( int p = 0; p < stack[k].length; ++p ) {
				sum += stack[k][p];
				n++;
			}
		return n == 0 ? 0 : sum / ( ( double ) n );
	}

	private static double frobeniusNorm( final float[][] D, final int HW )
	{
		double s = 0.0;
		for (final float[] Dk : D) {
			for (int p = 0; p < HW; ++p)
				s += (double) Dk[p] * Dk[p];
		}
		return Math.sqrt( s );
	}

	/**
	 * Largest singular value of the flattened (HW x N) stack D, via power
	 * iteration on the NxN Gram matrix G = D^T D. sigma_max = sqrt(lambda_max(G)).
	 */
	private static double spectralNorm( final float[][] D, final Random rng, final int HW )
	{
		// G[i][j] = sum_p D[i][p] * D[j][p]  (frame-major: D[k] is a frame plane)
		final int N = D.length;
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
		for ( int i = 0; i < N; ++i )
			v[ i ] = rng.nextDouble() + 1e-3;
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
	private static float[] getFrame( final RandomAccessibleInterval< ? extends RealType< ? > > image,
									 final int frameWidth, final int frameHeight )
	{
		final int H_orig = ( int ) image.dimension( 1 ); // rows (Y)
		final int W_orig = ( int ) image.dimension( 0 ); // cols (X)

		int size = H_orig * W_orig;
		final float[] plane = new float[ size ];
		Cursor< ? extends RealType< ? > > fc = image.cursor();
		int planeIndex = 0;
		while ( fc.hasNext() ) {
			plane[ planeIndex++ ] = fc.next().getRealFloat();
		}
		return frameWidth != W_orig || frameHeight != H_orig
				? resize( plane, H_orig, W_orig, frameHeight, frameWidth )
				: plane;
	}

	/**
	 * Resize a row-major HxW plane to newH x newW using imglib2's
	 * {@link RealViews#affineReal} with an {@link NLinearInterpolatorFactory}
	 * (bilinear).
	 * <p>
	 * {@code affineReal(source, t)} treats {@code t} as the source&rarr;target
	 * transform (it inverts internally, so target pixel {@code x} samples the source
	 * at {@code t}<sup>-1</sup>{@code (x)}). We use the standard "align pixel centers
	 * / scale by size ratio" mapping {@code source = (target + 0.5)*(W/newW) - 0.5},
	 * whose forward form is {@code target = source*(newW/W) + (0.5*newW/W - 0.5)}.
	 * <p>
	 * Caveat: bilinear does <b>not</b> anti-alias when downsampling frames by a large
	 * factor (e.g. 920&rarr;128) — to suppress aliasing there, low-pass first (e.g.
	 * {@code Gauss3}) before sampling. This does not match the Fiji BaSiC plugin's
	 * resize exactly.
	 */
	public static float[] resize( final float[] in, final int H, final int W, final int newH, final int newW )
	{
		if ( H == newH && W == newW )
			return in.clone();

		// wrap as (X=W, Y=H)
		final ArrayImg< FloatType, FloatArray > img = ArrayImgs.floats( in, W, H );

		final double scaleX = ( double ) newW / W;
		final double scaleY = ( double ) newH / H;
		final double transX = 0.5 * scaleX - 0.5;
		final double transY = 0.5 * scaleY - 0.5;

		final AffineTransform2D t = new AffineTransform2D();
		t.set(
				scaleX, 0, transX,
				0, scaleY, transY
		);

		final RealRandomAccessible< FloatType > interp =
				Views.interpolate( Views.extendBorder( img ), new NLinearInterpolatorFactory<>() );
		final RealRandomAccess< FloatType > ra = RealViews.affineReal( interp, t ).realRandomAccess();

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
