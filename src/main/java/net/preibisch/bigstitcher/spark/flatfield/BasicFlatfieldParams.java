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

import java.io.Serializable;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Immutable parameter holder for the BaSiC flatfield / darkfield estimation
 * (see {@link BasicFlatfield}). Defaults mirror the Julia reference
 * {@code BaSiCParams} / {@code load_basic_params()} in
 * {@code flatfield-correction/BigFlatFieldIlluminator.jl/src/basic.jl}.
 */
public class BasicFlatfieldParams implements Serializable
{
	private static final long serialVersionUID = 1L;

	/** Working image size for the auto-lambda calibration constants (128x128). */
	public static final int LAMBDA_CALIBRATION_SIZE = 128;

	/** whether to estimate the additive darkfield (default: true). */
	public final boolean estimateDarkfield;

	/** flatfield regularization strength; 0 = auto-derive. */
	public final float lambdaFlatfield;

	/** darkfield regularization strength; 0 = auto-derive. */
	public final float lambdaDarkfield;

	/** maximum inner ALM iterations (default: 500). */
	public final int maxIterations;

	/** inner ALM convergence tolerance on relative primal residual (default: 1e-6). */
	public final float optimizationTol;

	/** outer reweighting convergence tolerance (default: 1e-3). */
	public final float reweightTol;

	/** maximum outer reweighting iterations (default: 10). */
	public final int maxReweightIterations;

	/** reweighting stability term (default: 0.1). */
	public final float epsilon;

	/** resize each frame to workingSize x workingSize before optimizing; 0 = no resize. */
	public final int workingSize;

	public BasicFlatfieldParams(
			final boolean estimateDarkfield,
			final float lambdaFlatfield,
			final float lambdaDarkfield,
			final int maxIterations,
			final float optimizationTol,
			final float reweightTol,
			final int maxReweightIterations,
			final float epsilon,
			final int workingSize )
	{
		this.estimateDarkfield = estimateDarkfield;
		this.lambdaFlatfield = lambdaFlatfield;
		this.lambdaDarkfield = lambdaDarkfield;
		this.maxIterations = maxIterations;
		this.optimizationTol = optimizationTol;
		this.reweightTol = reweightTol;
		this.maxReweightIterations = maxReweightIterations;
		this.epsilon = epsilon;
		this.workingSize = workingSize;
	}

	/** Reference defaults (matches {@code load_basic_params()}). */
	public static BasicFlatfieldParams defaults()
	{
		return new BasicFlatfieldParams(
				true,    // estimateDarkfield
				0f,      // lambdaFlatfield (auto)
				0f,      // lambdaDarkfield (auto)
				500,     // maxIterations
				1e-6f,   // optimizationTol
				1e-3f,   // reweightTol
				10,      // maxReweightIterations
				0.1f,    // epsilon
				0 );     // workingSize
	}

	/**
	 * Auto-derive lambda / lambdaDarkfield from the sum of absolute DCT
	 * coefficients of the (spatial-)mean image, calibrated at 128x128.
	 * <p>
	 * Faithful port of the Julia {@code _auto_lambda}: the constants 800 / 2000
	 * are calibrated for a 128x128 working image, so {@code l1_dct} is always
	 * computed from a 128x128 version of the mean image. A value that is already
	 * non-zero (explicitly set by the user) is passed through unchanged.
	 *
	 * @param meanImg      row-major spatial-mean image, length {@code H*W}
	 * @param H            rows of {@code meanImg}
	 * @param W            columns of {@code meanImg}
	 * @return {@code {lambda, lambdaDarkfield}}
	 */
	public float[] deriveLambdas( final float[] meanImg, final int H, final int W )
	{
		float lam = lambdaFlatfield;
		float lamDf = lambdaDarkfield;

		if ( lam == 0f || lamDf == 0f )
		{
			final float[] meanForLambda;
			final int mh, mw;
			if ( H == LAMBDA_CALIBRATION_SIZE && W == LAMBDA_CALIBRATION_SIZE )
			{
				meanForLambda = meanImg;
				mh = H;
				mw = W;
			}
			else
			{
				meanForLambda = BasicFlatfield.resize( meanImg, H, W, LAMBDA_CALIBRATION_SIZE, LAMBDA_CALIBRATION_SIZE );
				mh = LAMBDA_CALIBRATION_SIZE;
				mw = LAMBDA_CALIBRATION_SIZE;
			}

			final float[] dct = new float[ mh * mw ];
			Dct2D.dct2( meanForLambda, dct, mh, mw );
			double l1 = 0.0;
			for ( final float v : dct )
				l1 += Math.abs( v );
			final float l1Dct = ( float ) l1;

			if ( lam == 0f )
				lam = l1Dct / 800f;
			if ( lamDf == 0f )
				lamDf = l1Dct / 2000f;
		}

		return new float[] { lam, lamDf };
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this)
				.append("estimateDarkfield", estimateDarkfield)
				.append("lambdaFlatfield", lambdaFlatfield)
				.append("lambdaDarkfield", lambdaDarkfield)
				.append("maxIterations", maxIterations)
				.append("optimizationTol", optimizationTol)
				.append("reweightTol", reweightTol)
				.append("maxReweightIterations", maxReweightIterations)
				.append("epsilon", epsilon)
				.append("workingSize", workingSize)
				.toString();
	}
}
