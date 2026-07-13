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

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.real.FloatType;

/**
 * Output of {@link BasicFlatfield#estimate}: the estimated multiplicative
 * flatfield and additive darkfield (both HxW, at the original frame size), the
 * per-frame illumination scales and the darkfield baseline for QC.
 */
public class BasicFlatfieldResult
{
	/** Multiplicative flatfield (HxW, non-negative, mean ~ 1). */
	public final RandomAccessibleInterval< FloatType > flatfield;

	/** Additive darkfield (HxW). */
	public final RandomAccessibleInterval< FloatType > darkfield;

	/** Per-frame illumination scale factors ({@code A1_coeff}), useful for QC. */
	public final double[] frameScales;

	/** Scalar darkfield offset ({@code B1_offset}) accumulated during ALM, for QC. */
	public final double baseline;

	public BasicFlatfieldResult(
			final RandomAccessibleInterval< FloatType > flatfield,
			final RandomAccessibleInterval< FloatType > darkfield,
			final double[] frameScales,
			final double baseline )
	{
		this.flatfield = flatfield;
		this.darkfield = darkfield;
		this.frameScales = frameScales;
		this.baseline = baseline;
	}
}
