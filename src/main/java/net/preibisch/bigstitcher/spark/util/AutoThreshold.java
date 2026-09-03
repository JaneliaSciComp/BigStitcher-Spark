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
package net.preibisch.bigstitcher.spark.util;

import ij.process.AutoThresholder;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

/**
 * Per-image foreground threshold from a 256-bin histogram over the image's
 * [min, max] range, handed to ImageJ's {@link AutoThresholder}. Same recipe as
 * scikit-image's {@code threshold_li} / {@code threshold_otsu}, which the
 * reference Python pipeline uses.
 */
public class AutoThreshold
{
	public enum Method { LI, OTSU }

	private static final int NUM_BINS = 256;

	/**
	 * @return the lowest intensity that counts as foreground, or {@link Double#NaN}
	 *         if the image is constant (nothing to split)
	 */
	public static < T extends RealType< T > > double compute(
			final Method method,
			final RandomAccessibleInterval< T > img )
	{
		double min = Double.POSITIVE_INFINITY;
		double max = Double.NEGATIVE_INFINITY;

		for ( final T t : Views.iterable( img ) )
		{
			final double v = t.getRealDouble();
			if ( v < min )
				min = v;
			if ( v > max )
				max = v;
		}

		if ( !( max > min ) )
			return Double.NaN;

		final double binWidth = ( max - min ) / NUM_BINS;
		final int[] histogram = new int[ NUM_BINS ];

		for ( final T t : Views.iterable( img ) )
			++histogram[ Math.min( NUM_BINS - 1, ( int ) ( ( t.getRealDouble() - min ) / binWidth ) ) ];

		// AutoThresholder returns the index of the last background bin (or -1 if it
		// cannot split); intensity matching discards anything strictly below the
		// threshold, so the floor is the next bin's lower edge
		final int bin = new AutoThresholder().getThreshold( ijMethod( method ), histogram );

		if ( bin < 0 )
			return Double.NaN;

		return min + ( bin + 1 ) * binWidth;
	}

	private static AutoThresholder.Method ijMethod( final Method method )
	{
		return method == Method.LI ? AutoThresholder.Method.Li : AutoThresholder.Method.Otsu;
	}
}
