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
import mpicbg.spim.data.generic.sequence.BasicImgLoader;
import mpicbg.spim.data.sequence.MultiResolutionImgLoader;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
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
	 * The coarsest STORED resolution level that is no coarser than {@code maxDownsampling},
	 * read as-is.
	 * <p>
	 * Deliberately not {@code SparkInterestPointDetection.openAndDownsample}: that refines the
	 * best stored level with extra factors of 2 through {@code LazyDownsample2x}, which is both
	 * needless here (a histogram does not need an exact scale) and throws
	 * {@code ArrayIndexOutOfBoundsException} on production-sized views. Reading a stored level
	 * streams cell by cell, so it also needs no extra memory.
	 *
	 * @return the image, and the downsampling factor it actually corresponds to
	 */
	public static Pair< RandomAccessibleInterval< ? >, Long > openForHistogram(
			final BasicImgLoader imgLoader,
			final ViewId viewId,
			final long maxDownsampling )
	{
		if ( imgLoader instanceof MultiResolutionImgLoader )
		{
			final MultiResolutionImgLoader mr = ( MultiResolutionImgLoader ) imgLoader;
			final double[][] resolutions = mr.getSetupImgLoader( viewId.getViewSetupId() ).getMipmapResolutions();

			int bestLevel = 0;
			long bestFactor = 1;

			for ( int level = 0; level < resolutions.length; ++level )
			{
				final long[] f = new long[ resolutions[ level ].length ];
				for ( int d = 0; d < f.length; ++d )
					f[ d ] = Math.round( resolutions[ level ][ d ] );

				// all dimensions no coarser than requested, and the coarsest such level
				boolean ok = true;
				for ( final long fd : f )
					ok &= fd <= maxDownsampling;

				if ( ok && f[ 0 ] >= bestFactor )
				{
					bestLevel = level;
					bestFactor = f[ 0 ];
				}
			}

			return new ValuePair<>( mr.getSetupImgLoader( viewId.getViewSetupId() ).getImage( viewId.getTimePointId(), bestLevel ), bestFactor );
		}

		return new ValuePair<>( imgLoader.getSetupImgLoader( viewId.getViewSetupId() ).getImage( viewId.getTimePointId() ), 1L );
	}

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
