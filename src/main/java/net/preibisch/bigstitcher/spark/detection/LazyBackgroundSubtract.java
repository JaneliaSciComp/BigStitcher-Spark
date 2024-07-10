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
package net.preibisch.bigstitcher.spark.detection;

import java.io.File;
import java.util.function.Consumer;

import ij.ImageJ;
import ij.plugin.filter.RankFilters;
import ij.process.FloatProcessor;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.AccessFlags;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import net.preibisch.legacy.io.IOFunctions;
import util.Lazy;

public class LazyBackgroundSubtract<T extends RealType<T> & NativeType<T>> implements Consumer<RandomAccessibleInterval<FloatType>>
{
	final private int radiusXY;
	final long[] globalMin;
	final private RandomAccessible<T> source;
	final int n;

	public LazyBackgroundSubtract(
			final long[] min,
			RandomAccessible<T> source,
			final int radiusXY )
	{
		while ( source.numDimensions() < 3 )
			source = Views.addDimension( source );

		if ( source.numDimensions() > 3 )
			throw new RuntimeException( "Currently only max 3 dimensions supported." );

		this.source = source;
		this.globalMin = min;
		this.radiusXY = radiusXY;
		this.n = 3;
	}

	// Note: the output RAI typically sits at 0,0...0 because it usually is a CachedCellImage
	// (but the actual interval to process in many blocks sits somewhere else) 
	@Override
	public void accept( final RandomAccessibleInterval<FloatType> outputZeroMin )
	{
		final RandomAccessibleInterval<FloatType> output = Views.translate( outputZeroMin, globalMin );

		// copy each slice to a FloatProcessor, size + kernelradius * 2
		final Interval iterateInterval = Intervals.expand( output, new long[] { radiusXY, radiusXY, 0 } );
		final FloatProcessor fp = new FloatProcessor( (int)iterateInterval.dimension( 0 ), (int)iterateInterval.dimension( 1 ) );
		final float[] pixelsMedian = (float[])fp.getPixels();
		final float[] pixels = new float[ pixelsMedian.length ];

		// just a wrapper around the arrays
		final Img<FloatType> imgMedian = ArrayImgs.floats( pixelsMedian, iterateInterval.dimension( 0 ), iterateInterval.dimension( 1 ) );
		final Img<FloatType> img = ArrayImgs.floats( pixels, iterateInterval.dimension( 0 ), iterateInterval.dimension( 1 ) );

		// the interval in the FloatProcessor to copy into the output
		final Interval cropInterval = Intervals.translate( new FinalInterval( output.dimension( 0 ), output.dimension( 1 ) ), new long[] { radiusXY, radiusXY } );
		final RandomAccessibleInterval< FloatType > imgMedianCrop = Views.interval( imgMedian, cropInterval );
		final RandomAccessibleInterval< FloatType > imgCrop = Views.interval( img, cropInterval );

		/*
		// init single-threaded
		final RankFilters rf;
		synchronized ( this )
		{
			final int numThreads = Prefs.getThreads();
			Prefs.setThreads( 1 );
			rf = new RankFilters();
			Prefs.setThreads( numThreads );
			rf.setNPasses( 0 ); // suppress progress bar
		}
		*/
		final RankFilters rf = new RankFilters();
		rf.setNPasses( 0 ); // suppress progress bar

		// for each z slice do
		for ( long z = iterateInterval.min( 2 ); z <= iterateInterval.max( 2 ); ++z )
		{
			int i = 0;
			for ( final T t : Views.flatIterable( Views.hyperSlice( Views.interval( source, iterateInterval ), 2, z ) ) )
				pixelsMedian[ i ] = pixels[ i++ ] = t.getRealFloat();

			// run median
			rf.rank(fp, radiusXY, RankFilters.MEDIAN );

			//ImageJFunctions.show( imgMedian );
			//ImageJFunctions.show( img );

			// subtract only the center
			final Cursor< FloatType > out = Views.flatIterable( Views.hyperSlice( output, 2, z ) ).cursor();
			final Cursor< FloatType > median = Views.flatIterable( imgMedianCrop ).cursor();
			final Cursor< FloatType > image = Views.flatIterable( imgCrop ).cursor();

			while ( out.hasNext() )
			{
				final float m = median.next().get();
				final float v = image.next().get();

				if ( m > 0 )
					out.next().setReal( v / m );
				else
					out.next().setReal( 0 );
			}

			//ImageJFunctions.show( output );
			//SimpleMultiThreading.threadHaltUnClean();
		}
	}

	public static final <T extends RealType<T> & NativeType<T>> RandomAccessibleInterval<FloatType> init(
			final RandomAccessible< T > input,
			final Interval processingInterval,
			final int radiusXY,
			final int[] blockSize )
	{
		final long[] min = processingInterval.minAsLongArray();

		final LazyBackgroundSubtract<T> lazyBG =
				new LazyBackgroundSubtract<>(
						min,
						input,
						radiusXY );

		final RandomAccessibleInterval<FloatType> bg =
				Views.translate(
						Lazy.process(
								processingInterval,
								blockSize,
								new FloatType(),
								AccessFlags.setOf(),
								lazyBG ),
						min );

		return bg;
	}

	public static void main( String[] args )
	{
		new ImageJ();

		final RandomAccessibleInterval< FloatType > raw =
				//IOFunctions.openAs32BitArrayImg( new File( "/Users/preibischs/Documents/Microscopy/SPIM/HisYFP-SPIM/spim_TL18_Angle0.tif"));
				//IOFunctions.openAs32BitArrayImg( new File( "/Users/preibischs/Documents/Janelia/Projects/BigStitcher/Allen/tile_x_0002_y_0000_z_0000_ch_488.zarr-s4.tif"));
				IOFunctions.openAs32BitArrayImg( new File( "/home/preibischs@hhmi.org/Desktop/Allen/tile_x_0002_y_0000_z_0000_ch_488.zarr-s4.tif"));

		//RandomAccessibleInterval< FloatType > inputCropped = Views.interval( Views.extendMirrorDouble( raw ), Intervals.expand( raw, new long[] {-200, -200, -30}) );
		RandomAccessibleInterval< FloatType > inputCropped = Views.interval( Views.extendMirrorDouble( raw ), Intervals.expand( raw, new long[] {0, 0, 0}) );

		ImageJFunctions.show( inputCropped );

		RandomAccessibleInterval<FloatType> bgCorrected = LazyBackgroundSubtract.init(
				inputCropped,
				new FinalInterval(inputCropped),
				10,
				new int[] {256, 256, 1} );

		ImageJFunctions.show( bgCorrected );
	}
}
