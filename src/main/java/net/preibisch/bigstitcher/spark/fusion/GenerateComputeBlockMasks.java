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
package net.preibisch.bigstitcher.spark.fusion;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import ij.ImageJ;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.multithreading.SimpleMultiThreading;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.util.ViewUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;

public class GenerateComputeBlockMasks
{
	private final long[] minBB, maxBB;

	private final SpimData2 data;

	private final HashMap< ViewId, AffineTransform3D > registrations;

	private final List< ViewId > overlappingViews;

	private final boolean uint8;

	private final boolean uint16;

	private final double[] maskOffset;

	public GenerateComputeBlockMasks(
			final SpimData2 dataLocal,
			final HashMap< ViewId, AffineTransform3D > registrations,
			final List< ViewId > overlappingViews,
			final long[] minBB,
			final long[] maxBB,
			final boolean uint8,
			final boolean uint16,
			final double[] maskOffset )
	{
		this.data = dataLocal;
		this.minBB = minBB;
		this.maxBB = maxBB;
		this.registrations = registrations;
		this.overlappingViews = overlappingViews;
		this.uint8 = uint8;
		this.uint16 = uint16;
		this.maskOffset = maskOffset;
	}

	public RandomAccessibleInterval call( final long[][] gridBlock )
	{
		final int n = minBB.length;

		// The min coordinates of the block that this job renders (in pixels)
		final long[] superBlockOffset = new long[ n ];
		Arrays.setAll( superBlockOffset, d -> gridBlock[ 0 ][ d ] + minBB[ d ] );

		// The size of the block that this job renders (in pixels)
		final long[] superBlockSize = gridBlock[ 1 ];

		//System.out.println( "Fusing block: offset=" + Util.printCoordinates( gridBlock[0] ) + ", dimension=" + Util.printCoordinates( gridBlock[1] ) );

		// The min grid coordinate of the block that this job renders, in units of the output grid.
		// Note, that the block that is rendered may cover multiple output grid cells.
		//final long[] outputGridOffset = gridBlock[ 2 ];

		// --------------------------------------------------------
		// initialization work that is happening in every job,
		// independent of gridBlock parameters
		// --------------------------------------------------------

		final long[] fusedBlockMin = new long[ n ];
		final long[] fusedBlockMax = new long[ n ];
		final Interval fusedBlock = FinalInterval.wrap( fusedBlockMin, fusedBlockMax );

		// pre-filter views that overlap the superBlock
		Arrays.setAll( fusedBlockMin, d -> superBlockOffset[ d ] );
		Arrays.setAll( fusedBlockMax, d -> superBlockOffset[ d ] + superBlockSize[ d ] - 1 );

		//final List< ViewId > overlappingViews = OverlappingViews.findOverlappingViews( data, viewIds, fusedBlock );

		final Img<UnsignedByteType> img = ArrayImgs.unsignedBytes( fusedBlock.dimensionsAsLongArray() );
		final RandomAccessibleInterval<UnsignedByteType> block = Views.translate( img, fusedBlockMin );

		for ( final ViewId viewId : overlappingViews )
		{
			final Cursor<UnsignedByteType> c = block.localizingCursor();
			final double[] l = new double[ 3 ];

			final Interval dim = new FinalInterval( ViewUtil.getDimensions( data, viewId ) );
			final AffineTransform3D model = registrations.get( viewId );

			final double[] min = new double[ 3 ];
			final double[] max = new double[ 3 ];

			Arrays.setAll( min, d -> dim.min( d ) - maskOffset[ d ] );
			Arrays.setAll( max, d -> dim.max( d ) + maskOffset[ d ] );

A:			while ( c.hasNext() )
			{
				final UnsignedByteType t = c.next();

				if ( t.get() > 0 )
					continue;

				c.localize(l);
				model.applyInverse(l, l);

				for ( int d = 0; d < 3; ++d )
					if ( l[ d ] < min[ d ] || l[ d ] > max[ d ] )
						continue A;

				t.set( 255 );
			}
		}

		final RandomAccessibleInterval<UnsignedByteType> fullImg =
				Views.interval( Views.extendZero( block ), minBB, maxBB );

		if ( uint8 )
		{
			return fullImg;
			//N5Utils.saveBlock(img, executorVolumeWriter, n5Dataset, gridBlock[2]);
		}
		else if ( uint16 )
		{
			return Converters.convertRAI(
					fullImg,
					(i, o) -> o.setInteger( i.get() > 0 ? 65535 : 0 ),
					new UnsignedShortType());

			//N5Utils.saveBlock(sourceUINT16, executorVolumeWriter, n5Dataset, gridBlock[2]);
		}
		else
		{
			return Converters.convertRAI(
					img,
					(i, o) -> o.set( i.get() > 0 ? 1.0f : 0.0f ),
					new FloatType());

			//N5Utils.saveBlock(sourceFloat, executorVolumeWriter, n5Dataset, gridBlock[2]);
		}
	}
}
