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
package net.preibisch.bigstitcher.spark.correction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;

/**
 * Unit tests for the {@link ViewCorrection} composition mechanism
 * ({@link ViewCorrections#applyCorrections}). Uses a trivial scaling correction
 * that ignores {@code data}/{@code viewId}, so no SpimData2 is required.
 */
public class TestViewCorrections
{
	/** Multiplies every pixel by {@code factor}, producing a FloatType copy. */
	private static class ScaleCorrection implements ViewCorrection
	{
		private static final long serialVersionUID = 1L;
		private final float factor;

		ScaleCorrection( final float factor ) { this.factor = factor; }

		@Override
		public < T extends RealType< T > > RandomAccessibleInterval< ? extends RealType< ? > > apply(
				final SpimData2 data, final ViewId viewId, final RandomAccessibleInterval< T > source )
		{
			final int w = ( int ) source.dimension( 0 );
			final int h = ( int ) source.dimension( 1 );
			final int d = ( int ) source.dimension( 2 );
			final ArrayImg< FloatType, ? > out = ArrayImgs.floats( w, h, d );
			final RandomAccess< T > sra = source.randomAccess();
			final RandomAccess< FloatType > ora = out.randomAccess();
			for ( int z = 0; z < d; ++z )
				for ( int y = 0; y < h; ++y )
					for ( int x = 0; x < w; ++x )
					{
						sra.setPosition( new int[] { x + ( int ) source.min( 0 ), y + ( int ) source.min( 1 ), z + ( int ) source.min( 2 ) } );
						ora.setPosition( new int[] { x, y, z } );
						ora.get().set( ( float ) ( sra.get().getRealDouble() * factor ) );
					}
			return out;
		}
	}

	private static ArrayImg< FloatType, ? > constImg( final int w, final int h, final int d, final float value )
	{
		final ArrayImg< FloatType, ? > img = ArrayImgs.floats( w, h, d );
		final RandomAccess< FloatType > ra = img.randomAccess();
		for ( int z = 0; z < d; ++z )
			for ( int y = 0; y < h; ++y )
				for ( int x = 0; x < w; ++x )
				{
					ra.setPosition( new int[] { x, y, z } );
					ra.get().set( value );
				}
		return img;
	}

	private static double valueAt( final RandomAccessibleInterval< ? extends RealType< ? > > img, final int... pos )
	{
		final RandomAccess< ? extends RealType< ? > > ra = img.randomAccess();
		ra.setPosition( pos );
		return ra.get().getRealDouble();
	}

	@Test
	public void testEmptyOrAllNullReturnsSourceUnchanged()
	{
		final ArrayImg< FloatType, ? > img = constImg( 2, 2, 1, 1f );

		// empty list -> the source instance is returned untouched
		assertSame( img, ViewCorrections.applyCorrections( Collections.emptyList(), null, null, img ) );

		// only null entries -> skipped, source returned untouched
		assertSame( img, ViewCorrections.applyCorrections(
				Arrays.asList( (ViewCorrection) null, null ), null, null, img ) );
	}

	@Test
	public void testNullEntriesSkippedButOthersApplied()
	{
		final ArrayImg< FloatType, ? > img = constImg( 4, 3, 2, 1f );

		// nulls around a single scale-by-2 correction
		final RandomAccessibleInterval< ? extends RealType< ? > > out =
				ViewCorrections.applyCorrections(
						Arrays.asList( null, new ScaleCorrection( 2f ), null ), null, null, img );

		assertEquals( 2.0, valueAt( out, 1, 1, 1 ), 1e-6 );
	}

	@Test
	public void testChainingAppliesInOrder()
	{
		final ArrayImg< FloatType, ? > img = constImg( 4, 3, 2, 1f );

		// x2 then x3 -> x6
		final RandomAccessibleInterval< ? extends RealType< ? > > out =
				ViewCorrections.applyCorrections(
						Arrays.asList( new ScaleCorrection( 2f ), new ScaleCorrection( 3f ) ), null, null, img );

		assertEquals( 6.0, valueAt( out, 1, 1, 1 ), 1e-6 );
	}
}
