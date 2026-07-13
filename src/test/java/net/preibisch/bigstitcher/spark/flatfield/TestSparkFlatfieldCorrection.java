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

import java.util.Random;

import org.janelia.saalfeldlab.n5.DataType;
import org.junit.jupiter.api.Test;

import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.preibisch.bigstitcher.spark.flatfield.FlatfieldApply.Field2D;

/**
 * Unit tests for the flatfield/darkfield block correction op
 * ({@link FlatfieldApply#applyCorrection}).
 */
public class TestSparkFlatfieldCorrection
{
	/**
	 * Correct a synthetic uint16 view with a known 2D flat/dark and assert
	 * corrected == max(round((raw-dark)/flat), 0), and that the 2D field is
	 * broadcast identically across z.
	 */
	@Test
	public void testBlockCorrectionUint16()
	{
		final int w = 8, h = 6, d = 5;
		final Random rnd = new Random( 17 );

		// synthetic 2D flat / dark
		final float[] flat = new float[ w * h ];
		final float[] dark = new float[ w * h ];
		for ( int p = 0; p < w * h; ++p )
		{
			flat[ p ] = 0.5f + rnd.nextFloat() * 1.5f; // in [0.5, 2.0], never near zero
			dark[ p ] = rnd.nextInt( 50 );             // [0, 49]
		}
		final Field2D field = new Field2D( flat, dark, w, h );

		// synthetic raw uint16 volume
		final short[] raw = new short[ w * h * d ];
		for ( int i = 0; i < raw.length; ++i )
			raw[ i ] = ( short ) ( 100 + rnd.nextInt( 5000 ) );
		final ArrayImg< UnsignedShortType, ? > src = ArrayImgs.unsignedShorts( raw, w, h, d );

		final ArrayImg< UnsignedShortType, ? > corrected =
				FlatfieldApply.applyCorrection( src, field, 0, 0, DataType.UINT16 );

		final var cRa = corrected.randomAccess();
		final var sRa = src.randomAccess();

		for ( int z = 0; z < d; ++z )
			for ( int y = 0; y < h; ++y )
				for ( int x = 0; x < w; ++x )
				{
					sRa.setPosition( new int[] { x, y, z } );
					cRa.setPosition( new int[] { x, y, z } );

					final double rawV = sRa.get().getRealDouble();
					final int fIdx = y * w + x;
					double expected = ( rawV - dark[ fIdx ] ) / flat[ fIdx ];
					expected = Math.rint( expected );
					if ( expected < 0.0 )
						expected = 0.0;
					if ( expected > 65535.0 )
						expected = 65535.0;

					assertEquals( ( int ) expected, cRa.get().get(),
							"corrected pixel (" + x + "," + y + "," + z + ")" );
				}
	}

	/** The 2D field must be broadcast identically across z: all z-slices equal. */
	@Test
	public void testFieldBroadcastAcrossZ()
	{
		final int w = 4, h = 4, d = 7;

		final float[] flat = new float[ w * h ];
		final float[] dark = new float[ w * h ];
		for ( int p = 0; p < w * h; ++p )
		{
			flat[ p ] = 1.0f + 0.1f * p;
			dark[ p ] = p;
		}
		final Field2D field = new Field2D( flat, dark, w, h );

		// constant raw value across z (same value at each z), so any per-z variation
		// in the output can only come from the field NOT being broadcast identically.
		final short[] raw = new short[ w * h * d ];
		for ( int z = 0; z < d; ++z )
			for ( int y = 0; y < h; ++y )
				for ( int x = 0; x < w; ++x )
					raw[ z * w * h + y * w + x ] = ( short ) ( 2000 + y * w + x );
		final ArrayImg< UnsignedShortType, ? > src = ArrayImgs.unsignedShorts( raw, w, h, d );

		final ArrayImg< UnsignedShortType, ? > corrected =
				FlatfieldApply.applyCorrection( src, field, 0, 0, DataType.UINT16 );

		final var ra = corrected.randomAccess();
		for ( int y = 0; y < h; ++y )
			for ( int x = 0; x < w; ++x )
			{
				ra.setPosition( new int[] { x, y, 0 } );
				final int v0 = ra.get().get();
				for ( int z = 1; z < d; ++z )
				{
					ra.setPosition( new int[] { x, y, z } );
					assertEquals( v0, ra.get().get(),
							"field must be broadcast identically across z at (" + x + "," + y + "), z=" + z );
				}
			}
	}

	/** flat <= eps => output 0 (divide-by-zero guard). */
	@Test
	public void testDivideByZeroGuard()
	{
		final int w = 3, h = 3, d = 2;
		final float[] flat = new float[ w * h ]; // all zeros
		final float[] dark = new float[ w * h ];
		final Field2D field = new Field2D( flat, dark, w, h );

		final short[] raw = new short[ w * h * d ];
		for ( int i = 0; i < raw.length; ++i )
			raw[ i ] = 1234;
		final ArrayImg< UnsignedShortType, ? > src = ArrayImgs.unsignedShorts( raw, w, h, d );

		final ArrayImg< UnsignedShortType, ? > corrected =
				FlatfieldApply.applyCorrection( src, field, 0, 0, DataType.UINT16 );

		final var cur = corrected.cursor();
		while ( cur.hasNext() )
			assertEquals( 0, cur.next().get(), "flat<=eps must produce 0" );
	}

	/** FLOAT32 output should not round/clamp; matches raw (raw-dark)/flat. */
	@Test
	public void testFloat32OutputNoRounding()
	{
		final int w = 4, h = 3, d = 2;
		final float[] flat = new float[ w * h ];
		final float[] dark = new float[ w * h ];
		for ( int p = 0; p < w * h; ++p )
		{
			flat[ p ] = 1.3f;
			dark[ p ] = 7f;
		}
		final Field2D field = new Field2D( flat, dark, w, h );

		final short[] raw = new short[ w * h * d ];
		for ( int i = 0; i < raw.length; ++i )
			raw[ i ] = ( short ) ( 100 + i );
		final ArrayImg< UnsignedShortType, ? > src = ArrayImgs.unsignedShorts( raw, w, h, d );

		final ArrayImg< FloatType, ? > corrected =
				FlatfieldApply.applyCorrection( src, field, 0, 0, DataType.FLOAT32 );

		final var cRa = corrected.randomAccess();
		final var sRa = src.randomAccess();
		for ( int z = 0; z < d; ++z )
			for ( int y = 0; y < h; ++y )
				for ( int x = 0; x < w; ++x )
				{
					sRa.setPosition( new int[] { x, y, z } );
					cRa.setPosition( new int[] { x, y, z } );
					final double rawV = sRa.get().getRealDouble();
					final int fIdx = y * w + x;
					final double expected = ( rawV - dark[ fIdx ] ) / flat[ fIdx ];
					assertEquals( expected, cRa.get().get(), 1e-3,
							"float32 corrected pixel (" + x + "," + y + "," + z + ")" );
				}
	}
}
