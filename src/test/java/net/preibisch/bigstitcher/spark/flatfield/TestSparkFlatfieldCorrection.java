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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.janelia.saalfeldlab.n5.DataType;
import org.junit.jupiter.api.Test;

import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.preibisch.bigstitcher.spark.flatfield.FlatfieldApply.BaselineGranularity;
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

	// ─── baseline (temporal drift) tests ─────────────────────────────────────────

	/** Percentile helper: median and a low percentile with linear interpolation. */
	@Test
	public void testPercentile()
	{
		// sorted: 1,2,3,4,5,6,7,8,9,10 -> median (50th) = 5.5 (linear interp between 5 and 6)
		final float[] a = { 5, 3, 8, 1, 9, 2, 7, 4, 10, 6 };
		assertEquals( 5.5, FlatfieldApply.percentile( a, 50.0 ), 1e-9, "median" );

		// 0th = min, 100th = max
		assertEquals( 1.0, FlatfieldApply.percentile( a, 0.0 ), 1e-9, "min" );
		assertEquals( 10.0, FlatfieldApply.percentile( a, 100.0 ), 1e-9, "max" );

		// 10th percentile: rank = 0.1*(10-1) = 0.9 -> interp between v[0]=1 and v[1]=2 => 1.9
		assertEquals( 1.9, FlatfieldApply.percentile( a, 10.0 ), 1e-9, "10th percentile" );

		// single element and empty
		assertEquals( 42.0, FlatfieldApply.percentile( new float[] { 42f }, 50.0 ), 1e-9 );
		assertEquals( 0.0, FlatfieldApply.percentile( new float[ 0 ], 50.0 ), 1e-9 );

		// input must not be mutated
		final float[] b = { 3, 1, 2 };
		FlatfieldApply.percentile( b, 50.0 );
		assertTrue( Arrays.equals( new float[] { 3, 1, 2 }, b ), "input array must not be mutated" );
	}

	/** groupRefMean = mean of ALL entries across all baseline arrays. */
	@Test
	public void testGroupRefMean()
	{
		final double[] v1 = { 10, 20, 30 }; // mean 20
		final double[] v2 = { 40, 60 };     // mean 50
		// overall mean of all 5 entries = (10+20+30+40+60)/5 = 32
		assertEquals( 32.0, FlatfieldApply.groupRefMean( Arrays.asList( v1, v2 ) ), 1e-9 );
		assertEquals( 0.0, FlatfieldApply.groupRefMean( List.of() ), 1e-9 );
	}

	/** delta math for all three modes. */
	@Test
	public void testBaselineDeltaModes()
	{
		final double[] bView = { 100, 110, 120 };
		final double refMean = 90;

		assertNull( FlatfieldApply.baselineDelta( "IGNORE", bView, refMean ), "IGNORE -> null (no-op)" );

		final double[] zero = FlatfieldApply.baselineDelta( "ZERO", bView, refMean );
		assertTrue( Arrays.equals( bView, zero ), "ZERO -> subtract own baseline fully" );

		final double[] mean = FlatfieldApply.baselineDelta( "MEAN", bView, refMean );
		assertEquals( 10.0, mean[ 0 ], 1e-9 );
		assertEquals( 20.0, mean[ 1 ], 1e-9 );
		assertEquals( 30.0, mean[ 2 ], 1e-9 );
	}

	/**
	 * End-to-end delta math on synthetic views: two views with different constant
	 * background offsets. ZERO drives each corrected background ~0; MEAN levels both
	 * to a common value (their corrected backgrounds become equal); IGNORE unchanged.
	 */
	@Test
	public void testBaselineLevelsViews()
	{
		final int w = 8, h = 8, d = 4;

		// identity fields (flat=1, dark=0) so shading-corrected == raw
		final float[] flat = new float[ w * h ];
		final float[] dark = new float[ w * h ];
		Arrays.fill( flat, 1.0f );
		final Field2D field = new Field2D( flat, dark, w, h );

		// view A: constant background 500; view B: constant background 800
		final double bgA = 500, bgB = 800;
		final ArrayImg< UnsignedShortType, ? > viewA = constantView( w, h, d, bgA );
		final ArrayImg< UnsignedShortType, ? > viewB = constantView( w, h, d, bgB );

		final double[] baseA = FlatfieldApply.computeViewBaseline( viewA, field, BaselineGranularity.VIEW, 50.0 );
		final double[] baseB = FlatfieldApply.computeViewBaseline( viewB, field, BaselineGranularity.VIEW, 50.0 );
		assertEquals( bgA, baseA[ 0 ], 1e-6, "baseline A" );
		assertEquals( bgB, baseB[ 0 ], 1e-6, "baseline B" );

		final double refMean = FlatfieldApply.groupRefMean( Arrays.asList( baseA, baseB ) ); // (500+800)/2 = 650

		// IGNORE: output unchanged
		final ArrayImg< UnsignedShortType, ? > ignoreA =
				FlatfieldApply.applyCorrection( viewA, field, 0, 0, DataType.UINT16, null, 0 );
		assertEquals( bgA, meanValue( ignoreA ), 1e-6, "IGNORE unchanged" );

		// ZERO: corrected background ~0 for both
		final double[] zeroA = FlatfieldApply.baselineDelta( "ZERO", baseA, refMean );
		final double[] zeroB = FlatfieldApply.baselineDelta( "ZERO", baseB, refMean );
		final ArrayImg< UnsignedShortType, ? > zA =
				FlatfieldApply.applyCorrection( viewA, field, 0, 0, DataType.UINT16, zeroA, 0 );
		final ArrayImg< UnsignedShortType, ? > zB =
				FlatfieldApply.applyCorrection( viewB, field, 0, 0, DataType.UINT16, zeroB, 0 );
		assertEquals( 0.0, meanValue( zA ), 1e-6, "ZERO drives A background to 0" );
		assertEquals( 0.0, meanValue( zB ), 1e-6, "ZERO drives B background to 0" );

		// MEAN: both corrected backgrounds equal (== refMean = 650)
		final double[] meanA = FlatfieldApply.baselineDelta( "MEAN", baseA, refMean );
		final double[] meanB = FlatfieldApply.baselineDelta( "MEAN", baseB, refMean );
		final ArrayImg< UnsignedShortType, ? > mA =
				FlatfieldApply.applyCorrection( viewA, field, 0, 0, DataType.UINT16, meanA, 0 );
		final ArrayImg< UnsignedShortType, ? > mB =
				FlatfieldApply.applyCorrection( viewB, field, 0, 0, DataType.UINT16, meanB, 0 );
		assertEquals( refMean, meanValue( mA ), 1e-6, "MEAN levels A to group mean" );
		assertEquals( refMean, meanValue( mB ), 1e-6, "MEAN levels B to group mean" );
		assertEquals( meanValue( mA ), meanValue( mB ), 1e-6, "MEAN: A and B backgrounds equal" );
	}

	/**
	 * SLICE removes per-z drift; VIEW removes only the whole-view level. A view with
	 * a z-dependent constant background: after SLICE-ZERO every plane is ~0; after
	 * VIEW-ZERO planes retain their relative offsets (only the median level removed).
	 */
	@Test
	public void testSliceVsView()
	{
		final int w = 6, h = 6, d = 5;
		final float[] flat = new float[ w * h ];
		final float[] dark = new float[ w * h ];
		Arrays.fill( flat, 1.0f );
		final Field2D field = new Field2D( flat, dark, w, h );

		// per-z background: 100, 200, 300, 400, 500
		final short[] raw = new short[ w * h * d ];
		final double[] zbg = { 100, 200, 300, 400, 500 };
		for ( int z = 0; z < d; ++z )
			for ( int y = 0; y < h; ++y )
				for ( int x = 0; x < w; ++x )
					raw[ z * w * h + y * w + x ] = ( short ) zbg[ z ];
		final ArrayImg< UnsignedShortType, ? > view = ArrayImgs.unsignedShorts( raw, w, h, d );

		// SLICE baseline == per-z background
		final double[] slice = FlatfieldApply.computeViewBaseline( view, field, BaselineGranularity.SLICE, 50.0 );
		assertEquals( d, slice.length, "SLICE baseline length == depth" );
		for ( int z = 0; z < d; ++z )
			assertEquals( zbg[ z ], slice[ z ], 1e-6, "SLICE baseline z=" + z );

		// VIEW baseline == whole-view median == 300
		final double[] viewBase = FlatfieldApply.computeViewBaseline( view, field, BaselineGranularity.VIEW, 50.0 );
		assertEquals( 1, viewBase.length );
		assertEquals( 300.0, viewBase[ 0 ], 1e-6, "VIEW median" );

		// SLICE-ZERO -> every plane ~0
		final double[] sliceDelta = FlatfieldApply.baselineDelta( "ZERO", slice, 0 );
		final ArrayImg< UnsignedShortType, ? > sCorr =
				FlatfieldApply.applyCorrection( view, field, 0, 0, DataType.UINT16, sliceDelta, 0 );
		for ( int z = 0; z < d; ++z )
			assertEquals( 0.0, planeMean( sCorr, z ), 1e-6, "SLICE-ZERO plane z=" + z );

		// VIEW-ZERO -> subtract 300 everywhere; planes retain relative offsets, clamped at 0
		final double[] viewDelta = FlatfieldApply.baselineDelta( "ZERO", viewBase, 0 );
		final ArrayImg< UnsignedShortType, ? > vCorr =
				FlatfieldApply.applyCorrection( view, field, 0, 0, DataType.UINT16, viewDelta, 0 );
		final double[] expectedVZ = { 0, 0, 0, 100, 200 }; // 100-300,200-300 clamp to 0; 300,400,500-300
		for ( int z = 0; z < d; ++z )
			assertEquals( expectedVZ[ z ], planeMean( vCorr, z ), 1e-6, "VIEW-ZERO plane z=" + z );
	}

	// ─── helpers ─────────────────────────────────────────────────────────────────

	private static ArrayImg< UnsignedShortType, ? > constantView( final int w, final int h, final int d, final double value )
	{
		final short[] raw = new short[ w * h * d ];
		Arrays.fill( raw, ( short ) value );
		return ArrayImgs.unsignedShorts( raw, w, h, d );
	}

	private static double meanValue( final ArrayImg< UnsignedShortType, ? > img )
	{
		final var cur = img.cursor();
		double sum = 0;
		long n = 0;
		while ( cur.hasNext() )
		{
			sum += cur.next().getRealDouble();
			++n;
		}
		return sum / n;
	}

	private static double planeMean( final ArrayImg< UnsignedShortType, ? > img, final int z )
	{
		final var ra = img.randomAccess();
		final int w = ( int ) img.dimension( 0 );
		final int h = ( int ) img.dimension( 1 );
		double sum = 0;
		for ( int y = 0; y < h; ++y )
			for ( int x = 0; x < w; ++x )
			{
				ra.setPosition( new int[] { x, y, z } );
				sum += ra.get().getRealDouble();
			}
		return sum / ( w * h );
	}
}
