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
package net.preibisch.bigstitcher.spark.lens;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import mpicbg.ij.TransformMeshMapping;
import mpicbg.models.CoordinateTransformMesh;
import mpicbg.trakem2.transform.AffineModel2D;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.real.FloatType;

/**
 * Exercises the full plane-warp plumbing of {@link LensApply#correctedFullView}
 * (imglib2 &lt;-&gt; ImageProcessor conversion, {@code Lazy} cell fill, coordinate
 * order) using an <em>identity</em> affine transform, which must reproduce the
 * source on the interior.
 */
public class TestLensApply
{
	@Test
	public void testIdentityWarpReproducesSource()
	{
		final int w = 32, h = 24, depth = 3;

		// synthetic gradient volume: value = x + 10*y + 100*z (distinct per voxel)
		final ArrayImg< FloatType, ? > img = ArrayImgs.floats( w, h, depth );
		final RandomAccess< FloatType > ra = img.randomAccess();
		for ( int z = 0; z < depth; ++z )
			for ( int y = 0; y < h; ++y )
				for ( int x = 0; x < w; ++x )
				{
					ra.setPosition( new int[] { x, y, z } );
					ra.get().set( x + 10f * y + 100f * z );
				}

		// identity affine -> identity mesh mapping
		final AffineModel2D identity = new AffineModel2D(); // defaults to identity
		final TransformMeshMapping< CoordinateTransformMesh > mapping =
				LensApply.createMapping( identity, 16, w, h );

		final RandomAccessibleInterval< FloatType > corrected =
				LensApply.correctedFullView( img, mapping, w, h, depth );

		// interior pixels must match the source (bilinear at integer coords = exact)
		final RandomAccess< FloatType > cra = corrected.randomAccess();
		for ( int z = 0; z < depth; ++z )
			for ( int y = 1; y < h - 1; ++y )
				for ( int x = 1; x < w - 1; ++x )
				{
					cra.setPosition( new int[] { x, y, z } );
					final float expected = x + 10f * y + 100f * z;
					assertEquals( expected, cra.get().get(), 1e-3f,
							"mismatch at (" + x + "," + y + "," + z + ")" );
				}
	}
}
