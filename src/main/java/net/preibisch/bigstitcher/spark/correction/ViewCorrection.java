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

import java.io.Serializable;

import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.RealType;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;

/**
 * A view-level correction: given the full source image of a view (X,Y,Z), return
 * a corrected full-view image with the same bounds. Corrections are lazy where
 * possible and are chained via {@link ViewCorrections}, so the output of
 * one correction is the input of the next.
 * <p>
 * The returned image is a {@link RealType} view (e.g. the raw type, or
 * {@code FloatType} for a geometric warp); downstream consumers read
 * {@code getRealDouble()} and do not depend on the concrete type. This lets, for
 * example, a lens/aberration warp ({@code lens.LensCorrection}) be composed ahead
 * of the pointwise flatfield correction ({@code flatfield.FlatfieldCorrection}).
 * <p>
 * Implementations must be {@link Serializable} (they are captured into Spark
 * closures) and carry only lightweight configuration; any heavy per-JVM state
 * (parsed models, meshes) should be cached statically, as
 * {@code lens.LensApply} does.
 */
public interface ViewCorrection extends Serializable
{
	/**
	 * Correct the full source view of {@code viewId}.
	 *
	 * @param data     the (per-task) SpimData2 the view belongs to
	 * @param viewId   the view being corrected
	 * @param source   the full source image (X,Y,Z)
	 * @return a corrected full-view image with the same bounds; may be
	 *         {@code source} itself when this correction does not apply
	 */
	< T extends RealType< T > > RandomAccessibleInterval< ? extends RealType< ? > > apply(
			SpimData2 data,
			ViewId viewId,
			RandomAccessibleInterval< T > source );
}
