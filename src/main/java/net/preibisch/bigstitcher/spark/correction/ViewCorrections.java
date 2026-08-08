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

import java.util.List;

import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Cast;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;

public class ViewCorrections
{


	/**
	 * Apply all {@link ViewCorrection} from an ordered list.
	 * @param corrections
	 * @param data
	 * @param viewId
	 * @param source
	 *
	 * @return a {@link RandomAccessibleInterval} after all corrections were applied to the source
	 */
	public static < T extends RealType< T > > RandomAccessibleInterval< ? extends RealType< ? > > applyCorrections(
			final List< ViewCorrection > corrections,
			final SpimData2 data,
			final ViewId viewId,
			final RandomAccessibleInterval< T > source )
	{
		RandomAccessibleInterval< ? extends RealType< ? > > current = source;
		for ( final ViewCorrection c : corrections ) {
			if ( c == null ) {
				continue;
			}
			// each stage consumes the previous stage's (wildcard) output; correctness
			// only depends on RealType, so the concrete type is erased via Cast
			current = c.apply(data, viewId, Cast.unchecked(current));
		}
		return current;
	}

}
