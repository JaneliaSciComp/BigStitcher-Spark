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
package net.preibisch.bigstitcher.spark.blk;

import java.io.IOException;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Writer;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.blocks.PrimitiveBlocks;
import net.imglib2.type.NativeType;

public class N5Helper
{
	public static < T extends NativeType< T > > void saveBlock(
			final RandomAccessibleInterval< T > source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset ) throws IOException
	{
		final DatasetAttributes attributes = n5.getDatasetAttributes( dataset );
		if ( attributes != null )
		{
			final int[] size = new int[ source.numDimensions() ];
			Arrays.setAll( size, d -> ( int ) source.dimension( d ) );
			final DataBlock< ? > dataBlock = attributes.getDataType().createDataBlock( size, gridOffset );
			PrimitiveBlocks.of( source ).copy( source.minAsLongArray(), dataBlock.getData(), size );
			n5.writeBlock( dataset, attributes, dataBlock );
		}
		else
		{
			throw new IOException( "Dataset " + dataset + " does not exist." );
		}
	}

}
