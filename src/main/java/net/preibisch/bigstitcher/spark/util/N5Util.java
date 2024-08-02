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

import java.io.IOException;

import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.hdf5.N5HDF5Writer;
import org.janelia.saalfeldlab.n5.universe.N5Factory;
import org.janelia.saalfeldlab.n5.universe.N5Factory.StorageFormat;
import org.janelia.saalfeldlab.n5.zarr.N5ZarrWriter;

import net.preibisch.mvrecon.process.export.ExportN5API.StorageType;

public class N5Util
{
	// only supported for local spark HDF5 writes, needs to share a writer instance
	public static N5HDF5Writer hdf5DriverVolumeWriter = null;

	public static N5Writer createWriter(
			final String path,
			final StorageType storageType ) throws IOException // can be null if N5 or ZARR is written 
	{
		if ( StorageType.N5.equals(storageType) )
		{
			if ( path.contains( ":/" ) )
				return new N5Factory().openWriter(StorageFormat.N5, path );
			else
				return new N5FSWriter(path);
		}
		else if ( StorageType.ZARR.equals(storageType) )
		{
			if ( path.contains( ":/" ) )
				return new N5Factory().openWriter(StorageFormat.ZARR, path );
			else
				return new N5ZarrWriter(path);
		}
		else if ( StorageType.HDF5.equals(storageType) )
		{
			if ( path.contains( ":/" ) )
				throw new RuntimeException( "storageType " + storageType + " not supported for CLOUD STORAGE." );
			else
				return hdf5DriverVolumeWriter == null ? hdf5DriverVolumeWriter = new N5HDF5Writer( path ) : hdf5DriverVolumeWriter;
		}
		else
			throw new RuntimeException( "storageType " + storageType + " not supported." );
	}
}
