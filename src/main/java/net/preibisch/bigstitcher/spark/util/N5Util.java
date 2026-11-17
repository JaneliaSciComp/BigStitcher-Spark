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

import java.io.File;
import java.net.URI;

import org.janelia.saalfeldlab.n5.Bzip2Compression;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.Lz4Compression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.XzCompression;
import org.janelia.saalfeldlab.n5.blosc.BloscCompression;
import org.janelia.saalfeldlab.n5.hdf5.N5HDF5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import org.janelia.scicomp.n5.zstandard.ZstandardCompression;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.preibisch.bigstitcher.spark.CreateFusionContainer.Compressions;
import net.preibisch.legacy.io.IOFunctions;
import util.URITools;

/**
 * Static helpers for writing fused volumes with N5: creating an {@link N5Writer} for a
 * {@link StorageFormat} (with one shared {@link N5HDF5Writer} per JVM for HDF5 output), mapping the
 * command-line {@link Compressions} choice to an N5 {@link Compression}, and saving blocks while
 * skipping empty ones where the backend allows it.
 */
public class N5Util
{
	/**
	 * The single {@link N5HDF5Writer} shared by the driver and all Spark tasks of this JVM when writing
	 * HDF5 (an HDF5 file can only be written through one writer instance). Created lazily by
	 * {@link #createN5Writer(URI, StorageFormat)}, {@code null} until then. Callers compare their writer
	 * against this field and only close it if it is not the shared one.
	 */
	public static N5HDF5Writer sharedHDF5Writer = null;

	/**
	 * Like {@link N5Utils#saveNonEmptyBlock(RandomAccessibleInterval, N5Writer, String, long[], NativeType)},
	 * i.e. blocks that only contain the default value are not written (deleted) - except for HDF5.
	 *
	 * HDF5 cannot delete chunks; n5-hdf5 (3.0.0) emulates deleteBlock() by writing a zero block of the
	 * full block size. At the dataset boundary that block extends beyond the extent and HDF5 fails with
	 * "selection + offset not within extent". For HDF5 we therefore always write every (cropped) block.
	 *
	 * (Same as N5ApiTools.saveNonEmptyBlock() in multiview-reconstruction &gt;= 9.0.12)
	 *
	 * @param <T> pixel type of the block
	 * @param source the block data to write (its size may be cropped at the dataset boundary)
	 * @param n5 the writer
	 * @param dataset path of the dataset inside the container
	 * @param gridOffset position of the block in the dataset's block grid (in units of blocks)
	 * @param defaultValue the value regarded as "empty"; a block containing only this value is deleted
	 *        instead of written (ignored for HDF5, where the block is always written)
	 */
	public static < T extends NativeType< T > > void saveNonEmptyBlock(
			final RandomAccessibleInterval< T > source,
			final N5Writer n5,
			final String dataset,
			final long[] gridOffset,
			final T defaultValue )
	{
		if ( n5 instanceof N5HDF5Writer )
			N5Utils.saveBlock( source, n5, dataset, gridOffset );
		else
			N5Utils.saveNonEmptyBlock( source, n5, dataset, gridOffset, defaultValue );
	}

	/**
	 * Creates or opens an {@link N5Writer} for a container. For {@code HDF5} the parent directory is
	 * created if necessary and a single {@link #sharedHDF5Writer} is created on the first call and
	 * returned on all subsequent ones; {@code N5}, {@code ZARR} and {@code ZARR2} are opened through
	 * {@code URITools.instantiateN5Writer}. Any failure is logged and results in {@code null}.
	 *
	 * @param n5PathURI location of the container (local path or cloud URI)
	 * @param storageType the container format
	 * @return the writer, or {@code null} if the container could not be created/opened or
	 *         {@code storageType} is not supported
	 */
	public static N5Writer createN5Writer(
			final URI n5PathURI,
			final StorageFormat storageType )
	{
		final N5Writer driverVolumeWriter;

		try
		{
			if ( storageType == StorageFormat.HDF5 )
			{
				if ( sharedHDF5Writer != null )
					return sharedHDF5Writer;

				final File dir = new File( URITools.fromURI( n5PathURI ) ).getParentFile();
				if ( !dir.exists() )
					dir.mkdirs();

				driverVolumeWriter = sharedHDF5Writer = new N5HDF5Writer( URITools.fromURI( n5PathURI ) );
			}
			else if ( storageType == StorageFormat.N5 || storageType == StorageFormat.ZARR || storageType == StorageFormat.ZARR2 )
			{
				driverVolumeWriter = URITools.instantiateN5Writer( storageType, n5PathURI );
			}
			else
				throw new RuntimeException( "storageType " + storageType + " not supported." );
		}
		catch ( Exception e )
		{
			IOFunctions.println( "Couldn't create/open " + storageType + " container '" + n5PathURI + "': " + e );
			return null;
		}

		return driverVolumeWriter;
	}

	/**
	 * Maps a command-line {@link Compressions} choice to an N5 {@link Compression}. The level is only
	 * used by {@code Gzip} (default 1), {@code Zstandard} (default 3) and {@code Xz} (default 6).
	 *
	 * @param compressionType the requested compression
	 * @param compressionLevel the compression level, or {@code null} to use the codec's default given above
	 * @return the compression, or {@code null} if {@code compressionType} is {@code null} or unknown
	 */
	public static Compression getCompression( Compressions compressionType, Integer compressionLevel )
	{
		final Compression compression;

		//Lz4, Gzip, Zstandard, Blosc, Bzip2, Xz, Raw };
		if ( compressionType == Compressions.Lz4 )
			compression = new Lz4Compression();
		else if ( compressionType == Compressions.Gzip )
			compression = new GzipCompression( compressionLevel == null ? 1 : compressionLevel );
		else if ( compressionType == Compressions.Zstandard )
			compression = new ZstandardCompression( compressionLevel == null ? 3 : compressionLevel );
		else if ( compressionType == Compressions.Blosc )
			compression = new BloscCompression();
		else if ( compressionType == Compressions.Bzip2 )
			compression = new Bzip2Compression();
		else if ( compressionType == Compressions.Xz )
			compression = new XzCompression( compressionLevel == null ? 6 : compressionLevel );
		else if ( compressionType == Compressions.Raw )
			compression = new RawCompression();
		else
			compression = null;

		return compression;
	}
	/*
	// only supported for local spark HDF5 writes, needs to share a writer instance
	public static N5HDF5Writer hdf5DriverVolumeWriter = null;

	public static N5Writer createWriter(
			final String path,
			final StorageFormat storageType ) throws IOException // can be null if N5 or ZARR is written 
	{
		if ( StorageFormat.N5.equals(storageType) )
			return new N5FSWriter(path);
		else if ( StorageFormat.ZARR.equals(storageType) )
			return new N5ZarrWriter(path);
		else if ( StorageFormat.HDF5.equals(storageType) )
			return hdf5DriverVolumeWriter == null ? hdf5DriverVolumeWriter = new N5HDF5Writer( path ) : hdf5DriverVolumeWriter;
		else
			throw new RuntimeException( "storageType " + storageType + " not supported." );
	}
	*/
}
