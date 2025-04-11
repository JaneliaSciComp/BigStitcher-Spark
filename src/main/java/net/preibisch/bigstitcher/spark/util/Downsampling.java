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

import java.net.URI;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;

import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.type.numeric.integer.ShortType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.mvrecon.process.downsampling.lazy.LazyHalfPixelDownsample2x;
import net.preibisch.mvrecon.process.n5api.N5ApiTools;
import util.Grid;

public class Downsampling
{
	// TODO: this code is almost identical to the code in ExportN5API in multiview-reconstruction (except it's for multi-threading there)
	public static boolean createDownsampling(
			final URI path,
			final String datasetS0,
			final N5Writer driverVolumeWriter,
			final long[] dimensionsS0,
			final StorageFormat storageType,
			final int[] blocksize,
			final DataType datatype,
			final Compression compression,
			final int[][] downsamplings,
			final boolean bdv,
			final JavaSparkContext sc )
	{
		long[] previousDim = dimensionsS0;
		String previousDataset = datasetS0;

		for ( int level = 1; level < downsamplings.length; ++level )
		{
			final int[] ds = new int[ downsamplings[ 0 ].length ];

			for ( int d = 0; d < ds.length; ++d )
				ds[ d ] = downsamplings[ level ][ d ] / downsamplings[ level - 1 ][ d ];

			System.out.println( "Downsampling: " + Util.printCoordinates( downsamplings[ level ] ) + " with relative downsampling of " + Util.printCoordinates( ds ));

			final long[] dim = new long[ previousDim.length ];
			for ( int d = 0; d < dim.length; ++d )
				dim[ d ] = previousDim[ d ] / ds[ d ];

			final String datasetDownsampling =
					bdv ?
							N5ApiTools.createDownsampledBDVPath( datasetS0, level, storageType)
							:
							datasetS0.substring( 0, datasetS0.length() - 3) + "/s" + level;

			try
			{
				driverVolumeWriter.createDataset(
						datasetDownsampling,
						dim, // dimensions
						blocksize,
						datatype,
						compression );

				driverVolumeWriter.setAttribute( datasetDownsampling, "downsamplingFactors", downsamplings[ level ] );
			}
			catch ( Exception e )
			{
				System.out.println( "Couldn't create downsampling level " + level + " for container '" + path + "', dataset '" + datasetDownsampling + "': " + e );
				return false;
			}

			final List<long[][]> gridDS = Grid.create(
					dim,
					new int[] {
							blocksize[0],
							blocksize[1],
							blocksize[2]
					},
					blocksize);

			System.out.println( new Date( System.currentTimeMillis() ) + ": s" + level + " num blocks=" + gridDS.size() );

			final String datasetPrev = previousDataset;

			System.out.println( new Date( System.currentTimeMillis() ) + ": Loading '" + datasetPrev + "', downsampled will be written as '" + datasetDownsampling + "'." );

			final JavaRDD<long[][]> rdd = sc.parallelize( gridDS );
			final long time = System.currentTimeMillis();

			rdd.foreach(
					gridBlock ->
					{
						final N5Writer executorVolumeWriter =
								N5Util.createN5Writer( path, storageType );

						try
						{
							if ( datatype == DataType.UINT16 )
							{
								RandomAccessibleInterval<UnsignedShortType> downsampled = N5Utils.open(executorVolumeWriter, datasetPrev);

								for ( int d = 0; d < downsampled.numDimensions(); ++d )
									if ( ds[ d ] > 1 )
										downsampled = LazyHalfPixelDownsample2x.init(
											downsampled,
											new FinalInterval( downsampled ),
											new UnsignedShortType(),
											blocksize,
											d);

								final RandomAccessibleInterval<UnsignedShortType> sourceGridBlock = Views.offsetInterval(downsampled, gridBlock[0], gridBlock[1]);
								N5Utils.saveNonEmptyBlock(sourceGridBlock, executorVolumeWriter, datasetDownsampling, gridBlock[2], new UnsignedShortType());
							}
							else if ( datatype == DataType.UINT8 )
							{
								RandomAccessibleInterval<UnsignedByteType> downsampled = N5Utils.open(executorVolumeWriter, datasetPrev);

								for ( int d = 0; d < downsampled.numDimensions(); ++d )
									if ( ds[ d ] > 1 )
										downsampled = LazyHalfPixelDownsample2x.init(
											downsampled,
											new FinalInterval( downsampled ),
											new UnsignedByteType(),
											blocksize,
											d);

								final RandomAccessibleInterval<UnsignedByteType> sourceGridBlock = Views.offsetInterval(downsampled, gridBlock[0], gridBlock[1]);
								N5Utils.saveNonEmptyBlock(sourceGridBlock, executorVolumeWriter, datasetDownsampling, gridBlock[2], new UnsignedByteType());
							}
							else if ( datatype == DataType.FLOAT32 )
							{
								RandomAccessibleInterval<FloatType> downsampled = N5Utils.open(executorVolumeWriter, datasetPrev);

								for ( int d = 0; d < downsampled.numDimensions(); ++d )
									if ( ds[ d ] > 1 )
										downsampled = LazyHalfPixelDownsample2x.init(
											downsampled,
											new FinalInterval( downsampled ),
											new FloatType(),
											blocksize,
											d);

								final RandomAccessibleInterval<FloatType> sourceGridBlock = Views.offsetInterval(downsampled, gridBlock[0], gridBlock[1]);
								N5Utils.saveNonEmptyBlock(sourceGridBlock, executorVolumeWriter, datasetDownsampling, gridBlock[2], new FloatType());
							}
							else if ( datatype == DataType.INT16 )
							{
								// Tobias: unfortunately I store as short and treat it as unsigned short in Java.
								// The reason is, that when I wrote this, the jhdf5 library did not support unsigned short. It's terrible and should be fixed.
								// https://github.com/bigdataviewer/bigdataviewer-core/issues/154
								// https://imagesc.zulipchat.com/#narrow/stream/327326-BigDataViewer/topic/XML.2FHDF5.20specification
								RandomAccessibleInterval<UnsignedShortType> downsampled =
										Converters.convertRAI(
												(RandomAccessibleInterval<ShortType>)(Object)N5Utils.open(executorVolumeWriter, datasetPrev),
												(i,o)->o.set( i.getShort() ),
												new UnsignedShortType());

								for ( int d = 0; d < downsampled.numDimensions(); ++d )
									if ( ds[ d ] > 1 )
										downsampled = LazyHalfPixelDownsample2x.init(
											downsampled,
											new FinalInterval( downsampled ),
											new UnsignedShortType(),
											blocksize,
											d);

								final RandomAccessibleInterval<ShortType> sourceGridBlock =
										Converters.convertRAI( Views.offsetInterval(downsampled, gridBlock[0], gridBlock[1]), (i,o)->o.set( i.getShort() ), new ShortType() );
								N5Utils.saveNonEmptyBlock(sourceGridBlock, executorVolumeWriter, datasetDownsampling, gridBlock[2], new ShortType());
							}
							else
							{
								System.out.println( "Unsupported pixel type: " + datatype );
								throw new RuntimeException("Unsupported pixel type: " + datatype );
							}
						}
						catch (Exception exc) 
						{
							System.out.println( "Error writing block offset=" + Util.printCoordinates( gridBlock[0] ) + "' ... " + exc );
							exc.printStackTrace();
						}

						// if it is not the shared HDF5 writer, then close
						if ( N5Util.sharedHDF5Writer != executorVolumeWriter )
							executorVolumeWriter.close();
					});

			System.out.println( "Saved level s " + level + ", took: " + (System.currentTimeMillis() - time ) + " ms." );

			// for next downsampling level
			previousDim = dim.clone();
			previousDataset = datasetDownsampling;
		}

		return true;
	}

	public static boolean testDownsamplingParameters( final boolean multiRes, final List<String> downsampling )
	{
		// no not create multi-res pyramid
		if ( !multiRes && downsampling == null )
			return true;

		if ( multiRes && downsampling != null )
		{
			System.out.println( "If you want to create a multi-resolution pyramid, you must select either automatic (--multiRes) - OR - manual mode (e.g. --downsampling 2,2,1; 2,2,1; 2,2,2)");
			return false;
		}

		return true;
	}

}
