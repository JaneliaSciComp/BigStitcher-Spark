package net.preibisch.bigstitcher.spark.util;

import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.type.numeric.integer.ShortType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.legacy.io.IOFunctions;
import net.preibisch.mvrecon.process.downsampling.lazy.LazyHalfPixelDownsample2x;
import net.preibisch.mvrecon.process.export.ExportN5API.StorageType;
import net.preibisch.mvrecon.process.export.ExportTools;
import util.Grid;

public class Downsampling
{
	// TODO: this code is almost identical to the code in ExportN5API in multiview-reconstruction (except it's for multi-threading there)
	public static boolean createDownsampling(
			final String path,
			final String datasetS0,
			final N5Writer driverVolumeWriter,
			final long[] dimensionsS0,
			final StorageType storageType,
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
							ExportTools.createDownsampledBDVPath( datasetS0, level, storageType)
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
						final N5Writer executorVolumeWriter = N5Util.createWriter( path, storageType );

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
								IOFunctions.println( "Unsupported pixel type: " + datatype );
								throw new RuntimeException("Unsupported pixel type: " + datatype );
							}
						}
						catch (Exception exc) 
						{
							IOFunctions.println( "Error writing block offset=" + Util.printCoordinates( gridBlock[0] ) + "' ... " + exc );
							exc.printStackTrace();
						}

						// not HDF5
						if ( N5Util.hdf5DriverVolumeWriter != executorVolumeWriter )
							executorVolumeWriter.close();
					});

			System.out.println( "Saved level s " + level + ", took: " + (System.currentTimeMillis() - time ) + " ms." );

			// for next downsampling level
			previousDim = dim.clone();
			previousDataset = datasetDownsampling;
		}

		return true;
	}

	public static boolean testDownsamplingParameters( final boolean multiRes, final List<String> downsampling, final String dataset )
	{
		// no not create multi-res pyramid
		if ( !multiRes && downsampling == null )
			return true;

		if ( multiRes && downsampling != null )
		{
			System.out.println( "If you want to create a multi-resolution pyramid, you must select either automatic (--multiRes) - OR - manual mode (e.g. --downsampling 2,2,1; 2,2,1; 2,2,2)");
			return false;
		}

		// non-bdv multi-res dataset
		if ( dataset != null )
		{
			if ( !dataset.endsWith("/s0") )
			{
				System.out.println( "In order to create a multi-resolution pyramid for a non-BDV dataset, the dataset must end with '/s0', right not it is '" + dataset + "'.");
				return false;
			}
		}

		return true;
	}

}
