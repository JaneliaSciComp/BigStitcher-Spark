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
package net.preibisch.bigstitcher.spark;

import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;

import mpicbg.spim.data.SpimDataException;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractInfrastructure;
import net.preibisch.bigstitcher.spark.util.DataTypeUtil;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.mvrecon.process.downsampling.lazy.LazyHalfPixelDownsample2x;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.Grid;
import util.URITools;

public class SparkDownsample extends AbstractInfrastructure implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = 5040141824053748124L;

	@Option(names = { "-i", "--n5PathIn" }, required = true, description = "N5 path for saving, e.g. /home/fused.n5")
	private String n5PathIn = null;

	@Option(names = { "-di", "--n5DatasetIn" }, required = true, description = "input N5 dataset, e.g. /ch488/s0")
	private String n5DatasetIn = null;

	@Option(names = { "-do", "--n5DatasetsOut" }, split = ";", description = "output N5 dataset(s), e.g. /ch488/s1;/ch488/s2;/ch488/s3;/ch488/s4")
	protected List<String> n5DatasetsOut = null;

	@Option(names = "--blockScale", description = "how many blocks to use for a single processing step, e.g. 4,4,1 means for blockSize a 128,128,32 that each spark thread writes 512,512,32 (default: 1,1,1)")
	private String blockScaleString = "1,1,1";

	@Option(names = {"-s", "--storage"}, defaultValue = "N5", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Dataset storage type, currently supported N5, ZARR (and ONLY for local, multithreaded Spark: HDF5)")
	private StorageFormat storageType = null;

	@Option(names = { "-ds", "--downsampling" }, split = ";", required = true, description = "consecutive downsample steps (e.g. 2,2,1; 2,2,1; 2,2,2; 2,2,2)")
	private List<String> downsampling = null;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		if (dryRun)
		{
			System.out.println( "dry-run not supported for downsampling.");
			System.exit( 0 );
		}

		if ( n5DatasetsOut == null || downsampling == null || n5DatasetsOut.size() != downsampling.size() )
		{
			System.out.println( "Please specify as many n5DatasetOut as you specify downsampling steps.");
			System.exit( 0 );
		}

		final SparkConf conf = new SparkConf().setAppName("Downsample");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("OFF");

		final int[][] downsampling = Import.csvStringListToDownsampling( this.downsampling );

		final URI n5Path = URITools.toURI( this.n5PathIn );
		final N5Writer n5 = N5Util.createN5Writer( n5Path, storageType );

		final Compression compression = n5.getAttribute( n5DatasetIn, "compression", Compression.class );
		final int[] blockSize = n5.getAttribute( n5DatasetIn, "blockSize", int[].class );
		final int[] blockScale = Import.csvStringToIntArray( blockScaleString );
		final DataType dataType = n5.getAttribute( n5DatasetIn, "dataType", DataType.class );
		final String dataTypeString = dataType.toString();

		System.out.println( "blockSize: " + Util.printCoordinates( blockSize ) );
		System.out.println( "dataType: " + dataTypeString );

		for ( int i = 0; i < n5DatasetsOut.size(); ++i )
			System.out.println( "'" + n5DatasetsOut.get( i ) + "' : " + Util.printCoordinates( downsampling[ i ] ) );

		final long time = System.currentTimeMillis();

		for ( int level = 0; level < n5DatasetsOut.size(); ++level )
		{
			final int[] ds = downsampling[ level ].clone();

			System.out.println( "Peforming downsampling: " + Util.printCoordinates( ds ) );

			final String n5DatasetIn = (level == 0) ? this.n5DatasetIn : n5DatasetsOut.get( level - 1 );
			final String n5DatasetOut = n5DatasetsOut.get( level );

			final long[] inputDim = n5.getAttribute( n5DatasetIn, "dimensions", long[].class );
			final long[] dim = new long[ inputDim.length ];
			for ( int d = 0; d < dim.length; ++d )
				dim[ d ] = inputDim[ d ] / ds[ d ];

			n5.createDataset(
					n5DatasetOut,
					dim, // dimensions
					blockSize,
					dataType,
					compression );

			final List<long[][]> grid = Grid.create(
					dim,
					new int[] {
							blockSize[0] * blockScale[ 0 ],
							blockSize[1] * blockScale[ 1 ],
							blockSize[2] * blockScale[ 2 ]
					},
					blockSize);

			System.out.println( "Input dimensions: " + Util.printCoordinates( inputDim ));
			System.out.println( "Output dimensions: " + Util.printCoordinates( dim ));
			System.out.println( "Tasks: " + grid.size() );

			final JavaRDD<long[][]> rdd = sc.parallelize(grid);


			final long timeLevel = System.currentTimeMillis();

			rdd.foreach(
					gridBlock -> {
						final N5Writer n5Lcl = N5Util.createN5Writer( n5Path, storageType );
						final DataType dataTypeLcl = DataType.fromString(dataTypeString);

						RandomAccessibleInterval downsampled = N5Utils.open( n5Lcl, n5DatasetIn );

						for ( int d = 0; d < downsampled.numDimensions(); ++d )
							if ( ds[ d ] > 1 )
								downsampled = LazyHalfPixelDownsample2x.init(
									downsampled,
									new FinalInterval( downsampled ),
									(RealType & NativeType)DataTypeUtil.toType( dataTypeLcl ),
									blockSize,
									d);

						final RandomAccessibleInterval sourceGridBlock = Views.offsetInterval(downsampled, gridBlock[0], gridBlock[1]);
						N5Utils.saveNonEmptyBlock(sourceGridBlock, n5Lcl, n5DatasetOut, gridBlock[2], (RealType & NativeType)DataTypeUtil.toType( dataTypeLcl ));
					});

			Thread.sleep( 100 );
			System.out.println( "downsampled level=" + level +", took: " + (System.currentTimeMillis() - timeLevel ) + " ms." );
		}

		sc.close();

		n5.close();

		Thread.sleep( 100 );
		System.out.println( "finished downsampling, in total took: " + (System.currentTimeMillis() - time ) + " ms." );

		return null;
	}

	public static void main(final String... args) throws SpimDataException {

		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new SparkDownsample()).execute(args));
	}

}
