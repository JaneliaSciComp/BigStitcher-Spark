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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;

import mpicbg.spim.data.SpimDataException;
import net.imglib2.util.Util;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractInfrastructure;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.mvrecon.process.n5api.N5ApiTools;
import net.preibisch.mvrecon.process.n5api.N5ApiTools.MultiResolutionLevelInfo;
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

		// per-step (relative) downsampling factors; unlike CreateFusionContainer we do NOT
		// require the first entry to be 1,1,1.
		final int[][] relativeDs = new int[ this.downsampling.size() ][];
		for ( int i = 0; i < this.downsampling.size(); ++i )
			relativeDs[ i ] = Import.csvStringToIntArray( this.downsampling.get( i ) );

		final URI n5Path = URITools.toURI( this.n5PathIn );
		final N5Writer n5 = N5Util.createN5Writer( n5Path, storageType );

		// Cross-format dataset attribute lookup (works for N5 and Zarr v2/v3),
		// matching the pattern in mvr's AllenOMEZarrProperties / N5ApiTools.
		final DatasetAttributes srcAttrs = n5.getDatasetAttributes( n5DatasetIn );
		if ( srcAttrs == null )
			throw new RuntimeException( "Could not find dataset attributes for '" + n5DatasetIn + "' in '" + n5PathIn + "'." );

		final long[] srcDim = srcAttrs.getDimensions();
		final int[] srcBlockSize = srcAttrs.getBlockSize();
		final DataType dataType = srcAttrs.getDataType();
		final Compression compression = srcAttrs.getCompression();
		final int n = srcDim.length;
		final boolean is5DOmeZarr = ( n == 5 )
				&& ( storageType == StorageFormat.ZARR || storageType == StorageFormat.ZARR2 );

		final int[] blockScale = Import.csvStringToIntArray( blockScaleString );

		System.out.println( "input '" + n5DatasetIn + "' dims=" + Util.printCoordinates( srcDim )
				+ ", blockSize=" + Util.printCoordinates( srcBlockSize ) + ", dataType=" + dataType );

		// Build MultiResolutionLevelInfo per level (index 0 = input, 1..N = outputs) and create
		// the output datasets. relativeDownsampling describes the step from the previous level;
		// absoluteDownsampling is cumulative from the input.
		final MultiResolutionLevelInfo[] mrInfo = new MultiResolutionLevelInfo[ relativeDs.length + 1 ];
		final int[] dsOne = new int[ n ];
		Arrays.fill( dsOne, 1 );
		mrInfo[ 0 ] = new MultiResolutionLevelInfo(
				n5DatasetIn, srcDim.clone(), dataType, dsOne.clone(), dsOne.clone(), srcBlockSize, null );

		for ( int level = 1; level <= relativeDs.length; ++level )
		{
			final int[] relDs = new int[ n ];
			Arrays.fill( relDs, 1 );
			for ( int d = 0; d < Math.min( relativeDs[ level - 1 ].length, n ); ++d )
				relDs[ d ] = relativeDs[ level - 1 ][ d ];

			final long[] dim = new long[ n ];
			for ( int d = 0; d < n; ++d )
				dim[ d ] = mrInfo[ level - 1 ].dimensions[ d ] / relDs[ d ];

			final int[] absDs = new int[ n ];
			for ( int d = 0; d < n; ++d )
				absDs[ d ] = mrInfo[ level - 1 ].absoluteDownsampling[ d ] * relDs[ d ];

			final String datasetOut = n5DatasetsOut.get( level - 1 );
			mrInfo[ level ] = new MultiResolutionLevelInfo(
					datasetOut, dim, dataType, relDs, absDs, srcBlockSize, null );

			n5.createDataset( datasetOut, dim, srcBlockSize, dataType, compression );
			System.out.println( "level " + level + " '" + datasetOut + "': relDs=" + Util.printCoordinates( relDs ) + ", dims=" + Util.printCoordinates( dim ) );
		}

		final long time = System.currentTimeMillis();

		final long numChannels = is5DOmeZarr ? srcDim[ 3 ] : 1;
		final long numTimepoints = is5DOmeZarr ? srcDim[ 4 ] : 1;

		for ( int level = 1; level <= relativeDs.length; ++level )
		{
			final int s = level;
			final long timeLevel = System.currentTimeMillis();

			// 5D OME-ZARR: writeDownsampledBlock5dOMEZARR expects 3D gridBlock and lifts to 5D internally.
			// N5/HDF5: use the dataset's native dimensionality.
			final long[] gridDim = is5DOmeZarr
					? new long[] { mrInfo[ s ].dimensions[ 0 ], mrInfo[ s ].dimensions[ 1 ], mrInfo[ s ].dimensions[ 2 ] }
					: mrInfo[ s ].dimensions;
			final int gn = gridDim.length;
			final int[] gridBlockSize = new int[ gn ];
			final int[] innerBlockSize = new int[ gn ];
			for ( int d = 0; d < gn; ++d )
			{
				innerBlockSize[ d ] = srcBlockSize[ d ];
				gridBlockSize[ d ] = srcBlockSize[ d ] * ( d < blockScale.length ? blockScale[ d ] : 1 );
			}

			final List<long[][]> grid = Grid.create( gridDim, gridBlockSize, innerBlockSize );

			// Expand grid over (c,t) for 5D OME-ZARR so each task is one (gridBlock, c, t).
			final List<long[][]> allJobs;
			if ( is5DOmeZarr )
			{
				allJobs = new ArrayList<>( grid.size() * (int)( numChannels * numTimepoints ) );
				for ( long t = 0; t < numTimepoints; ++t )
					for ( long c = 0; c < numChannels; ++c )
						for ( final long[][] gb : grid )
							allJobs.add( new long[][] { gb[ 0 ], gb[ 1 ], gb[ 2 ], new long[] { c, t } } );
			}
			else
			{
				allJobs = grid;
			}

			System.out.println( "level " + level + ": " + grid.size() + " grid blocks"
					+ ( is5DOmeZarr ? " x " + ( numChannels * numTimepoints ) + " (c,t) = " + allJobs.size() + " tasks" : "" ) );

			final JavaRDD<long[][]> rdd = sc.parallelize( allJobs );

			final boolean is5DLambda = is5DOmeZarr;
			final StorageFormat storageTypeLcl = storageType;

			rdd.foreach( job -> {
				final N5Writer n5Lcl = N5Util.createN5Writer( n5Path, storageTypeLcl );
				try
				{
					if ( is5DLambda )
					{
						final long[][] gridBlock = new long[][] { job[ 0 ], job[ 1 ], job[ 2 ] };
						final long cIdx = job[ 3 ][ 0 ];
						final long tIdx = job[ 3 ][ 1 ];
						N5ApiTools.writeDownsampledBlock5dOMEZARR( n5Lcl, mrInfo[ s ], mrInfo[ s - 1 ], gridBlock, cIdx, tIdx );
					}
					else
					{
						N5ApiTools.writeDownsampledBlock( n5Lcl, mrInfo[ s ], mrInfo[ s - 1 ], job );
					}
				}
				finally
				{
					n5Lcl.close();
				}
			});

			Thread.sleep( 100 );
			System.out.println( "downsampled level=" + level + ", took: " + ( System.currentTimeMillis() - timeLevel ) + " ms." );
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
