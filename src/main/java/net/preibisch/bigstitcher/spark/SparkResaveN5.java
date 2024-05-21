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

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import bdv.export.ExportMipmapInfo;
import bdv.img.n5.N5ImageLoader;
import mpicbg.spim.data.sequence.SetupImgLoader;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.ViewSetup;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.bigstitcher.spark.util.Grid;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.plugin.resave.Resave_HDF5;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.process.downsampling.lazy.LazyHalfPixelDownsample2x;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class SparkResaveN5 extends AbstractBasic implements Callable<Void>, Serializable
{
	/*
	-x '/Users/preibischs/Documents/Microscopy/Stitching/Truman/testspark/dataset.xml'
	-xo '/Users/preibischs/Documents/Microscopy/Stitching/Truman/testspark/dataset-n5.xml'
	-ds '1,1,1; 2,2,1; 4,4,1; 8,8,2'
	*/

	private static final long serialVersionUID = 1890656279324908516L;

	@Option(names = { "-xo", "--xmlout" }, required = true, description = "path to the output BigStitcher xml, e.g. /home/project-n5.xml")
	private String xmloutPath = null;

	@Option(names = "--blockSize", description = "blockSize, you can use smaller blocks for HDF5 (default: 128,128,64)")
	private String blockSizeString = "128,128,64";

	@Option(names = "--blockScale", description = "how many blocks to use for a single processing step, e.g. 4,4,1 means for blockSize a 128,128,32 that each spark thread writes 512,512,32 (default: 16,16,1)")
	private String blockScaleString = "16,16,1";

	@Option(names = { "-ds", "--downsampling" }, description = "downsampling pyramid (must contain full res 1,1,1 that is always created), e.g. 1,1,1; 2,2,1; 4,4,1; 8,8,2 (default: automatically computed)")
	private String downsampling = null;

	@Option(names = { "-o", "--n5Path" }, description = "N5 path for saving, (default: 'folder of the xml'/dataset.n5)")
	private String n5Path = null;

	@Override
	public Void call() throws Exception
	{
		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		// process all views
		final ArrayList< ViewId > viewIdsGlobal = Import.getViewIds( dataGlobal );

		if ( viewIdsGlobal.size() == 0 )
		{
			throw new IllegalArgumentException( "No views to resave." );
		}
		else
		{
			System.out.println( "Following ViewIds will be resaved: ");
			for ( final ViewId v : viewIdsGlobal )
				System.out.print( "[" + v.getTimePointId() + "," + v.getViewSetupId() + "] " );
			System.out.println();
		}

		final String n5Path = this.n5Path == null ? dataGlobal.getBasePath() + "/dataset.n5" : this.n5Path;
		final Compression compression = new GzipCompression( 1 );

		final int[] blockSize = Import.csvStringToIntArray(blockSizeString);
		final int[] blockScale = Import.csvStringToIntArray(blockScaleString);

		final int[] computeBlock = new int[] {
				blockSize[0] * blockScale[ 0 ],
				blockSize[1] * blockScale[ 1 ],
				blockSize[2] * blockScale[ 2 ] };

		final N5Writer n5 = new N5FSWriter(n5Path);

		System.out.println( "N5 block size=" + Util.printCoordinates( blockSize ) );
		System.out.println( "Compute block size=" + Util.printCoordinates( computeBlock ) );

		System.out.println( "Setting up N5 write for basepath: " + n5Path );

		// all grids across all ViewId's
		final ArrayList<long[][]> allGrids = new ArrayList<>();

		// all ViewSetupIds (needed to create N5 datasets)
		final HashMap<Integer, long[]> viewSetupIdToDimensions = new HashMap<>();

		// all ViewSetups for estimating downsampling
		final List< ViewSetup > viewSetups = new ArrayList<>();

		for ( final ViewId viewId : viewIdsGlobal )
		{
			final ViewDescription vd = dataGlobal.getSequenceDescription().getViewDescription( viewId );

			final List<long[][]> grid = Grid.create(
					vd.getViewSetup().getSize().dimensionsAsLongArray(),
					computeBlock,
					blockSize);

			// add timepointId and ViewSetupId & dimensions to the gridblock
			for ( final long[][] gridBlock : grid )
				allGrids.add( new long[][]{
					gridBlock[ 0 ].clone(),
					gridBlock[ 1 ].clone(),
					gridBlock[ 2 ].clone(),
					new long[] { viewId.getTimePointId(), viewId.getViewSetupId() },
					vd.getViewSetup().getSize().dimensionsAsLongArray()
				});

			viewSetupIdToDimensions.put( viewId.getViewSetupId(), vd.getViewSetup().getSize().dimensionsAsLongArray() );
			viewSetups.add( vd.getViewSetup() );
		}

		// estimate or read downsampling factors
		final int[][] downsampling;

		if ( this.downsampling == null )
		{
			final Map<Integer, ExportMipmapInfo> mipmaps = Resave_HDF5.proposeMipmaps( viewSetups );

			int[][] tmp = mipmaps.values().iterator().next().getExportResolutions();

			for ( final ExportMipmapInfo info : mipmaps.values() )
				if (info.getExportResolutions().length > tmp.length)
					tmp = info.getExportResolutions();

			downsampling = tmp;
		}
		else
		{
			downsampling = Import.csvStringToDownsampling( this.downsampling );
		}

		if ( !Import.testFirstDownsamplingIsPresent( downsampling ) )
			throw new RuntimeException( "First downsampling step is not [1,1,...1], stopping." );

		System.out.println( "Selected downsampling steps:" );

		for ( int i = 0; i < downsampling.length; ++i )
			System.out.println( Util.printCoordinates( downsampling[i] ) );

		if ( dryRun )
		{
			System.out.println( "This is a dry-run, stopping here.");
			return null;
		}

		// create one dataset per ViewSetupId
		for ( final Entry<Integer, long[]> viewSetup: viewSetupIdToDimensions.entrySet() )
		{
			final Object type = dataGlobal.getSequenceDescription().getImgLoader().getSetupImgLoader( viewSetup.getKey() ).getImageType();
			final DataType dataType;

			if ( UnsignedShortType.class.isInstance( type ) )
				dataType = DataType.UINT16;
			else if ( UnsignedByteType.class.isInstance( type ) )
				dataType = DataType.UINT8;
			else if ( FloatType.class.isInstance( type ) )
				dataType = DataType.FLOAT32;
			else
				throw new RuntimeException("Unsupported pixel type: " + type.getClass().getCanonicalName() );

			// TODO: ViewSetupId needs to contain: {"downsamplingFactors":[[1,1,1],[2,2,1]],"dataType":"uint16"}
			final String n5Dataset = "setup" + viewSetup.getKey();

			System.out.println( "Creating group: " + "'setup" + viewSetup.getKey() + "'" );

			n5.createGroup( n5Dataset );

			System.out.println( "setting attributes for '" + "setup" + viewSetup.getKey() + "'");

			n5.setAttribute( n5Dataset, "downsamplingFactors", downsampling );
			n5.setAttribute( n5Dataset, "dataType", dataType );
			n5.setAttribute( n5Dataset, "blockSize", blockSize );
			n5.setAttribute( n5Dataset, "dimensions", viewSetup.getValue() );
			n5.setAttribute( n5Dataset, "compression", compression );
		}

		// create all image (s0) datasets
		for ( final ViewId viewId : viewIdsGlobal )
		{
			System.out.println( "Creating dataset for " + Group.pvid( viewId ) );
			
			final String dataset = "setup" + viewId.getViewSetupId() + "/timepoint" + viewId.getTimePointId() + "/s0";
			final DataType dataType = n5.getAttribute( "setup" + viewId.getViewSetupId(), "dataType", DataType.class );

			n5.createDataset(
					dataset,
					viewSetupIdToDimensions.get( viewId.getViewSetupId() ), // dimensions
					blockSize,
					dataType,
					compression );

			System.out.println( "Setting attributes for " + Group.pvid( viewId ) );

			// set N5 attributes for timepoint
			// e.g. {"resolution":[1.0,1.0,3.0],"saved_completely":true,"multiScale":true}
			String ds ="setup" + viewId.getViewSetupId() + "/" + "timepoint" + viewId.getTimePointId();
			n5.setAttribute(ds, "resolution", new double[] {1,1,1} );
			n5.setAttribute(ds, "saved_completely", true );
			n5.setAttribute(ds, "multiScale", true );

			// set additional N5 attributes for s0 dataset
			ds = ds + "/s0";
			n5.setAttribute(ds, "downsamplingFactors", new int[] {1,1,1} );
		}

		System.out.println( "numBlocks = " + allGrids.size() );

		final SparkConf conf = new SparkConf().setAppName("SparkResaveN5");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		//
		// Save s0 level
		//
		final long time = System.currentTimeMillis();

		final JavaRDD<long[][]> rdds0 = sc.parallelize(allGrids);

		rdds0.foreach(
				gridBlock -> {
					final SpimData2 dataLocal = Spark.getSparkJobSpimData2("", xmlPath);
					final ViewId viewId = new ViewId( (int)gridBlock[ 3 ][ 0 ], (int)gridBlock[ 3 ][ 1 ]);

					final SetupImgLoader< ? > imgLoader = dataLocal.getSequenceDescription().getImgLoader().getSetupImgLoader( viewId.getViewSetupId() );

					@SuppressWarnings("rawtypes")
					final RandomAccessibleInterval img = imgLoader.getImage( viewId.getTimePointId() );

					final N5Writer n5Lcl = new N5FSWriter(n5Path);

					final DataType dataType = n5Lcl.getAttribute( "setup" + viewId.getViewSetupId(), "dataType", DataType.class );
					final String dataset = "setup" + viewId.getViewSetupId() + "/timepoint" + viewId.getTimePointId() + "/s0";

					if ( dataType == DataType.UINT16 )
					{
						@SuppressWarnings("unchecked")
						final RandomAccessibleInterval<UnsignedShortType> sourceGridBlock = Views.offsetInterval(img, gridBlock[0], gridBlock[1]);
						N5Utils.saveNonEmptyBlock(sourceGridBlock, n5Lcl, dataset, gridBlock[2], new UnsignedShortType());
					}
					else if ( dataType == DataType.UINT8 )
					{
						@SuppressWarnings("unchecked")
						final RandomAccessibleInterval<UnsignedByteType> sourceGridBlock = Views.offsetInterval(img, gridBlock[0], gridBlock[1]);
						N5Utils.saveNonEmptyBlock(sourceGridBlock, n5Lcl, dataset, gridBlock[2], new UnsignedByteType());
					}
					else if ( dataType == DataType.FLOAT32 )
					{
						@SuppressWarnings("unchecked")
						final RandomAccessibleInterval<FloatType> sourceGridBlock = Views.offsetInterval(img, gridBlock[0], gridBlock[1]);
						N5Utils.saveNonEmptyBlock(sourceGridBlock, n5Lcl, dataset, gridBlock[2], new FloatType());
					}
					else
					{
						n5Lcl.close();
						throw new RuntimeException("Unsupported pixel type: " + dataType );
					}

					System.out.println( "ViewId " + Group.pvid( viewId ) + ", written block: offset=" + Util.printCoordinates( gridBlock[0] ) + ", dimension=" + Util.printCoordinates( gridBlock[1] ) );
				});

		System.out.println( "Resaved N5 s0 level, took: " + (System.currentTimeMillis() - time ) + " ms." );

		//
		// Save remaining downsampling levels (s1 ... sN)
		//
		for ( int level = 1; level < downsampling.length; ++level )
		{
			final int[] ds = new int[ downsampling[ 0 ].length ];

			for ( int d = 0; d < ds.length; ++d )
				ds[ d ] = downsampling[ level ][ d ] / downsampling[ level - 1 ][ d ];

			System.out.println( "Downsampling: " + Util.printCoordinates( downsampling[ level ] ) + " with relative downsampling of " + Util.printCoordinates( ds ));

			// all grids across all ViewId's
			final ArrayList<long[][]> allGridsDS = new ArrayList<>();

			// adjust dimensions
			for ( final ViewId viewId : viewIdsGlobal )
			{
				final long[] previousDim = n5.getAttribute( "setup" + viewId.getViewSetupId() + "/timepoint" + viewId.getTimePointId() + "/s" + (level-1), "dimensions", long[].class );
				final long[] dim = new long[ previousDim.length ];
				for ( int d = 0; d < dim.length; ++d )
					dim[ d ] = previousDim[ d ] / ds[ d ];
				final DataType dataType = n5.getAttribute( "setup" + viewId.getViewSetupId(), "dataType", DataType.class );

				System.out.println( Group.pvid( viewId ) + ": s" + (level-1) + " dim=" + Util.printCoordinates( previousDim ) + ", s" + level + " dim=" + Util.printCoordinates( dim ) + ", datatype=" + dataType );

				final String dataset = "setup" + viewId.getViewSetupId() + "/timepoint" + viewId.getTimePointId() + "/s" + level;

				n5.createDataset(
						dataset,
						dim, // dimensions
						blockSize,
						dataType,
						compression );

				final List<long[][]> grid = Grid.create(
						dim,
						new int[] {
								blockSize[0],
								blockSize[1],
								blockSize[2]
						},
						blockSize);

				// add timepointId and ViewSetupId to the gridblock
				for ( final long[][] gridBlock : grid )
					allGridsDS.add( new long[][]{
						gridBlock[ 0 ].clone(),
						gridBlock[ 1 ].clone(),
						gridBlock[ 2 ].clone(),
						new long[] { viewId.getTimePointId(), viewId.getViewSetupId() }
					});

				// set additional N5 attributes for sN dataset
				n5.setAttribute(dataset, "downsamplingFactors", downsampling[ level ] );
			}

			System.out.println( "s" + level + " num blocks=" + allGridsDS.size() );

			final JavaRDD<long[][]> rddsN = sc.parallelize(allGridsDS);

			final int s = level;
			final long timeS = System.currentTimeMillis();

			rddsN.foreach(
					gridBlock -> {
						final ViewId viewId = new ViewId( (int)gridBlock[ 3 ][ 0 ], (int)gridBlock[ 3 ][ 1 ]);

						final N5Writer n5Lcl = new N5FSWriter(n5Path);

						final DataType dataType = n5Lcl.getAttribute( "setup" + viewId.getViewSetupId(), "dataType", DataType.class );
						final String datasetPrev = "setup" + viewId.getViewSetupId() + "/timepoint" + viewId.getTimePointId() + "/s" + (s-1);
						final String dataset = "setup" + viewId.getViewSetupId() + "/timepoint" + viewId.getTimePointId() + "/s" + (s);

						if ( dataType == DataType.UINT16 )
						{
							RandomAccessibleInterval<UnsignedShortType> downsampled = N5Utils.open(n5Lcl, datasetPrev);;

							for ( int d = 0; d < downsampled.numDimensions(); ++d )
								if ( ds[ d ] > 1 )
									downsampled = LazyHalfPixelDownsample2x.init(
										downsampled,
										new FinalInterval( downsampled ),
										new UnsignedShortType(),
										blockSize,
										d);

							final RandomAccessibleInterval<UnsignedShortType> sourceGridBlock = Views.offsetInterval(downsampled, gridBlock[0], gridBlock[1]);
							N5Utils.saveNonEmptyBlock(sourceGridBlock, n5Lcl, dataset, gridBlock[2], new UnsignedShortType());
						}
						else if ( dataType == DataType.UINT8 )
						{
							RandomAccessibleInterval<UnsignedByteType> downsampled = N5Utils.open(n5Lcl, datasetPrev);

							for ( int d = 0; d < downsampled.numDimensions(); ++d )
								if ( ds[ d ] > 1 )
									downsampled = LazyHalfPixelDownsample2x.init(
										downsampled,
										new FinalInterval( downsampled ),
										new UnsignedByteType(),
										blockSize,
										d);

							final RandomAccessibleInterval<UnsignedByteType> sourceGridBlock = Views.offsetInterval(downsampled, gridBlock[0], gridBlock[1]);
							N5Utils.saveNonEmptyBlock(sourceGridBlock, n5Lcl, dataset, gridBlock[2], new UnsignedByteType());
						}
						else if ( dataType == DataType.FLOAT32 )
						{
							RandomAccessibleInterval<FloatType> downsampled = N5Utils.open(n5Lcl, datasetPrev);;

							for ( int d = 0; d < downsampled.numDimensions(); ++d )
								if ( ds[ d ] > 1 )
									downsampled = LazyHalfPixelDownsample2x.init(
										downsampled,
										new FinalInterval( downsampled ),
										new FloatType(),
										blockSize,
										d);

							final RandomAccessibleInterval<FloatType> sourceGridBlock = Views.offsetInterval(downsampled, gridBlock[0], gridBlock[1]);
							N5Utils.saveNonEmptyBlock(sourceGridBlock, n5Lcl, dataset, gridBlock[2], new FloatType());
						}
						else
						{
							n5Lcl.close();
							throw new RuntimeException("Unsupported pixel type: " + dataType );
						}
					});

			System.out.println( "Resaved N5 s" + s + " level, took: " + (System.currentTimeMillis() - timeS ) + " ms." );
		}

		sc.close();

		System.out.println( "resaved successfully." );

		// things look good, let's save the new XML
		System.out.println( "Saving new xml to: " + xmloutPath );

		dataGlobal.getSequenceDescription().setImgLoader( new N5ImageLoader( new File( n5Path ), dataGlobal.getSequenceDescription()));
		new XmlIoSpimData2( null ).save( dataGlobal, xmloutPath );

		n5.close();

		Thread.sleep( 100 );
		System.out.println( "Resaved project, in total took: " + (System.currentTimeMillis() - time ) + " ms." );
		System.out.println( "done." );

		return null;
	}

	public static void main(final String... args) {

		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new SparkResaveN5()).execute(args));
	}

}
