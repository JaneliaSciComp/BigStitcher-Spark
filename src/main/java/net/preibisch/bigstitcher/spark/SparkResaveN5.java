package net.preibisch.bigstitcher.spark;

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
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import bdv.export.ExportMipmapInfo;
import mpicbg.spim.data.sequence.SetupImgLoader;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.ViewSetup;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.util.Grid;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.plugin.Data_Explorer;
import net.preibisch.mvrecon.fiji.plugin.resave.Resave_HDF5;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.process.downsampling.lazy.LazyHalfPixelDownsample2x;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class SparkResaveN5 implements Callable<Void>, Serializable
{

	private static final long serialVersionUID = 1890656279324908516L;

	@Option(names = { "-x", "--xml" }, required = true, description = "path to the BigStitcher xml, e.g. /home/project.xml")
	private String xmlPath = null;

	@Option(names = "--blockSize", description = "blockSize, you can use smaller blocks for HDF5 (default: 128,128,128)")
	private String blockSizeString = "128,128,128";

	@Option(names = "--blockSizeScale", description = "how much the blocksize is scaled for processing, e.g. 4,4,1 means for blockSize 128,128,32 that each spark thread writes 512,512,32 (default: 3,3,1)")
	private String blockSizeScaleString = "2,2,1";

	@Option(names = { "-ds", "--downsampling" }, description = "downsampling pyramid (must contain full res 1,1,1 that is always created), e.g. 1,1,1; 2,2,1; 4,4,1; 8,8,2 (default: automatically computed)")
	private String downsampling = null;

	@Option(names = { "-o", "--n5Path" }, description = "N5 path for saving, (default: 'folder of the xml'/dataset.n5)")
	private String n5Path = null;

	@Override
	public Void call() throws Exception
	{
		final SpimData2 data = Spark.getSparkJobSpimData2("", xmlPath);

		// process all views
		final ArrayList< ViewId > viewIds = Import.getViewIds( data );

		if ( viewIds.size() == 0 )
		{
			throw new IllegalArgumentException( "No views to resave." );
		}
		else
		{
			System.out.println( "Following ViewIds will be resaved: ");
			for ( final ViewId v : viewIds )
				System.out.print( "[" + v.getTimePointId() + "," + v.getViewSetupId() + "] " );
			System.out.println();
		}

		final String n5Path = this.n5Path == null ? data.getBasePath() + "/dataset.n5" : this.n5Path;

		final int[] blockSize = Import.csvStringToIntArray(blockSizeString);
		final int[] blockSizeScale = Import.csvStringToIntArray(blockSizeScaleString);

		final N5Writer n5 = new N5FSWriter(n5Path);
		System.out.println( "Setting up N5 write for basepath: " + n5Path );

		// all grids across all ViewId's
		final ArrayList<long[][]> allGrids = new ArrayList<>();

		// all ViewSetupIds (needed to create N5 datasets)
		final HashMap<Integer, long[]> viewSetupIdToDimensions = new HashMap<>();

		// all ViewSetups for estimating downsampling
		final List< ViewSetup > viewSetups = new ArrayList<>();

		for ( final ViewId viewId : viewIds )
		{
			final ViewDescription vd = data.getSequenceDescription().getViewDescription( viewId );

			final List<long[][]> grid = Grid.create(
					vd.getViewSetup().getSize().dimensionsAsLongArray(),
					new int[] {
							blockSize[0] * blockSizeScale[ 0 ],
							blockSize[1] * blockSizeScale[ 1 ],
							blockSize[2] * blockSizeScale[ 2 ]
					},
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

		// create one dataset per ViewSetupId
		for ( final Entry<Integer, long[]> viewSetup: viewSetupIdToDimensions.entrySet() )
		{
			final Object type = data.getSequenceDescription().getImgLoader().getSetupImgLoader( viewSetup.getKey() ).getImageType();
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
			n5.setAttribute( n5Dataset, "downsamplingFactors", downsampling );
			n5.setAttribute( n5Dataset, "dataType", dataType );
		}

		// create all image (s0) datasets
		for ( final ViewId viewId : viewIds )
		{
			final String dataset = "setup" + viewId.getViewSetupId() + "/timepoint" + viewId.getTimePointId() + "/s0";
			final DataType dataType = n5.getAttribute( "setup" + viewId.getViewSetupId(), "dataType", DataType.class );

			n5.createDataset(
					dataset,
					viewSetupIdToDimensions.get( viewId.getViewSetupId() ), // dimensions
					blockSize,
					dataType,
					new GzipCompression( 1 ) );
		}

		System.out.println( "numBlocks = " + allGrids.size() );

		final SparkConf conf = new SparkConf().setAppName("SparkResaveN5");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		//
		// Save s0 level
		//
		final long time = System.currentTimeMillis();
		/*
		final JavaRDD<long[][]> rdds0 = sc.parallelize(allGrids);

		rdds0.foreach(
				gridBlock -> {
					final SpimData2 dataLocal = Spark.getSparkJobSpimData2("", xmlPath);
					final ViewId viewId = new ViewId( (int)gridBlock[ 3 ][ 0 ], (int)gridBlock[ 3 ][ 1 ]);

					final SetupImgLoader< ? > imgLoader = dataLocal.getSequenceDescription().getImgLoader().getSetupImgLoader( viewId.getViewSetupId() );

					final RandomAccessibleInterval img = imgLoader.getImage( viewId.getTimePointId() );

					final N5Writer n5Lcl = new N5FSWriter(n5Path);

					final DataType dataType = n5Lcl.getAttribute( "setup" + viewId.getViewSetupId(), "dataType", DataType.class );
					final String dataset = "setup" + viewId.getViewSetupId() + "/timepoint" + viewId.getTimePointId() + "/s0";

					if ( dataType == DataType.UINT16 )
					{
						final RandomAccessibleInterval<UnsignedShortType> sourceGridBlock = Views.offsetInterval(img, gridBlock[0], gridBlock[1]);
						N5Utils.saveNonEmptyBlock(sourceGridBlock, n5Lcl, dataset, gridBlock[2], new UnsignedShortType());
					}
					else if ( dataType == DataType.UINT8 )
					{
						final RandomAccessibleInterval<UnsignedByteType> sourceGridBlock = Views.offsetInterval(img, gridBlock[0], gridBlock[1]);
						N5Utils.saveNonEmptyBlock(sourceGridBlock, n5Lcl, dataset, gridBlock[2], new UnsignedByteType());
					}
					else if ( dataType == DataType.FLOAT32 )
					{
						final RandomAccessibleInterval<FloatType> sourceGridBlock = Views.offsetInterval(img, gridBlock[0], gridBlock[1]);
						N5Utils.saveNonEmptyBlock(sourceGridBlock, n5Lcl, dataset, gridBlock[2], new FloatType());
					}
					else
					{
						throw new RuntimeException("Unsupported pixel type: " + dataType );
					}
				});
		*/
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
			//for ( final Entry<Integer, long[]> viewSetup: viewSetupIds.entrySet() )
			for ( final ViewId viewId : viewIds )
			{
				final long[] previousDim = n5.getAttribute( "setup" + viewId.getViewSetupId() + "/timepoint" + viewId.getTimePointId() + "/s" + (level-1), "dimensions", long[].class );
				final long[] dim = new long[ previousDim.length ];
				for ( int d = 0; d < dim.length; ++d )
					dim[ d ] = previousDim[ d ] / 2;
				final DataType dataType = n5.getAttribute( "setup" + viewId.getViewSetupId(), "dataType", DataType.class );

				System.out.println( Group.pvid( viewId ) + ": s" + (level-1) + " dim=" + Util.printCoordinates( previousDim ) + ", s" + level + " dim=" + Util.printCoordinates( dim ) + ", datatype=" + dataType );

				final String dataset = "setup" + viewId.getViewSetupId() + "/timepoint" + viewId.getTimePointId() + "/s" + level;

				n5.createDataset(
						dataset,
						dim, // dimensions
						blockSize,
						dataType,
						new GzipCompression( 1 ) );

				final List<long[][]> grid = Grid.create(
						dim,
						new int[] {
								blockSize[0],
								blockSize[1],
								blockSize[2]
						},
						blockSize);

				allGridsDS.addAll( grid );
			}

			System.out.println( "s" + level + " num blocks=" + allGridsDS.size() );
		}

		sc.close();

		Thread.sleep( 100 );
		System.out.println( "resaved successfully." );

		return null;
	}

	public static void main(final String... args) {

		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new SparkResaveN5()).execute(args));
	}

}
