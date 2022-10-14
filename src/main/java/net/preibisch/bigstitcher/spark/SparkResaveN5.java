package net.preibisch.bigstitcher.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

import mpicbg.spim.data.sequence.ImgLoader;
import mpicbg.spim.data.sequence.SetupImgLoader;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Dimensions;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.util.Grid;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class SparkResaveN5 implements Callable<Void>, Serializable
{

	private static final long serialVersionUID = 1890656279324908516L;

	@Option(names = { "-x", "--xml" }, required = true, description = "path to the BigStitcher xml, e.g. /home/project.xml")
	private String xmlPath = null;

	@Option(names = "--blockSize", description = "blockSize, e.g. 128,128,32")
	private String blockSizeString = "128,128,32";

	@Option(names = "--blockSizeScale", description = "how much the blocksize is scaled for processing, e.g. 4,4,1 means for blockSize 128,128,32 that each spark thread writes 512,512,32")
	private String blockSizeScaleString = "4,4,1";

	@Option(names = { "-o", "--n5Path" }, description = "N5 path for saving, default: 'folder of the xml'/dataset.n5")
	private String n5Path = null;

	@Option(names = { "--angleId" }, description = "list the angle ids that should be fused into a single image, you can find them in the XML, e.g. --angleId '0,1,2' (default: all angles)")
	private String angleIds = null;

	@Option(names = { "--tileId" }, description = "list the tile ids that should be fused into a single image, you can find them in the XML, e.g. --tileId '0,1,2' (default: all tiles)")
	private String tileIds = null;

	@Option(names = { "--illuminationId" }, description = "list the illumination ids that should be fused into a single image, you can find them in the XML, e.g. --illuminationId '0,1,2' (default: all illuminations)")
	private String illuminationIds = null;

	@Option(names = { "--channelId" }, description = "list the channel ids that should be fused into a single image, you can find them in the XML (usually just ONE!), e.g. --channelId '0,1,2' (default: all channels)")
	private String channelIds = null;

	@Option(names = { "--timepointId" }, description = "list the timepoint ids that should be fused into a single image, you can find them in the XML (usually just ONE!), e.g. --timepointId '0,1,2' (default: all time points)")
	private String timepointIds = null;

	@Option(names = { "-vi" }, description = "specifically list the view ids (time point, view setup) that should be fused into a single image, e.g. -vi '0,0' -vi '0,1' (default: all view ids)")
	private String[] vi = null;

	@Override
	public Void call() throws Exception
	{
		final SpimData2 data = Spark.getSparkJobSpimData2("", xmlPath);

		// select views to process
		final ArrayList< ViewId > viewIds =
				Import.createViewIds(
						data, vi, angleIds, channelIds, illuminationIds, tileIds, timepointIds);

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

		final ArrayList<long[][]> allGrids = new ArrayList<>();
		final HashMap<Integer, long[]> viewSetupIds = new HashMap<>();

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

			viewSetupIds.put( viewId.getViewSetupId(), vd.getViewSetup().getSize().dimensionsAsLongArray() );
		}

		// create one dataset per ViewSetupId
		for ( final Entry<Integer, long[]> viewSetup: viewSetupIds.entrySet() )
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

			// ViewSetupId needs to contain: {"downsamplingFactors":[[1,1,1],[2,2,1]],"dataType":"uint16"}
			n5.createDataset(
					"setup" + viewSetup.getKey(),
					viewSetup.getValue(),
					blockSize,
					dataType,
					new GzipCompression( 1 ) );

			System.out.println( "Creating dataset: " + "'setup" + viewSetup.getKey() + "'" );
		}

		System.out.println( "numBlocks = " + allGrids.size() );

		final SparkConf conf = new SparkConf().setAppName("SparkResaveN5");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final JavaRDD<long[][]> rdd = sc.parallelize(allGrids);

		rdd.foreach(
				gridBlock -> {
					final SpimData2 dataLocal = Spark.getSparkJobSpimData2("", xmlPath);
					final ViewId viewId = new ViewId( (int)gridBlock[ 3 ][ 0 ], (int)gridBlock[ 3 ][ 1 ]);

					final SetupImgLoader< ? > imgLoader = dataLocal.getSequenceDescription().getImgLoader().getSetupImgLoader( viewId.getViewSetupId() );
					final RandomAccessibleInterval img = imgLoader.getImage( viewId.getTimePointId() );

					final N5Writer n5Lcl = new N5FSWriter(n5Path);

					final DataType dataType = n5Lcl.getAttribute( "setup" + viewId.getViewSetupId(), "dataType", DataType.class );
					final String dataset = "setup" + viewId.getViewSetupId() + "/timepoint" + viewId.getTimePointId() + "/s0";

					n5Lcl.createDataset(
							dataset,
							gridBlock[ 4 ], // dimensions
							blockSize,
							dataType,
							new GzipCompression( 1 ) );

					if ( dataType == DataType.UINT16 )
					{
						final RandomAccessibleInterval<UnsignedShortType> sourceGridBlock = Views.offsetInterval(img, gridBlock[0], gridBlock[1]);
						N5Utils.saveNonEmptyBlock(sourceGridBlock, n5Lcl, dataset, gridBlock[2], new UnsignedShortType());
					}
				});

		final long time = System.currentTimeMillis();

		sc.close();

		System.out.println( "Resaved N5, took: " + (System.currentTimeMillis() - time ) + " ms." );

		return null;
	}

	public static void main(final String... args) {

		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new SparkResaveN5()).execute(args));
	}

}
