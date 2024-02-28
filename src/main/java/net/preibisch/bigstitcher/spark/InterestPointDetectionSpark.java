package net.preibisch.bigstitcher.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Pair;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.Threads;
import net.preibisch.mvrecon.fiji.plugin.interestpointdetection.DifferenceOfGUI;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.process.downsampling.DownsampleTools;
import net.preibisch.mvrecon.process.interestpointdetection.InterestPointTools;
import net.preibisch.mvrecon.process.interestpointdetection.methods.dog.DoGImgLib2;
import net.preibisch.mvrecon.process.interestpointdetection.methods.dog.DoGParameters;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import scala.Tuple2;

public class InterestPointDetectionSpark implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = -7654397945854689628L;

	public enum IP { MIN, MAX, BOTH };

	@Option(names = { "-x", "--xml" }, required = true, description = "Path to the existing BigStitcher project xml, e.g. -x /home/project.xml")
	String xmlPath = null;


	@Option(names = { "-l", "--label" }, required = true, description = "label for the interest points (e.g. beads)")
	private String label = null;


	@Option(names = { "-s", "--sigma" }, required = true, description = "sigma for segmentation, e.g. 1.8")
	private Double sigma = null;

	@Option(names = { "-t", "--threshold" }, required = true, description = "threshold for segmentation, e.g. 0.008")
	private Double threshold = null;

	@Option(names = { "--type" }, description = "the type of interestpoints to find, MIN, MAX or BOTH (default: MAX)")
	private IP type = IP.MAX;


	@Option(names = { "-i0", "--minIntensity" }, description = "min intensity for segmentation, e.g. 0.0 (default: load from image)")
	private Double minIntensity = null;

	@Option(names = { "-i1", "--maxIntensity" }, description = "max intensity for segmentation, e.g. 2048.0 (default: load from image)")
	private Double maxIntensity = null;


	@Option(names = { "-dsxy", "--downsampleXY" }, description = "downsampling in XY to use for segmentation, e.g. 4 (default: 1)")
	private Integer dsxy = 1;

	@Option(names = { "-dsz", "--downsampleZ" }, description = "downsampling in Z to use for segmentation, e.g. 2 (default: 1)")
	private Integer dsz = 1;

	@Option(names = { "--angleId" }, description = "list the angle ids that should be fused into a single image, you can find them in the XML, e.g. --angleId '0,1,2' (default: all angles)")
	String angleIds = null;

	@Option(names = { "--tileId" }, description = "list the tile ids that should be fused into a single image, you can find them in the XML, e.g. --tileId '0,1,2' (default: all tiles)")
	String tileIds = null;

	@Option(names = { "--illuminationId" }, description = "list the illumination ids that should be fused into a single image, you can find them in the XML, e.g. --illuminationId '0,1,2' (default: all illuminations)")
	String illuminationIds = null;

	@Option(names = { "--channelId" }, description = "list the channel ids that should be fused into a single image, you can find them in the XML (usually just ONE!), e.g. --channelId '0,1,2' (default: all channels)")
	String channelIds = null;

	@Option(names = { "--timepointId" }, description = "list the timepoint ids that should be fused into a single image, you can find them in the XML (usually just ONE!), e.g. --timepointId '0,1,2' (default: all time points)")
	String timepointIds = null;

	@Option(names = { "-vi" }, description = "specifically list the view ids (time point, view setup) that should be fused into a single image, e.g. -vi '0,0' -vi '0,1' (default: all view ids)")
	String[] vi = null;


	@Override
	public Void call() throws Exception
	{
		final SpimData2 dataGlobal = Spark.getSparkJobSpimData2("", xmlPath);

		Import.validateInputParameters(vi, angleIds, channelIds, illuminationIds, tileIds, timepointIds);

		// select views to process
		final ArrayList< ViewId > viewIds =
				Import.createViewIds(
						dataGlobal, vi, angleIds, channelIds, illuminationIds, tileIds, timepointIds);

		if ( viewIds.size() == 0 )
		{
			throw new IllegalArgumentException( "No views to fuse." );
		}
		else
		{
			System.out.println( "For the following ViewIds interest point detections will be performed: ");
			for ( final ViewId v : viewIds )
				System.out.print( "[" + v.getTimePointId() + "," + v.getViewSetupId() + "] " );
			System.out.println();
		}

		System.out.println( "label: " + label );
		System.out.println( "sigma: " + sigma );
		System.out.println( "threshold: " + threshold );
		System.out.println( "type: " + type );
		System.out.println( "minIntensity: " + minIntensity );
		System.out.println( "maxIntensity: " + maxIntensity );
		System.out.println( "downsampleXY: " + dsxy );
		System.out.println( "downsampleZ: " + dsz );

		final ArrayList<int[]> serializedViewIds = Spark.serializeViewIdsForRDD( viewIds );

		final SparkConf conf = new SparkConf().setAppName("SparkResaveN5");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final JavaRDD<int[]> rdd = sc.parallelize( serializedViewIds );

		final int downsampleXY = this.dsxy;
		final int downsampleZ = this.dsz;
		final double minIntensity = this.minIntensity == null ? Double.NaN : this.minIntensity;
		final double maxIntensity = this.maxIntensity == null ? Double.NaN : this.maxIntensity;
		final double sigma = this.sigma;
		final double threshold = this.threshold;
		final boolean findMin = (this.type == IP.MIN || this.type == IP.BOTH);
		final boolean findMax = (this.type == IP.MAX || this.type == IP.BOTH);

		final JavaPairRDD< ArrayList< InterestPoint >, int[] > rddResults = rdd.mapToPair( serializedView ->
		{
			final SpimData2 data = Spark.getSparkJobSpimData2( "", xmlPath );
			final ViewId viewId = Spark.deserializeViewId( serializedView );
			final ViewDescription vd = data.getSequenceDescription().getViewDescription( viewId );

			if ( !vd.isPresent() )
			{
				System.out.println( Group.pvid(viewId) + " is not present. skipping." );
				return null;
			}

			final DoGParameters dog = new DoGParameters();

			dog.imgloader = data.getSequenceDescription().getImgLoader();
			dog.toProcess = new ArrayList< ViewDescription >( Arrays.asList( vd ) );

			dog.localization = DifferenceOfGUI.defaultLocalization;
			dog.downsampleZ = downsampleZ;
			dog.downsampleXY = downsampleXY;
			dog.imageSigmaX = DifferenceOfGUI.defaultImageSigmaX;
			dog.imageSigmaY = DifferenceOfGUI.defaultImageSigmaY;
			dog.imageSigmaZ = DifferenceOfGUI.defaultImageSigmaZ;

			dog.minIntensity = minIntensity;
			dog.maxIntensity = maxIntensity;

			dog.sigma = sigma;
			dog.threshold = threshold;
			dog.findMin = findMin;
			dog.findMax = findMax;

			dog.cuda = null;
			dog.deviceCUDA = null;
			dog.accurateCUDA = false;
			dog.percentGPUMem = 0;

			dog.limitDetections = false;
			dog.maxDetections = 0;
			dog.maxDetectionsTypeIndex = 0;

			dog.showProgressMin = Double.NaN;
			dog.showProgressMax = Double.NaN;

			final ExecutorService service = Threads.createFixedExecutorService( 1 );

			// TODO: downsampling is not virtual!
			@SuppressWarnings({"rawtypes" })
			final Pair<RandomAccessibleInterval, AffineTransform3D> input =
					DownsampleTools.openAndDownsample(
							dog.imgloader,
							vd,
							new long[] { dog.downsampleXY, dog.downsampleXY, dog.downsampleZ } );

			@SuppressWarnings("unchecked")
			ArrayList< InterestPoint > ips = DoGImgLib2.computeDoG(
						input.getA(),
						null, // mask
						dog.sigma,
						dog.threshold,
						dog.localization,
						dog.findMin,
						dog.findMax,
						dog.minIntensity,
						dog.maxIntensity,
						DoGImgLib2.blockSize,
						service,
						dog.cuda,
						dog.deviceCUDA,
						dog.accurateCUDA,
						dog.percentGPUMem );

			service.shutdown();

			DownsampleTools.correctForDownsampling( ips, input.getB() );

			return new Tuple2<>( ips, serializedView );
		});

		rddResults.cache();
		rddResults.count();

		final List<Tuple2<ArrayList<InterestPoint>, int[]>> results = rddResults.collect();

		System.out.println( "Computed all interest points. Merging and saving." );

		final HashMap< ViewId, List< InterestPoint > > interestPoints = new HashMap< ViewId, List< InterestPoint > >();

		for ( final Tuple2< ArrayList<InterestPoint>, int[] > tuple : results )
		{
			final ViewId viewId = Spark.deserializeViewId( tuple._2() );
			final ArrayList<InterestPoint> ips = tuple._1();

			interestPoints.put( viewId, ips );

			System.out.println( Group.pvid( viewId ) + ": " + ips.size() );
		}

		final String params = "DOG (Spark) s=" + sigma + " t=" + threshold + " min=" + findMin + " max=" + findMax +
				" downsampleXY=" + downsampleXY + " downsampleZ=" + downsampleZ + " minIntensity=" + minIntensity + " maxIntensity=" + maxIntensity;

		InterestPointTools.addInterestPoints( dataGlobal, label, interestPoints, params );

		sc.close();

		System.out.println( "Saving XML and interest points ..." );

		SpimData2.saveXML( dataGlobal, xmlPath, null );

		System.out.println( "Done ..." );

		return null;
	}


	public static void main(final String... args)
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new InterestPointDetectionSpark()).execute(args));
	}
}
