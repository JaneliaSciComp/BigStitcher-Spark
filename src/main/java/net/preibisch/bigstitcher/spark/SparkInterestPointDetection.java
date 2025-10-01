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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.HashSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import org.janelia.scicomp.n5.zstandard.ZstandardCompression;

import mpicbg.spim.data.generic.sequence.BasicImgLoader;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.sequence.ImgLoader;
import mpicbg.spim.data.sequence.MultiResolutionImgLoader;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Dimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.KDTree;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccess;
import net.imglib2.RealRandomAccessible;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.neighborsearch.NearestNeighborSearchOnKDTree;
import net.imglib2.position.FunctionRandomAccessible;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.detection.LazyBackgroundSubtract;
import net.preibisch.bigstitcher.spark.fusion.OverlappingViews;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.bigstitcher.spark.util.ViewUtil;
import net.preibisch.bigstitcher.spark.util.ViewUtil.PrefetchPixel;
import net.preibisch.mvrecon.Threads;
import net.preibisch.mvrecon.fiji.plugin.interestpointdetection.DifferenceOfGUI;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsN5;
import net.preibisch.mvrecon.process.downsampling.Downsample;
import net.preibisch.mvrecon.process.downsampling.DownsampleTools;
import net.preibisch.mvrecon.process.downsampling.lazy.LazyDownsample2x;
import net.preibisch.mvrecon.process.fusion.transformed.TransformVirtual;
import net.preibisch.mvrecon.process.interestpointdetection.InterestPointTools;
import net.preibisch.mvrecon.process.interestpointdetection.methods.dog.DoGImgLib2;
import net.preibisch.mvrecon.process.interestpointdetection.methods.dog.DoGParameters;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import util.Grid;
import util.URITools;

public class SparkInterestPointDetection extends AbstractSelectableViews implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = -7654397945854689628L;

	public static double combineDistance = 0.5; // when to merge interestpoints that were found in overlapping ROIS (overlappingOnly)

	public enum IP { MIN, MAX, BOTH };
	public enum Localization { NONE, QUADRATIC };

	@Option(names = { "-l", "--label" }, required = true, description = "label for the interest points (e.g. beads)")
	protected String label = null;

	@Option(names = { "-s", "--sigma" }, required = true, description = "sigma for segmentation, e.g. 1.8")
	protected Double sigma = null;

	@Option(names = { "-t", "--threshold" }, required = true, description = "threshold for segmentation, e.g. 0.008")
	protected Double threshold = null;

	@Option(names = { "--type" }, description = "the type of interestpoints to find, MIN, MAX or BOTH (default: MAX)")
	protected IP type = IP.MAX;

	@Option(names = { "--localization" }, description = "Subpixel localization method, NONE or QUADRATIC (default: QUADRATIC)")
	protected Localization localization = Localization.QUADRATIC;

	@Option(names = { "--overlappingOnly" }, description = "only find interest points in areas that currently overlap with another view. WARNING: this is usually only useful when running it on a single channel/timepoint, otherwise they usually fully overlap (default: false)")
	protected boolean overlappingOnly = false;

	@Option(names = { "--onlyCompareOverlapTiles" }, description = "if --overlappingOnly is selected, only test overlap for the Tile attribute; you might need this if you have multiple channels/timepoints (default: false)")
	protected boolean onlyCompareOverlapTiles = false;

	@Option(names = { "--storeIntensities" }, description = "creates an additional N5 dataset with the intensities of each detection, linearly interpolated (default: false)")
	protected boolean storeIntensities = false;

	@Option(names = { "-i0", "--minIntensity" }, required = true, description = "min intensity for segmentation, e.g. 0.0")
	protected Double minIntensity = null;

	@Option(names = { "-i1", "--maxIntensity" }, required = true, description = "max intensity for segmentation, e.g. 2048.0")
	protected Double maxIntensity = null;

	@Option(names = { "--prefetch" }, description = "prefetch all blocks required to process DoG in each Spark job using unlimited threads, useful in cloud environments (default: false)")
	protected boolean prefetch = false;

	@Option(names = { "--keepTemporaryN5" }, description = "do NOT delete the temporary spark N5 in interestpoints.n5 (default: false)")
	protected boolean keepTemporaryN5 = false;


	@Option(names = {"--maxSpots" }, description = "limit the number of spots per view (choose the brightest ones), e.g. --maxSpots 10000 (default: NO LIMIT)")
	protected int maxSpots = -1;

	@Option(names = { "--maxSpotsPerOverlap" }, description = "apply the maximum number of spots individually to every overlapping area, needs --overlappingOnly & --maxSpots to be set to work (default: false)")
	protected boolean maxSpotsPerOverlap = false;

	@Option(names = "--blockSize", description = "blockSize for running the interest point detection - at the scale of detection (default: 512,512,128)")
	protected String blockSizeString = "512,512,128";

	@Option(names = { "--medianFilter" }, description = "divide by the median filtered image of the given radius prior to interest point detection, e.g. --medianFilter 10")
	protected Integer medianFilter = null;


	@Option(names = { "-dsxy", "--downsampleXY" }, description = "downsampling in XY to use for segmentation, e.g. 4 (default: 2)")
	protected Integer dsxy = 2;

	@Option(names = { "-dsz", "--downsampleZ" }, description = "downsampling in Z to use for segmentation, e.g. 2 (default: 1)")
	protected Integer dsz = 1;

	//-x /Users/preibischs/SparkTest/IP/dataset.xml -l beadsTest500 -s 1.8 -t 0.008 -dsxy 2 --minIntensity 0 --maxIntensity 255 --prefetch
	//-x /Users/preibischs/Downloads/dataset-allen.xml -l beadsTest500 -vi '0,0' -s 1.8 -t 0.008 -dsxy 32 -dsz 32 --minIntensity 0 --maxIntensity 255 --prefetch

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		if ( maxSpotsPerOverlap && ( !overlappingOnly || maxSpots <= 0 ) )
		{
			System.out.println( "--maxSpotsPerOverlap only works when --overlappingOnly AND --maxSpots is set.");
			System.exit( 0 );
		}

		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIdsGlobal = this.loadViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			return null;

		// Global variables that need to be serialized for Spark as each job needs access to them
		final int[] blockSize = Import.csvStringToIntArray(blockSizeString);
		final URI xmlURI = this.xmlURI;
		final String label = this.label;
		final int downsampleXY = this.dsxy;
		final int downsampleZ = this.dsz;
		final double minIntensity = this.minIntensity == null ? Double.NaN : this.minIntensity;
		final double maxIntensity = this.maxIntensity == null ? Double.NaN : this.maxIntensity;
		final double sigma = this.sigma;
		final double threshold = this.threshold;
		final boolean findMin = (this.type == IP.MIN || this.type == IP.BOTH);
		final boolean findMax = (this.type == IP.MAX || this.type == IP.BOTH);
		final boolean onlyOverlappingRegions = overlappingOnly;
		final double combineDistance = SparkInterestPointDetection.combineDistance;
		final Localization localization = this.localization;
		final int maxSpots = this.maxSpots;
		final boolean maxSpotsPerOverlap = this.maxSpotsPerOverlap;
		final boolean prefetch = this.prefetch;
		final boolean storeIntensities = this.storeIntensities;
		final Integer medianFilter = this.medianFilter;

		System.out.println( "label: " + label );
		System.out.println( "sigma: " + sigma );
		System.out.println( "threshold: " + threshold );
		System.out.println( "type: " + type );
		System.out.println( "localization: " + localization );
		System.out.println( "minIntensity: " + minIntensity );
		System.out.println( "maxIntensity: " + maxIntensity );
		System.out.println( "downsampleXY: " + downsampleXY );
		System.out.println( "downsampleZ: " + downsampleZ );
		System.out.println( "overlappingOnly: " + onlyOverlappingRegions );
		System.out.println( "onlyCompareOverlapTiles: " + onlyCompareOverlapTiles );
		System.out.println( "prefetching: " + prefetch );
		if ( maxSpots > 0 ) {
			System.out.println( "maxSpots: " + maxSpots );
			System.out.println( "maxSpotsPerOverlap: " + maxSpotsPerOverlap );
		}
		System.out.println( "blockSize: " + Util.printCoordinates( blockSize ) );
		System.out.println( "medianFilter: " + medianFilter );
		System.out.println( "storeIntensities: " + storeIntensities );

		//
		// assemble all intervals that need to be processed
		//
		final ArrayList< Pair< ViewId, Interval > > toProcess = new ArrayList<>();
		final HashMap< ViewId, long[] > downsampledDimensions = new HashMap<>();

		// assemble all pairs for parallelization with Spark
		final ArrayList< Tuple2< ViewId, ViewId > > metadataJobs = new ArrayList<>();

		final HashMap< ViewId, AffineTransform3D > registrations =
				TransformVirtual.adjustAllTransforms(
						viewIdsGlobal,
						dataGlobal.getViewRegistrations().getViewRegistrations(),
						Double.NaN,
						Double.NaN );

		for ( final ViewId viewDesc : viewIdsGlobal )
		{
			final ViewId viewId = new ViewId( viewDesc.getTimePointId(), viewDesc.getViewSetupId() );

			if ( onlyOverlappingRegions )
			{
				for ( final ViewId otherViewId : OverlappingViews.findAllOverlappingViewsFor( viewId, dataGlobal, registrations, viewIdsGlobal ) )
				{
					if ( !otherViewId.equals( viewId ) )
					{
						if ( onlyCompareOverlapTiles )
						{
							final ViewDescription vd = dataGlobal.getSequenceDescription().getViewDescription( viewId );
							final ViewDescription othervd = dataGlobal.getSequenceDescription().getViewDescription( otherViewId );
							if ( viewId.getTimePointId() == otherViewId.getTimePointId() && vd.getViewSetup().getChannel().getId() == othervd.getViewSetup().getChannel().getId() )
								metadataJobs.add( new Tuple2<>( viewId, new ViewId( otherViewId.getTimePointId(), otherViewId.getViewSetupId() ) ) );
						}
						else
						{
							metadataJobs.add( new Tuple2<>( viewId, new ViewId( otherViewId.getTimePointId(), otherViewId.getViewSetupId() ) ) );
						}
					}
				}
			}
			else
			{
				metadataJobs.add( new Tuple2<>( viewId, null ) );
			}
		}

		final SparkConf conf = new SparkConf().setAppName("SparkInterestPointDetection");

		if ( localSparkBindAddress )
			conf.set("spark.driver.bindAddress", "127.0.0.1");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final JavaRDD<Tuple2<ViewId, ViewId>> metadataJobsSpark = sc.parallelize( metadataJobs, Math.min( Spark.maxPartitions, metadataJobs.size() ) );

		final JavaRDD< ArrayList< Tuple4< ViewId, long[], long[], long[] > > > metadataJobRDD = metadataJobsSpark.map( metaData ->
		{
			final SpimData2 dataLocal = Spark.getSparkJobSpimData2( xmlURI );

			final ViewDescription vd = dataLocal.getSequenceDescription().getViewDescription( metaData._1() );
			final ImgLoader imgLoader = dataLocal.getSequenceDescription().getImgLoader();

			final long[] ds = new long[] { downsampleXY, downsampleXY, downsampleZ };

			if ( overlappingOnly )
				System.out.println( "Fetching metadata for " + Group.pvid( vd ) + " <=> " + Group.pvid( metaData._2() ) + ", level " + Arrays.toString( ds ));
			else
				System.out.println( "Fetching metadata for " + Group.pvid( vd ) + ", level " + Arrays.toString( ds ));
			

			// load mipmap transform and bounds
			// TODO: can we load the dimensions without (Virtually) opening the image?
			final Pair<RandomAccessibleInterval, AffineTransform3D> input = openAndDownsample(
					imgLoader,
					vd,
					ds,
					true );

			final ArrayList< Tuple4< ViewId, long[], long[], long[] > > resultIntervals = new ArrayList<>();

			if ( overlappingOnly )
			{
				final ViewId otherViewId = metaData._2();

				final AffineTransform3D mipmapTransform = input.getB(); // maps downsampled image into global coordinate system
				final AffineTransform3D t1 = mipmapTransform.inverse(); // maps global coordinates into coordinate system of the downsampled image

				//
				// does it overlap?
				//
				final Dimensions dim = ViewUtil.getDimensions( dataLocal, vd );
				final Dimensions dimOtherViewId = ViewUtil.getDimensions( dataLocal, otherViewId );
				final ViewDescription vdOtherViewId = dataLocal.getSequenceDescription().getViewDescription( vd );

				final ViewRegistration reg = ViewUtil.getViewRegistration( dataLocal, vd );
				final ViewRegistration regOtherViewId = ViewUtil.getViewRegistration( dataLocal, otherViewId );

				// load other mipmap transform
				final AffineTransform3D mipmapTransformOtherViewId = new AffineTransform3D();

				openAndDownsample(imgLoader, vdOtherViewId, mipmapTransformOtherViewId, ds, true, true );

				// map the other view into the local coordinate space of the view we find interest points in
				// apply inverse of the mipmap transform of each
				final AffineTransform3D t2 = regOtherViewId.getModel().preConcatenate( reg.getModel().inverse() ).preConcatenate( mipmapTransformOtherViewId.inverse() );

				final Interval boundingBox = Intervals.smallestContainingInterval( t1.estimateBounds(new FinalInterval( dim ) ) );
				final Interval boundingBoxOther = Intervals.smallestContainingInterval( t2.estimateBounds( new FinalInterval( dimOtherViewId ) ) );

				if ( ViewUtil.overlaps( boundingBox, boundingBoxOther ) )
				{
					final Interval intersectionBoxes = Intervals.intersect( boundingBox, boundingBoxOther );
					final Interval intersection = Intervals.intersect( input.getA(), intersectionBoxes ); // make sure it fits (e.g. rounding errors)

					//final long size = ViewUtil.size( intersection );
					//System.out.println( "intersectionBoxes=" + Util.printInterval( intersectionBoxes ) );
					//System.out.println( "intersection=" + Util.printInterval( intersection ) + ", size (#px)=" + size );
					//maxIntervalSize = Math.max( maxIntervalSize, size );

					resultIntervals.add( new Tuple4<>( metaData._1(), intersection.minAsLongArray(), intersection.maxAsLongArray(), input.getA().dimensionsAsLongArray() ) );
				}

			}
			else
			{
				resultIntervals.add( new Tuple4<>( metaData._1(), input.getA().minAsLongArray(), input.getA().maxAsLongArray(), input.getA().dimensionsAsLongArray() ));
			}

			return resultIntervals;
		});

		metadataJobRDD.collect().forEach(
				l -> l.forEach( md -> {
					toProcess.add(new ValuePair<ViewId, Interval>(md._1(), new FinalInterval(md._2(), md._3())));
					downsampledDimensions.put(md._1(), md._4());
				}));

		long maxIntervalSize = 0;

		if ( overlappingOnly )
		{
			for ( final Pair<ViewId, Interval> pair : toProcess )
			{
				final long size = ViewUtil.size( pair.getB() );
	
				//System.out.println( "intersectionBoxes=" + Util.printInterval( intersectionBoxes ) );
				//System.out.println( "intersection=" + Util.printInterval( interval ) + ", size (#px)=" + size );
	
				maxIntervalSize = Math.max( maxIntervalSize, size );
			}
		}

		//
		// turn all areas into grids and serializable objects (ViewId, intervalOffset, gridEntry)
		//
		final ArrayList< Tuple3<ViewId, long[], long[][] > > sparkProcess = new ArrayList<>();

		System.out.println( "The following intervals will be processed:");

		for ( final Pair< ViewId, Interval > pair : toProcess )
		{
			final List<long[][]> grid = Grid.create( pair.getB().dimensionsAsLongArray(), blockSize );
			final long[] intervalOffset = pair.getB().minAsLongArray();
			final ViewId viewId = new ViewId( pair.getA().getTimePointId(), pair.getA().getViewSetupId() );

			grid.forEach( gridEntry -> {
				sparkProcess.add( new Tuple3<>( viewId, intervalOffset, gridEntry ) );

				final long[] superBlockMin = new long[ intervalOffset.length ];
				Arrays.setAll( superBlockMin, d -> gridEntry[ 0 ][ d ] + intervalOffset[ d ] );

				final long[] superBlockMax = new long[ intervalOffset.length ];
				Arrays.setAll( superBlockMax, d -> superBlockMin[ d ] + gridEntry[ 1 ][ d ] - 1 );

				// expand each interval boundary that is within an image by one, otherwise there are gaps between neighboring blocks
				// as each block does "true convolutions" for the 3x3x3 min/max finding
				final long[] dim = downsampledDimensions.get( pair.getA() );
				for ( int d = 0; d < superBlockMin.length; ++d )
				{
					if ( superBlockMin[ d ] > 0 )
						--superBlockMin[ d ];

					if ( superBlockMax[ d ] < dim[ d ] - 1 )
						++superBlockMax[ d ];
				}

				System.out.println( "Processing " + Group.pvid(pair.getA()) + ", " + Util.printInterval( new FinalInterval(superBlockMin, superBlockMax) ) + " of full interval " + Util.printInterval( pair.getB() ) );
			});
		}

		System.out.println( "Total number of jobs for interest point detection: " + sparkProcess.size() );

		if ( sparkProcess.size() == 0 )
		{
			System.out.println( "Nothing to do, stopping." );
			System.exit( 0 );
		}

		// create temporary N5 folder
		final String tempLocation = URITools.appendName( dataGlobal.getBasePathURI(), InterestPointsN5.baseN5 );
		final URI tempURI = URITools.toURI( tempLocation );
		final String tempDataset = "spark_tmp_" + System.currentTimeMillis() + "_" + new Random( System.nanoTime() ).nextInt();

		System.out.println( "Creating temporary N5 for dataset for spark jobs in '" + tempURI + ":/" + tempDataset + "'" );

		final N5Writer n5Writer = URITools.instantiateN5Writer( StorageFormat.N5, tempURI );
		n5Writer.createGroup( tempDataset );

		// returning all points can exceed Spark boundaries, save it to N5 and load instead
		// e.g. Total size of serialized results of 4317 tasks (1024.6 MiB) is bigger than spark.driver.maxResultSize (1024.0 MiB)
		final JavaRDD<Tuple3<ViewId, long[], long[][]>> rddJob = sc.parallelize( sparkProcess, Math.min( Spark.maxPartitions, sparkProcess.size() ) );

		// return ViewId, interval, filename for serialized SparkIPResults[locations, intensities]
		final JavaRDD< Tuple3< ViewId, long[][], String> > rddResult = rddJob.map( serializedInput ->
		{
			final SpimData2 data = Spark.getSparkJobSpimData2( xmlURI );
			final ViewId viewId = serializedInput._1();
			final ViewDescription vd = data.getSequenceDescription().getViewDescription( serializedInput._1() );

			// The min coordinates of the block that this job processes (in pixels)
			final long[] superBlockMin = new long[ serializedInput._2().length ];
			Arrays.setAll( superBlockMin, d -> serializedInput._3()[ 0 ][ d ] + serializedInput._2()[ d ] );

			// The size of the block that this job renders (in pixels)
			final long[] superBlockSize = serializedInput._3()[ 1 ];

			// The min coordinates of the block that this job processes (in pixels)
			final long[] superBlockMax = new long[ serializedInput._2().length ];
			Arrays.setAll( superBlockMax, d -> superBlockMin[ d ] + superBlockSize[ d ] - 1 );

			// the interval this Spark job will process
			final Interval processInterval = new FinalInterval( superBlockMin, superBlockMax );

			// the parameters for Difference-of-Gaussian (DoG)
			final DoGParameters dog = new DoGParameters();

			dog.imgloader = data.getSequenceDescription().getImgLoader();
			dog.toProcess = new ArrayList< ViewDescription >( Arrays.asList( vd ) );

			dog.localization = localization == Localization.NONE ? 0 : 1;
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

			//
			// runs virtual downsampling so it only loads what it needs
			// ideally only run with pre-computed downsample steps for efficiency
			//
			final Pair<RandomAccessibleInterval, AffineTransform3D> input = openAndDownsample(
						dog.imgloader,
						vd,
						new long[] { dog.downsampleXY, dog.downsampleXY, dog.downsampleZ },
						true );

			System.out.println( "Processing " + Group.pvid(viewId) + ", " + Util.printInterval( processInterval ) + " of full interval " + Util.printInterval( input.getA() ) );

			if ( prefetch )
			{
				// how big is the biggest sigma? It defines the overlap with neighboring blocks that we need
				final Pair< double[][], Float > sigmas = DoGImgLib2.computeSigmas( (float)dog.sigma, input.getA().numDimensions() );
				final int[] halfKernelSizes = Gauss3.halfkernelsizes( sigmas.getA()[ 1 ] );
				final int maxKernelSize = Collections.max( Arrays.stream( halfKernelSizes ).boxed().collect( Collectors.toList()) );

				// here we put in the inverse mipmap transform and pretend its a fusion so we can re-use Tobi's code
				// that finds which blocks need to be prefetched from an input image
				final List< PrefetchPixel< ? > > prefetchBlocks = ViewUtil.findOverlappingBlocks( data, viewId, processInterval, input.getB().inverse(), maxKernelSize );

				System.out.println( "Prefetching " + prefetchBlocks.size() + " blocks for " + Group.pvid(viewId) + ", " + Util.printInterval( processInterval ) );

				final ExecutorService prefetchExecutor = Executors.newCachedThreadPool(); //Executors.newFixedThreadPool( SparkAffineFusion.N_PREFETCH_THREADS );
				prefetchExecutor.invokeAll( prefetchBlocks );
				prefetchExecutor.shutdown();
			}

			final RandomAccessibleInterval inputImage;

			if ( medianFilter != null && medianFilter > 0 )
			{
				inputImage = LazyBackgroundSubtract.init(
						(RandomAccessible)Views.extendMirrorDouble( input.getA() ),
						new FinalInterval(input.getA()),
						medianFilter,
						new int[] {512, 512, 128} );
			}
			else
			{
				inputImage = input.getA();
			}

			final ExecutorService service = Threads.createFixedExecutorService( 1 );

			@SuppressWarnings("unchecked")
			final ArrayList< InterestPoint > ips = DoGImgLib2.computeDoG(
					(RandomAccessible)Views.extendMirrorDouble( inputImage ), // the entire image, extended to infinity
					null, // mask
					processInterval,
					dog.sigma,
					dog.threshold,
					dog.localization,
					dog.findMin,
					dog.findMax,
					dog.minIntensity,
					dog.maxIntensity,
					new int[] {128, 128, 64},
					service,
					dog.cuda,
					dog.deviceCUDA,
					dog.accurateCUDA,
					dog.percentGPUMem );

			service.shutdown();

			if ( ips == null || ips.size() == 0 )
			{
				System.out.println( "No interest points found for " + Group.pvid(viewId) + ", " + Util.printInterval( processInterval ) );
				//return new Tuple4<>( serializedInput._1(), Spark.serializeInterval( processInterval ), null, null );
				return new Tuple3<>( viewId, Spark.serializeInterval( processInterval ), "" );
			}

			final double[] intensities;

			if ( storeIntensities || maxSpots > 0 )
			{
				System.out.println( "Retrieving intensities for interest points '" + label + "' for " + Group.pvid(viewId) + ", " + Util.printInterval( processInterval ) + " ... " );

				// for image interpolation
				final RealRandomAccessible<FloatType> rra = Views.interpolate(
						Views.extendBorder(
								Converters.convertRAI(
										(RandomAccessibleInterval<RealType>)(Object)input.getA(),
										(a,b) -> b.set( a.getRealFloat() ),
										new FloatType() ) ),
						new NLinearInterpolatorFactory<>() );
				final RealRandomAccess< FloatType> r = rra.realRandomAccess();

				intensities = new double[ ips.size() ];

				for ( int i = 0; i < ips.size(); ++i )
				{
					r.setPosition( ips.get( i ) );
					intensities[ i ] = r.get().get();
				}
			}
			else
			{
				intensities = null;
			}

			// correcting for downsampling
			System.out.println( "Correcting interest points '" + label + "', " + Group.pvid(viewId) + ", " + Util.printInterval( processInterval ) + " for downsampling ... " );

			DownsampleTools.correctForDownsampling( ips, input.getB() );

			//final double[][] points = new double[ ips.size() ][];

			//for ( int i = 0; i < ips.size(); ++i )
			//	points[ i ] = ips.get( i ).getL();

			System.out.println( "Returning " + ips.size() + " interest points '" + label + "' for " + Group.pvid(viewId) + ", " + Util.printInterval( processInterval ) + " ... " );

			// serialize -- actually we can't serialize because of cloud storage ... need to use N5
			String serializeDataset = Group.pvid( viewId ) + "_" + Arrays.toString( processInterval.minAsLongArray() ) + "_" + Arrays.toString( processInterval.maxAsLongArray() );
			serializeDataset = serializeDataset.replaceAll( " ", "" );
			serializeDataset = serializeDataset.replaceAll( "\\[", "_" );
			serializeDataset = serializeDataset.replaceAll( "\\]", "_" );

			final N5Writer n5WriterLocal = URITools.instantiateN5Writer( StorageFormat.N5, tempURI );

			if ( ips.size() > 0 )
			{
				final int n = ips.get( 0 ).getL().length;
				final double[] points = new double[ ips.size() * n ];

				int j = 0;
				for ( int i = 0; i < ips.size(); ++i )
					for ( int d = 0; d < n; ++d )
						points[ j++ ] = ips.get( i ).getL()[ d ];

				N5Utils.save(
						ArrayImgs.doubles( points, new long[] { n, ips.size() } ),
						n5WriterLocal,
						tempDataset + "/" + serializeDataset + "/points",
						new int[] { n, ips.size() },
						new ZstandardCompression() );
			}

			if ( intensities != null && intensities.length > 0 )
			{
				N5Utils.save(
						ArrayImgs.doubles( intensities, new long[] { intensities.length } ),
						n5WriterLocal,
						tempDataset + "/" + serializeDataset + "/intensities",
						new int[] { intensities.length },
						new ZstandardCompression() );
			}

			n5WriterLocal.close();

			// return ViewId, interval, filename for [locations, intensities]
			return new Tuple3<>( viewId, Spark.serializeInterval( processInterval ), serializeDataset );
		});


		rddResult.cache();
		rddResult.count();

		final List<Tuple3<ViewId, long[][], String>> results = rddResult.collect();
		final Set<String> datasetSet = new HashSet<String>();
		for (Tuple3<ViewId, long[][], String> res : results) {
            datasetSet.add(res._3());
        }
		final List<String> datasetNoDups = new ArrayList<>(datasetSet);

		// assemble all interest point intervals per ViewId
		final HashMap< ViewId, List< List< InterestPoint > > > interestPointsPerViewId = new HashMap<>();
		final HashMap< ViewId, List< List< Double > > > intensitiesPerViewId = new HashMap<>();
		final HashMap< ViewId, List< Interval > > intervalsPerViewId = new HashMap<>();

		for ( final Tuple3<ViewId, long[][], String> tuple : results )
		{
			final ViewId viewId = tuple._1();

			//if ( points != null && points.length > 0 )
			if ( n5Writer.datasetExists( tempDataset + "/" + tuple._3() + "/points" ))
			{
				// load from N5
				final Img<DoubleType> points = N5Utils.open( n5Writer, tempDataset + "/" + tuple._3() + "/points" );

				interestPointsPerViewId.putIfAbsent(viewId, new ArrayList<>() );
				interestPointsPerViewId.get( viewId ).add( Spark.deserializeInterestPoints(points) );

				intervalsPerViewId.putIfAbsent(viewId, new ArrayList<>() );
				intervalsPerViewId.get( viewId ).add( Spark.deserializeInterval( tuple._2() ) );

				if ( storeIntensities || maxSpots > 0 )
				{
					intensitiesPerViewId.putIfAbsent(viewId, new ArrayList<>() );

					if ( n5Writer.datasetExists( tempDataset + "/" + tuple._3() + "/intensities" ) )
					{
						// load from N5
						final Img<DoubleType> intensities = N5Utils.open( n5Writer, tempDataset + "/" + tuple._3() + "/intensities" );
						final ArrayList<Double> intensitiesList = new ArrayList<>();
						Views.flatIterable( intensities ).forEach( v -> intensitiesList.add( v.get() ) );
						intensitiesPerViewId.get( viewId ).add( intensitiesList );
					}
				}
			}
		}

		if ( !keepTemporaryN5 )
		{
			System.out.println( "Deleting temporary Spark files ... ");

			final JavaRDD<String> rdd = sc.parallelize( datasetNoDups, Math.min( Spark.maxPartitions, datasetNoDups.size() ) );

			rdd.foreach( boundingBox ->
			{
				final N5Writer n5WriterLocal = URITools.instantiateN5Writer( StorageFormat.N5, tempURI );

				if ( n5WriterLocal.datasetExists( tempDataset + "/" + boundingBox + "/points" ))
				{
					n5WriterLocal.remove( tempDataset + "/" + boundingBox + "/points" );

					if ( n5WriterLocal.datasetExists( tempDataset + "/" + boundingBox + "/intensities" ) )
						n5WriterLocal.remove( tempDataset + "/" + boundingBox + "/intensities" );

					n5WriterLocal.close();
				}
			});

			n5Writer.remove( tempDataset );

			System.out.println( "All deleted.");
		}

		sc.close();

		System.out.println( "Computed all interest points, statistics:" );

		// assemble all ViewIds
		final ArrayList< ViewId > viewIds = new ArrayList<>( interestPointsPerViewId.keySet() );
		Collections.sort( viewIds );

		// we need to filter per overlap before combining
		if ( maxSpotsPerOverlap && maxSpots > 0 )
		{
			for ( final ViewId viewId : viewIds )
			{
				final List< List< InterestPoint > > ipsList = interestPointsPerViewId.get( viewId );
				final List< List< Double > > intensitiesList = intensitiesPerViewId.get( viewId );
				final List< Interval > intervalsList = intervalsPerViewId.get( viewId ); // note: in downsampled coordinates(!)

				// find all intervals of this view, then assign points to it
				final List< Tuple3< Interval, List< InterestPoint >, List< Double > > > intervalData = new ArrayList<>();

				// for each toProcess block do
				for ( final Pair< ViewId, Interval > p : toProcess )
				{
					if ( p.getA().equals( viewId ) )
					{
						final Interval toProcessInterval = p.getB();

						final List< InterestPoint > ipsBlock = new ArrayList<>();
						final List< Double > intensitiesBlock = new ArrayList<>();

						// figure out which computed blockIntervals fall into this toProcessInterval
						for ( int l = 0; l < ipsList.size(); ++l )
						{
							final Interval blockInterval = intervalsList.get( l );

							if ( Intervals.contains( toProcessInterval, blockInterval ) )
							{
								ipsBlock.addAll( ipsList.get( l ) );
								intensitiesBlock.addAll( intensitiesList.get( l ) );
							}
						}

						intervalData.add( new Tuple3<>( toProcessInterval, ipsBlock, intensitiesBlock ) );
					}
				}

				// to later put back into interestPointsPerViewId and intensitiesPerViewId
				interestPointsPerViewId.get( viewId ).clear();
				intensitiesPerViewId.get( viewId ).clear();

				// now filter each interval of each view
				for ( final Tuple3< Interval, List< InterestPoint >, List< Double > > tuple : intervalData )
				{
					final int myMaxSpots = (int)Math.round( maxSpots * ( (double)ViewUtil.size( tuple._1() ) /(double)maxIntervalSize ) );

					if ( myMaxSpots > 0 && myMaxSpots < tuple._2().size() )
					{
						final int oldSize = tuple._2().size();
						filterPoints( tuple._2(), tuple._3(), myMaxSpots );
						System.out.println( "Filtered interval (limit=" + myMaxSpots + ") " + Util.printInterval( tuple._1() ) + " (" + Group.pvid( viewId ) + "): " + oldSize + " >>> " + tuple._2().size() );
					}
					else
					{
						System.out.println( "NOT filtered interval (limit=" + myMaxSpots + ") " + Util.printInterval( tuple._1() ) + " (" + Group.pvid( viewId ) + "): " + tuple._2().size() );
					}

					interestPointsPerViewId.get( viewId ).add( tuple._2() );
					intensitiesPerViewId.get( viewId ).add( tuple._3() );
				}
			}
		}

		// now combine all jobs and fix potential overlap (overlappingOnly)
		final HashMap< ViewId, List< InterestPoint > > interestPoints = new HashMap<>();
		final HashMap< ViewId, List< Double > > intensitiesIPs = new HashMap<>();

		for ( final ViewId viewId : viewIds )
		{
			final ArrayList< InterestPoint > myIps = new ArrayList<>();
			final ArrayList< Double > myIntensities = new ArrayList<>();

			final List< List< InterestPoint > > ipsList = interestPointsPerViewId.get( viewId );
			final List< List< Double > > intensitiesList;

			if ( storeIntensities || maxSpots > 0 )
				intensitiesList = intensitiesPerViewId.get( viewId );
			else
				intensitiesList = null;

			// combine points since overlapping areas might exist
			for ( int l = 0; l < ipsList.size(); ++l )
			{
				final List< InterestPoint > ips = ipsList.get( l );
				final List< Double > intensities;

				if ( storeIntensities || maxSpots > 0 )
					intensities = intensitiesList.get( l );
				else
					intensities = null;

				if ( !overlappingOnly || myIps.size() == 0 )
				{
					myIps.addAll( ips );

					if ( storeIntensities || maxSpots > 0 )
						myIntensities.addAll( intensities );
				}
				else
				{
					final KDTree< InterestPoint > tree = new KDTree<>(myIps, myIps);
					final NearestNeighborSearchOnKDTree< InterestPoint > search = new NearestNeighborSearchOnKDTree<>( tree );

					for ( int i = 0; i < ips.size(); ++i )
					{
						final InterestPoint ip = ips.get( i );
						search.search( ip );

						if ( search.getDistance() > combineDistance )
						{
							myIps.add( ip );

							if ( storeIntensities || maxSpots > 0 )
								myIntensities.add( intensities.get( i ) );
						}
					}
				}
			}

			if ( myIps.size() > 0 )
			{
				// we need to sort and assign new ids since order is assumed when loading corresponding interest points, and we will have duplicate ids otherwise
				final ArrayList< InterestPoint > myIpsNewId = new ArrayList<>();

				for ( int id = 0; id < myIps.size(); ++id )
					myIpsNewId.add( new InterestPoint( id, myIps.get( id ).getL() ) );

				System.out.println( Group.pvid( viewId ) + ": " + myIpsNewId.size() );

				if ( !maxSpotsPerOverlap && maxSpots > 0 && maxSpots < myIpsNewId.size() )
				{
					filterPoints( myIpsNewId, myIntensities, maxSpots );

					System.out.println( Group.pvid( viewId ) + " (after applying maxSpots): " + myIpsNewId.size() );
				}

				interestPoints.put(viewId, myIpsNewId);

				if ( storeIntensities )
					intensitiesIPs.put(viewId, myIntensities );
			}
			else
			{
				interestPoints.put(viewId, new ArrayList<>());

				System.out.println( Group.pvid( viewId ) + ": no points found." );
			}
		}

		if ( !dryRun )
		{
			// save interest points for ALL views that were processed, not only those where we found points
			// otherwise they are not saved into the XML and into the N5
			for ( final ViewId viewId : viewIdsGlobal )
				if ( interestPoints.get( viewId ) == null )
					interestPoints.put( viewId, new ArrayList<>() );

			// save XML
			System.out.println( "Saving XML and interest points ..." );

			final String params = "DOG (Spark) s=" + sigma + " t=" + threshold + " overlappingOnly=" + overlappingOnly + " min=" + findMin + " max=" + findMax +
					" downsampleXY=" + downsampleXY + " downsampleZ=" + downsampleZ + " minIntensity=" + minIntensity + " maxIntensity=" + maxIntensity;

			InterestPointTools.addInterestPoints( dataGlobal, label, interestPoints, params );

			new XmlIoSpimData2().save( dataGlobal, xmlURI );

			// store image intensities for interest points
			if( storeIntensities )
			{
				viewIdsGlobal.parallelStream().forEach( viewId ->
				{
					try
					{
						System.out.println( "Retrieving intensities for interest points '" + label + "' for " + Group.pvid(viewId) + " ... " );

						final InterestPointsN5 i = (InterestPointsN5)dataGlobal.getViewInterestPoints().getViewInterestPointLists( viewId ).getInterestPointList( label );

						final String datasetIntensities = i.ipDataset() + "/intensities";

						if ( interestPoints.get( viewId ).size() == 0 )
						{
							n5Writer.createDataset(
									datasetIntensities,
									new long[] {0},
									new int[] {1},
									DataType.FLOAT32,
									new ZstandardCompression());
						}
						else
						{
							List<Double> intensitiesList = intensitiesIPs.get( viewId );

							// 1 x N array (which is a 2D array)
							final FunctionRandomAccessible< FloatType > intensities =
									new FunctionRandomAccessible<>(
											2,
											(location, value) ->
											{
												final int index = location.getIntPosition( 1 );
												value.set( intensitiesList.get( index ).floatValue() );
											},
											FloatType::new );

							final RandomAccessibleInterval< FloatType > intensityData =
									Views.interval( intensities, new long[] { 0, 0 }, new long[] { 0, intensitiesList.size() - 1 } );

							N5Utils.save( intensityData, n5Writer, datasetIntensities, new int[] { 1, InterestPointsN5.defaultBlockSize }, new ZstandardCompression() );
						}

						System.out.println( "Saved: " + tempURI + "/" + datasetIntensities );
						
					}
					catch ( Exception e )
					{
						System.out.println( "Could not save intensities for: " + Group.pvid(viewId) + ": " + e  );
					}
				});
			}
		}

		n5Writer.close();

		System.out.println( "Done ..." );

		return null;
	}

	public static void filterPoints(
			final List< InterestPoint > myIps,
			final List< Double > myIntensities,
			final int maxSpots )
	{
		// filter for the brightnest N spots
		final ArrayList< Pair< Double, InterestPoint > > combinedList = new ArrayList<>();

		for ( int i = 0; i < myIps.size(); ++i )
			combinedList.add( new ValuePair<Double, InterestPoint>(myIntensities.get( i ), myIps.get( i )));

		// sort from large to small
		Collections.sort(combinedList, (a,b) -> b.getA().compareTo( a.getA() ) );

		myIps.clear();
		myIntensities.clear();

		for ( int i = 0; i < maxSpots; ++i )
		{
			myIntensities.add( combinedList.get( i ).getA() );
			myIps.add( new InterestPoint( i, combinedList.get( i ).getB().getL() ) ); // new id's again ...
		}
	}

	// TODO: this has been pushed up to the multiview-reconstruction code, use new version
	public static Pair<RandomAccessibleInterval, AffineTransform3D> openAndDownsample(
			final BasicImgLoader imgLoader,
			final ViewId vd,
			final long[] downsampleFactors,
			final boolean virtualDownsampling )
	{
		final AffineTransform3D mipMapTransform = new AffineTransform3D();

		final RandomAccessibleInterval img = openAndDownsample(imgLoader, vd, mipMapTransform, downsampleFactors, false, virtualDownsampling );

		return new ValuePair<RandomAccessibleInterval, AffineTransform3D>( img, mipMapTransform );
	}

	// TODO: this has been pushed up to the multiview-reconstruction code, use new version
	protected static final int[] ds = { 1, 2, 4, 8, 16, 32, 64, 128 };
	private static RandomAccessibleInterval openAndDownsample(
			final BasicImgLoader imgLoader,
			final ViewId vd,
			final AffineTransform3D mipMapTransform,
			long[] downsampleFactors,
			final boolean transformOnly, // only for ImgLib1 legacy code
			final boolean virtualDownsampling )
	{
		long dsx = downsampleFactors[0];
		long dsy = downsampleFactors[1];
		long dsz = (downsampleFactors.length > 2) ? downsampleFactors[ 2 ] : 1;

		RandomAccessibleInterval input = null;

		if ( ( dsx > 1 || dsy > 1 || dsz > 1 ) && MultiResolutionImgLoader.class.isInstance( imgLoader ) )
		{
			MultiResolutionImgLoader mrImgLoader = ( MultiResolutionImgLoader ) imgLoader;

			double[][] mipmapResolutions = mrImgLoader.getSetupImgLoader( vd.getViewSetupId() ).getMipmapResolutions();

			int bestLevel = 0;
			for ( int level = 0; level < mipmapResolutions.length; ++level )
			{
				double[] factors = mipmapResolutions[ level ];
				
				// this fails if factors are not ints
				final int fx = (int)Math.round( factors[ 0 ] );
				final int fy = (int)Math.round( factors[ 1 ] );
				final int fz = (int)Math.round( factors[ 2 ] );
				
				if ( fx <= dsx && fy <= dsy && fz <= dsz && contains( fx, ds ) && contains( fy, ds ) && contains( fz, ds ) )
					bestLevel = level;
			}

			final int fx = (int)Math.round( mipmapResolutions[ bestLevel ][ 0 ] );
			final int fy = (int)Math.round( mipmapResolutions[ bestLevel ][ 1 ] );
			final int fz = (int)Math.round( mipmapResolutions[ bestLevel ][ 2 ] );

			if ( mipMapTransform != null )
				mipMapTransform.set( mrImgLoader.getSetupImgLoader( vd.getViewSetupId() ).getMipmapTransforms()[ bestLevel ] );

			dsx /= fx;
			dsy /= fy;
			dsz /= fz;

			if ( !transformOnly )
			{
				input = mrImgLoader.getSetupImgLoader( vd.getViewSetupId() ).getImage( vd.getTimePointId(), bestLevel );
			}
		}
		else
		{
			if ( !transformOnly )
			{
				input = imgLoader.getSetupImgLoader( vd.getViewSetupId() ).getImage( vd.getTimePointId() );
			}

			if ( mipMapTransform != null )
				mipMapTransform.identity();
		}

		if ( mipMapTransform != null )
		{
			// the additional downsampling (performed below)
			final AffineTransform3D additonalDS = new AffineTransform3D();
			additonalDS.set( dsx, 0.0, 0.0, 0.0, 0.0, dsy, 0.0, 0.0, 0.0, 0.0, dsz, 0.0 );

			// we need to concatenate since when correcting for the downsampling we first multiply by whatever
			// the manual downsampling did, and just then by the scaling+offset of the HDF5
			//
			// Here is an example of what happens (note that the 0.5 pixel shift is not changed)
			// HDF5 MipMap Transform   (2.0, 0.0, 0.0, 0.5, 0.0, 2.0, 0.0, 0.5, 0.0, 0.0, 2.0, 0.5)
			// Additional Downsampling (4.0, 0.0, 0.0, 0.0, 0.0, 4.0, 0.0, 0.0, 0.0, 0.0, 2.0, 0.0)
			// Resulting model         (8.0, 0.0, 0.0, 0.5, 0.0, 8.0, 0.0, 0.5, 0.0, 0.0, 4.0, 0.5)
			mipMapTransform.concatenate( additonalDS );
		}

		if ( !transformOnly )
		{
			if ( virtualDownsampling )
			{
				for ( ;dsx > 1; dsx /= 2 )
					input = LazyDownsample2x.init( Views.extendBorder( input ), input, new FloatType(), DoGImgLib2.blockSize, 0 );

				for ( ;dsy > 1; dsy /= 2 )
					input = LazyDownsample2x.init( Views.extendBorder( input ), input, new FloatType(), DoGImgLib2.blockSize, 1 );

				for ( ;dsz > 1; dsz /= 2 )
					input = LazyDownsample2x.init( Views.extendBorder( input ), input, new FloatType(), DoGImgLib2.blockSize, 2 );
			}
			else
			{
				// note: every pixel is read exactly once, therefore caching the virtual input would not give any advantages
				for ( ;dsx > 1; dsx /= 2 )
					input = Downsample.simple2x( input, new boolean[]{ true, false, false } );

				for ( ;dsy > 1; dsy /= 2 )
					input = Downsample.simple2x( input, new boolean[]{ false, true, false } );

				for ( ;dsz > 1; dsz /= 2 )
					input = Downsample.simple2x( input, new boolean[]{ false, false, true } );
			}
		}

		return input;
	}

	private static final boolean contains( final int i, final int[] values )
	{
		for ( final int j : values )
			if ( i == j )
				return true;

		return false;
	}

	public static void main(final String... args)
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new SparkInterestPointDetection()).execute(args));
	}
}
