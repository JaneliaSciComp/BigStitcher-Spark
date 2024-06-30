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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import ij.ImageJ;
import mpicbg.spim.data.generic.sequence.BasicImgLoader;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewTransformAffine;
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
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.multithreading.SimpleMultiThreading;
import net.imglib2.neighborsearch.NearestNeighborSearchOnKDTree;
import net.imglib2.position.FunctionRandomAccessible;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.detection.LazyBackgroundSubtract;
import net.preibisch.bigstitcher.spark.util.Grid;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.bigstitcher.spark.util.ViewUtil;
import net.preibisch.bigstitcher.spark.util.ViewUtil.PrefetchPixel;
import net.preibisch.legacy.io.IOFunctions;
import net.preibisch.mvrecon.Threads;
import net.preibisch.mvrecon.fiji.plugin.interestpointdetection.DifferenceOfGUI;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.CorrespondingInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsN5;
import net.preibisch.mvrecon.process.downsampling.Downsample;
import net.preibisch.mvrecon.process.downsampling.DownsampleTools;
import net.preibisch.mvrecon.process.downsampling.lazy.LazyDownsample2x;
import net.preibisch.mvrecon.process.interestpointdetection.InterestPointTools;
import net.preibisch.mvrecon.process.interestpointdetection.methods.dog.DoGImgLib2;
import net.preibisch.mvrecon.process.interestpointdetection.methods.dog.DoGParameters;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import scala.Tuple3;
import scala.Tuple4;

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

	@Option(names = { "--overlappingOnly" }, description = "only find interest points in areas that currently overlap with another view (default: false)")
	protected boolean overlappingOnly = false;

	@Option(names = { "--storeIntensities" }, description = "creates an additional N5 dataset with the intensities of each detection, linearly interpolated (default: false)")
	protected boolean storeIntensities = false;

	@Option(names = { "-i0", "--minIntensity" }, required = true, description = "min intensity for segmentation, e.g. 0.0")
	protected Double minIntensity = null;

	@Option(names = { "-i1", "--maxIntensity" }, required = true, description = "max intensity for segmentation, e.g. 2048.0")
	protected Double maxIntensity = null;

	@Option(names = { "--prefetch" }, description = "prefetch all blocks required to process DoG in each Spark job using unlimited threads, useful in cloud environments (default: false)")
	protected boolean prefetch = false;


	@Option(names = "--blockSize", description = "blockSize for running the interest point detection - at the scale of detection (default: 512,512,128)")
	protected String blockSizeString = "512,512,128";

	@Option(names = { "--medianFilter" }, description = "divide by the median filtered image of the given radius prior to interest point detection, e.g. --medianFilter 10")
	protected Integer medianFilter = null;


	@Option(names = { "-dsxy", "--downsampleXY" }, description = "downsampling in XY to use for segmentation, e.g. 4 (default: 2)")
	protected Integer dsxy = 2;

	@Option(names = { "-dsz", "--downsampleZ" }, description = "downsampling in Z to use for segmentation, e.g. 2 (default: 1)")
	protected Integer dsz = 1;

	@Override
	public Void call() throws Exception
	{
		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIdsGlobal = this.loadViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			return null;

		// Global variables that need to be serialized for Spark as each job needs access to them
		final int[] blockSize = Import.csvStringToIntArray(blockSizeString);
		final String xmlPath = this.xmlPath;
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
		final boolean prefetch = this.prefetch;
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
		System.out.println( "prefetching: " + prefetch );
		System.out.println( "blockSize: " + Util.printCoordinates( blockSize ) );
		System.out.println( "medianFilter: " + medianFilter );

		//
		// assemble all intervals that need to be processed
		//
		final ArrayList< Pair< ViewId, Interval > > toProcess = new ArrayList<>();

		for ( final ViewId viewId : viewIdsGlobal )
		{
			final ViewDescription vd = dataGlobal.getSequenceDescription().getViewDescription( viewId );
			final ImgLoader imgLoader = dataGlobal.getSequenceDescription().getImgLoader();

			// load mipmap transform and bounds
			// TODO: can we load the dimensions without (Virtually) opening the image?
			final Pair<RandomAccessibleInterval, AffineTransform3D> input = openAndDownsample(
					imgLoader,
					vd,
					new long[] { downsampleXY, downsampleXY, downsampleZ },
					true );

			// only find interest points in regions that are currently overlapping with another view
			if ( onlyOverlappingRegions )
			{
				final ArrayList< Interval > allIntervals = new ArrayList<>();

				final AffineTransform3D mipmapTransform = input.getB(); // maps downsampled image into global coordinate system
				final AffineTransform3D t1 = mipmapTransform.inverse(); // maps global coordinates into coordinate system of the downsampled image

				for ( final ViewId otherViewId : viewIdsGlobal )
				{
					if ( otherViewId.equals( viewId ) )
						continue;

					//
					// does it overlap?
					//
					final Dimensions dim = ViewUtil.getDimensions( dataGlobal, viewId );
					final Dimensions dimOtherViewId = ViewUtil.getDimensions( dataGlobal, otherViewId );
					final ViewDescription vdOtherViewId = dataGlobal.getSequenceDescription().getViewDescription( viewId );

					final ViewRegistration reg = ViewUtil.getViewRegistration( dataGlobal, viewId );
					final ViewRegistration regOtherViewId = ViewUtil.getViewRegistration( dataGlobal, otherViewId );

					// load other mipmap transform
					final AffineTransform3D mipmapTransformOtherViewId = new AffineTransform3D();
					openAndDownsample(imgLoader, vdOtherViewId, mipmapTransformOtherViewId, new long[] { downsampleXY, downsampleXY, downsampleZ }, true, true );

					// map the other view into the local coordinate space of the view we find interest points in
					// apply inverse of the mipmap transform of each
					final AffineTransform3D t2 = regOtherViewId.getModel().preConcatenate( reg.getModel().inverse() ).preConcatenate( mipmapTransformOtherViewId.inverse() );

					final Interval boundingBox = Intervals.smallestContainingInterval( t1.estimateBounds(new FinalInterval( dim ) ) );
					final Interval boundingBoxOther = Intervals.smallestContainingInterval( t2.estimateBounds( new FinalInterval( dimOtherViewId ) ) );

					if ( ViewUtil.overlaps( boundingBox, boundingBoxOther ) )
					{
						final Interval intersectionBoxes = Intervals.intersect( boundingBox, boundingBoxOther );
						final Interval intersection = Intervals.intersect( input.getA(), intersectionBoxes ); // make sure it fits (e.g. rounding errors)

						System.out.println( "intersectionBoxes=" + Util.printInterval( intersectionBoxes ) );
						System.out.println( "intersection=" + Util.printInterval( intersection ) );

						allIntervals.add( intersection );
					}
				}

				// TODO: some sort of intersections might be useful
				// find the sum of intersections ...
				allIntervals.forEach( interval -> toProcess.add( new ValuePair<>( viewId, interval ) ) );
			}
			else
			{
				toProcess.add( new ValuePair<>( viewId, new FinalInterval( input.getA() ) ) );
			}
		}

		//
		// turn all areas into grids and serializable objects
		//
		final ArrayList< Tuple3<int[], long[], long[][] > > sparkProcess = new ArrayList<>();

		System.out.println( "The following intervals will be processed:");

		for ( final Pair< ViewId, Interval > pair : toProcess )
		{
			final List<long[][]> grid = Grid.create( pair.getB().dimensionsAsLongArray(), blockSize );
			final long[] intervalOffset = pair.getB().minAsLongArray();
			final int[] serializedViewId = Spark.serializeViewId( pair.getA() );

			grid.forEach( gridEntry -> {
				sparkProcess.add( new Tuple3<>( serializedViewId, intervalOffset, gridEntry ) );

				final long[] superBlockMin = new long[ intervalOffset.length ];
				Arrays.setAll( superBlockMin, d -> gridEntry[ 0 ][ d ] + intervalOffset[ d ] );

				final long[] superBlockMax = new long[ intervalOffset.length ];
				Arrays.setAll( superBlockMax, d -> superBlockMin[ d ] + gridEntry[ 1 ][ d ] - 1 );

				System.out.println( "Processing " + Group.pvid(pair.getA()) + ", " + Util.printInterval( new FinalInterval(superBlockMin, superBlockMax) ) + " of full interval " + Util.printInterval( pair.getB() ) );
			});
		}

		System.out.println( "Total number of jobs: " + sparkProcess.size() );
		
		final SparkConf conf = new SparkConf().setAppName("SparkInterestPointDetection");

		if ( localSparkBindAddress )
			conf.set("spark.driver.bindAddress", "127.0.0.1");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final JavaRDD<Tuple3<int[], long[], long[][] >> rddJob = sc.parallelize( sparkProcess );

		// return ViewId, interval, locations, intensities
		final JavaRDD< Tuple4<int[], long[][], double[][], double[] > > rddResult = rddJob.map( serializedInput ->
		{
			final SpimData2 data = Spark.getSparkJobSpimData2( "", xmlPath );
			final ViewId viewId = Spark.deserializeViewId( serializedInput._1() );
			final ViewDescription vd = data.getSequenceDescription().getViewDescription( viewId );

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
				final List< PrefetchPixel< ? > > prefetchBlocks = ViewUtil.findOverlappingBlocks( data, viewId, input.getB().inverse(), processInterval, maxKernelSize );

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
				return new Tuple4<>( serializedInput._1(), Spark.serializeInterval( processInterval ), null, null );
			}

			final double[] intensities;

			if ( storeIntensities )
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

			final double[][] points = new double[ ips.size() ][];

			for ( int i = 0; i < ips.size(); ++i )
				points[ i ] = ips.get( i ).getL();

			System.out.println( "Returning " + ips.size() + " interest points '" + label + "' for " + Group.pvid(viewId) + ", " + Util.printInterval( processInterval ) + " ... " );

			// return ViewId, interval, locations, intensities
			return new Tuple4<>( serializedInput._1(), Spark.serializeInterval( processInterval ), points, intensities );
		});


		rddResult.cache();
		rddResult.count();

		final List<Tuple4<int[], long[][], double[][], double[]>> results = rddResult.collect();

		sc.close();

		System.out.println( "Computed all interest points, statistics:" );

		final HashMap< ViewId, List< InterestPoint > > interestPoints = new HashMap<>();
		final HashMap< ViewId, List< Double > > intensitiesIPs = new HashMap<>();

		// assemble all interest point intervals per ViewId
		final HashMap< ViewId, List< List< InterestPoint > > > interestPointsPerViewId = new HashMap<>();
		final HashMap< ViewId, List< List< Double > > > intensitiesPerViewId = new HashMap<>();

		for ( final Tuple4<int[], long[][], double[][], double[]> tuple : results )
		{
			final ViewId viewId = Spark.deserializeViewId( tuple._1() );
			final double[][] points = tuple._3();

			if ( points != null && points.length > 0 )
			{
				interestPointsPerViewId.putIfAbsent(viewId, new ArrayList<>() );
				interestPointsPerViewId.get( viewId ).add( Spark.deserializeInterestPoints(points) );

				if ( storeIntensities )
				{
					intensitiesPerViewId.putIfAbsent(viewId, new ArrayList<>() );
					intensitiesPerViewId.get( viewId ).add( DoubleStream.of(tuple._4()).boxed().collect(Collectors.toList() ) );
				}
			}
		}

		// now combine all jobs and fix potential overlap (overlappingOnly)
		final ArrayList< ViewId > viewIds = new ArrayList<>( interestPointsPerViewId.keySet() );
		Collections.sort( viewIds );

		for ( final ViewId viewId : viewIds )
		{
			final ArrayList< InterestPoint > myIps = new ArrayList<>();
			final ArrayList< Double > myIntensities = new ArrayList<>();

			final List< List< InterestPoint > > ipsList = interestPointsPerViewId.get( viewId );
			final List< List< Double > > intensitiesList;

			if ( storeIntensities )
				intensitiesList = intensitiesPerViewId.get( viewId );
			else
				intensitiesList = null;

			// combine points since overlapping areas might exist
			for ( int l = 0; l < ipsList.size(); ++l )
			{
				final List< InterestPoint > ips = ipsList.get( l );
				final List< Double > intensities;

				if ( storeIntensities )
					intensities = intensitiesList.get( l );
				else
					intensities = null;

				if ( !overlappingOnly || myIps.size() == 0 )
				{
					myIps.addAll( ips );

					if ( storeIntensities )
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

							if ( storeIntensities )
								myIntensities.add( intensities.get( i ) );
						}
					}
				}
			}

			if ( myIps.size() > 0 )
			{
				// we need to sort and assign new ids since order is assumed when loading corresponding interest points, and we will have duplicate ids
				final ArrayList< InterestPoint > myIpsNewId = new ArrayList<>();

				for ( int id = 0; id < myIps.size(); ++id )
					myIpsNewId.add( new InterestPoint( id, myIps.get( id ).getL() ) );

				interestPoints.put(viewId, myIpsNewId);

				if ( storeIntensities )
					intensitiesIPs.put(viewId, myIntensities );

				System.out.println( Group.pvid( viewId ) + ": " + myIpsNewId.size() );
			}
			else
			{
				System.out.println( Group.pvid( viewId ) + ": no points found." );
			}
		}

		if ( !dryRun )
		{
			// save interest points
			for ( final ViewId viewId : viewIds )
			{
				System.out.println( "Saving interest point '" + label + "' N5 for " + Group.pvid(viewId) + " ... " );
				
				final InterestPoints ipl = InterestPoints.newInstance( dataGlobal.getBasePath(), viewId, label );
	
				ipl.setInterestPoints( interestPoints.get( viewId ) );
				ipl.setCorrespondingInterestPoints( new ArrayList< CorrespondingInterestPoints >() );
	
				ipl.saveInterestPoints( true );
				ipl.saveCorrespondingInterestPoints( true );

				// store image intensities for interest points
				if ( storeIntensities )
				{
					System.out.println( "Retrieving intensities for interest points '" + label + "' for " + Group.pvid(viewId) + " ... " );

					final InterestPointsN5 i = (InterestPointsN5)ipl;

					final N5FSWriter n5Writer = new N5FSWriter( new File( i.getBaseDir().getAbsolutePath(), InterestPointsN5.baseN5 ).getAbsolutePath() );
					final String datasetIntensities = i.ipDataset() + "/intensities";

					if ( interestPoints.get( viewId ).size() == 0 )
					{
						n5Writer.createDataset(
								datasetIntensities,
								new long[] {0},
								new int[] {1},
								DataType.FLOAT32,
								new GzipCompression());
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
	
						N5Utils.save( intensityData, n5Writer, datasetIntensities, new int[] { 1, InterestPointsN5.defaultBlockSize }, new GzipCompression() );
					}
	
					IOFunctions.println( "Saved: " + new File( i.getBaseDir().getAbsolutePath(), InterestPointsN5.baseN5 ).getAbsolutePath() + ":/" + datasetIntensities );
	
					n5Writer.close();
				}
			}

			// save XML
			final String params = "DOG (Spark) s=" + sigma + " t=" + threshold + " overlappingOnly=" + overlappingOnly + " min=" + findMin + " max=" + findMax +
					" downsampleXY=" + downsampleXY + " downsampleZ=" + downsampleZ + " minIntensity=" + minIntensity + " maxIntensity=" + maxIntensity;
	
			InterestPointTools.addInterestPoints( dataGlobal, label, interestPoints, params );

			System.out.println( "Saving XML (metadata only) ..." );
	
			new XmlIoSpimData2( null ).save( dataGlobal, xmlPath );
		}

		System.out.println( "Done ..." );

		return null;
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
