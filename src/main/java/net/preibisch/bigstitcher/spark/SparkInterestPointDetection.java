package net.preibisch.bigstitcher.spark;

import java.io.File;
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
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import mpicbg.spim.data.generic.sequence.BasicImgLoader;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.sequence.MultiResolutionImgLoader;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Dimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.KDTree;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccess;
import net.imglib2.RealRandomAccessible;
import net.imglib2.converter.Converters;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
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
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.bigstitcher.spark.util.ViewUtil;
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
import scala.Tuple2;

public class SparkInterestPointDetection extends AbstractSelectableViews implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = -7654397945854689628L;

	public static double combineDistance = 0.5; // when to merge interestpoints that were found in overlapping ROIS (overlappingOnly)

	public enum IP { MIN, MAX, BOTH };

	@Option(names = { "-l", "--label" }, required = true, description = "label for the interest points (e.g. beads)")
	private String label = null;

	@Option(names = { "-s", "--sigma" }, required = true, description = "sigma for segmentation, e.g. 1.8")
	private Double sigma = null;

	@Option(names = { "-t", "--threshold" }, required = true, description = "threshold for segmentation, e.g. 0.008")
	private Double threshold = null;

	@Option(names = { "--type" }, description = "the type of interestpoints to find, MIN, MAX or BOTH (default: MAX)")
	private IP type = IP.MAX;

	@Option(names = { "--overlappingOnly" }, description = "only find interest points in areas that currently overlap with another view (default: false)")
	private boolean overlappingOnly = false;

	@Option(names = { "--storeIntensities" }, description = "creates an additional N5 dataset with the intensities of each detection, linearly interpolated (default: false)")
	private boolean storeIntensities = false;

	@Option(names = { "-i0", "--minIntensity" }, description = "min intensity for segmentation, e.g. 0.0 (default: load from image)")
	private Double minIntensity = null;

	@Option(names = { "-i1", "--maxIntensity" }, description = "max intensity for segmentation, e.g. 2048.0 (default: load from image)")
	private Double maxIntensity = null;


	@Option(names = { "-dsxy", "--downsampleXY" }, description = "downsampling in XY to use for segmentation, e.g. 4 (default: 2)")
	private Integer dsxy = 2;

	@Option(names = { "-dsz", "--downsampleZ" }, description = "downsampling in Z to use for segmentation, e.g. 2 (default: 1)")
	private Integer dsz = 1;

	@Override
	public Void call() throws Exception
	{
		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIdsGlobal = this.loadViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			return null;

		System.out.println( "label: " + label );
		System.out.println( "sigma: " + sigma );
		System.out.println( "threshold: " + threshold );
		System.out.println( "type: " + type );
		System.out.println( "minIntensity: " + minIntensity );
		System.out.println( "maxIntensity: " + maxIntensity );
		System.out.println( "downsampleXY: " + dsxy );
		System.out.println( "downsampleZ: " + dsz );
		System.out.println( "overlappingOnly: " + overlappingOnly );

		final ArrayList<int[]> serializedViewIds = Spark.serializeViewIdsForRDD( viewIdsGlobal );

		final SparkConf conf = new SparkConf().setAppName("SparkResaveN5");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final JavaRDD<int[]> rdd = sc.parallelize( serializedViewIds );

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

		// we need this in case we want to detect in overlapping areas only
		final int[][] allSerializedViewIds = Spark.serializeViewIds( viewIdsGlobal );

		final JavaPairRDD< Integer, int[] > rddResults = rdd.mapToPair( serializedView ->
		{
			final SpimData2 data = Spark.getSparkJobSpimData2( "", xmlPath );
			final ViewId viewId = Spark.deserializeViewId( serializedView );
			final ViewDescription vd = data.getSequenceDescription().getViewDescription( viewId );

			System.out.println( "Processing " + Group.pvid(viewId) + " ... " );

			if ( !vd.isPresent() )
			{
				System.out.println( Group.pvid(viewId) + " is not present. skipping." );
				return new Tuple2<>( -1, serializedView );
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

			final ArrayList< InterestPoint > ips;
			final Pair<RandomAccessibleInterval, AffineTransform3D> input;
			final AffineTransform3D mipmapTransform;

			if ( onlyOverlappingRegions )
			{
				//
				// runs virtual downsampling so it only loads what it needs
				// ideally only run with pre-computed downsample steps for efficiency
				//
				input = openAndDownsample(
							dog.imgloader,
							vd,
							new long[] { dog.downsampleXY, dog.downsampleXY, dog.downsampleZ },
							true );

				mipmapTransform = input.getB();
				ips = new ArrayList<>();

				// for each overlapping area do (we merge the points later)
				for ( final ViewId otherViewId : Spark.deserializeViewIds( allSerializedViewIds ) )
				{
					if ( otherViewId.equals( viewId ) )
						continue;

					//
					// does it overlap?
					//
					final Dimensions dim = ViewUtil.getDimensions( data, viewId );
					final Dimensions dimOtherViewId = ViewUtil.getDimensions( data, otherViewId );

					final ViewRegistration reg = ViewUtil.getViewRegistration( data, viewId );
					final ViewRegistration regOtherViewId = ViewUtil.getViewRegistration( data, otherViewId );

					// load other mipmap transform
					final AffineTransform3D mipmapTransformOtherViewId = new AffineTransform3D();
					openAndDownsample(dog.imgloader, vd, mipmapTransformOtherViewId, new long[] { dog.downsampleXY, dog.downsampleXY, dog.downsampleZ }, true, true );

					// map the other view into the local coordinate space of the view we find interest points in
					// apply inverse of the mipmap transform of each
					final AffineTransform3D t1 = mipmapTransform.inverse();
					final AffineTransform3D t2 = regOtherViewId.getModel().preConcatenate( reg.getModel().inverse() ).preConcatenate( mipmapTransformOtherViewId.inverse() );

					final Interval boundingBox = Intervals.largestContainedInterval( t1.estimateBounds(new FinalInterval( dim ) ) );
					final Interval boundingBoxOther = Intervals.largestContainedInterval( t2.estimateBounds( new FinalInterval( dimOtherViewId ) ) );

					if ( ViewUtil.overlaps( boundingBox, boundingBoxOther ) )
					{
						final Interval intersectionBoxes = Intervals.intersect( boundingBox, boundingBoxOther );
						final Interval intersection = Intervals.intersect( input.getA(), intersectionBoxes ); // make sure it fits (e.g. rounding errors)

						System.out.println( "intersectionBoxes=" + Util.printInterval( intersectionBoxes ) );
						System.out.println( "intersection=" + Util.printInterval( intersectionBoxes ) );

						//
						// run DoG only in that area
						//
						final ArrayList< InterestPoint > localPoints = DoGImgLib2.computeDoG(
								Views.interval( input.getA(), intersection ),
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

						// combine points since overlapping areas might exist
						if ( ips.size() == 0 )
						{
							ips.addAll( localPoints );
						}
						else
						{
							final KDTree< InterestPoint > tree = new KDTree<>(ips, ips);
							final NearestNeighborSearchOnKDTree< InterestPoint > search = new NearestNeighborSearchOnKDTree<>( tree );

							for ( final InterestPoint ip : localPoints )
							{
								search.search( ip );
								if ( search.getDistance() > combineDistance )
									ips.add( ip );
							}
						}
					}
				}
			}
			else
			{
				input = openAndDownsample(
							dog.imgloader,
							vd,
							new long[] { dog.downsampleXY, dog.downsampleXY, dog.downsampleZ },
							false );

				mipmapTransform = input.getB();

				ips = DoGImgLib2.computeDoG(
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
			}

			service.shutdown();

			// correcting for downsampling
			System.out.println( "Correcting interest points '" + label + "', " + Group.pvid(viewId) + " for downsampling ... " );

			DownsampleTools.correctForDownsampling( ips, mipmapTransform );

			// save interest point N5
			if (!dryRun)
			{
				System.out.println( "Saving interest point '" + label + "' N5 for " + Group.pvid(viewId) + " ... " );
	
				final InterestPoints ipl = InterestPoints.newInstance( data.getBasePath(), viewId, label );
	
				ipl.setInterestPoints( ips );
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
	
					if ( ips.size() == 0 )
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
						// for image interpolation
						final RealRandomAccessible<FloatType> rra = Views.interpolate(
								Views.extendBorder(
										Converters.convertRAI(
												(RandomAccessibleInterval<RealType>)(Object)input.getA(),
												(a,b) -> b.set( a.getRealFloat() ),
												new FloatType() ) ),
								new NLinearInterpolatorFactory<>() );
	
						final RealRandomAccess< FloatType> r = rra.realRandomAccess();
	
						// to undo the mipmap transform correction
						final AffineTransform3D invMM = mipmapTransform.inverse();
						final double[] tmp = new double[ ips.get( 0 ).getL().length ];
	
						// 1 x N array (which is a 2D array)
						final FunctionRandomAccessible< FloatType > intensities =
								new FunctionRandomAccessible<>(
										2,
										(location, value) ->
										{
											final int index = location.getIntPosition( 1 );
											final InterestPoint ip = ips.get( index );
	
											invMM.apply( ip.getL(), tmp );
											r.setPosition( tmp );
	
											value.set( r.get().get() );
										},
										FloatType::new );
	
						final RandomAccessibleInterval< FloatType > intensityData =
								Views.interval( intensities, new long[] { 0, 0 }, new long[] { 0, ips.size() - 1 } );
	
						N5Utils.save( intensityData, n5Writer, datasetIntensities, new int[] { 1, InterestPointsN5.defaultBlockSize }, new GzipCompression() );
					}
	
					IOFunctions.println( "Saved: " + new File( i.getBaseDir().getAbsolutePath(), InterestPointsN5.baseN5 ).getAbsolutePath() + ":/" + datasetIntensities );
	
					n5Writer.close();
				}
			}

			System.out.println( "Finished " + Group.pvid(viewId) + "." );

			return new Tuple2<>( ips.size(), serializedView );
		});

		rddResults.cache();
		rddResults.count();

		final List<Tuple2<Integer, int[]>> results = rddResults.collect();

		System.out.println( "Computed all interest points, statistics:" );

		final HashMap< ViewId, List< InterestPoint > > interestPoints = new HashMap< ViewId, List< InterestPoint > >();

		for ( final Tuple2<Integer, int[] > tuple : results )
		{
			final ViewId viewId = Spark.deserializeViewId( tuple._2() );
			final int numDetection = tuple._1();

			if ( numDetection >= 0 )
			{
				interestPoints.put( viewId, new ArrayList<>() ); // just for saving the XML - it does not save the actual list
				System.out.println( Group.pvid( viewId ) + ": " + numDetection );
			}
			else
			{
				System.out.println( Group.pvid( viewId ) + ": missing view, skipping " );
			}
		}

		final String params = "DOG (Spark) s=" + sigma + " t=" + threshold + " overlappingOnly=" + overlappingOnly + " min=" + findMin + " max=" + findMax +
				" downsampleXY=" + downsampleXY + " downsampleZ=" + downsampleZ + " minIntensity=" + minIntensity + " maxIntensity=" + maxIntensity;

		InterestPointTools.addInterestPoints( dataGlobal, label, interestPoints, params );

		sc.close();

		if (!dryRun)
		{
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
