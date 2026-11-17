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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import org.janelia.scicomp.n5.zstandard.ZstandardCompression;

import mpicbg.spim.data.generic.sequence.BasicImgLoader;
import mpicbg.spim.data.sequence.ImgLoader;
import mpicbg.spim.data.sequence.MultiResolutionImgLoader;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.gauss3.Gauss3;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.detection.LazyBackgroundNormalize;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.bigstitcher.spark.util.ViewUtil;
import net.preibisch.bigstitcher.spark.util.ViewUtil.PrefetchPixel;
import net.preibisch.mvrecon.Threads;
import net.preibisch.mvrecon.fiji.plugin.interestpointdetection.DifferenceOfGUI;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.legacy.registration.bead.laplace.LaPlaceFunctions;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointFilter;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointFilter.Axis;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointFilter.Limit;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsN5;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsZarr;
import net.preibisch.mvrecon.process.downsampling.Downsample;
import net.preibisch.mvrecon.process.downsampling.DownsampleTools;
import net.preibisch.mvrecon.process.downsampling.lazy.LazyDownsample2x;
import net.preibisch.mvrecon.process.interestpointdetection.InterestPointTools;
import net.preibisch.mvrecon.process.interestpointdetection.OverlappingRegions;
import net.preibisch.mvrecon.process.interestpointdetection.NoiseFloor;
import net.preibisch.mvrecon.process.interestpointdetection.methods.dog.DoGImgLib2;
import net.preibisch.mvrecon.process.interestpointdetection.methods.scalespace.DoGScaleSpace;
import net.preibisch.mvrecon.process.interestpointdetection.methods.scalespace.ScaleSpaceParameters;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import util.Grid;
import util.URITools;

/**
 * Spark command-line tool for distributed Difference-of-Gaussian (DoG) interest point detection.
 * Each selected view is virtually downsampled ({@code --downsampleXY}, {@code --downsampleZ}),
 * optionally restricted to the regions that overlap other views ({@code --overlappingOnly}), cut into
 * blocks of {@code --blockSize} and every block is processed as one Spark task. Per-block detections
 * are written to a scratch N5 group ({@code interestpoints.n5/spark_tmp_*}, removed at the end together with a container
 * that holds nothing else), merged per view (dropping points closer than {@link OverlappingRegions#combineDistance} to an already collected
 * one), optionally limited to the brightest {@code --maxSpots} and staged by the view's task into the dataset's interest
 * point store ({@code interestpoints.zarr}, {@link InterestPointsZarr#stagePoints}); the driver registers the entries of
 * {@code --label} in the XML (after dropping the references to a label of that name) and its XML save commits them, unless
 * this is a dry run.
 */
public class SparkInterestPointDetection extends AbstractSelectableViews implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = -7654397945854689628L;

	/** Which Difference-of-Gaussian extrema to detect. */
	public enum IP { MIN, MAX, BOTH };
	/** Sub-pixel localization of the detected peaks. */
	public enum Localization {
		/** No refinement; integer peak positions. */
		NONE,
		/** Refine each peak to sub-pixel precision by a quadratic fit around the detected extremum. */
		QUADRATIC
	};
	/** The detection: the single-scale Difference-of-Gaussian of BigStitcher (DoGImgLib2) or the scale space (DoGScaleSpace), as in the BigStitcher GUI */
	public enum Method { DOG, SCALE_SPACE };

	@Option(names = { "-l", "--label" }, required = true, description = "label for the interest points (e.g. beads)")
	protected String label = null;

	@Option(names = { "--method" }, description = "the detection, DOG (the Difference-of-Gaussian of one sigma) or SCALE_SPACE (the scale-space Difference-of-Gaussian, finds structures of all sizes starting at --sigma and stores the sigma of each) (default: DOG)")
	protected Method method = Method.DOG;

	@Option(names = { "-s", "--sigma" }, required = true, description = "sigma for segmentation, e.g. 1.8; for SCALE_SPACE the sigma of the finest level (the initial blur), e.g. 1.6")
	protected Double sigma = null;

	@Option(names = { "-t", "--threshold" }, description = "threshold for segmentation, e.g. 0.008 (on the image normalized to [0,1]); either this or --noiseFactor")
	protected Double threshold = null;

	@Option(names = { "--noiseFactor" }, description = "derive the threshold from the noise: noiseFactor x the noise floor (the robust std of the finest Difference-of-Gaussian level, estimated in 16 random blocks of every view, pooled as the highest of the views), e.g. 2.5 as in the BigStitcher GUI; either this or --threshold")
	protected Double noiseFactor = null;

	@Option(names = { "--anisotropy" }, description = "z voxel / x voxel of the raw data (at full resolution), the Gaussians are then isotropic in physical units; the BigStitcher GUI suggests 0.8 x the calibration ratio since the PSF blurs z more than the calibration says (default: DOG isotropic in pixels, SCALE_SPACE the calibration ratio)")
	protected Double anisotropy = null;

	@Option(names = { "--steps" }, description = "SCALE_SPACE: steps per octave (default: 4)")
	protected Integer steps = null;

	@Option(names = { "--octaves" }, description = "SCALE_SPACE: number of octaves, -1 = as many as the image allows (default: -1)")
	protected Integer octaves = null;

	@Option(names = { "--finestLevel" }, negatable = true, description = "SCALE_SPACE: keep all spatial extrema of the finest level (sub-resolution structures like beads are no scale extrema), --no-finestLevel keeps scale extrema only (default: true)")
	protected Boolean finestLevel = null;

	@Option(names = { "--type" }, description = "the type of interestpoints to find, MIN, MAX or BOTH (default: MAX)")
	protected IP type = IP.MAX;

	@Option(names = { "--localization" }, description = "Subpixel localization method, NONE or QUADRATIC (default: QUADRATIC)")
	protected Localization localization = Localization.QUADRATIC;

	@Option(names = { "--overlappingOnly" }, description = "only find interest points in the regions of each view that overlap other selected views, by the current registrations (the regions are detected one by one and combined, points closer than 0.5 px to one of another region are dropped; a view without partner gets none). Partners are all other selected views, or with --onlyCompareOverlapTiles only other volumes. WARNING: other channels or illuminations of the same volume overlap it fully, so with several of them the regions are the whole view unless --onlyCompareOverlapTiles is set (default: false)")
	protected boolean overlappingOnly = false;

	@Option(names = { "--onlyCompareOverlapTiles" }, description = "with --overlappingOnly: a partner must be another volume of the same timepoint, i.e. differ in tile or angle (the rule of the BigStitcher GUI); other channels or illuminations of the same volume do not count. Before 2026-10-27 this kept partners of the same timepoint and channel (default: false)")
	protected boolean onlyCompareOverlapTiles = false;

	@Option(names = { "-i0", "--minIntensity" }, required = true, description = "min intensity for segmentation, e.g. 0.0")
	protected Double minIntensity = null;

	@Option(names = { "-i1", "--maxIntensity" }, required = true, description = "max intensity for segmentation, e.g. 2048.0")
	protected Double maxIntensity = null;

	@Option(names = { "--prefetch" }, description = "prefetch all blocks required to process DoG in each Spark job using unlimited threads, useful in cloud environments (default: false)")
	protected boolean prefetch = false;

	@Option(names = { "--keepTemporaryN5" }, description = "do NOT delete the temporary spark N5 of the blocks (interestpoints.n5/spark_tmp_*; the results themselves are in interestpoints.zarr) (default: false)")
	protected boolean keepTemporaryN5 = false;


	@Option(names = {"--maxSpots" }, description = "limit the number of spots per view (choose the brightest ones by their image intensity), e.g. --maxSpots 10000; the same as a later filter-interestpoints --keepPoints 10000 --keepBy intensity, which keeps the full detection as a base label (default: NO LIMIT)")
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

		if ( ( this.threshold == null ) == ( this.noiseFactor == null ) )
		{
			System.out.println( "Set either --threshold or --noiseFactor (the threshold is then noiseFactor x the pooled noise floor of the views)." );
			System.exit( 0 );
		}

		if ( this.method == Method.DOG && ( steps != null || octaves != null || finestLevel != null ) )
		{
			System.out.println( "--steps, --octaves and --finestLevel/--no-finestLevel belong to --method SCALE_SPACE." );
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
		final boolean findMin = (this.type == IP.MIN || this.type == IP.BOTH);
		final boolean findMax = (this.type == IP.MAX || this.type == IP.BOTH);
		final boolean onlyOverlappingRegions = overlappingOnly;
		final double combineDistance = OverlappingRegions.combineDistance; // the distance (full-resolution px) below which a point of one region of --overlappingOnly duplicates one of another, as in the GUI
		final Localization localization = this.localization;
		final int maxSpots = this.maxSpots;
		final boolean maxSpotsPerOverlap = this.maxSpotsPerOverlap;
		final boolean prefetch = this.prefetch;
		final Integer medianFilter = this.medianFilter;

		// the method and its parameters (the defaults of the scale space are the ones of the GUI, ScaleSpaceParameters)
		final Method method = this.method;
		final ScaleSpaceParameters defaults = new ScaleSpaceParameters();
		final int steps = this.steps == null ? defaults.steps : this.steps;
		final int octaves = this.octaves == null ? defaults.octaves : this.octaves;
		final boolean finestLevel = this.finestLevel == null ? defaults.detectFinestLevel : this.finestLevel;
		final double scaleSpaceCombineDistance = defaults.combineDistance;
		final double anisotropyZ = this.anisotropy == null ? Double.NaN : this.anisotropy;

		final SparkConf conf = new SparkConf().setAppName("SparkInterestPointDetection");

		if ( localSparkBindAddress )
		{
			conf.set("spark.driver.bindAddress", "127.0.0.1");
			conf.set("spark.driver.host", "localhost");
			org.apache.spark.util.Utils.setCustomHostname("localhost");
		}

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		// the threshold: given, or noiseFactor x the pooled noise floor of the views (one Spark job per view)
		final double threshold = this.threshold != null ? this.threshold :
				thresholdFromNoiseFloor( sc, viewIdsGlobal, xmlURI, method, sigma, steps, anisotropyZ, new long[] { downsampleXY, downsampleXY, downsampleZ }, minIntensity, maxIntensity, this.noiseFactor );

		System.out.println( "label: " + label );
		System.out.println( "method: " + method );
		System.out.println( "sigma: " + sigma );
		System.out.println( "threshold: " + threshold + ( this.noiseFactor != null ? " (" + this.noiseFactor + " x the noise floor)" : "" ) );
		if ( method == Method.SCALE_SPACE )
		{
			System.out.println( "steps: " + steps );
			System.out.println( "octaves: " + octaves );
			System.out.println( "finestLevel: " + finestLevel );
		}
		System.out.println( "anisotropy: " + ( Double.isNaN( anisotropyZ ) ? ( method == Method.SCALE_SPACE ? "calibration" : "none (isotropic in pixels)" ) : Double.toString( anisotropyZ ) ) );
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

		// the label replaces an existing one of the processed views: drop the correspondences of other views that point at its
		// old points (and the action history records of it) now, as the GUI does for a re-detection; before the tasks run, since a
		// staging file of the new points shadows the old entry for every read, and the XML save at the end stages the pruned lists
		if ( !dryRun )
		{
			final List< ViewId > existing = InterestPointTools.viewsWithLabel( dataGlobal, label, viewIdsGlobal );

			if ( !existing.isEmpty() )
			{
				System.out.println( "Label '" + label + "' exists in " + existing.size() + " of the selected views, removing the references to it before the re-detection ..." );
				InterestPointTools.removeReferences( dataGlobal, label, existing );
			}
		}

		//
		// assemble all intervals that need to be processed
		//
		final ArrayList< Pair< ViewId, Interval > > toProcess = new ArrayList<>();
		final HashMap< ViewId, long[] > downsampledDimensions = new HashMap<>();

		// assemble all pairs for parallelization with Spark: (view, partner) when only the overlapping regions are processed
		// (OverlappingRegions of multiview-reconstruction, as the GUI's "Only in regions overlapping other views": the partners are
		// the other selected views whose bounding boxes intersect the view's, with --onlyCompareOverlapTiles only the other volumes
		// of the same timepoint, i.e. another tile or angle), else (view, null) for the whole view
		final ArrayList< Tuple2< ViewId, ViewId > > metadataJobs = new ArrayList<>();

		for ( final ViewId viewDesc : viewIdsGlobal )
		{
			final ViewId viewId = new ViewId( viewDesc.getTimePointId(), viewDesc.getViewSetupId() );

			if ( onlyOverlappingRegions )
			{
				final List< ViewId > candidates = onlyCompareOverlapTiles ? OverlappingRegions.otherVolumes( dataGlobal, viewId, viewIdsGlobal ) : new ArrayList<>( viewIdsGlobal );

				for ( final ViewId otherViewId : OverlappingRegions.overlapping( dataGlobal, viewId, candidates ) )
					metadataJobs.add( new Tuple2<>( viewId, otherViewId ) );
			}
			else
			{
				metadataJobs.add( new Tuple2<>( viewId, null ) );
			}
		}

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
				// the region of this view that the partner covers, in the pixels of the downsampled image (OverlappingRegions: the
				// partner's full-resolution box mapped by the current registrations and this view's mipmap transform, clipped to the image)
				final Interval intersection = OverlappingRegions.intersection( dataLocal, metaData._1(), metaData._2(), new FinalInterval( input.getA() ), input.getB() );

				if ( intersection != null )
					resultIntervals.add( new Tuple4<>( metaData._1(), intersection.minAsLongArray(), intersection.maxAsLongArray(), input.getA().dimensionsAsLongArray() ) );
			}
			else
			{
				resultIntervals.add( new Tuple4<>( metaData._1(), input.getA().minAsLongArray(), input.getA().maxAsLongArray(), input.getA().dimensionsAsLongArray() ));
			}

			return resultIntervals;
		});

		// overlappingOnly yields one (view, intersection) entry per overlapping partner. Partners that
		// fully overlap (e.g. channels) produce identical intersections, so the same block would be
		// detected twice and its temp datasets written and deleted concurrently. Keep each once.
		final LinkedHashSet< String > seenIntervals = new LinkedHashSet<>();

		metadataJobRDD.collect().forEach(
				l -> l.forEach( md -> {
					final String key = Group.pvid( md._1() ) + Arrays.toString( md._2() ) + Arrays.toString( md._3() );
					if ( seenIntervals.add( key ) )
						toProcess.add(new ValuePair<ViewId, Interval>(md._1(), new FinalInterval(md._2(), md._3())));
					downsampledDimensions.put(md._1(), md._4());
				}));

		// a region inside another one of the same view adds nothing but its own blocks (the corner a diagonal neighbour covers
		// lies inside the strips of the side neighbours): dropped, as the GUI does (OverlappingRegions.dropContained)
		if ( overlappingOnly )
		{
			final LinkedHashMap< ViewId, List< Interval > > perView = new LinkedHashMap<>();
			for ( final Pair< ViewId, Interval > p : toProcess )
				perView.computeIfAbsent( p.getA(), k -> new ArrayList<>() ).add( p.getB() );

			final int before = toProcess.size();
			toProcess.clear();
			for ( final Map.Entry< ViewId, List< Interval > > e : perView.entrySet() )
				for ( final Interval interval : OverlappingRegions.dropContained( e.getValue() ) )
					toProcess.add( new ValuePair<>( e.getKey(), interval ) );

			System.out.println( "Overlapping regions: " + toProcess.size() + " (" + ( before - toProcess.size() ) + " inside another one dropped)" );
		}

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

		// return ViewId, interval, the group of the points of the block (with their attributes) in the temporary N5
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

			//
			// runs virtual downsampling so it only loads what it needs
			// ideally only run with pre-computed downsample steps for efficiency
			//
			// Note: virtual downsampling uses LazyDownsample2x which produces slightly different
			// results than Downsample.simple2x used by non-Spark DoG.java (typically ±1 point)
			//
			final Pair<RandomAccessibleInterval, AffineTransform3D> input = openAndDownsample(
						data.getSequenceDescription().getImgLoader(),
						vd,
						new long[] { downsampleXY, downsampleXY, downsampleZ },
						true );

			System.out.println( "Processing " + Group.pvid(viewId) + ", " + Util.printInterval( processInterval ) + " of full interval " + Util.printInterval( input.getA() ) );

			// the voxel size of the opened image per dimension relative to x, which the Gaussians compensate (null = isotropic in pixels)
			final double[] anisotropy = anisotropyOf( method, vd, input.getB(), anisotropyZ );

			if ( prefetch )
			{
				// how far do the Gaussians reach? It defines the overlap with neighboring blocks that we need
				final int maxKernelSize = prefetchHalo( method, sigma, anisotropy, input.getA().numDimensions(), steps, octaves, new FinalInterval( input.getA() ) );

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
				inputImage = LazyBackgroundNormalize.init(
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
			final ArrayList< InterestPoint > ips;

			// the detections carry their response, sigma (in pixels of the opened image) and the intensity of the image at their position
			try
			{
				if ( method == Method.SCALE_SPACE )
				{
					@SuppressWarnings("unchecked")
					final ArrayList< InterestPoint > detected = DoGScaleSpace.computeDoGScaleSpace(
							(RandomAccessible)Views.extendMirrorSingle( inputImage ), // the entire image, extended to infinity (must match ScaleSpace.java)
							new FinalInterval( input.getA() ),
							processInterval,
							null, // mask
							scaleSpaceParameters( sigma, threshold, steps, octaves, finestLevel, localization, findMin, findMax, minIntensity, maxIntensity, anisotropy ),
							service );
					ips = detected;
				}
				else
				{
					@SuppressWarnings("unchecked")
					final ArrayList< InterestPoint > detected = DoGImgLib2.computeDoG(
							(RandomAccessible)Views.extendMirrorSingle( inputImage ), // the entire image, extended to infinity (must match DoG.java)
							null, // mask
							processInterval,
							sigma,
							anisotropy,
							threshold,
							localization == Localization.NONE ? 0 : 1,
							findMin,
							findMax,
							minIntensity,
							maxIntensity,
							new int[] {32, 32, 16}, // smaller blocks for testing
							service,
							null, // CUDA
							null,
							false,
							0 );
					ips = detected;
				}
			}
			finally
			{
				service.shutdown();
			}

			if ( ips == null || ips.size() == 0 )
			{
				System.out.println( "No interest points found for " + Group.pvid(viewId) + ", " + Util.printInterval( processInterval ) );
				//return new Tuple4<>( serializedInput._1(), Spark.serializeInterval( processInterval ), null, null );
				return new Tuple3<>( viewId, Spark.serializeInterval( processInterval ), "" );
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
			InterestPointsN5.saveInterestPointsStatic( n5WriterLocal, tempDataset + "/" + serializeDataset, ips );
			n5WriterLocal.close();

			// return ViewId, interval, the group of the points (with their attributes)
			return new Tuple3<>( viewId, Spark.serializeInterval( processInterval ), serializeDataset );
		});


		rddResult.cache();
		rddResult.count();

		final List<Tuple3<ViewId, long[][], String>> results = rddResult.collect();

		// group the blocks (metadata only) per ViewId; the points themselves are loaded and combined per view on the Spark workers below
		final HashMap< ViewId, List< Tuple3<ViewId, long[][], String> > > blocksPerViewId = new HashMap<>();

		for ( final Tuple3<ViewId, long[][], String> tuple : results )
			blocksPerViewId.computeIfAbsent( tuple._1(), k -> new ArrayList<>() ).add( tuple );

		// serializable copies for the Spark tasks (Interval is not Serializable, maxIntervalSize is not effectively final)
		final ArrayList< Tuple2< ViewId, long[][] > > toProcessSerialized = new ArrayList<>();

		for ( final Pair< ViewId, Interval > p : toProcess )
			toProcessSerialized.add( new Tuple2<>( p.getA(), Spark.serializeInterval( p.getB() ) ) );

		final long maxIntervalSizeFinal = maxIntervalSize;

		// assemble all ViewIds
		final ArrayList< ViewId > viewIds = new ArrayList<>( blocksPerViewId.keySet() );
		Collections.sort( viewIds );

		// now combine all jobs and fix potential overlap (overlappingOnly), one Spark task per view; each task writes its view's
		// points to a staging file of the dataset's interest point store, the XML save on the driver commits them
		final URI baseDirURI = dataGlobal.getBasePathURI();

		System.out.println( "Combining interest points per view (" + results.size() + " blocks) ... " + new java.util.Date() );

		// returns ViewId and the number of combined points
		final List< Tuple2< ViewId, Integer > > combined = sc.parallelize( viewIds, Math.max( 1, viewIds.size() ) ).map( viewId ->
		{
			final N5Writer n5WriterLocal = URITools.instantiateN5Writer( StorageFormat.N5, tempURI );

			// load all blocks of this view from N5 (the points with their attributes, corrected for the downsampling)
			final List< List< InterestPoint > > ipsList = new ArrayList<>();
			final List< Interval > intervalsList = new ArrayList<>(); // note: in downsampled coordinates(!)

			for ( final Tuple3<ViewId, long[][], String> tuple : blocksPerViewId.get( viewId ) )
			{
				if ( tuple._3().isEmpty() ) // no points in this block, nothing was written
					continue;

				ipsList.add( InterestPointsN5.loadInterestPointsStatic( n5WriterLocal, tempDataset + "/" + tuple._3() ) );
				intervalsList.add( Spark.deserializeInterval( tuple._2() ) );
			}

			// we need to filter per overlap before combining
			if ( maxSpotsPerOverlap && maxSpots > 0 )
			{
				// find all intervals of this view, then assign points to it
				final List< Tuple2< Interval, List< InterestPoint > > > intervalData = new ArrayList<>();

				// for each toProcess block do
				for ( final Tuple2< ViewId, long[][] > p : toProcessSerialized )
				{
					if ( p._1().equals( viewId ) )
					{
						final Interval toProcessInterval = Spark.deserializeInterval( p._2() );
						final List< InterestPoint > ipsBlock = new ArrayList<>();

						// figure out which computed blockIntervals fall into this toProcessInterval
						for ( int l = 0; l < ipsList.size(); ++l )
							if ( Intervals.contains( toProcessInterval, intervalsList.get( l ) ) )
								ipsBlock.addAll( ipsList.get( l ) );

						intervalData.add( new Tuple2<>( toProcessInterval, ipsBlock ) );
					}
				}

				// to later put back into ipsList
				ipsList.clear();

				// now filter each interval of each view
				for ( final Tuple2< Interval, List< InterestPoint > > tuple : intervalData )
				{
					final int myMaxSpots = (int)Math.round( maxSpots * ( (double)ViewUtil.size( tuple._1() ) /(double)maxIntervalSizeFinal ) );

					if ( myMaxSpots > 0 && myMaxSpots < tuple._2().size() )
					{
						final int oldSize = tuple._2().size();
						filterPoints( tuple._2(), myMaxSpots );
						System.out.println( "Filtered interval (limit=" + myMaxSpots + ") " + Util.printInterval( tuple._1() ) + " (" + Group.pvid( viewId ) + "): " + oldSize + " >>> " + tuple._2().size() );
					}
					else
					{
						System.out.println( "NOT filtered interval (limit=" + myMaxSpots + ") " + Util.printInterval( tuple._1() ) + " (" + Group.pvid( viewId ) + "): " + tuple._2().size() );
					}

					ipsList.add( tuple._2() );
				}
			}

			// combine the lists: with --overlappingOnly the regions of different partners overlap each other, duplicates are dropped
			// (OverlappingRegions.combine, the rule and distance of the GUI); otherwise the blocks are disjoint and the lists are appended
			ArrayList< InterestPoint > myIps = new ArrayList<>();

			if ( overlappingOnly )
				myIps = OverlappingRegions.combine( ipsList, combineDistance );
			else
				for ( final List< InterestPoint > ips : ipsList )
					myIps.addAll( ips );

			// the scale space merges near-identical peaks (same sign, closer than combineDistance x 2^octave in pixels of the
			// detection), which the blocks could not do across their boundaries: once per view, as a run on the whole image does
			if ( method == Method.SCALE_SPACE && myIps.size() > 1 )
			{
				final SpimData2 data = Spark.getSparkJobSpimData2( xmlURI );
				final AffineTransform3D mipmapTransform = openAndDownsample(
						data.getSequenceDescription().getImgLoader(),
						data.getSequenceDescription().getViewDescription( viewId ),
						new long[] { downsampleXY, downsampleXY, downsampleZ },
						true ).getB();
				final double[] scale = new double[] { Math.abs( mipmapTransform.get( 0, 0 ) ), Math.abs( mipmapTransform.get( 1, 1 ) ), Math.abs( mipmapTransform.get( 2, 2 ) ) };
				final int before = myIps.size();

				myIps = DoGScaleSpace.mergeDuplicates( myIps, scale, sigma, scaleSpaceCombineDistance );

				System.out.println( Group.pvid( viewId ) + ": " + ( before - myIps.size() ) + " duplicate(s) across blocks merged" );
			}

			if ( myIps.size() > 0 )
			{
				// we need to sort and assign new ids since order is assumed when loading corresponding interest points, and we will have duplicate ids otherwise
				final ArrayList< InterestPoint > myIpsNewId = new ArrayList<>();

				for ( int id = 0; id < myIps.size(); ++id )
					myIpsNewId.add( myIps.get( id ).derive( id, myIps.get( id ).getL() ) );

				System.out.println( Group.pvid( viewId ) + ": " + myIpsNewId.size() );

				if ( !maxSpotsPerOverlap && maxSpots > 0 && maxSpots < myIpsNewId.size() )
				{
					filterPoints( myIpsNewId, maxSpots );

					System.out.println( Group.pvid( viewId ) + " (after applying maxSpots): " + myIpsNewId.size() );
				}

				if ( !dryRun )
					InterestPointsZarr.stagePoints( baseDirURI, viewId, label, myIpsNewId );

				n5WriterLocal.close();

				return new Tuple2<>( viewId, myIpsNewId.size() );
			}
			else
			{
				System.out.println( Group.pvid( viewId ) + ": no points found." );

				if ( !dryRun )
					InterestPointsZarr.stagePoints( baseDirURI, viewId, label, new ArrayList<>() );

				n5WriterLocal.close();

				return new Tuple2<>( viewId, 0 );
			}
		} ).collect();

		System.out.println( "Combined interest points ... " + new java.util.Date() );

		if ( !keepTemporaryN5 )
		{
			System.out.println( "Deleting temporary Spark files ... ");

			final JavaRDD<Tuple3<ViewId, long[][], String>> rdd = sc.parallelize( results, Math.min( Spark.maxPartitions, results.size() ) );

			rdd.foreach( boundingBox ->
			{
				// remove the whole block group (not just its datasets), otherwise n5Writer.remove( tempDataset ) below has to delete thousands of empty directories single-threaded
				// _3() is "" for blocks without points, which have no group
				if ( !boundingBox._3().isEmpty() )
				{
					final N5Writer n5WriterLocal = URITools.instantiateN5Writer( StorageFormat.N5, tempURI );

					if ( n5WriterLocal.exists( tempDataset + "/" + boundingBox._3() ) )
						n5WriterLocal.remove( tempDataset + "/" + boundingBox._3() );

					n5WriterLocal.close();
				}
			});

			n5Writer.remove( tempDataset );

			// a scratch container that holds nothing else (every label of the dataset is in the store) goes too, so that no stray
			// legacy interestpoints.n5 is left behind for the GUI's "Resave as ZARR store" or InterestPointsN5ToZarr to look into
			if ( n5Writer.list( "" ).length == 0 )
			{
				System.out.println( "Removing the empty scratch container '" + tempURI + "' ... " );
				n5Writer.remove();
			}

			System.out.println( "All deleted.");
		}

		sc.close();

		System.out.println( "Computed all interest points, statistics:" );

		for ( final Tuple2< ViewId, Integer > viewAndCount : combined )
			System.out.println( Group.pvid( viewAndCount._1() ) + ": " + viewAndCount._2() );

		if ( !dryRun )
		{
			// save interest points for ALL views that were processed, not only those with blocks (and points),
			// otherwise they are not saved into the XML and into the store
			final HashSet< ViewId > staged = new HashSet<>( viewIds );
			for ( final ViewId viewId : viewIdsGlobal )
				if ( !staged.contains( viewId ) )
					InterestPointsZarr.stagePoints( baseDirURI, viewId, label, new ArrayList<>() );

			// save XML
			System.out.println( "Saving XML and interest points ..." );

			// as the GUI's (DifferenceOfGaussianGUI / ScaleSpaceGUI, "t=" is parsed by Filter_Detections.parseThreshold), plus the Spark
			// specifics; the tail is the GUI's DifferenceOfGUI.commonParameters() (subpixel localization, grouping, threshold type),
			// which StoredDetectionParameters reads back when the GUI detects the label again
			final String commonTail = " localization=" + DifferenceOfGUI.localizationKey( localization == Localization.NONE ? 0 : 1 ) + " groupTiles=false groupIllums=false" +
					( this.noiseFactor != null ? " thresholdType=noisefloor noiseFactor=" + this.noiseFactor : " thresholdType=fixed" );
			final String params = method == Method.SCALE_SPACE ?
					"DOG-SS (Spark) s=" + sigma + " steps=" + steps + " octaves=" + octaves + " finestLevel=" + finestLevel + " t=" + threshold + " overlappingOnly=" + overlappingOnly + " min=" + findMin + " max=" + findMax +
							" downsampleXY=" + downsampleXY + " downsampleZ=" + downsampleZ + " anisotropy=" + anisotropyZ + " minIntensity=" + minIntensity + " maxIntensity=" + maxIntensity + commonTail :
					"DOG (Spark) s=" + sigma + " t=" + threshold + " overlappingOnly=" + overlappingOnly + " min=" + findMin + " max=" + findMax +
							" downsampleXY=" + downsampleXY + " downsampleZ=" + downsampleZ + " anisotropy=" + anisotropyZ + " minIntensity=" + minIntensity + " maxIntensity=" + maxIntensity + commonTail;

			// the entries of the label for all processed views; the XML save commits their staged points
			InterestPointTools.addInterestPointEntries( dataGlobal, label, viewIdsGlobal, params );

			new XmlIoSpimData2().save( dataGlobal, xmlURI );

		}

		n5Writer.close();

		System.out.println( "Done ..." );

		return null;
	}

	/**
	 * Keeps the {@code maxSpots} brightest interest points of a list, by the image intensity each point carries
	 * (InterestPointFilter.limit, what a filter-interestpoints --keepPoints --keepBy intensity does with a base label). The list
	 * is modified in place: the kept points stay in their order and receive new consecutive ids starting at 0.
	 * {@code maxSpots} must not exceed the number of points.
	 */
	public static void filterPoints( final List< InterestPoint > myIps, final int maxSpots )
	{
		final ArrayList< InterestPoint > kept = InterestPointFilter.limit( myIps, Axis.INTENSITY, Limit.HIGHEST, maxSpots, true );

		myIps.clear();

		for ( int i = 0; i < kept.size(); ++i )
			myIps.add( kept.get( i ).derive( i, kept.get( i ).getL() ) ); // new id's again ...
	}

	/**
	 * The anisotropy the Gaussians compensate (DownsampleTools.anisotropy, the voxel size of the opened image per dimension
	 * relative to x): for the Difference-of-Gaussian only if --anisotropy is given (as in the GUI, DoG.java: NaN = isotropic
	 * in pixels, what BigStitcher always did), for the scale space always (NaN = the ratio of the calibration, ScaleSpace.java)
	 */
	protected static double[] anisotropyOf( final Method method, final ViewDescription vd, final AffineTransform3D mipmapTransform, final double anisotropyZ )
	{
		if ( method == Method.DOG && Double.isNaN( anisotropyZ ) )
			return null;

		return DownsampleTools.anisotropy( vd, mipmapTransform, anisotropyZ );
	}

	/**
	 * The parameters of the scale space of a block (ScaleSpaceParameters, the defaults of the GUI for everything not given)
	 */
	protected static ScaleSpaceParameters scaleSpaceParameters(
			final double sigma,
			final double threshold,
			final int steps,
			final int octaves,
			final boolean finestLevel,
			final Localization localization,
			final boolean findMin,
			final boolean findMax,
			final double minIntensity,
			final double maxIntensity,
			final double[] anisotropy )
	{
		final ScaleSpaceParameters p = new ScaleSpaceParameters();

		p.sigmaMin = sigma;
		p.steps = steps;
		p.octaves = octaves;
		p.threshold = threshold;
		p.detectFinestLevel = finestLevel;
		p.localization = localization == Localization.NONE ? 0 : 1;
		p.findMin = findMin;
		p.findMax = findMax;
		p.minIntensity = minIntensity;
		p.maxIntensity = maxIntensity;
		p.anisotropy = anisotropy;

		return p;
	}

	/**
	 * How far (in pixels of the opened image) the Gaussians reach beyond a block, for prefetching: the half kernel of the
	 * largest sigma of the Difference-of-Gaussian, or of the coarsest octave of the scale space (an upper bound: its half
	 * kernel in pixels of that octave times its subsampling)
	 */
	protected static int prefetchHalo( final Method method, final double sigma, final double[] anisotropy, final int n, final int steps, final int octaves, final Interval imageInterval )
	{
		if ( method == Method.SCALE_SPACE )
		{
			final ScaleSpaceParameters p = new ScaleSpaceParameters();
			p.sigmaMin = sigma;
			p.steps = steps;
			p.octaves = octaves;
			p.anisotropy = anisotropy;

			final int numOctaves = octaves > 0 ? octaves : DoGScaleSpace.autoOctaves( imageInterval, p );
			final double sigmaLast = sigma * Math.pow( LaPlaceFunctions.computeK( steps ), steps + 1 ); // the last Gaussian level of an octave

			return Gauss3.halfkernelsize( sigmaLast ) * ( 1 << Math.max( 0, numOctaves - 1 ) );
		}
		else
		{
			final Pair< double[][], Float > sigmas = DoGImgLib2.computeSigmas( (float)sigma, anisotropy, n );
			final int[] halfKernelSizes = Gauss3.halfkernelsizes( sigmas.getA()[ 1 ] );

			return Collections.max( Arrays.stream( halfKernelSizes ).boxed().collect( Collectors.toList()) );
		}
	}

	/**
	 * The threshold as the GUI's "Estimate from noise floor" finds it (DifferenceOfGUI.estimateNoiseFloor): the noise floor of
	 * every view (NoiseFloor, the robust std of the finest Difference-of-Gaussian level in 16 random blocks), one Spark job per
	 * view on the virtually downsampled image normalized with the given intensity range, pooled as the highest of the views,
	 * times the factor. The per-view floors and the pooled one are printed.
	 */
	protected static double thresholdFromNoiseFloor(
			final JavaSparkContext sc,
			final List< ViewId > viewIds,
			final URI xmlURI,
			final Method method,
			final double sigma,
			final int steps,
			final double anisotropyZ,
			final long[] downsampling,
			final double minIntensity,
			final double maxIntensity,
			final double factor )
	{
		// the legacy Difference-of-Gaussian corresponds to 4 steps per octave (DifferenceOfGaussianGUI)
		final float k = LaPlaceFunctions.computeK( method == Method.SCALE_SPACE ? steps : 4 );

		System.out.println( "Estimating the noise floor of " + viewIds.size() + " view(s) at downsampling " + Util.printCoordinates( downsampling ) + " (sigma " + sigma + ", k " + k + ") ..." );

		final List< Tuple2< ViewId, double[] > > estimates = sc.parallelize( Spark.toViewIds( viewIds ), Math.min( Spark.maxPartitions, viewIds.size() ) ).map( viewId ->
		{
			final SpimData2 data = Spark.getSparkJobSpimData2( xmlURI );
			final ViewDescription vd = data.getSequenceDescription().getViewDescription( viewId );

			if ( !vd.isPresent() )
				return new Tuple2<>( viewId, (double[])null );

			final Pair<RandomAccessibleInterval, AffineTransform3D> input = openAndDownsample( data.getSequenceDescription().getImgLoader(), vd, downsampling, true );
			final ExecutorService service = Threads.createFixedExecutorService( 1 );

			try
			{
				@SuppressWarnings("unchecked")
				final NoiseFloor.Estimate e = NoiseFloor.estimate( (RandomAccessibleInterval)input.getA(), minIntensity, maxIntensity, sigma, k, anisotropyOf( method, vd, input.getB(), anisotropyZ ), service );

				System.out.println( "noise floor of " + Group.pvid( viewId ) + ": " + e );

				return new Tuple2<>( viewId, new double[] { e.floor, e.median, e.numSamples, e.numExcluded, e.numBlocks } );
			}
			finally
			{
				service.shutdown();
			}
		} ).collect();

		final NoiseFloor.Pooled pooled = new NoiseFloor.Pooled();

		for ( final Tuple2< ViewId, double[] > t : estimates )
			if ( t._2() != null )
				pooled.perView.put( t._1(), new NoiseFloor.Estimate( t._2()[ 0 ], t._2()[ 1 ], (long)t._2()[ 2 ], (long)t._2()[ 3 ], (int)t._2()[ 4 ] ) );

		System.out.println( pooled.describe() );

		if ( Double.isNaN( pooled.floor() ) )
			throw new RuntimeException( "No view yielded a noise floor (no data in the sampled blocks?), cannot derive a threshold from --noiseFactor." );

		final double threshold = pooled.threshold( factor );

		System.out.println( "threshold = " + factor + " x " + pooled.floor() + " = " + threshold );

		return threshold;
	}

	// TODO: this has been pushed up to the multiview-reconstruction code, use new version
	/**
	 * Opens a view and downsamples it by the given factors	// TODO: this has been pushed up to the multiview-reconstruction code, use new version
	/**
	 * Opens a view and downsamples it by the given factors, reading the closest suitable pre-computed
	 * level if the loader is a {@link MultiResolutionImgLoader} and downsampling the remainder 2x per
	 * step and dimension.
	 *
	 * @param imgLoader the image loader of the dataset
	 * @param vd the view to open
	 * @param downsampleFactors the downsampling per dimension ({@code x, y[, z]}, z defaults to 1),
	 *            expected to be powers of two
	 * @param virtualDownsampling if {@code true} the remaining downsampling is done lazily and
	 *            block-wise ({@code LazyDownsample2x}), otherwise eagerly ({@code Downsample.simple2x})
	 * @return the downsampled image and the transformation that maps its pixel coordinates to the
	 *         full-resolution pixel coordinates of the view
	 */
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
		if ( imgLoader == null )
			throw new RuntimeException( "ImgLoader is null for " + vd + ". The image loader failed to instantiate - check earlier log output for 'ERROR: cannot instantiate' messages." );

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
				if ( mrImgLoader.getSetupImgLoader( vd.getViewSetupId() ) == null )
					throw new RuntimeException( "SetupImgLoader is null for " + Group.pvid( vd ) + ", this should not happen!" );

				input = mrImgLoader.getSetupImgLoader( vd.getViewSetupId() ).getImage( vd.getTimePointId(), bestLevel );
			}
		}
		else
		{
			if ( !transformOnly )
			{
				if ( imgLoader.getSetupImgLoader( vd.getViewSetupId() ) == null )
					throw new RuntimeException( "BasicSetupImgLoader is null for " + Group.pvid( vd ) + ", this should not happen!" );

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
