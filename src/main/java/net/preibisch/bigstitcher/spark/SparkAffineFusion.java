package net.preibisch.bigstitcher.spark;

import static net.preibisch.bigstitcher.spark.blk.Fusion.fuseVirtual_blk;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import mpicbg.spim.data.SpimData;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewRegistrations;
import mpicbg.spim.data.registration.ViewTransformAffine;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealTypeConverters;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.blk.N5Helper;
import net.preibisch.bigstitcher.spark.util.BDVSparkInstantiateViewSetup;
import net.preibisch.bigstitcher.spark.util.Downsampling;
import net.preibisch.bigstitcher.spark.util.Grid;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.bigstitcher.spark.util.ViewUtil;
import net.preibisch.bigstitcher.spark.util.ViewUtil.PrefetchPixel;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBox;
import net.preibisch.mvrecon.process.export.ExportN5API.StorageType;
import net.preibisch.mvrecon.process.export.ExportTools;
import net.preibisch.mvrecon.process.export.ExportTools.InstantiateViewSetup;
import net.preibisch.mvrecon.process.fusion.FusionTools;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class SparkAffineFusion extends AbstractSelectableViews implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = -6103761116219617153L;

	@Option(names = { "-o", "--n5Path" }, required = true, description = "N5/ZARR/HDF5 basse path for saving (must be combined with the option '-d' or '--bdv'), e.g. -o /home/fused.n5")
	private String n5Path = null;

	@Option(names = { "-d", "--n5Dataset" }, required = false, description = "Custom N5/ZARR/HDF5 dataset - it must end with '/s0' to be able to compute a multi-resolution pyramid, e.g. -d /ch488/s0")
	private String n5Dataset = null;

	@Option(names = { "--bdv" }, required = false, description = "Write a BigDataViewer-compatible dataset specifying TimepointID, ViewSetupId, e.g. --bdv 0,0 or --bdv 4,1")
	private String bdvString = null;

	@Option(names = { "-xo", "--xmlout" }, required = false, description = "path to the new BigDataViewer xml project (only valid if --bdv was selected), e.g. -xo /home/project.xml (default: dataset.xml in basepath for H5, dataset.xml one directory level above basepath for N5)")
	private String xmlOutPath = null;

	@Option(names = {"-s", "--storage"}, defaultValue = "N5", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Dataset storage type, currently supported N5, ZARR (and ONLY for local, multithreaded Spark: HDF5)")
	private StorageType storageType = null;

	@Option(names = "--blockSize", description = "blockSize, you can use smaller blocks for HDF5 (default: 128,128,128)")
	private String blockSizeString = "128,128,128";

	@Option(names = "--blockScale", description = "how many blocks to use for a single processing step, e.g. 4,4,1 means for blockSize a 128,128,64 that each spark thread writes 512,512,64 (default: 4,4,1)")
	private String blockScaleString = "4,4,1";

	@Option(names = { "-b", "--boundingBox" }, description = "fuse a specific bounding box listed in the XML (default: fuse everything)")
	private String boundingBoxName = null;

	@Option(names = { "--multiRes" }, description = "Automatically create a multi-resolution pyramid (default: false)")
	private boolean multiRes = false;

	@Option(names = { "-ds", "--downsampling" }, split = ";", required = false, description = "Manually define steps to create of a multi-resolution pyramid (e.g. -ds 2,2,1; 2,2,1; 2,2,2; 2,2,2)")
	private List<String> downsampling = null;

	@Option(names = { "--preserveAnisotropy" }, description = "preserve the anisotropy of the data (default: false)")
	private boolean preserveAnisotropy = false;

	@Option(names = { "--anisotropyFactor" }, description = "define the anisotropy factor if preserveAnisotropy is set to true (default: compute from data)")
	private double anisotropyFactor = Double.NaN;

	// TODO: make a variable just as -s is
	@Option(names = { "--UINT16" }, description = "save as UINT16 [0...65535], if you choose it you must define min and max intensity (default: fuse as 32 bit float)")
	private boolean uint16 = false;

	@Option(names = { "--UINT8" }, description = "save as UINT8 [0...255], if you choose it you must define min and max intensity (default: fuse as 32 bit float)")
	private boolean uint8 = false;

	@Option(names = { "--minIntensity" }, description = "min intensity for scaling values to the desired range (required for UINT8 and UINT16), e.g. 0.0")
	private Double minIntensity = null;

	@Option(names = { "--maxIntensity" }, description = "max intensity for scaling values to the desired range (required for UINT8 and UINT16), e.g. 2048.0")
	private Double maxIntensity = null;

	// TODO: support create custom downsampling pyramids, null is fine for now (used by multiRes later)
	private int[][] downsamplings;


	/**
	 * Assumes that
	 * at most 8 views are required per output block, and
	 * at most 8 input blocks per output block are required from one input view per output block.
	 */
	static final int N_PREFETCH_THREADS = 72;

	@Override
	public Void call() throws Exception
	{
		if (dryRun)
		{
			System.out.println( "dry-run not supported for affine fusion.");
			System.exit( 0 );
		}

		if ( (this.n5Dataset == null && this.bdvString == null) || (this.n5Dataset != null && this.bdvString != null) )
		{
			System.out.println( "You must define either the n5dataset (e.g. -d /ch488/s0) - OR - the BigDataViewer specification (e.g. --bdv 0,1)");
			return null;
		}

		Import.validateInputParameters(uint8, uint16, minIntensity, maxIntensity);

		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIdsGlobal = this.loadViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			return null;

		BoundingBox boundingBox = Import.getBoundingBox( dataGlobal, viewIdsGlobal, boundingBoxName );

		final int[] blockSize = Import.csvStringToIntArray(blockSizeString);
		final int[] blocksPerJob = Import.csvStringToIntArray(blockScaleString);
		System.out.println( "Fusing: " + boundingBox.getTitle() +
				": " + Util.printInterval( boundingBox ) +
				" with blocksize " + Util.printCoordinates( blockSize ) +
				" and " + Util.printCoordinates( blocksPerJob ) + " blocks per job" );

		final DataType dataType;

		if ( uint8 )
		{
			System.out.println( "Fusing to UINT8, min intensity = " + minIntensity + ", max intensity = " + maxIntensity );
			dataType = DataType.UINT8;
		}
		else if ( uint16 )
		{
			System.out.println( "Fusing to UINT16, min intensity = " + minIntensity + ", max intensity = " + maxIntensity );
			dataType = DataType.UINT16;
		}
		else
		{
			System.out.println( "Fusing to FLOAT32" );
			dataType = DataType.FLOAT32;
		}

		//
		// final variables for Spark
		//
		final long[] minBB = boundingBox.minAsLongArray();
		final long[] maxBB = boundingBox.maxAsLongArray();

		if ( preserveAnisotropy )
		{
			System.out.println( "Preserving anisotropy.");

			if ( Double.isNaN( anisotropyFactor ) )
			{
				anisotropyFactor = TransformationTools.getAverageAnisotropyFactor( dataGlobal, viewIdsGlobal );

				System.out.println( "Anisotropy factor [computed from data]: " + anisotropyFactor );
			}
			else
			{
				System.out.println( "Anisotropy factor [provided]: " + anisotropyFactor );
			}

			// prepare downsampled boundingbox
			minBB[ 2 ] = Math.round( Math.floor( minBB[ 2 ] / anisotropyFactor ) );
			maxBB[ 2 ] = Math.round( Math.ceil( maxBB[ 2 ] / anisotropyFactor ) );

			boundingBox = new BoundingBox( new FinalInterval(minBB, maxBB) );

			System.out.println( "Adjusted bounding box (anisotropy preserved: " + Util.printInterval( boundingBox ) );
		}

		//
		// set up downsampling (if wanted)
		//
		if ( !Downsampling.testDownsamplingParameters( this.multiRes, this.downsampling, this.n5Dataset ) )
			return null;

		if ( multiRes )
			downsamplings = ExportTools.estimateMultiResPyramid( new FinalDimensions( boundingBox ), anisotropyFactor );
		else if ( this.downsampling != null )
			downsamplings = Import.csvStringListToDownsampling( this.downsampling );
		else
			downsamplings = null;

		final long[] dimensions = boundingBox.dimensionsAsLongArray();

		// display virtually
		//final RandomAccessibleInterval< FloatType > virtual = FusionTools.fuseVirtual( data, viewIds, bb, Double.NaN ).getA();
		//new ImageJ();
		//ImageJFunctions.show( virtual, Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() ) );
		//SimpleMultiThreading.threadHaltUnClean();

		final String n5Dataset = this.n5Dataset != null ? this.n5Dataset : Import.createBDVPath( this.bdvString, this.storageType );
		final Compression compression = new GzipCompression( 1 );

		final double minIntensity = ( uint8 || uint16 ) ? this.minIntensity : 0;
		final double range;
		if ( uint8 )
			range = ( this.maxIntensity - this.minIntensity ) / 255.0;
		else if ( uint16 )
			range = ( this.maxIntensity - this.minIntensity ) / 65535.0;
		else
			range = 0;

		// TODO: improve (e.g. make ViewId serializable)
		final int[][] serializedViewIds = Spark.serializeViewIds(viewIdsGlobal);

		try
		{
			// trigger the N5-blosc error, because if it is triggered for the first
			// time inside Spark, everything crashes
			new N5FSWriter(null);
		}
		catch (Exception e ) {}

		final N5Writer driverVolumeWriter = N5Util.createWriter( n5Path, storageType );

		System.out.println( "Format being written: " + storageType );

		driverVolumeWriter.createDataset(
				n5Dataset,
				dimensions,
				blockSize,
				dataType,
				compression );

		// using bigger blocksizes than being stored for efficiency (needed for very large datasets)
		final int[] superBlockSize = new int[ 3 ];
		Arrays.setAll( superBlockSize, d -> blockSize[ d ] * blocksPerJob[ d ] );
		final List<long[][]> grid = Grid.create(dimensions,
				superBlockSize,
				blockSize);

		System.out.println( "numJobs = " + grid.size() );

		driverVolumeWriter.setAttribute( n5Dataset, "offset", minBB );

		// saving metadata if it is bdv-compatible (we do this first since it might fail)
		if ( bdvString != null )
		{
			// A Functional Interface that converts a ViewId to a ViewSetup, only called if the ViewSetup does not exist
			final InstantiateViewSetup instantiate =
					new BDVSparkInstantiateViewSetup( angleIds, illuminationIds, channelIds, tileIds );

			final ViewId viewId = Import.getViewId( bdvString );

			try
			{
				if ( !ExportTools.writeBDVMetaData(
						driverVolumeWriter,
						storageType,
						dataType,
						dimensions,
						compression,
						blockSize,
						downsamplings,
						viewId,
						n5Path,
						xmlOutPath,
						instantiate ) )
				{
					System.out.println( "Failed to write metadata for '" + n5Dataset + "'." );
					return null;
				}
			}
			catch (SpimDataException | IOException e)
			{
				e.printStackTrace();
				System.out.println( "Failed to write metadata for '" + n5Dataset + "': " + e );
				return null;
			}

			System.out.println( "Done writing BDV metadata.");
		}

		final SparkConf conf = new SparkConf().setAppName("AffineFusion");
		// TODO: REMOVE
		//conf.set("spark.driver.bindAddress", "127.0.0.1");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final JavaRDD<long[][]> rdd = sc.parallelize( grid );

		final long time = System.currentTimeMillis();
		rdd.foreach( new WriteSuperBlock(
				xmlPath,
				preserveAnisotropy,
				anisotropyFactor,
				minBB,
				n5Path,
				n5Dataset,
				bdvString,
				storageType,
				serializedViewIds,
				uint8,
				uint16,
				minIntensity,
				range,
				blockSize ) );

		if ( this.downsamplings != null )
		{
			// TODO: run common downsampling code (affine, non-rigid, downsampling-only)
			Downsampling.createDownsampling(
					n5Path,
					n5Dataset,
					driverVolumeWriter,
					dimensions,
					storageType,
					blockSize,
					dataType,
					compression,
					downsamplings,
					bdvString != null,
					sc );
		}

		sc.close();

		// close HDF5 writer
		if ( N5Util.hdf5DriverVolumeWriter != null )
			N5Util.hdf5DriverVolumeWriter.close();
		else
			System.out.println( "Saved, e.g. view with './n5-view -i " + n5Path + " -d " + n5Dataset );

		System.out.println( "done, took: " + (System.currentTimeMillis() - time ) + " ms." );

		return null;
	}

	public static void main(final String... args) throws SpimDataException {

		//final XmlIoSpimData io = new XmlIoSpimData();
		//final SpimData spimData = io.load( "/Users/preibischs/Documents/Microscopy/Stitching/Truman/standard/output/dataset.xml" );
		//BdvFunctions.show( spimData );
		//SimpleMultiThreading.threadHaltUnClean();

		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new SparkAffineFusion()).execute(args));
	}



	static class WriteSuperBlock implements VoidFunction< long[][] >
	{
		private final String xmlPath;

		private final boolean preserveAnisotropy;

		private final double anisotropyFactor;

		private final long[] minBB;

		private final String n5Path;

		private final String n5Dataset;

		private final String bdvString;

		private final StorageType storageType;

		private final int[][] serializedViewIds;

		private final boolean uint8;

		private final boolean uint16;

		private final double minIntensity;

		private final double range;

		private final int[] blockSize;

		public WriteSuperBlock(
				final String xmlPath,
				final boolean preserveAnisotropy,
				final double anisotropyFactor,
				final long[] minBB,
				final String n5Path,
				final String n5Dataset,
				final String bdvString,
				final StorageType storageType,
				final int[][] serializedViewIds,
				final boolean uint8,
				final boolean uint16,
				final double minIntensity,
				final double range,
				final int[] blockSize )
		{
			this.xmlPath = xmlPath;
			this.preserveAnisotropy = preserveAnisotropy;
			this.anisotropyFactor = anisotropyFactor;
			this.minBB = minBB;
			this.n5Path = n5Path;
			this.n5Dataset = n5Dataset;
			this.bdvString = bdvString;
			this.storageType = storageType;
			this.serializedViewIds = serializedViewIds;
			this.uint8 = uint8;
			this.uint16 = uint16;
			this.minIntensity = minIntensity;
			this.range = range;
			this.blockSize = blockSize;
		}

		/**
		 * Find all views among the given {@code viewIds} that overlap the given {@code interval}.
		 * The image interval of each view is transformed into world coordinates
		 * and checked for overlap with {@code interval}, with a conservative
		 * extension of 2 pixels in each direction.
		 *
		 * @param spimData contains bounds and registrations for all views
		 * @param viewIds which views to check
		 * @param interval interval in world coordinates
		 * @return views that overlap {@code interval}
		 */
		private static List<ViewId> findOverlappingViews(
				final SpimData spimData,
				final List<ViewId> viewIds,
				final Interval interval )
		{
			final List< ViewId > overlapping = new ArrayList<>();

			// expand to be conservative ...
			final Interval expandedInterval = Intervals.expand( interval, 2 );

			for ( final ViewId viewId : viewIds )
			{
				final Interval bounds = ViewUtil.getTransformedBoundingBox( spimData, viewId );
				if ( ViewUtil.overlaps( expandedInterval, bounds ) )
					overlapping.add( viewId );
			}

			return overlapping;
		}

		@Override
		public void call( final long[][] gridBlock ) throws Exception
		{
			final int n = blockSize.length;

			// The min coordinates of the block that this job renders (in pixels)
			final long[] superBlockOffset = new long[ n ];
			Arrays.setAll( superBlockOffset, d -> gridBlock[ 0 ][ d ] + minBB[ d ] );

			// The size of the block that this job renders (in pixels)
			final long[] superBlockSize = gridBlock[ 1 ];

			System.out.println( "Fusing block: offset=" + Util.printCoordinates( gridBlock[0] ) + ", dimension=" + Util.printCoordinates( gridBlock[1] ) );

			// The min grid coordinate of the block that this job renders, in units of the output grid.
			// Note, that the block that is rendered may cover multiple output grid cells.
			final long[] outputGridOffset = gridBlock[ 2 ];




			// --------------------------------------------------------
			// initialization work that is happening in every job,
			// independent of gridBlock parameters
			// --------------------------------------------------------

			// custom serialization
			final SpimData2 dataLocal = Spark.getSparkJobSpimData2("", xmlPath);
			final List< ViewId > viewIds = Spark.deserializeViewIds( serializedViewIds );

			// If requested, preserve the anisotropy of the data (such that
			// output data has the same anisotropy as input data) by prepending
			// an affine to each ViewRegistration
			if ( preserveAnisotropy )
			{
				final AffineTransform3D aniso = new AffineTransform3D();
				aniso.set(
						1.0, 0.0, 0.0, 0.0,
						0.0, 1.0, 0.0, 0.0,
						0.0, 0.0, 1.0 / anisotropyFactor, 0.0 );
				final ViewTransformAffine preserveAnisotropy = new ViewTransformAffine( "preserve anisotropy", aniso );

				final ViewRegistrations registrations = dataLocal.getViewRegistrations();
				for ( final ViewId viewId : viewIds )
				{
					final ViewRegistration vr = registrations.getViewRegistration( viewId );
					vr.preconcatenateTransform( preserveAnisotropy );
					vr.updateModel();
				}
			}


			final long[] gridPos = new long[ n ];
			final long[] fusedBlockMin = new long[ n ];
			final long[] fusedBlockMax = new long[ n ];
			final Interval fusedBlock = FinalInterval.wrap( fusedBlockMin, fusedBlockMax );

			// pre-filter views that overlap the superBlock
			Arrays.setAll( fusedBlockMin, d -> superBlockOffset[ d ] );
			Arrays.setAll( fusedBlockMax, d -> superBlockOffset[ d ] + superBlockSize[ d ] - 1 );
			final List< ViewId > overlappingViews = findOverlappingViews( dataLocal, viewIds, fusedBlock );

			final N5Writer executorVolumeWriter = N5Util.createWriter( n5Path, storageType );
			final ExecutorService prefetchExecutor = Executors.newFixedThreadPool( N_PREFETCH_THREADS );

			final CellGrid blockGrid = new CellGrid( superBlockSize, blockSize );
			final int numCells = ( int ) Intervals.numElements( blockGrid.getGridDimensions() );
			for ( int gridIndex = 0; gridIndex < numCells; ++gridIndex )
			{
				blockGrid.getCellGridPositionFlat( gridIndex, gridPos );
				blockGrid.getCellInterval( gridPos, fusedBlockMin, fusedBlockMax );

				for ( int d = 0; d < n; ++d )
				{
					gridPos[ d ] += outputGridOffset[ d ];
					fusedBlockMin[ d ] += superBlockOffset[ d ];
					fusedBlockMax[ d ] += superBlockOffset[ d ];
				}
				// gridPos is now the grid coordinate in the N5 output

				// determine which Cells and Views we need to compute the fused block
				final OverlappingBlocks overlappingBlocks = OverlappingBlocks.find( dataLocal, overlappingViews, fusedBlock );

				if ( overlappingBlocks.overlappingViews().isEmpty() )
					continue;

				try ( AutoCloseable prefetched = overlappingBlocks.prefetch( prefetchExecutor ) )
				{
					final RandomAccessibleInterval< FloatType > source = fuseVirtual_blk(
							dataLocal,
							overlappingBlocks.overlappingViews(),
							fusedBlock );

					saveBlock( source, executorVolumeWriter, gridPos );
				}
			}
			prefetchExecutor.shutdown();

			// not HDF5
			if ( N5Util.hdf5DriverVolumeWriter != executorVolumeWriter )
				executorVolumeWriter.close();

		}

		private < T extends NativeType< T > > void saveBlock(
				final RandomAccessibleInterval< FloatType > source,
				final N5Writer executorVolumeWriter,
				final long[] gridPos ) throws IOException
		{
			final RandomAccessibleInterval< T > convertedSource = convertToOutputType( source );
			N5Helper.saveBlock( convertedSource, executorVolumeWriter, n5Dataset, gridPos );
		}

		@SuppressWarnings( "unchecked" )
		private < T extends NativeType< T > > RandomAccessibleInterval< T > convertToOutputType(
				final RandomAccessibleInterval< FloatType > rai )
		{
			if ( uint8 )
			{
				final double a = 1 / range;
				final double b = 0.5 - minIntensity / range;
				return ( RandomAccessibleInterval< T > ) Converters.convert(
						rai, ( i, o ) -> o.setByte( ( byte ) ( i.get() * a + b ) ),
						new UnsignedByteType() );
			}
			else if ( uint16 )
			{
				final double a = 1 / range;
				final double b = 0.5 - minIntensity / range;
				return ( RandomAccessibleInterval< T > ) Converters.convert(
						rai, ( i, o ) -> o.setShort( ( short ) ( i.get() * a + b ) ),
						new UnsignedShortType() );
			}
			else
			{
				return ( RandomAccessibleInterval< T > ) rai;
			}
		}
	}



	/**
	 * Determine which input blocks overlap a given interval using {@link
	 * OverlappingBlocks#find}. Then prefetch those blocks using {@link
	 * OverlappingBlocks#prefetch}.
	 */
	static class OverlappingBlocks
	{
		/**
		 * Determine which of the given {@code viewIds} have blocks that overlap
		 * {@code interval}.
		 *
		 * @param data
		 * 		has all images and transformations
		 * @param viewIds
		 * 		which views to check
		 * @param interval
		 * 		the interval that will be processed (in world coordinates)
		 * @return list of views with overlapping blocks, and a prefetcher for those blocks.
		 */
		public static OverlappingBlocks find(
				final SpimData data,
				final List<ViewId> viewIds,
				Interval interval )
		{
			final List< ViewId > overlapping = new ArrayList<>();
			final List< Callable< Object > > prefetch = new ArrayList<>();

			// expand to be conservative ...
			final Interval expandedInterval = Intervals.expand( interval, 2 );

			for ( final ViewId viewId : viewIds )
			{
				final Interval bounds = ViewUtil.getTransformedBoundingBox( data, viewId );
				if ( ViewUtil.overlaps( expandedInterval, bounds ) )
				{
					// determine which Cells exactly we need to compute the fused block
					final List< PrefetchPixel< ? > > blocks = ViewUtil.findOverlappingBlocks( data, viewId, interval );
					if ( !blocks.isEmpty() )
					{
						prefetch.addAll( blocks );
						overlapping.add( viewId );
					}
				}
			}

			return new OverlappingBlocks( overlapping, prefetch );
		}

		/**
		 * Get the list of views with overlapping blocks.
		 *
		 * @return list of views with overlapping blocks
		 */
		public List< ViewId > overlappingViews()
		{
			return overlappingViews;
		}

		/**
		 * Prefetch all overlapping blocks.
		 * <p>
		 * The returned {@code AutoCloseable} holds strong reference to all
		 * prefetched blocks (until it is closed), preventing those blocks
		 * from being garbage-collected.
		 *
		 * @param executor blocks are loaded in parallel using this executor
		 *
		 * @return {@code AutoCloseable} that holds strong reference to all prefetched blocks (until it is closed), preventing those blocks from being garbage-collected.
		 */
		public AutoCloseable prefetch( final ExecutorService executor ) throws InterruptedException
		{
			return new Prefetched( executor.invokeAll( prefetchBlocks ) );
		}

		private final List< ViewId > overlappingViews;

		private final List< Callable< Object > > prefetchBlocks;

		private OverlappingBlocks(
				final List< ViewId > overlappingViews,
				final List< Callable< Object > > prefetchBlocks )
		{
			this.overlappingViews = overlappingViews;
			this.prefetchBlocks = prefetchBlocks;
		}

		/**
		 * Result of {@link OverlappingBlocks#prefetch}. Holds strong
		 * references to prefetched data, until it is {@link #close()
		 * closed}.
		 */
		private static class Prefetched implements AutoCloseable
		{
			private final List< Future< Object > > prefetched;

			public Prefetched( final List< Future< Object > > prefetched )
			{
				this.prefetched = prefetched;
			}

			@Override
			public void close() throws Exception
			{
				// let go of references to the prefetched cells
				prefetched.clear();
			}
		}
	}
}
