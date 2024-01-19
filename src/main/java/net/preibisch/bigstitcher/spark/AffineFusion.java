package net.preibisch.bigstitcher.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import mpicbg.spim.data.registration.ViewRegistrations;
import net.preibisch.bigstitcher.spark.util.ViewUtil.PrefetchPixel;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewTransformAffine;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.integer.ShortType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.util.BDVSparkInstantiateViewSetup;
import net.preibisch.bigstitcher.spark.util.Downsampling;
import net.preibisch.bigstitcher.spark.util.Grid;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.bigstitcher.spark.util.ViewUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBox;
import net.preibisch.mvrecon.process.export.ExportN5API.StorageType;
import net.preibisch.mvrecon.process.export.ExportTools;
import net.preibisch.mvrecon.process.export.ExportTools.InstantiateViewSetup;
import net.preibisch.mvrecon.process.fusion.FusionTools;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class AffineFusion implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = -6103761116219617153L;

	@Option(names = { "-x", "--xml" }, required = true, description = "Path to the existing BigStitcher project xml, e.g. -x /home/project.xml")
	private String xmlPath = null;

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

	@Option(names = { "-b", "--boundingBox" }, description = "fuse a specific bounding box listed in the XML (default: fuse everything)")
	private String boundingBoxName = null;

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

	// TODO: support create downsampling pyramids, null is fine for now
	private int[][] downsamplings;

	@Override
	public Void call() throws Exception
	{
		if ( (this.n5Dataset == null && this.bdvString == null) || (this.n5Dataset != null && this.bdvString != null) )
		{
			System.out.println( "You must define either the n5dataset (e.g. -d /ch488/s0) - OR - the BigDataViewer specification (e.g. --bdv 0,1)");
			return null;
		}

		Import.validateInputParameters(uint8, uint16, minIntensity, maxIntensity, vi, angleIds, channelIds, illuminationIds, tileIds, timepointIds);

		if ( StorageType.HDF5.equals( storageType ) && bdvString != null && !uint16 )
		{
			System.out.println( "BDV-compatible HDF5 only supports 16-bit output for now. Please use '--UINT16' flag for fusion." );
			return null;
		}

		final SpimData2 data = Spark.getSparkJobSpimData2("", xmlPath);

		// select views to process
		final ArrayList< ViewId > viewIds =
				Import.createViewIds(
						data, vi, angleIds, channelIds, illuminationIds, tileIds, timepointIds);

		if ( viewIds.size() == 0 )
		{
			throw new IllegalArgumentException( "No views to fuse." );
		}
		else
		{
			System.out.println( "Following ViewIds will be fused: ");
			for ( final ViewId v : viewIds )
				System.out.print( "[" + v.getTimePointId() + "," + v.getViewSetupId() + "] " );
			System.out.println();
		}

		BoundingBox boundingBox = Import.getBoundingBox( data, viewIds, boundingBoxName );

		final int[] blockSize = Import.csvStringToIntArray(blockSizeString);

		System.out.println( "Fusing: " + boundingBox.getTitle() + ": " + Util.printInterval( boundingBox )  + " with blocksize " + Util.printCoordinates( blockSize ) );

		final DataType dataType;

		if ( uint8 )
		{
			System.out.println( "Fusing to UINT8, min intensity = " + minIntensity + ", max intensity = " + maxIntensity );
			dataType = DataType.UINT8;
		}
		else if ( uint16 && bdvString != null && StorageType.HDF5.equals( storageType ) )
		{
			System.out.println( "Fusing to INT16 (for BDV compliance, which is treated as UINT16), min intensity = " + minIntensity + ", max intensity = " + maxIntensity );
			dataType = DataType.INT16;
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
				anisotropyFactor = TransformationTools.getAverageAnisotropyFactor( data, viewIds );

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

		final String n5Path = this.n5Path;
		final String n5Dataset = this.n5Dataset != null ? this.n5Dataset : Import.createBDVPath( this.bdvString, this.storageType );
		final String xmlPath = this.xmlPath;
		final StorageType storageType = this.storageType;
		final Compression compression = new GzipCompression( 1 );

		final boolean uint8 = this.uint8;
		final boolean uint16 = this.uint16;
		final double minIntensity = (uint8 || uint16 ) ? this.minIntensity : 0;
		final double range;
		if ( uint8 )
			range = ( this.maxIntensity - this.minIntensity ) / 255.0;
		else if ( uint16 )
			range = ( this.maxIntensity - this.minIntensity ) / 65535.0;
		else
			range = 0;

		// TODO: improve (e.g. make ViewId serializable)
		final int[][] serializedViewIds = Spark.serializeViewIds(viewIds);
		final boolean useAF = preserveAnisotropy;
		final double af = anisotropyFactor;

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

		final List<long[][]> grid = Grid.create( dimensions, blockSize );

		/*
		// TODO: start doing this

		// using bigger blocksizes than being stored for efficiency (needed for very large datasets)

		final List<long[][]> grid = Grid.create(dimensions,
				new int[] {
						blockSize[0] * 4,
						blockSize[1] * 4,
						blockSize[2] * 4
				},
				blockSize);
		*/

		System.out.println( "numBlocks = " + grid.size() );

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
						this.n5Path,
						this.xmlOutPath,
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
		rdd.foreach(
				gridBlock -> {
					final long[] gridBlockSize = gridBlock[ 1 ];
					final long[] gridBlockOffset = gridBlock[ 0 ];

					// custom serialization
					final SpimData2 dataLocal = Spark.getSparkJobSpimData2("", xmlPath);
					final List< ViewId > viewIds2 = Spark.deserializeViewIds( serializedViewIds );

					// be smarter, test which ViewIds are actually needed for the block we want to fuse
					final Interval fusedBlock =
							Intervals.translate(
									FinalInterval.createMinSize( gridBlockOffset, gridBlockSize ),
									minBB ); // min of the randomaccessbileinterval

					// recover views to process
					final ArrayList< ViewId > viewIdsLocal = new ArrayList<>();
					final List< Callable< Object > > prefetch = new ArrayList<>();

					if ( useAF )
					{
						final AffineTransform3D aniso = new AffineTransform3D();
						aniso.set(
								1.0, 0.0, 0.0, 0.0,
								0.0, 1.0, 0.0, 0.0,
								0.0, 0.0, 1.0 / af, 0.0 );
						final ViewTransformAffine preserveAnisotropy = new ViewTransformAffine( "preserve anisotropy", aniso );

						final ViewRegistrations registrations = dataLocal.getViewRegistrations();
						for ( final ViewId viewId : viewIds2 )
						{
							// get updated registration for views to fuse AND all other views that may influence the fusion
							final ViewRegistration vr = registrations.getViewRegistration( viewId );
							vr.preconcatenateTransform( preserveAnisotropy );
							vr.updateModel();
						}
					}

					for ( final ViewId viewId : viewIds2 )
					{
						// expand to be conservative ...
						final Interval boundingBoxLocal = ViewUtil.getTransformedBoundingBox( dataLocal, viewId );
						final Interval bounds = Intervals.expand( boundingBoxLocal, 2 );

						if ( ViewUtil.overlaps( fusedBlock, bounds ) )
						{
							// determine which Cells exactly we need to compute the fused block
							final List< PrefetchPixel< ? > > blocks = ViewUtil.findOverlappingBlocks( dataLocal, viewId, fusedBlock );
							if ( !blocks.isEmpty() )
							{
								prefetch.addAll( blocks );
								viewIdsLocal.add( viewId );
							}
						}
					}

					//SimpleMultiThreading.threadWait( 10000 );

					// nothing to save...
					if ( viewIdsLocal.size() == 0 )
						return;

					// prefetch cells: each cell on a separate thread
					final ExecutorService executor = Executors.newFixedThreadPool( prefetch.size() );
					final List< Future< Object > > prefetched = executor.invokeAll( prefetch );
					executor.shutdown();

					final RandomAccessibleInterval<FloatType> source = FusionTools.fuseVirtual(
								dataLocal,
								viewIdsLocal,
								new FinalInterval(minBB, maxBB)
					);

					final N5Writer executorVolumeWriter = N5Util.createWriter( n5Path, storageType );

					if ( uint8 )
					{
						final RandomAccessibleInterval< UnsignedByteType > sourceUINT8 =
								Converters.convert(
										source,(i, o) -> o.setReal( ( i.get() - minIntensity ) / range ),
										new UnsignedByteType());

						final RandomAccessibleInterval<UnsignedByteType> sourceGridBlock = Views.offsetInterval(sourceUINT8, gridBlockOffset, gridBlockSize );
						//N5Utils.saveNonEmptyBlock(sourceGridBlock, n5Writer, n5Dataset, gridBlock[2], new UnsignedByteType());
						N5Utils.saveBlock(sourceGridBlock, executorVolumeWriter, n5Dataset, gridBlock[2]);
					}
					else if ( uint16 )
					{
						final RandomAccessibleInterval< UnsignedShortType > sourceUINT16 =
								Converters.convert(
										source,(i, o) -> o.setReal( ( i.get() - minIntensity ) / range ),
										new UnsignedShortType());

						if ( bdvString != null && StorageType.HDF5.equals( storageType ) )
						{
							// Tobias: unfortunately I store as short and treat it as unsigned short in Java.
							// The reason is, that when I wrote this, the jhdf5 library did not support unsigned short. It's terrible and should be fixed.
							// https://github.com/bigdataviewer/bigdataviewer-core/issues/154
							// https://imagesc.zulipchat.com/#narrow/stream/327326-BigDataViewer/topic/XML.2FHDF5.20specification
							final RandomAccessibleInterval< ShortType > sourceINT16 =
									Converters.convertRAI( sourceUINT16, (i,o)->o.set( i.getShort() ), new ShortType() );

							final RandomAccessibleInterval<ShortType> sourceGridBlock = Views.offsetInterval(sourceINT16, gridBlockOffset, gridBlockSize );
							N5Utils.saveBlock(sourceGridBlock, executorVolumeWriter, n5Dataset, gridBlock[2]);
						}
						else
						{
							final RandomAccessibleInterval<UnsignedShortType> sourceGridBlock = Views.offsetInterval(sourceUINT16, gridBlockOffset, gridBlockSize );
							N5Utils.saveBlock(sourceGridBlock, executorVolumeWriter, n5Dataset, gridBlock[2]);
						}
					}
					else
					{
						final RandomAccessibleInterval<FloatType> sourceGridBlock = Views.offsetInterval(source, gridBlockOffset, gridBlockSize );
						N5Utils.saveBlock(sourceGridBlock, executorVolumeWriter, n5Dataset, gridBlock[2]);
					}

					// let go of references to the prefetched cells
					prefetched.clear();

					// not HDF5
					if ( N5Util.hdf5DriverVolumeWriter != executorVolumeWriter )
						executorVolumeWriter.close();
				});

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

		System.exit(new CommandLine(new AffineFusion()).execute(args));
	}

}
