package net.preibisch.bigstitcher.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.hdf5.N5HDF5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.zarr.N5ZarrWriter;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewTransformAffine;
import mpicbg.spim.data.sequence.ViewId;
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
import net.preibisch.bigstitcher.spark.util.BDV;
import net.preibisch.bigstitcher.spark.util.Grid;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.bigstitcher.spark.util.ViewUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBox;
import net.preibisch.mvrecon.process.fusion.FusionTools;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class AffineFusion implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = 2279327568867124470L;

	public enum StorageType { N5, ZARR, HDF5 }

	@Option(names = { "-o", "--n5Path" }, required = true, description = "N5 path for saving, e.g. /home/fused.n5")
	private String n5Path = null;

	@Option(names = { "-d", "--n5Dataset" }, required = false, description = "N5 dataset - it is  recommended to add s0 to be able to compute a multi-resolution pyramid later, e.g. /ch488/s0")
	private String n5Dataset = null;

	@Option(names = { "--bdv" }, required = false, description = "Write a BigDataViewer-compatible dataset specifying TimepointID, ViewSetupId, e.g. -b 0,0 or -b 4,1")
	private String bdvString = null;

	@Option(names = { "-xo", "--xmlout" }, required = false, description = "path to the new BigDataViewer xml project (if --bdv was selected), e.g. /home/project.xml (default: dataset.xml in basepath for H5, dataset.xml one directory level above basepath for N5)")
	private String xmlOutPath = null;

	@Option(names = {"-s", "--storage"}, defaultValue = "N5", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Dataset storage type, currently supported N5, ZARR (and ONLY for local, multithreaded Spark: HDF5)")
	private StorageType storageType = null;

	@Option(names = "--blockSize", description = "blockSize, you can use smaller blocks for HDF5 (default: 128,128,128)")
	private String blockSizeString = "128,128,128";

	@Option(names = { "-x", "--xml" }, required = true, description = "path to the existing BigStitcher xml, e.g. /home/project.xml")
	private String xmlPath = null;

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

	// only supported for local spark HDF5 writes, needs to share a writer instance
	private static N5HDF5Writer hdf5DriverVolumeWriter = null;

	@Override
	public Void call() throws Exception
	{
		if ( (this.n5Dataset == null && this.bdvString == null) || (this.n5Dataset != null && this.bdvString != null) )
		{
			System.out.println( "You must define either the n5dataset (e.g. -d /ch488/s0) OR the BigDataViewer specification (e.g. --bdv 0,1)");
			System.exit( 0 );
		}

		Import.validateInputParameters(uint8, uint16, minIntensity, maxIntensity, vi, angleIds, channelIds, illuminationIds, tileIds, timepointIds);

		if ( StorageType.HDF5.equals( storageType ) && bdvString != null && !uint16 )
		{
			System.out.println( "BDV-compatible HDF5 only supports 16-bit output for now. Please use '--UINT16' flag for fusion." );
			System.exit( 0 );
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

		final N5Writer driverVolumeWriter;
		if ( StorageType.N5.equals(storageType) )
			driverVolumeWriter = new N5FSWriter(n5Path);
		else if ( StorageType.ZARR.equals(storageType) )
			driverVolumeWriter = new N5ZarrWriter(n5Path);
		else if ( StorageType.HDF5.equals(storageType) )
			driverVolumeWriter = hdf5DriverVolumeWriter = new N5HDF5Writer(n5Path);
		else
			throw new RuntimeException( "storageType " + storageType + " not supported." );

		System.out.println( "Format being written: " + storageType );

		driverVolumeWriter.createDataset(
				n5Dataset,
				dimensions,
				blockSize,
				dataType,
				compression );

		final List<long[][]> grid = Grid.create( dimensions, blockSize );

		/*
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

		driverVolumeWriter.setAttribute( n5Dataset, "min", minBB );

		// saving metadata if it is bdv-compatible (we do this first since it might fail)
		if ( bdvString != null )
		{
			BDV.writeBDVMetaData(
					driverVolumeWriter,
					storageType,
					dataType,
					dimensions,
					compression,
					blockSize,
					this.bdvString,
					this.n5Path,
					this.xmlOutPath,
					this.angleIds,
					this.illuminationIds, 
					this.channelIds,
					this.tileIds );

			System.out.println( "Done writing BDV metadata.");
			//System.exit( 0 );
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
					// custom serialization
					final SpimData2 dataLocal = Spark.getSparkJobSpimData2("", xmlPath);

					// be smarter, test which ViewIds are actually needed for the block we want to fuse
					final Interval fusedBlock =
							Intervals.translate(
									Intervals.translate(
											new FinalInterval( gridBlock[1] ), // blocksize
											gridBlock[0] ), // block offset
									minBB ); // min of the randomaccessbileinterval

					// recover views to process
					final ArrayList< ViewId > viewIdsLocal = new ArrayList<>();

					for ( int i = 0; i < serializedViewIds.length; ++i )
					{
						final ViewId viewId = Spark.deserializeViewIds(serializedViewIds, i);

						if ( useAF )
						{
							// get updated registration for views to fuse AND all other views that may influence the fusion
							final ViewRegistration vr = dataLocal.getViewRegistrations().getViewRegistration( viewId );
							final AffineTransform3D aniso = new AffineTransform3D();
							aniso.set(
									1.0, 0.0, 0.0, 0.0,
									0.0, 1.0, 0.0, 0.0,
									0.0, 0.0, 1.0/af, 0.0 );
							vr.preconcatenateTransform( new ViewTransformAffine( "preserve anisotropy", aniso));
							vr.updateModel();
						}

						// expand to be conservative ...
						final Interval boundingBoxLocal = ViewUtil.getTransformedBoundingBox( dataLocal, viewId );
						final Interval bounds = Intervals.expand( boundingBoxLocal, 2 );

						if ( ViewUtil.overlaps( fusedBlock, bounds ) )
							viewIdsLocal.add( viewId );
					}

					//SimpleMultiThreading.threadWait( 10000 );

					// nothing to save...
					if ( viewIdsLocal.size() == 0 )
						return;

					final RandomAccessibleInterval<FloatType> source = FusionTools.fuseVirtual(
								dataLocal,
								viewIdsLocal,
								new FinalInterval(minBB, maxBB),
								Double.NaN ).getA();

					final N5Writer executorVolumeWriter;

					if ( StorageType.N5.equals(storageType) )
						executorVolumeWriter = new N5FSWriter(n5Path);
					else if ( StorageType.ZARR.equals(storageType) )
						executorVolumeWriter = new N5ZarrWriter(n5Path);
					else if ( StorageType.HDF5.equals(storageType) )
						executorVolumeWriter = hdf5DriverVolumeWriter;
					else
						throw new RuntimeException( "storageType " + storageType + " not supported." );

					if ( uint8 )
					{
						final RandomAccessibleInterval< UnsignedByteType > sourceUINT8 =
								Converters.convert(
										source,(i, o) -> o.setReal( ( i.get() - minIntensity ) / range ),
										new UnsignedByteType());

						final RandomAccessibleInterval<UnsignedByteType> sourceGridBlock = Views.offsetInterval(sourceUINT8, gridBlock[0], gridBlock[1]);
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

							final RandomAccessibleInterval<ShortType> sourceGridBlock = Views.offsetInterval(sourceINT16, gridBlock[0], gridBlock[1]);
							N5Utils.saveBlock(sourceGridBlock, executorVolumeWriter, n5Dataset, gridBlock[2]);
						}
						else
						{
							final RandomAccessibleInterval<UnsignedShortType> sourceGridBlock = Views.offsetInterval(sourceUINT16, gridBlock[0], gridBlock[1]);
							N5Utils.saveBlock(sourceGridBlock, executorVolumeWriter, n5Dataset, gridBlock[2]);
						}
					}
					else
					{
						final RandomAccessibleInterval<FloatType> sourceGridBlock = Views.offsetInterval(source, gridBlock[0], gridBlock[1]);
						N5Utils.saveBlock(sourceGridBlock, executorVolumeWriter, n5Dataset, gridBlock[2]);
					}
				});

		sc.close();

		// close HDF5 writer
		if ( hdf5DriverVolumeWriter != null )
			hdf5DriverVolumeWriter.close();

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
