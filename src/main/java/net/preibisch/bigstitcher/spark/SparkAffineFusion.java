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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.StorageType;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.util.Util;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.fusion.WriteSuperBlock;
import net.preibisch.bigstitcher.spark.fusion.WriteSuperBlockMasks;
import net.preibisch.bigstitcher.spark.util.BDVSparkInstantiateViewSetup;
import net.preibisch.bigstitcher.spark.util.Downsampling;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBox;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import net.preibisch.mvrecon.process.n5api.SpimData2Tools.InstantiateViewSetup;
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

	@Option(names = { "--masks" }, description = "save only the masks (this will not fuse the images)")
	private boolean masks = false;

	@Option(names = { "--firstTileWins" }, description = "use firstTileWins fusion strategy (default: false - using weighted average blending fusion)")
	private boolean firstTileWins = false;

	@Option(names = "--maskOffset", description = "allows to make masks larger (+, the mask will include some background) or smaller (-, some fused content will be cut off), warning: in the non-isotropic coordinate space of the raw input images (default: 0.0,0.0,0.0)")
	private String maskOffset = "0.0,0.0,0.0";

	// TODO: support create custom downsampling pyramids, null is fine for now (used by multiRes later)
	private int[][] downsamplings;


	/**
	 * Assumes that
	 * at most 8 views are required per output block, and
	 * at most 8 input blocks per output block are required from one input view per output block.
	 */
	//static final int N_PREFETCH_THREADS = 72;

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

		if ( this.bdvString != null && xmlOutPath == null )
		{
			System.out.println( "Please specify the output XML for the BDV dataset: -xo");
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

		final double[] maskOff = Import.csvStringToDoubleArray(maskOffset);
		if ( masks )
			System.out.println( "Fusing ONLY MASKS! Mask offset: " + Util.printCoordinates( maskOff ) );

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

		if (localSparkBindAddress)
			conf.set("spark.driver.bindAddress", "127.0.0.1");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final JavaRDD<long[][]> rdd = sc.parallelize( grid );

		final long time = System.currentTimeMillis();

		if ( masks )
			rdd.foreach( new WriteSuperBlockMasks(
					xmlPath,
					preserveAnisotropy,
					anisotropyFactor,
					minBB,
					n5Path,
					n5Dataset,
					storageType,
					serializedViewIds,
					uint8,
					uint16,
					maskOff,
					blockSize ) );
		else
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
				blockSize,
				firstTileWins ) );

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
		else if ( multiRes )
			System.out.println( "Saved, e.g. view with './n5-view -i " + n5Path + " -d " + n5Dataset.substring( 0, n5Dataset.length() - 3) + "'" );
		else
			System.out.println( "Saved, e.g. view with './n5-view -i " + n5Path + " -d " + n5Dataset + "'" );

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
}
