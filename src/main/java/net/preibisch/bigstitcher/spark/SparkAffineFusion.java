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
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.universe.N5Factory.StorageFormat;
import org.janelia.scicomp.n5.zstandard.ZstandardCompression;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.util.Util;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractInfrastructure;
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
import net.preibisch.mvrecon.process.export.ExportN5Api;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import net.preibisch.mvrecon.process.n5api.N5ApiTools;
import net.preibisch.mvrecon.process.n5api.N5ApiTools.MultiResolutionLevelInfo;
import net.preibisch.mvrecon.process.n5api.SpimData2Tools;
import net.preibisch.mvrecon.process.n5api.SpimData2Tools.InstantiateViewSetup;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.Grid;
import util.URITools;

public class SparkAffineFusion extends AbstractInfrastructure implements Callable<Void>, Serializable
{
	public static enum DataTypeFusion
	{
		UINT8, UINT16, FLOAT32
	}

	private static final long serialVersionUID = -6103761116219617153L;

	@Option(names = { "-o", "--n5Path" }, required = true, description = "N5/ZARR/HDF5 basse path for saving (must be combined with the option '-d' or '--bdv'), e.g. -o /home/fused.n5 or e.g. s3://myBucket/data.n5")
	private String outputPathURIString = null;

	@Option(names = {"-s", "--storage"}, description = "Dataset storage type, can be used to override guessed format (default: guess from file/directory-ending)")
	private StorageFormat storageType = null;

	@Option(names = "--blockScale", description = "how many blocks to use for a single processing step, e.g. 4,4,1 means for blockSize a 128,128,64 that each spark thread writes 512,512,64 (default: 2,2,1)")
	private String blockScaleString = "2,2,1";

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

	URI outPathURI = null;
	/**
	 * Prefetching now works with a Executors.newCachedThreadPool();
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

		this.outPathURI = URITools.toURI( outputPathURIString );
		System.out.println( "Fused volume: " + outPathURI );

		if ( storageType == null )
		{
			if ( outputPathURIString.toLowerCase().endsWith( ".zarr" ) )
				storageType = StorageFormat.ZARR;
			else if ( outputPathURIString.toLowerCase().endsWith( ".n5" ) )
				storageType = StorageFormat.N5;
			else if ( outputPathURIString.toLowerCase().endsWith( ".h5" ) || outPathURI.toString().toLowerCase().endsWith( ".hdf5" ) )
				storageType = StorageFormat.HDF5;
			else
			{
				System.out.println( "Unable to guess format from URI '" + outPathURI + "', please specify using '-s'");
				return null;
			}

			System.out.println( "Guessed format " + storageType + " will be used to open URI '" + outPathURI + "', you can override it using '-s'");
		}
		else
		{
			System.out.println( "Format " + storageType + " will be used to open " + outPathURI );
		}

		final N5Writer driverVolumeWriter = N5Util.createN5Writer( outPathURI, storageType );

		final String fusionFormat = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/FusionFormat", String.class );

		final boolean bdv = fusionFormat.toLowerCase().contains( "BDV" );

		final URI xmlURI = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/InputXML", URI.class );
		final int numTimepoints = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/NumTimepoints", int.class );
		final int numChannels = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/NumChannels", int.class );

		final long[] bbMin = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/Boundingbox_min", long[].class );
		final long[] bbMax = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/Boundingbox_max", long[].class );
 
		final BoundingBox bb = new BoundingBox( new FinalInterval( bbMin, bbMax ) );

		final boolean preserveAnisotropy = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/PreserveAnisotropy", boolean.class );
		final double anisotropyFactor = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/AnisotropyFactor", double.class );
		final DataType dataType = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/DataType", DataType.class );

		System.out.println( "FusionFormat: " + fusionFormat );
		System.out.println( "Input XML: " + xmlURI );
		System.out.println( "BDV project: " + bdv );
		System.out.println( "numTimepoints of fused dataset(s): " + numTimepoints );
		System.out.println( "numChannels of fused dataset(s): " + numChannels );
		System.out.println( "BoundingBox: " + bb );
		System.out.println( "preserveAnisotropy: " + preserveAnisotropy );
		System.out.println( "anisotropyFactor: " + anisotropyFactor );
		System.out.println( "dataType: " + dataType );

		final MultiResolutionLevelInfo[][] mrInfos =
				driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/MultiResolutionInfos", MultiResolutionLevelInfo[][].class );

		System.out.println( "Loaded " + mrInfos.length + " metadata object for fused " + storageType + " volume(s)" );

		final SpimData2 dataGlobal = Spark.getJobSpimData2( xmlURI, 0 );

		if ( dataGlobal == null )
			return null;

		/*

		final ArrayList< ViewId > viewIdsGlobal = this.loadViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			return null;

 		BoundingBox boundingBox = loadFromContainer;


		if ( this.bdvString != null )
		{
			this.xmlOutURI = URITools.toURI( xmlOutURIString );
			System.out.println( "XML: " + xmlOutURI );
		}

		final int[] blockSize = Import.csvStringToIntArray(blockSizeString);
		final int[] blocksPerJob = Import.csvStringToIntArray(blockScaleString);
		System.out.println( "Fusing: " + boundingBox.getTitle() + ": " + Util.printInterval( boundingBox ) +
				" with blocksize " + Util.printCoordinates( blockSize ) + " and " + Util.printCoordinates( blocksPerJob ) + " blocks per job" );

		final DataType dataType;

		if ( dataTypeFusion == DataTypeFusion.UINT8 )
		{
			System.out.println( "Fusing to UINT8, min intensity = " + minIntensity + ", max intensity = " + maxIntensity );
			dataType = DataType.UINT8;
		}
		else if ( dataTypeFusion == DataTypeFusion.UINT16 )
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
			downsamplings = ExportN5Api.estimateMultiResPyramid( new FinalDimensions( boundingBox ), anisotropyFactor );
		else if ( this.downsampling != null )
			downsamplings = Import.csvStringListToDownsampling( this.downsampling );
		else
			downsamplings = null;// for new code: new int[][]{{ 1, 1, 1 }};

		final long[] dimensions = boundingBox.dimensionsAsLongArray();

		final String n5Dataset = this.n5Dataset != null ? this.n5Dataset : N5ApiTools.createBDVPath( this.bdvString, 0, this.storageType );

		// TODO: expose
		final Compression compression = new ZstandardCompression( 3 );// new GzipCompression( 1 );

		final double minIntensity = ( dataTypeFusion != DataTypeFusion.FLOAT32 ) ? this.minIntensity : 0;
		final double range;
		if ( dataTypeFusion == DataTypeFusion.UINT8 )
			range = ( this.maxIntensity - this.minIntensity ) / 255.0;
		else if ( dataTypeFusion == DataTypeFusion.UINT16 )
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

		System.out.println( "Format being written: " + storageType );
		final N5Writer driverVolumeWriter = N5Util.createN5Writer( n5PathURI, storageType );

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
				if ( SpimData2Tools.writeBDVMetaData(
						driverVolumeWriter,
						storageType,
						dataType,
						dimensions,
						compression,
						blockSize,
						downsamplings,
						viewId,
						n5PathURI,
						xmlOutURI,
						instantiate ) == null )
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
					xmlURI,
					preserveAnisotropy,
					anisotropyFactor,
					minBB,
					n5PathURI,
					n5Dataset,
					storageType,
					serializedViewIds,
					dataTypeFusion == DataTypeFusion.UINT8,
					dataTypeFusion == DataTypeFusion.UINT16,
					maskOff,
					blockSize ) );
		else
			rdd.foreach( new WriteSuperBlock(
					xmlURI,
					preserveAnisotropy,
					anisotropyFactor,
					minBB,
					n5PathURI,
					n5Dataset,
					bdvString,
					storageType,
					serializedViewIds,
					dataTypeFusion == DataTypeFusion.UINT8,
					dataTypeFusion == DataTypeFusion.UINT16,
					minIntensity,
					range,
					blockSize,
					firstTileWins ) );

		if ( this.downsamplings != null )
		{
			// TODO: run common downsampling code (affine, non-rigid, downsampling-only)
			Downsampling.createDownsampling(
					n5PathURI,
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

		// close main writer (is shared over Spark-threads if it's HDF5, thus just closing it here)
		driverVolumeWriter.close();

		if ( multiRes )
			System.out.println( "Saved, e.g. view with './n5-view -i " + n5PathURI + " -d " + n5Dataset.substring( 0, n5Dataset.length() - 3) + "'" );
		else
			System.out.println( "Saved, e.g. view with './n5-view -i " + n5PathURI + " -d " + n5Dataset + "'" );

		System.out.println( "done, took: " + (System.currentTimeMillis() - time ) + " ms." );
		*/
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
