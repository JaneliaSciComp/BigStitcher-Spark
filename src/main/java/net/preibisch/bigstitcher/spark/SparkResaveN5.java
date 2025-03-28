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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bigdataviewer.n5.N5CloudImageLoader;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.universe.N5Factory.StorageFormat;
import org.janelia.scicomp.n5.zstandard.ZstandardCompression;

import bdv.img.n5.N5ImageLoader;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.preibisch.bigstitcher.spark.CreateFusionContainer.Compressions;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.plugin.resave.Resave_HDF5;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.AllenOMEZarrLoader;
import net.preibisch.mvrecon.process.n5api.N5ApiTools;
import net.preibisch.mvrecon.process.n5api.N5ApiTools.MultiResolutionLevelInfo;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

public class SparkResaveN5 extends AbstractBasic implements Callable<Void>, Serializable
{
	/*
	-x '/Users/preibischs/Documents/Microscopy/Stitching/Truman/testspark/dataset.xml'
	-xo '/Users/preibischs/Documents/Microscopy/Stitching/Truman/testspark/dataset-n5.xml'
	-ds '1,1,1; 2,2,1; 4,4,1; 8,8,2'
	*/

	// TOOD: there is a bug:
	// -x s3://janelia-bigstitcher-spark/Stitching/dataset.xml -xo /Users/preibischs/SparkTest/Stitching/dataset.xml
	// sets the wrong path for the N5:
	// file:/Users/preibischs/workspace/BigStitcher-Spark/file:/Users/preibischs/SparkTest/Stitching-fromcloud/dataset.n5
	
	private static final long serialVersionUID = 1890656279324908516L;

	@Option(names = { "-xo", "--xmlout" }, required = false, description = "path to the output BigStitcher xml, e.g. /home/project-n5.xml or s3://myBucket/dataset.xml (default: overwrite input and keep a backup ~1)")
	private String xmlOutURIString = null;

	private URI xmlOutURI = null;

	@Option(names = { "--N5" }, description = "Export as N5 (default: OMEZARR)")
	private boolean useN5 = false;

	@Option(names = "--blockSize", description = "blockSize, you can use smaller blocks for HDF5 (default: 128,128,64)")
	private String blockSizeString = "128,128,64";

	@Option(names = "--blockScale", description = "how many blocks to use for a single processing step, e.g. 4,4,1 means for blockSize a 128,128,32 that each spark thread writes 512,512,32 (default: 16,16,1)")
	private String blockScaleString = "16,16,1";

	@Option(names = { "-ds", "--downsampling" }, description = "downsampling pyramid (must contain full res 1,1,1 that is always created), e.g. 1,1,1; 2,2,1; 4,4,1; 8,8,2 (default: automatically computed)")
	private String downsampling = null;

	@Option(names = {"-c", "--compression"}, defaultValue = "Zstandard", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Dataset compression")
	private Compressions compression = null;

	@Option(names = {"-cl", "--compressionLevel" }, description = "compression level, if supported by the codec (default: gzip 1, Zstandard 3, xz 6)")
	private Integer compressionLevel = null;

	@Option(names = { "-o", "--n5Path" }, description = "N5/OME-ZARR path for saving, (default: 'folder of the xml'/dataset.n5 or e.g. s3://myBucket/data.n5)")
	private String n5PathURIString = null;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		//System.out.println( com.google.common.collect.ImmutableList.class.getProtectionDomain().getCodeSource().getLocation() );
		//System.exit( 0 );

		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		if ( xmlOutURIString == null )
			xmlOutURIString = xmlURIString;

		xmlOutURI = URITools.toURI( xmlOutURIString );
		System.out.println( "xmlout: " + xmlOutURI );

		// process all views
		final ArrayList< ViewId > viewIdsGlobal = Import.getViewIds( dataGlobal );

		if ( viewIdsGlobal.size() == 0 )
		{
			throw new IllegalArgumentException( "No views to resave." );
		}
		else
		{
			System.out.println( "Following ViewIds will be resaved: ");
			for ( final ViewId v : viewIdsGlobal )
				System.out.print( "[" + v.getTimePointId() + "," + v.getViewSetupId() + "] " );
			System.out.println();
		}

		final URI n5PathURI = URITools.toURI( this.n5PathURIString == null ? URITools.appendName( URITools.getParentURI( xmlOutURI ), (useN5 ? "dataset.n5" : "dataset.ome.zarr") ) : n5PathURIString );
		final Compression compression = N5Util.getCompression( this.compression, this.compressionLevel );

		final int[] blockSize = Import.csvStringToIntArray(blockSizeString);
		final int[] blockScale = Import.csvStringToIntArray(blockScaleString);

		final int[] computeBlockSize = new int[] {
				blockSize[0] * blockScale[ 0 ],
				blockSize[1] * blockScale[ 1 ],
				blockSize[2] * blockScale[ 2 ] };

		//final N5Writer n5 = new N5FSWriter(n5Path);
		final N5Writer n5Writer = URITools.instantiateN5Writer( useN5 ? StorageFormat.N5 : StorageFormat.ZARR, n5PathURI );

		System.out.println( "Compression: " + this.compression );
		System.out.println( "Compression level: " + ( compressionLevel == null ? "default" : compressionLevel ) );
		System.out.println( "N5 block size=" + Util.printCoordinates( blockSize ) );
		System.out.println( "Compute block size=" + Util.printCoordinates( computeBlockSize ) );
		System.out.println( "Setting up XML at: " + xmlOutURI );
		System.out.println( "Setting up N5 writing to basepath: " + n5PathURI );

		// all ViewSetupIds (needed to create N5 datasets)
		final HashMap<Integer, long[]> dimensions =
				N5ApiTools.assembleDimensions( dataGlobal, viewIdsGlobal );

		// all grids across all ViewId's
		final List<long[][]> gridS0 =
				viewIdsGlobal.stream().map( viewId ->
						N5ApiTools.assembleJobs(
								viewId,
								dimensions.get( viewId.getViewSetupId() ),
								blockSize,
								computeBlockSize ) ).flatMap(List::stream).collect( Collectors.toList() );

		final Map<Integer, DataType> dataTypes =
				N5ApiTools.assembleDataTypes( dataGlobal, dimensions.keySet() );

		// estimate or read downsampling factors
		final int[][] downsamplings;

		if ( this.downsampling == null )
			downsamplings = N5ApiTools.mipMapInfoToDownsamplings( Resave_HDF5.proposeMipmaps( N5ApiTools.assembleViewSetups(dataGlobal, viewIdsGlobal) ) );
		else
			downsamplings = Import.csvStringToDownsampling( this.downsampling );

		if ( !Import.testFirstDownsamplingIsPresent( downsamplings ) )
			throw new RuntimeException( "First downsampling step must be full resolution [1,1,...1], stopping." );

		System.out.println( "Downsamplings: " + Arrays.deepToString( downsamplings ) );

		if ( dryRun )
		{
			System.out.println( "This is a dry-run, stopping here.");
			return null;
		}

		// create all datasets and write BDV metadata for all ViewIds (including downsampling) in parallel
		long time = System.currentTimeMillis();

		// TODO: is this map serializable?
		final Map< ViewId, MultiResolutionLevelInfo[] > viewIdToMrInfo =
				viewIdsGlobal.parallelStream().map( viewId ->
				{
					final MultiResolutionLevelInfo[] mrInfo;

					if ( useN5 )
					{
						mrInfo = N5ApiTools.setupBdvDatasetsN5(
								n5Writer,
								viewId,
								dataTypes.get( viewId.getViewSetupId() ),
								dimensions.get( viewId.getViewSetupId() ),
								compression,
								blockSize,
								downsamplings );
					}
					else
					{
						mrInfo = N5ApiTools.setupBdvDatasetsOMEZARR(
								n5Writer,
								viewId,
								dataTypes.get( viewId.getViewSetupId() ),
								dimensions.get( viewId.getViewSetupId() ),
								dataGlobal.getSequenceDescription().getViewDescription( viewId ).getViewSetup().getVoxelSize().dimensionsAsDoubleArray(),
								compression,
								blockSize,
								downsamplings);
					}

					return new ValuePair<>(
						new ViewId( viewId.getTimePointId(), viewId.getViewSetupId() ), // viewId is actually a ViewDescripton object, thus not serializable
						mrInfo );
				}).collect(Collectors.toMap( e -> e.getA(), e -> e.getB() ));

		System.out.println( "Created BDV-metadata, took " + (System.currentTimeMillis() - time ) + " ms." );
		System.out.println( "Number of compute blocks = " + gridS0.size() );

		final SparkConf conf = new SparkConf().setAppName("SparkResaveN5");

		if ( localSparkBindAddress )
			conf.set("spark.driver.bindAddress", "127.0.0.1");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		//
		// Save s0 level
		//
		time = System.currentTimeMillis();

		final JavaRDD<long[][]> rdds0 = sc.parallelize( gridS0, Math.min( Spark.maxPartitions, gridS0.size() ) );

		rdds0.foreach(
				gridBlock ->
				{
					final SpimData2 dataLocal = Spark.getSparkJobSpimData2(xmlURI);
					final N5Writer n5Lcl = URITools.instantiateN5Writer( useN5 ? StorageFormat.N5 : StorageFormat.ZARR, n5PathURI );

					N5ApiTools.resaveS0Block(
							dataLocal,
							n5Lcl,
							useN5 ? StorageFormat.N5 : StorageFormat.ZARR,
							dataTypes.get( N5ApiTools.gridBlockToViewId( gridBlock ).getViewSetupId() ),
							N5ApiTools.gridToDatasetBdv( 0, useN5 ? StorageFormat.N5 : StorageFormat.ZARR ), // a function mapping the gridblock to the dataset name for level 0 and N5
							gridBlock );

					n5Lcl.close();
				});

		System.out.println( "Resaved " + (useN5 ? "N5 s0" : "OME-ZARR 0") + "-level, took: " + (System.currentTimeMillis() - time ) + " ms." );

		//
		// Save remaining downsampling levels (s1 ... sN)
		//
		for ( int level = 1; level < downsamplings.length; ++level )
		{
			final int s = level;

			final List<long[][]> allBlocks =
					viewIdsGlobal.stream().map( viewId ->
							N5ApiTools.assembleJobs(
									viewId,
									viewIdToMrInfo.get(viewId)[s] )).flatMap(List::stream).collect( Collectors.toList() );

			System.out.println( "Downsampling level " + (useN5 ? "s" : "") + s + "... " );
			System.out.println( "Number of compute blocks: " + allBlocks.size() );

			final JavaRDD<long[][]> rddsN = sc.parallelize(allBlocks, Math.min( Spark.maxPartitions, allBlocks.size() ) );

			final long timeS = System.currentTimeMillis();

			rddsN.foreach(
					gridBlock ->
					{
						final N5Writer n5Lcl = URITools.instantiateN5Writer( useN5 ? StorageFormat.N5 : StorageFormat.ZARR, n5PathURI );

						if ( useN5 )
						{
							N5ApiTools.writeDownsampledBlock(
									n5Lcl,
									viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s ], //N5ResaveTools.gridToDatasetBdv( s, StorageType.N5 ),
									viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s - 1 ],//N5ResaveTools.gridToDatasetBdv( s - 1, StorageType.N5 ),
									gridBlock );
						}
						else
						{
							N5ApiTools.writeDownsampledBlock5dOMEZARR(
									n5Lcl,
									viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s ], //N5ResaveTools.gridToDatasetBdv( s, StorageType.N5 ),
									viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s - 1 ],//N5ResaveTools.gridToDatasetBdv( s - 1, StorageType.N5 ),
									gridBlock,
									0,
									0 );
						}

						n5Lcl.close();
					});

			System.out.println( "Resaved " + (useN5 ? "N5 s" : "OME-ZARR ") + s + " level, took: " + (System.currentTimeMillis() - timeS ) + " ms." );
		}

		sc.close();

		System.out.println( "resaved successfully." );

		// things look good, let's save the new XML
		System.out.println( "Saving new xml to: " + xmlOutURI );

		if ( useN5 && URITools.isFile( n5PathURI ))
		{
			dataGlobal.getSequenceDescription().setImgLoader(
					new N5ImageLoader( n5PathURI, dataGlobal.getSequenceDescription()));
		}
		else if ( useN5 )
		{
			dataGlobal.getSequenceDescription().setImgLoader(
					new N5CloudImageLoader( null, n5PathURI, dataGlobal.getSequenceDescription())); // null is OK because the instance is not used now
		}
		else
		{
			final Map< ViewId, String > viewIdToPath = new HashMap<>();

			viewIdToMrInfo.forEach( (viewId, mrInfo ) ->
				viewIdToPath.put(
						viewId,
						mrInfo[ 0 ].dataset.substring(0,  mrInfo[ 0 ].dataset.lastIndexOf( "/" ) ) )
			);

			dataGlobal.getSequenceDescription().setImgLoader(
					new AllenOMEZarrLoader( n5PathURI, dataGlobal.getSequenceDescription(), viewIdToPath )); // null is OK because the instance is not used now
		}

		new XmlIoSpimData2().save( dataGlobal, xmlOutURI );

		n5Writer.close();

		Thread.sleep( 100 );
		System.out.println( "Resaved project, in total took: " + (System.currentTimeMillis() - time ) + " ms." );
		System.out.println( "done." );

		return null;
	}

	public static void main(final String... args) {

		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new SparkResaveN5()).execute(args));
	}

}
