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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.N5Factory.StorageFormat;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converter;
import net.imglib2.converter.RealUnsignedByteConverter;
import net.imglib2.converter.RealUnsignedShortConverter;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractInfrastructure;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.fusion.GenerateComputeBlockMasks;
import net.preibisch.bigstitcher.spark.fusion.OverlappingBlocks;
import net.preibisch.bigstitcher.spark.fusion.OverlappingViews;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.plugin.fusion.FusionGUI.FusionType;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBox;
import net.preibisch.mvrecon.process.fusion.blk.BlkAffineFusion;
import net.preibisch.mvrecon.process.fusion.transformed.TransformVirtual;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import net.preibisch.mvrecon.process.n5api.N5ApiTools;
import net.preibisch.mvrecon.process.n5api.N5ApiTools.MultiResolutionLevelInfo;
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

	@Option(names = { "--masks" }, description = "save only the masks (this will not fuse the images)")
	private boolean masks = false;

	@Option(names = "--maskOffset", description = "allows to make masks larger (+, the mask will include some background) or smaller (-, some fused content will be cut off), warning: in the non-isotropic coordinate space of the raw input images (default: 0.0,0.0,0.0)")
	private String maskOffset = "0.0,0.0,0.0";

	@Option(names = { "--firstTileWins" }, description = "use firstTileWins fusion strategy, with lowest ViewIds winning (default: false - using weighted average blending fusion)")
	private boolean firstTileWins = false;

	@Option(names = { "--firstTileWinsInverse" }, description = "use firstTileWins fusion strategy, with highest ViewIds winning (default: false - using weighted average blending fusion)")
	private boolean firstTileWinsInverse = false;


	@Option(names = { "-t", "--timepointIndex" }, description = "specify a specific timepoint index of the output container that should be fused, usually you would also specify what --angleId, --tileId, ... or ViewIds -vi are being fused.")
	private Integer timepointIndex = null;

	@Option(names = { "-c", "--channelIndex" }, description = "specify a specific channel index of the output container that should be fused, usually you would also specify what --angleId, --tileId, ... or ViewIds -vi are being fused.")
	private Integer channelIndex = null;


	// To specify what goes into the current 3D volume
	@Option(names = { "--angleId" }, description = "list the angle ids that should be processed, you can find them in the XML, e.g. --angleId '0,1,2' (default: all angles)")
	protected String angleIds = null;

	@Option(names = { "--tileId" }, description = "list the tile ids that should be processed, you can find them in the XML, e.g. --tileId '0,1,2' (default: all tiles)")
	protected String tileIds = null;

	@Option(names = { "--illuminationId" }, description = "list the illumination ids that should be processed, you can find them in the XML, e.g. --illuminationId '0,1,2' (default: all illuminations)")
	protected String illuminationIds = null;

	@Option(names = { "--channelId" }, description = "list the channel ids that should be processed, you can find them in the XML (usually just one when fusing), e.g. --channelId '0,1,2' (default: all channels)")
	protected String channelIds = null;

	@Option(names = { "--timepointId" }, description = "list the timepoint ids that should be processed, you can find them in the XML (usually just one when fusing), e.g. --timepointId '0,1,2' (default: all time points)")
	protected String timepointIds = null;

	@Option(names = { "-vi" }, description = "specifically list the view ids (time point, view setup) that should be fused into a single image, e.g. -vi '0,0' -vi '0,1' (default: all view ids)")
	protected String[] vi = null;

	@Option(names = { "--prefetch" }, description = "prefetch all blocks required for fusion in each Spark job using unlimited threads, useful in cloud environments (default: false)")
	protected boolean prefetch = false;

	URI outPathURI = null;
	/**
	 * Prefetching now works with a Executors.newCachedThreadPool();
	 */
	//static final int N_PREFETCH_THREADS = 72;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		if (dryRun)
		{
			System.out.println( "dry-run not supported for affine fusion.");
			return null;
		}

		if ( firstTileWins && firstTileWinsInverse )
		{
			System.out.println( "You can only choose one of the two firstTileWins or firstTileWinsInverse.");
			return null;
		}

		if ( timepointIndex != null && channelIndex == null || timepointIndex == null && channelIndex != null )
		{
			System.out.println( "You have to specify timepointId and channelId together, one alone does not work. timepointId =" + timepointIndex + ", channelId=" + channelIndex );
			return null;
		}

		if ( timepointIndex == null && ( vi != null || timepointIds != null || channelIds != null || illuminationIds != null || tileIds != null || angleIds != null ) )

		{
			System.out.println( "You can only specify specify angles, tiles, ..., ViewIds if you provided a specific timepointIndex & channelIndex.");
			return null;
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

		final int numTimepoints, numChannels;

		if ( timepointIndex == null )
		{
			numTimepoints = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/NumTimepoints", int.class );
			numChannels = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/NumChannels", int.class );
		}
		else
		{
			System.out.println( "Overriding numChannels and numTimepoints from metadata, instead processing timepointIndex=" + timepointIndex + ", channelIndex=" + channelIndex + " only.");
			numTimepoints = numChannels = 1;
		}

		final long[] bbMin = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/Boundingbox_min", long[].class );
		final long[] bbMax = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/Boundingbox_max", long[].class );

		final BoundingBox boundingBox = new BoundingBox( new FinalInterval( bbMin, bbMax ) );

		final boolean preserveAnisotropy = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/PreserveAnisotropy", boolean.class );
		final double anisotropyFactor = preserveAnisotropy ? driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/AnisotropyFactor", double.class ) : Double.NaN;
		final int[] blockSize = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/BlockSize", int[].class );

		final DataType dataType = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/DataType", DataType.class );

		final long[] orig_bbMin = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/Boundingbox_min", long[].class );
		if ( !Double.isNaN( anisotropyFactor ) )
			orig_bbMin[ 2 ] = Math.round( Math.floor( orig_bbMin[ 2 ] * anisotropyFactor ) );

		System.out.println( "FusionFormat: " + fusionFormat );
		System.out.println( "Input XML: " + xmlURI );
		System.out.println( "BDV project: " + bdv );
		System.out.println( "numTimepoints of fused dataset(s): " + numTimepoints );
		System.out.println( "numChannels of fused dataset(s): " + numChannels );
		System.out.println( "BoundingBox: " + boundingBox );
		System.out.println( "preserveAnisotropy: " + preserveAnisotropy );
		System.out.println( "anisotropyFactor: " + anisotropyFactor );
		System.out.println( "blockSize: " + Arrays.toString( blockSize ) );
		System.out.println( "dataType: " + dataType );

		double minI = Double.NaN, maxI = Double.NaN;
		try
		{
			minI = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/MinIntensity", double.class );
			maxI = driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/MaxIntensity", double.class );
		}
		catch ( Exception e )
		{
			System.out.println( "Min/Max intensity not stored." );
		}

		final double minIntensity = minI;
		final double maxIntensity = maxI;

		System.out.println( "minIntensity: " + minI );
		System.out.println( "maxIntensity: " + maxI );

		final MultiResolutionLevelInfo[][] mrInfos =
				driverVolumeWriter.getAttribute( "/", "Bigstitcher-Spark/MultiResolutionInfos", MultiResolutionLevelInfo[][].class );

		System.out.println( "Loaded " + mrInfos.length + " metadata object for fused " + storageType + " volume(s)" );

		final double[] maskOff = Import.csvStringToDoubleArray(maskOffset);

		final SpimData2 dataGlobal = Spark.getJobSpimData2( xmlURI, 0 );

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIdsGlobal;

		if (
				dataGlobal.getSequenceDescription().getAllChannelsOrdered().size() != numChannels ||
						dataGlobal.getSequenceDescription().getTimePoints().getTimePointsOrdered().size() != numTimepoints )
		{
			System.out.println(
					"The number of channels and timepoint in XML does not match the number in the export dataset."
							+ "You have to specify which ViewIds/Channels/Illuminations/Tiles/Angles/Timepoints should be fused into"
							+ "a specific 3D volume in the fusion dataset:");

			viewIdsGlobal = AbstractSelectableViews.loadViewIds( dataGlobal, vi, angleIds, channelIds, illuminationIds, tileIds, timepointIds  );

			if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
				return null;
		}
		else
		{
			viewIdsGlobal = Import.getViewIds( dataGlobal );
		}

		final int[] blocksPerJob = Import.csvStringToIntArray(blockScaleString);
		System.out.println( "Fusing: " + boundingBox.getTitle() + ": " + Util.printInterval( boundingBox ) +
				" with blocksize " + Util.printCoordinates( blockSize ) + " and " + Util.printCoordinates( blocksPerJob ) + " blocks per job" );

		if ( dataType == DataType.UINT8 )
			System.out.println( "Fusing to UINT8, min intensity = " + minIntensity + ", max intensity = " + maxIntensity );
		else if ( dataType == DataType.UINT16 )
			System.out.println( "Fusing to UINT16, min intensity = " + minIntensity + ", max intensity = " + maxIntensity );
		else
			System.out.println( "Fusing to FLOAT32" );

		//
		// final variables for Spark
		//
		final long[] dimensions = boundingBox.dimensionsAsLongArray();

		// TODO: do we still need this?
		try
		{
			// trigger the N5-blosc error, because if it is triggered for the first
			// time inside Spark, everything crashes
			new N5FSWriter(null);
		}
		catch (Exception e ) {}

		final SparkConf conf = new SparkConf().setAppName("AffineFusion");

		if (localSparkBindAddress)
			conf.set("spark.driver.bindAddress", "127.0.0.1");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final SequenceDescription sd = dataGlobal.getSequenceDescription();

		final HashMap< Integer, Integer > tpIdToTpIndex = new HashMap<>();
		final HashMap< Integer, Integer > chIdToChIndex = new HashMap<>();

		for ( int t = 0; t < sd.getTimePoints().getTimePointsOrdered().size(); ++t )
			tpIdToTpIndex.put( sd.getTimePoints().getTimePointsOrdered().get( t ).getId(), t );

		for ( int c = 0; c < sd.getAllChannelsOrdered().size(); ++c )
			chIdToChIndex.put( sd.getAllChannelsOrdered().get( c ).getId(), c );

		final long totalTime = System.currentTimeMillis();

		for ( int c = 0; c < numChannels; ++c )
			for ( int t = 0; t < numTimepoints; ++t )
			{
				final int tIndex = (timepointIndex == null) ? t : timepointIndex;
				final int cIndex = (channelIndex == null) ? c : channelIndex;

				System.out.println( "\nProcessing channel " + cIndex + ", timepoint " + tIndex );
				System.out.println( "-----------------------------------" );

				final ArrayList< ViewId > viewIds = new ArrayList<>();
				viewIdsGlobal.forEach( viewId -> {
					final ViewDescription vd = sd.getViewDescription( viewId );

					if ( timepointIndex != null || tpIdToTpIndex.get( vd.getTimePointId() ) == tIndex && chIdToChIndex.get( vd.getViewSetup().getChannel().getId() ) == cIndex )
						viewIds.add( new ViewId( viewId.getTimePointId(), viewId.getViewSetupId() ) );
				});

				if ( masks )
					System.out.println( "Creating masks for " + viewIds.size() + " views of this 3D volume ... " );
				else
					System.out.println( "Fusing " + viewIds.size() + " views for this 3D volume ... " );

				viewIds.forEach( vd -> System.out.println( Group.pvid( vd ) ) );
				final MultiResolutionLevelInfo[] mrInfo;

				if ( storageType == StorageFormat.ZARR )
					mrInfo = mrInfos[ 0 ];
				else
					mrInfo = mrInfos[ cIndex + tIndex*numChannels ];

				// using bigger blocksizes than being stored for efficiency (needed for very large datasets)
				final int[] computeBlockSize = new int[ 3 ];
				Arrays.setAll( computeBlockSize, d -> blockSize[ d ] * blocksPerJob[ d ] );
				final List<long[][]> grid = Grid.create(dimensions,
						computeBlockSize,
						blockSize);

				System.out.println( "numJobs = " + grid.size() );

				//driverVolumeWriter.setAttribute( n5Dataset, "offset", minBB );

				final JavaRDD<long[][]> rdd = sc.parallelize( grid, Math.min( Spark.maxPartitions, grid.size() ) );

				long time = System.currentTimeMillis();

				rdd.foreach(
						gridBlock ->
						{
							final SpimData2 dataLocal = Spark.getSparkJobSpimData2(xmlURI);

							final HashMap< ViewId, AffineTransform3D > registrations =
									TransformVirtual.adjustAllTransforms(
											viewIds,
											dataLocal.getViewRegistrations().getViewRegistrations(),
											anisotropyFactor,
											Double.NaN );

							final HashMap< ViewId, AffineTransform3D > orig_registrations =
									TransformVirtual.adjustAllTransforms(
											viewIds,
											dataLocal.getViewRegistrations().getViewRegistrations(),
											Double.NaN,
											Double.NaN );

							final Converter conv;
							final Type type;
							final boolean uint8, uint16;

							if ( dataType == DataType.UINT8 )
							{
								conv = new RealUnsignedByteConverter<>( minIntensity, maxIntensity );
								type = new UnsignedByteType();
								uint8 = true;
								uint16 = false;
							}
							else if ( dataType == DataType.UINT16 )
							{
								conv = new RealUnsignedShortConverter<>( minIntensity, maxIntensity );
								type = new UnsignedShortType();
								uint8 = false;
								uint16 = true;
							}
							else
							{
								conv = null;
								type = new FloatType();
								uint8 = false;
								uint16 = false;
							}

							// The min coordinates of the block that this job renders (in pixels)
							final int n = gridBlock[ 0 ].length;
							final long[] superBlockOffset = new long[ n ];
							Arrays.setAll( superBlockOffset, d -> gridBlock[ 0 ][ d ] + orig_bbMin[ d ] );

							// The size of the block that this job renders (in pixels)
							final long[] superBlockSize = gridBlock[ 1 ];

							//TODO: prefetchExecutor!!
							final long[] fusedBlockMin = new long[ n ];
							final long[] fusedBlockMax = new long[ n ];
							final Interval fusedBlock = FinalInterval.wrap( fusedBlockMin, fusedBlockMax );

							// pre-filter views that overlap the superBlock
							Arrays.setAll( fusedBlockMin, d -> superBlockOffset[ d ] );
							Arrays.setAll( fusedBlockMax, d -> superBlockOffset[ d ] + superBlockSize[ d ] - 1 );

							final List< ViewId > overlappingViews =
									OverlappingViews.findOverlappingViews( dataLocal, viewIds, orig_registrations, fusedBlock );

							final RandomAccessibleInterval img;

							if ( masks )
							{
								System.out.println( "Creating masks for block: offset=" + Util.printCoordinates( gridBlock[0] ) + ", dimension=" + Util.printCoordinates( gridBlock[1] ) );

								img = Views.zeroMin(
										new GenerateComputeBlockMasks(
												dataLocal,
												registrations,
												overlappingViews,
												bbMin,
												bbMax,
												uint8,
												uint16,
												maskOff ).call( gridBlock ) );
							}
							else
							{
								//
								// PREFETCHING, TODO: should be part of BlkAffineFusion.init
								//
								final OverlappingBlocks overlappingBlocks = OverlappingBlocks.find( dataLocal, overlappingViews, fusedBlock );
								if ( overlappingBlocks.overlappingViews().isEmpty() )
									return;

								if ( prefetch )
								{
									System.out.println( "Prefetching: " + overlappingBlocks.numPrefetchBlocks() + " block(s) from " + overlappingBlocks.overlappingViews().size() + " overlapping view(s) in the input data." );

									final ExecutorService prefetchExecutor = Executors.newCachedThreadPool();
									overlappingBlocks.prefetch(prefetchExecutor);
									prefetchExecutor.shutdown();
								}

								System.out.println( "Fusing block: offset=" + Util.printCoordinates( gridBlock[0] ) + ", dimension=" + Util.printCoordinates( gridBlock[1] ) );

								final FusionType fusionType;

								if ( firstTileWins )
									fusionType = FusionType.FIRST_LOW;
								else if ( firstTileWinsInverse )
									fusionType = FusionType.FIRST_HIGH;
								else
									fusionType = FusionType.AVG_BLEND;

								// returns a zero-min interval
								img = BlkAffineFusion.init(
										conv,
										dataLocal.getSequenceDescription().getImgLoader(),
										viewIds,
										registrations,
										dataLocal.getSequenceDescription().getViewDescriptions(),
										fusionType,//fusion.getFusionType(),
										1, // linear interpolation
										null, // intensity correction
										new BoundingBox( new FinalInterval( bbMin, bbMax ) ),
										(RealType & NativeType)type,
										blockSize );
							}

							final long[] blockOffset, blockSizeExport, gridOffset;

							final RandomAccessible image;

							// 5D OME-ZARR CONTAINER
							if ( storageType == StorageFormat.ZARR )
							{
								// gridBlock is 3d, make it 5d
								blockOffset = new long[] { gridBlock[0][0], gridBlock[0][1], gridBlock[0][2], cIndex, tIndex };
								blockSizeExport = new long[] { gridBlock[1][0], gridBlock[1][1], gridBlock[1][2], 1, 1 };
								gridOffset = new long[] { gridBlock[2][0], gridBlock[2][1], gridBlock[2][2], cIndex, tIndex }; // because blocksize in C & T is 1

								// img is 3d, make it 5d
								// the same information is returned no matter which index is queried in C and T
								image = Views.addDimension( Views.addDimension( img ) );
							}
							else
							{
								blockOffset = gridBlock[0];
								blockSizeExport = gridBlock[1];
								gridOffset = gridBlock[2];

								image = img;
							}

							final Interval block =
									Intervals.translate(
											new FinalInterval( blockSizeExport ),
											blockOffset );

							final RandomAccessibleInterval source =
									Views.interval( image, block );

							final RandomAccessibleInterval sourceGridBlock =
									Views.offsetInterval(source, blockOffset, blockSizeExport);

							final N5Writer driverVolumeWriterLocal = N5Util.createN5Writer( outPathURI, storageType );

							N5Utils.saveBlock(sourceGridBlock, driverVolumeWriterLocal, mrInfo[ 0 ].dataset, gridOffset );

							if ( N5Util.sharedHDF5Writer == null )
								driverVolumeWriterLocal.close();
						} );

				System.out.println( new Date( System.currentTimeMillis() ) + ": Saved full resolution, took: " + (System.currentTimeMillis() - time ) + " ms." );

				//
				// save multiresolution pyramid (s1 ... sN)
				//
				for ( int level = 1; level < mrInfo.length; ++level )
				{
					final int s = level;

					final List<long[][]> allBlocks = Grid.create(
							new long[] { mrInfo[ level ].dimensions[ 0 ], mrInfo[ level ].dimensions[ 1 ], mrInfo[ level ].dimensions[ 2 ] },
							computeBlockSize,
							blockSize);

					System.out.println( new Date( System.currentTimeMillis() ) + ": Downsampling: " + Util.printCoordinates( mrInfo[ level ].absoluteDownsampling ) + " with relative downsampling of " + Util.printCoordinates( mrInfo[ level ].relativeDownsampling ));
					System.out.println( new Date( System.currentTimeMillis() ) + ": s" + level + " num blocks=" + allBlocks.size() );
					System.out.println( new Date( System.currentTimeMillis() ) + ": Loading '" + mrInfo[ level - 1 ].dataset + "', downsampled will be written as '" + mrInfo[ level ].dataset + "'." );

					time = System.currentTimeMillis();

					final JavaRDD<long[][]> rddDS = sc.parallelize( allBlocks, Math.min( Spark.maxPartitions, allBlocks.size() ) );

					rddDS.foreach(
							gridBlock ->
							{
								final N5Writer driverVolumeWriterLocal = N5Util.createN5Writer( outPathURI, storageType );

								// 5D OME-ZARR CONTAINER
								if ( storageType == StorageFormat.ZARR )
								{
									N5ApiTools.writeDownsampledBlock5dOMEZARR(
											driverVolumeWriterLocal,
											mrInfo[ s ],
											mrInfo[ s - 1 ],
											gridBlock,
											cIndex,
											tIndex );
								}
								else
								{
									N5ApiTools.writeDownsampledBlock(
											driverVolumeWriterLocal,
											mrInfo[ s ],
											mrInfo[ s - 1 ],
											gridBlock );
								}

								if ( N5Util.sharedHDF5Writer == null )
									driverVolumeWriterLocal.close();

							});

					System.out.println( new Date( System.currentTimeMillis() ) + ": Saved level s " + level + ", took: " + (System.currentTimeMillis() - time ) + " ms." );
				}
			}

		// close main writer (is shared over Spark-threads if it's HDF5, thus just closing it here)
		driverVolumeWriter.close();

		/*
		if ( multiRes )
			System.out.println( "Saved, e.g. view with './n5-view -i " + n5PathURI + " -d " + n5Dataset.substring( 0, n5Dataset.length() - 3) + "'" );
		else
			System.out.println( "Saved, e.g. view with './n5-view -i " + n5PathURI + " -d " + n5Dataset + "'" );
		*/

		System.out.println( "done, took: " + (System.currentTimeMillis() - totalTime ) + " ms." );

		sc.close();

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