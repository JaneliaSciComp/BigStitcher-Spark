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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bigdataviewer.n5.N5CloudImageLoader;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.shard.ShardCodecInfo;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;

import bdv.img.n5.N5ImageLoader;
import mpicbg.spim.data.generic.sequence.BasicImgLoader;
import mpicbg.spim.data.sequence.SetupImgLoader;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.ViewSetup;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Cast;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.flatfield.BasicFlatfield;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.correction.ViewCorrection;
import net.preibisch.bigstitcher.spark.correction.ViewCorrections;
import net.preibisch.bigstitcher.spark.flatfield.FlatfieldApply;
import net.preibisch.bigstitcher.spark.flatfield.FlatfieldCorrection;
import net.preibisch.bigstitcher.spark.lens.LensApply;
import net.preibisch.bigstitcher.spark.lens.LensCorrection;
import net.preibisch.bigstitcher.spark.lens.LensModels;
import net.preibisch.bigstitcher.spark.lens.LensModels.Model;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.bigstitcher.spark.util.RetryTrackerSpark;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.plugin.resave.Resave_HDF5;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.AllenOMEZarrLoader;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.AllenOMEZarrLoader.OMEZARREntry;
import net.preibisch.mvrecon.process.n5api.N5ApiTools;
import net.preibisch.mvrecon.process.n5api.N5ApiTools.MultiResolutionLevelInfo;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

/**
 * Apply illumination (BaSiC flatfield / darkfield) and, optionally, per-channel 2D
 * lens/aberration correction to all (selected) views of a dataset, writing a
 * corrected multi-resolution OME-ZARR / N5 container and emitting an updated
 * SpimData2 XML pointing at it.
 * <p>
 * When {@code --lensCorrection} is given, the per-channel 2D warp (mpicbg TrakEM2
 * {@code NonLinearCoordinateTransform} + optional {@code AffineModel2D}, matched to
 * a view by channel-name substring) is applied to the full view <em>before</em> the
 * flatfield correction, in the same s0 pass, so the volume is resaved only once. A
 * view whose channel matches no lens model is logged and left un-warped (flatfield
 * only). Without {@code --lensCorrection} behavior is byte-identical to a pure
 * flatfield correction.
 * <p>
 * Only s0 is corrected; the downsampling pyramid is generated from the corrected
 * s0. Orchestration mirrors {@link SparkResaveN5}: each s0 block applies the composed
 * {@link ViewCorrection}s ({@code lens.LensCorrection} + {@code flatfield.FlatfieldCorrection})
 * via {@link ViewCorrections#applyCorrections} and writes the result. Output format
 * defaults to inherit the
 * source volume's per-container layout (block/shard size, compression, storage
 * format, pyramid factors) unless the corresponding CLI option is explicitly set.
 */
public class SparkIlluminationAndLensCorrections extends AbstractSelectableViews implements Callable< Void >, Serializable
{
	private static final long serialVersionUID = 4213764132648765311L;

	public enum MissingFields { ERROR, SKIP, COPY }

	public enum OutputDataType { SAME, FLOAT32 }

	public enum BaselineDrift { IGNORE, ZERO, MEAN }

	public enum BaselineGranularity { VIEW, SLICE }

	@Option(names = { "--fields" }, required = true, description = "container path holding the estimated fields (channelX/illuminationY/flatfield|darkfield), as written by BasicFlatfieldEstimation")
	private String fieldsPathURIString = null;

	@Option(names = { "--lensCorrection" }, description = "optional lens/aberration-correction JSON file (mpicbg TrakEM2 NonLinearCoordinateTransform + AffineModel2D per channel). When set, the per-channel 2D warp is applied before the flatfield correction. Matched to a view when the view's channel name is a substring of the JSON entry name. If omitted, lens correction is skipped.")
	private String lensCorrectionURIString = null;

	@Option(names = { "--lensMeshResolution" }, defaultValue = "64", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "mesh resolution used to render the lens warp (only used with --lensCorrection)")
	private int lensMeshResolution = 64;

	@Option(names = { "--lensIncludeAffine" }, defaultValue = "true", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "also apply the per-channel AffineModel2D after the nonlinear warp (only used with --lensCorrection)")
	private boolean lensIncludeAffine = true;

	@Option(names = { "-o", "--output" }, required = true, description = "output container for the corrected views, e.g. /home/corrected.ome.zarr or s3://myBucket/corrected.ome.zarr")
	private String outputPathURIString = null;

	@Option(names = { "-xo", "--xmlout" }, description = "path to the output BigStitcher xml (default: <input>.corrected.xml, no overwrite)")
	private String xmlOutURIString = null;

	@Option(names = { "-s", "--storage" }, description = "output storage type: ZARR (v3) | ZARR2 (v2) | N5 | HDF5 (default: inherit source; else guess from -o extension)")
	private StorageFormat storageFormat = null;

	@Option(names = { "--outputDataType" }, defaultValue = "SAME", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "output pixel type: SAME (source dtype) | FLOAT32")
	private OutputDataType outputDataType = OutputDataType.SAME;

	@Option(names = { "--blockSize" }, description = "inner chunk size (default: inherit source s0 chunk size; else 128,128,64)")
	private String blockSizeString = null;

	@Option(names = { "--blockScale" }, description = "compute-block multiplier / shard-size factor (default: inherit source; else 16,16,1)")
	private String blockScaleString = null;

	@Option(names = { "-ds", "--downsampling" }, description = "downsampling pyramid (must contain 1,1,1), e.g. 1,1,1; 2,2,1; 4,4,1 (default: inherit source pyramid; else auto)")
	private String downsampling = null;

	@Option(names = { "-c", "--compression" }, description = "dataset compression (default: inherit source; else Zstandard)")
	private Compressions compressionType = null;

	@Option(names = { "-cl", "--compressionLevel" }, description = "compression level, if supported by the codec (default: gzip 1, Zstandard 3, xz 6)")
	private Integer compressionLevel = null;

	@Option(names = { "--useSharding" }, description = "enable Zarr v3 sharding (default: inherit source; true/false to force)")
	private Boolean useSharding = null; // null = inherit / auto-detect

	@Option(names = { "--missingFields" }, defaultValue = "ERROR", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "behavior when a view's (channel,illumination) group has no estimated field: ERROR | SKIP | COPY")
	private MissingFields missingFields = MissingFields.ERROR;

	@Option(names = { "--baselineDrift" }, defaultValue = "IGNORE", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "temporal baseline drift correction (BaSiC 'basefluor'): IGNORE (current behavior) | ZERO (remove each view's baseline) | MEAN (level each view to the per-(channel,illumination) group mean)")
	private BaselineDrift baselineDrift = BaselineDrift.IGNORE;

	@Option(names = { "--baselineGranularity" }, defaultValue = "SLICE", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "baseline granularity: SLICE (one value per z-plane, removes depth-dependent drift) | VIEW (one value per view)")
	private BaselineGranularity baselineGranularity = BaselineGranularity.SLICE;

	@Option(names = { "--baselinePercentile" }, defaultValue = "50.0", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "percentile of shading-corrected pixels used as the background estimate (50 = median; lower for crowded fluorescence)")
	private double baselinePercentile = 50.0;

	@Option(names = { "--baselineScale" }, defaultValue = "128", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "coarse XY size (square) for the baseline estimate; z is kept full (<=0 = full resolution)")
	private int baselineScale = 128;

	// resolved after probing/CLI merge; must be effectively final for the Spark closures
	private transient URI fieldsURI;
	private transient StorageFormat fieldsFormat;
	private transient URI lensURI; // null = no lens correction

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		final SpimData2 dataGlobal = this.loadSpimData2();
		if ( dataGlobal == null )
			return null;

		final List< ViewId > viewIdsGlobal = loadViewIds( dataGlobal );
		if ( viewIdsGlobal == null || viewIdsGlobal.isEmpty() )
			throw new IllegalArgumentException( "No views to correct." );

		// -- resolve fields container --
		fieldsURI = URITools.toURI( fieldsPathURIString );
		fieldsFormat = guessFormat( fieldsPathURIString );
		if ( fieldsFormat == null )
			throw new IllegalArgumentException( "Cannot determine the storage format of --fields '" + fieldsPathURIString + "'." );
		System.out.println( "Fields container: " + fieldsURI + " (" + fieldsFormat + ")" );

		// -- resolve optional lens-correction file --
		lensURI = ( lensCorrectionURIString != null ) ? URITools.toURI( lensCorrectionURIString ) : null;
		if ( lensURI != null )
			System.out.println( "Lens correction: " + lensURI + " (meshResolution=" + lensMeshResolution
					+ ", includeAffine=" + lensIncludeAffine + ")" );
		else
			System.out.println( "Lens correction: none" );

		// -- resolve output XML --
		final URI xmlOutURI;
		if ( xmlOutURIString == null )
		{
			final String base = xmlURIString.toLowerCase().endsWith( ".xml" )
					? xmlURIString.substring( 0, xmlURIString.length() - 4 )
					: xmlURIString;
			xmlOutURI = URITools.toURI( base + ".corrected.xml" );
		}
		else
		{
			xmlOutURI = URITools.toURI( xmlOutURIString );
		}
		System.out.println( "Output XML: " + xmlOutURI );

		// -- probe source volume format (per representative view) for inheritance --
		final SourceFormat srcFmt = probeSourceFormat( dataGlobal, viewIdsGlobal );

		// -- resolve effective storage format --
		StorageFormat effStorage = storageFormat;
		if ( effStorage == null )
			effStorage = guessFormat( outputPathURIString );
		if ( effStorage == null )
			effStorage = srcFmt.storage;
		if ( effStorage == null )
			throw new IllegalArgumentException( "Cannot determine output storage format; specify -s." );
		System.out.println( "Output storage format: " + effStorage
				+ ( ( storageFormat == null && srcFmt.storage == effStorage ) ? " (inherited from source)" : "" ) );

		final URI n5PathURI = URITools.toURI( outputPathURIString );

		// -- resolve block size (inherit source s0 chunk size) --
		final int[] blockSize;
		if ( blockSizeString != null )
			blockSize = Import.csvStringToIntArray( blockSizeString );
		else if ( srcFmt.blockSize != null )
			blockSize = srcFmt.blockSize.clone();
		else
			blockSize = new int[] { 128, 128, 64 };

		// -- resolve block scale / shard factor --
		final int[] blockScale;
		if ( blockScaleString != null )
			blockScale = Import.csvStringToIntArray( blockScaleString );
		else
			blockScale = new int[] { 16, 16, 1 };

		// -- resolve sharding --
		boolean effSharding;
		if ( useSharding != null )
			effSharding = useSharding;
		else if ( srcFmt.sharded != null )
			effSharding = srcFmt.sharded;
		else
			effSharding = ( effStorage == StorageFormat.ZARR );

		if ( effSharding && effStorage != StorageFormat.ZARR )
		{
			System.out.println( "WARNING: Sharding only supported for ZARR v3. Disabling sharding." );
			effSharding = false;
		}

		// -- resolve shard size --
		final int[] shardSize;
		if ( effSharding )
		{
			// inherit source shard size if we have it and the user did not override blockScale
			if ( blockScaleString == null && srcFmt.shardSize != null )
				shardSize = srcFmt.shardSize.clone();
			else
				shardSize = new int[] { blockSize[ 0 ] * blockScale[ 0 ], blockSize[ 1 ] * blockScale[ 1 ], blockSize[ 2 ] * blockScale[ 2 ] };
			System.out.println( "Sharding enabled. Shard size: " + Util.printCoordinates( shardSize ) );
		}
		else
		{
			shardSize = null;
			System.out.println( "Sharding disabled." );
		}

		// -- compute block size (write granularity) --
		final int[] computeBlockSize;
		if ( effSharding )
			computeBlockSize = shardSize.clone();
		else
			computeBlockSize = new int[] {
					blockSize[ 0 ] * blockScale[ 0 ],
					blockSize[ 1 ] * blockScale[ 1 ],
					blockSize[ 2 ] * blockScale[ 2 ] };

		// -- resolve compression --
		final Compression compression;
		if ( compressionType != null )
			compression = N5Util.getCompression( compressionType, compressionLevel );
		else if ( srcFmt.compression != null )
			compression = srcFmt.compression;
		else
			compression = N5Util.getCompression( Compressions.Zstandard, compressionLevel );

		// -- resolve downsampling --
		final int[][] downsamplings;
		if ( this.downsampling != null )
			downsamplings = Import.csvStringToDownsampling( this.downsampling );
		else if ( srcFmt.downsamplings != null )
			downsamplings = srcFmt.downsamplings;
		else
			downsamplings = N5ApiTools.mipMapInfoToDownsamplings(
					Resave_HDF5.proposeMipmaps( N5ApiTools.assembleViewSetups( dataGlobal, viewIdsGlobal ) ) );

		if ( !Import.testFirstDownsamplingIsPresent( downsamplings ) )
			throw new RuntimeException( "First downsampling step must be full resolution [1,1,...1], stopping." );

		System.out.println( "Block size: " + Util.printCoordinates( blockSize ) );
		System.out.println( "Compute/shard block size: " + Util.printCoordinates( computeBlockSize ) );
		System.out.println( "Downsamplings: " + Arrays.deepToString( downsamplings ) );

		// -- data types --
		final HashMap< Integer, long[] > dimensions = N5ApiTools.assembleDimensions( dataGlobal, viewIdsGlobal );
		final Map< Integer, DataType > sourceDataTypes = N5ApiTools.assembleDataTypes( dataGlobal, dimensions.keySet() );

		final DataType outType = ( outputDataType == OutputDataType.FLOAT32 ) ? DataType.FLOAT32 : null; // null = per-view source
		final Map< Integer, DataType > outDataTypes = new HashMap<>();
		for ( final Map.Entry< Integer, DataType > e : sourceDataTypes.entrySet() )
			outDataTypes.put( e.getKey(), outType != null ? outType : e.getValue() );

		// -- missing-field check / view filtering --
		final List< ViewId > viewIdsToProcess = filterByMissingFields( dataGlobal, viewIdsGlobal );
		if ( viewIdsToProcess.isEmpty() )
			throw new IllegalArgumentException( "No views left to correct after --missingFields=" + missingFields + " filtering." );

		// -- validate lens models up front (fail fast before Spark starts) --
		if ( lensURI != null )
			validateLensModels( dataGlobal, viewIdsToProcess, dimensions );

		if ( dryRun )
		{
			System.out.println( "This is a dry-run, stopping here." );
			return null;
		}

		final N5Writer n5Writer = URITools.instantiateN5Writer( effStorage, n5PathURI );

		// capture effectively-final copies for closures
		final StorageFormat storage = effStorage;
		final boolean sharding = effSharding;
		final URI fieldsURIf = this.fieldsURI;
		final StorageFormat fieldsFormatf = this.fieldsFormat;

		// -- Phase A: driver metadata setup --
		long time = System.currentTimeMillis();

		final Map< ViewId, MultiResolutionLevelInfo[] > viewIdToMrInfo =
				viewIdsToProcess.parallelStream().map( viewId ->
				{
					final MultiResolutionLevelInfo[] mrInfo;
					final DataType dt = outDataTypes.get( viewId.getViewSetupId() );

					if ( storage == StorageFormat.N5 )
					{
						mrInfo = N5ApiTools.setupBdvDatasetsN5(
								n5Writer,
								viewId,
								dt,
								dimensions.get( viewId.getViewSetupId() ),
								compression,
								blockSize,
								downsamplings );
					}
					else
					{
						final VoxelDimensions vx = dataGlobal.getSequenceDescription().getViewDescription( viewId ).getViewSetup().getVoxelSize();
						mrInfo = N5ApiTools.setupBdvDatasetsOMEZARR_ResaveRaw(
								n5Writer,
								viewId,
								dt,
								dimensions.get( viewId.getViewSetupId() ),
								vx.dimensionsAsDoubleArray(),
								vx.unit(),
								compression,
								blockSize,
								downsamplings,
								sharding,
								shardSize );
					}

					return new ValuePair<>( new ViewId( viewId.getTimePointId(), viewId.getViewSetupId() ), mrInfo );
				} ).collect( Collectors.toMap( e -> e.getA(), e -> e.getB() ) );

		System.out.println( "Created BDV-metadata, took " + ( System.currentTimeMillis() - time ) + " ms." );

		// all s0 grids across all ViewIds
		final List< long[][] > gridS0 =
				viewIdsToProcess.stream().map( viewId ->
						N5ApiTools.assembleJobs(
								viewId,
								dimensions.get( viewId.getViewSetupId() ),
								blockSize,
								computeBlockSize ) ).flatMap( List::stream ).collect( Collectors.toList() );
		System.out.printf( "Process %d s0 grid blocks%n", gridS0.size() );

		final SparkConf conf = new SparkConf().setAppName( "SparkIlluminationAndLensCorrections" );
		if ( localSparkBindAddress )
		{
			conf.set( "spark.driver.bindAddress", "127.0.0.1" );
			conf.set( "spark.driver.host", "localhost" );
			org.apache.spark.util.Utils.setCustomHostname( "localhost" );
		}

		final JavaSparkContext sc = new JavaSparkContext( conf );
		sc.setLogLevel( "ERROR" );

		final URI xmlURIf = this.xmlURI;

		// -- Phase 0: baseline (temporal drift) estimation (only when != IGNORE) --
		final Map< String, double[] > baselineDeltas; // key = tp + "_" + setup -> per-z delta (null = none)
		if ( baselineDrift != BaselineDrift.IGNORE )
		{
			baselineDeltas = computeBaselineDeltas(
					sc,
					viewIdsToProcess,
					xmlURIf,
					fieldsURIf,
					fieldsFormatf,
					baselineDrift.name(),
					baselineGranularity == BaselineGranularity.SLICE
							? net.preibisch.bigstitcher.spark.flatfield.FlatfieldApply.BaselineGranularity.SLICE
							: net.preibisch.bigstitcher.spark.flatfield.FlatfieldApply.BaselineGranularity.VIEW,
					baselinePercentile,
					baselineScale,
					n5Writer );
		}
		else
		{
			baselineDeltas = null;
		}

		// instantiate the ordered view corrections and apply them (in order) with
		// ViewCorrections.applyCorrections: optional lens/aberration warp first, then the
		// pointwise flatfield correction (terminal, produces the output data type).
		final List< ViewCorrection > corrections = new ArrayList<>();
		if ( lensURI != null )
			corrections.add( new LensCorrection( lensURI, lensMeshResolution, lensIncludeAffine ) );
		corrections.add( new FlatfieldCorrection( fieldsURIf, fieldsFormatf, outDataTypes, baselineDeltas ) );

		// -- Phase B: Spark s0 correction --
		processSNBlocks(
				sc,
				gridS0,
				blockCount -> Math.min( Math.max( sc.defaultParallelism(), 1 ), blockCount ),
				"s0 illumination/lens correction",
				"Corrected s0 level, took: ",
				true,
				true,
				gridBlock ->
				{
					final SpimData2 dataLocal = Spark.getSparkJobSpimData2( xmlURIf );
					final N5Writer n5Lcl = URITools.instantiateN5Writer( storage, n5PathURI );

					final ViewId vId = N5ApiTools.gridBlockToViewId( gridBlock );

					// load the full source view and apply the composed corrections
					final SetupImgLoader< ? > imgLoader =
							dataLocal.getSequenceDescription().getImgLoader().getSetupImgLoader( vId.getViewSetupId() );
					final RandomAccessibleInterval< ? > img = imgLoader.getImage( vId.getTimePointId() );

					final RandomAccessibleInterval< ? extends RealType< ? > > corrected =
							ViewCorrections.applyCorrections( corrections, dataLocal, vId, Cast.unchecked( img ) );

					// crop this grid block from the corrected view and write it
					resaveCorrectedBlock(
							corrected,
							n5Lcl,
							storage,
							N5ApiTools.gridToDatasetBdv( 0, storage ).apply( gridBlock ),
							gridBlock );

					n5Lcl.close();
				} );

		// -- Phase C: Spark downsampling (from corrected s0) --
		processDownsampledViews( sc, downsamplings, viewIdsToProcess, viewIdToMrInfo, storage, n5PathURI );

		sc.close();

		// -- Phase D: XML update --
		System.out.println( "Saving new xml to: " + xmlOutURI );

		if ( storage == StorageFormat.N5 && URITools.isFile( n5PathURI ) )
		{
			dataGlobal.getSequenceDescription().setImgLoader(
					new N5ImageLoader( n5PathURI, dataGlobal.getSequenceDescription() ) );
		}
		else if ( storage == StorageFormat.N5 )
		{
			dataGlobal.getSequenceDescription().setImgLoader(
					new N5CloudImageLoader( null, n5PathURI, dataGlobal.getSequenceDescription() ) );
		}
		else
		{
			final Map< ViewId, OMEZARREntry > viewIdToPath = new HashMap<>();
			viewIdToMrInfo.forEach( ( viewId, mrInfo ) ->
					viewIdToPath.put(
							viewId,
							new OMEZARREntry( mrInfo[ 0 ].dataset.substring( 0, mrInfo[ 0 ].dataset.lastIndexOf( "/" ) ), new int[] { 0, 0 } ) ) );

			dataGlobal.getSequenceDescription().setImgLoader(
					new AllenOMEZarrLoader( n5PathURI, storage, dataGlobal.getSequenceDescription(), viewIdToPath ) );
		}

		new XmlIoSpimData2().save( dataGlobal, xmlOutURI );

		n5Writer.close();

		Thread.sleep( 100 );
		System.out.println( "Flatfield correction done, in total took: " + ( System.currentTimeMillis() - time ) + " ms." );

		return null;
	}

	// ─── source-format probing (for inheritance) ─────────────────────────────────

	private static final class SourceFormat
	{
		StorageFormat storage = null;
		int[] blockSize = null;    // 3D X,Y,Z inner chunk
		int[] shardSize = null;    // 3D X,Y,Z shard (null if not sharded)
		Boolean sharded = null;
		Compression compression = null;
		int[][] downsamplings = null;
	}

	private SourceFormat probeSourceFormat( final SpimData2 data, final List< ViewId > viewIds )
	{
		final SourceFormat fmt = new SourceFormat();
		final BasicImgLoader imgLoader = data.getSequenceDescription().getImgLoader();

		// storage format from the ImgLoader type
		String srcDataset = null;
		URI srcURI = null;
		StorageFormat srcStorage = null;

		final ViewId probeView = viewIds.get( 0 );

		if ( imgLoader instanceof AllenOMEZarrLoader )
		{
			final AllenOMEZarrLoader zl = ( AllenOMEZarrLoader ) imgLoader;
			srcStorage = zl.getFormat();
			srcURI = zl.getN5URI();
			final OMEZARREntry entry = zl.getViewIdToPath().get( new ViewId( probeView.getTimePointId(), probeView.getViewSetupId() ) );
			if ( entry != null )
				srcDataset = entry.getPath() + "/0";
		}
		else if ( imgLoader instanceof N5ImageLoader )
		{
			srcStorage = StorageFormat.N5;
			srcURI = ( ( N5ImageLoader ) imgLoader ).getN5URI();
			srcDataset = N5ApiTools.createBDVPath( probeView, 0, StorageFormat.N5 );
		}

		fmt.storage = srcStorage;

		if ( srcStorage != null && srcURI != null && srcDataset != null )
		{
			try
			{
				final N5Reader n5 = URITools.instantiateN5Reader( srcStorage, srcURI );
				try
				{
					if ( n5.datasetExists( srcDataset ) )
					{
						final DatasetAttributes attrs = n5.getDatasetAttributes( srcDataset );
						fmt.compression = attrs.getCompression();

						if ( attrs.getBlockCodecInfo() instanceof ShardCodecInfo )
						{
							final ShardCodecInfo sc = ( ShardCodecInfo ) attrs.getBlockCodecInfo();
							fmt.sharded = true;
							fmt.blockSize = first3( sc.getInnerBlockSize() );
							fmt.shardSize = first3( attrs.getBlockSize() );
						}
						else
						{
							fmt.sharded = false;
							fmt.blockSize = first3( attrs.getBlockSize() );
						}
						System.out.println( "Probed source s0 '" + srcDataset + "': blockSize=" + Arrays.toString( fmt.blockSize )
								+ ", sharded=" + fmt.sharded + ", compression=" + ( fmt.compression == null ? "?" : fmt.compression.getType() ) );
					}
				}
				finally
				{
					n5.close();
				}
			}
			catch ( final Exception e )
			{
				System.out.println( "WARNING: could not probe source format (" + e + "); falling back to defaults." );
			}
		}

		// downsampling factors from the source pyramid
		try
		{
			final Object sil = imgLoader.getSetupImgLoader( probeView.getViewSetupId() );
			if ( sil instanceof mpicbg.spim.data.generic.sequence.BasicMultiResolutionSetupImgLoader )
			{
				final double[][] res = ( ( mpicbg.spim.data.generic.sequence.BasicMultiResolutionSetupImgLoader< ? > ) sil ).getMipmapResolutions();
				final int[][] ds = new int[ res.length ][ 3 ];
				for ( int l = 0; l < res.length; ++l )
					for ( int d = 0; d < 3; ++d )
						ds[ l ][ d ] = ( int ) Math.round( res[ l ][ d ] );
				if ( Import.testFirstDownsamplingIsPresent( ds ) )
					fmt.downsamplings = ds;
			}
		}
		catch ( final Throwable t )
		{
			System.out.println( "WARNING: could not probe source pyramid (" + t + "); using auto/CLI downsampling." );
		}

		return fmt;
	}

	private static int[] first3( final int[] a )
	{
		return new int[] { a[ 0 ], a[ 1 ], a[ 2 ] };
	}

	private static StorageFormat guessFormat( final String path )
	{
		if ( path == null )
			return null;
		final String lc = path.toLowerCase();
		if ( lc.endsWith( ".zarr2" ) )
			return StorageFormat.ZARR2;
		else if ( lc.endsWith( ".zarr" ) )
			return StorageFormat.ZARR;
		else if ( lc.endsWith( ".n5" ) )
			return StorageFormat.N5;
		else if ( lc.endsWith( ".h5" ) || lc.endsWith( ".hdf5" ) )
			return StorageFormat.HDF5;
		return null;
	}

	private List< ViewId > filterByMissingFields( final SpimData2 data, final List< ViewId > viewIds )
	{
		final N5Reader n5 = URITools.instantiateN5Reader( fieldsFormat, fieldsURI );
		try
		{
			final java.util.List< ViewId > keep = new java.util.ArrayList<>();
			for ( final ViewId v : viewIds )
			{
				final var vs = data.getSequenceDescription().getViewDescription( v ).getViewSetup();
				final int ch = ( vs.getChannel() != null ) ? vs.getChannel().getId() : 0;
				final int il = ( vs.getIllumination() != null ) ? vs.getIllumination().getId() : 0;
				final String groupKey = "channel" + ch + "/illumination" + il;
				final boolean has = n5.datasetExists( groupKey + "/flatfield" );

				if ( has )
				{
					keep.add( v );
				}
				else if ( missingFields == MissingFields.ERROR )
				{
					throw new IllegalArgumentException(
							"No flatfield for group '" + groupKey + "' (view [" + v.getTimePointId() + "," + v.getViewSetupId()
									+ "]) in '" + fieldsURI + "'. Use --missingFields SKIP or COPY to change behavior." );
				}
				else if ( missingFields == MissingFields.SKIP )
				{
					System.out.println( "WARNING: no field for group '" + groupKey + "', SKIPping view ["
							+ v.getTimePointId() + "," + v.getViewSetupId() + "]." );
				}
				else // COPY: not yet supported; treat as ERROR to avoid silently emitting an inconsistent XML
				{
					throw new UnsupportedOperationException(
							"--missingFields COPY is not implemented; use SKIP or provide fields for group '" + groupKey + "'." );
				}
			}
			return keep;
		}
		finally
		{
			n5.close();
		}
	}

	// ─── lens-model validation (driver, fail-fast) ───────────────────────────────

	/**
	 * Validate the lens models against the views to process before Spark starts:
	 * for each distinct (channel name, view X/Y size), resolve the matching model
	 * (error on an ambiguous match, error on a fitted-size mismatch, warn on no
	 * match). A no-match is not fatal — that view will be flatfield-corrected only.
	 */
	private void validateLensModels(
			final SpimData2 data,
			final List< ViewId > viewIds,
			final Map< Integer, long[] > dimensions )
	{
		final List< Model > models = LensApply.loadModelsCached( lensURI );
		System.out.println( "Loaded " + models.size() + " lens model(s) from " + lensURI );

		final Set< String > checked = new java.util.HashSet<>();
		int matched = 0, unmatched = 0;

		for ( final ViewId v : viewIds )
		{
			final ViewSetup vs = data.getSequenceDescription().getViewDescription( v ).getViewSetup();
			final String channelName = ( vs.getChannel() != null ) ? vs.getChannel().getName() : null;
			final long[] dim = dimensions.get( v.getViewSetupId() );
			final int w = ( int ) dim[ 0 ];
			final int h = ( int ) dim[ 1 ];

			final String key = channelName + "#" + w + "x" + h;
			if ( !checked.add( key ) )
				continue;

			final Model m = LensModels.findForChannel( models, channelName ); // throws on ambiguity
			if ( m == null )
			{
				System.out.println( "WARNING: no lens model matches channel '" + channelName
						+ "'; those views will be flatfield-corrected only." );
				++unmatched;
			}
			else
			{
				if ( m.fittedWidth != w || m.fittedHeight != h )
					throw new IllegalArgumentException(
							"Lens model '" + m.name + "' was fitted at " + m.fittedWidth + "x" + m.fittedHeight
									+ " but channel '" + channelName + "' views are " + w + "x" + h
									+ "; the non-linear model is only valid at its fitted size." );
				System.out.println( "Channel '" + channelName + "' (" + w + "x" + h + ") -> lens model '" + m.name + "'" );
				++matched;
			}
		}
		System.out.println( "Lens-model validation: " + matched + " channel/size group(s) matched, " + unmatched + " unmatched." );
	}

	// ─── baseline (temporal drift) — Phase 0 ─────────────────────────────────────

	/** Serializable ViewId key: "<timepoint>_<viewsetup>". */
	private static String viewKey( final ViewId v )
	{
		return v.getTimePointId() + "_" + v.getViewSetupId();
	}

	/** (channel,illumination) group key of a view, matching the fields container layout. */
	private static String groupKeyOf( final SpimData2 data, final ViewId v )
	{
		final ViewDescription vd = data.getSequenceDescription().getViewDescription( v );
		final ViewSetup vs = vd.getViewSetup();
		final int ch = ( vs.getChannel() != null ) ? vs.getChannel().getId() : 0;
		final int il = ( vs.getIllumination() != null ) ? vs.getIllumination().getId() : 0;
		return "channel" + ch + "/illumination" + il;
	}

	/**
	 * Phase 0. Distribute a coarse-XY, full-z pass over the selected views; for each
	 * view compute a per-view baseline ({@code double[]}: length=depth for SLICE,
	 * length 1 for VIEW), collect to the driver, compute per-(channel,illumination)
	 * group {@code refMean} (mean of all baseline entries in the group), and turn each
	 * view's baseline into the per-z delta to subtract at s0 (per drift mode). Writes
	 * QC (per-view baselines + per-group refMean) to the output container and logs a
	 * per-group drift summary.
	 *
	 * @return map {@code viewKey -> per-z delta}; {@code null} deltas are omitted.
	 */
	private static Map< String, double[] > computeBaselineDeltas(
			final JavaSparkContext sc,
			final List< ViewId > viewIds,
			final URI xmlURIf,
			final URI fieldsURIf,
			final StorageFormat fieldsFormatf,
			final String driftMode,
			final net.preibisch.bigstitcher.spark.flatfield.FlatfieldApply.BaselineGranularity granularity,
			final double percentile,
			final int coarseScale,
			final N5Writer n5Writer ) throws Exception
	{
		final long t0 = System.currentTimeMillis();
		System.out.println( "Phase 0: estimating baseline drift (mode=" + driftMode
				+ ", granularity=" + granularity + ", percentile=" + percentile + ", scale=" + coarseScale + ")..." );

		final List< int[] > viewList = viewIds.stream()
				.map( v -> new int[] { v.getTimePointId(), v.getViewSetupId() } )
				.collect( Collectors.toList() );

		final int nPartitions = Math.min( Math.max( sc.defaultParallelism(), 1 ), Math.max( 1, viewList.size() ) );
		final JavaRDD< int[] > rdd = sc.parallelize( viewList, nPartitions );

		// each task: load coarse-XY full-z, shading-correct, percentile -> baseline
		final List< ValuePair< String, double[] > > perViewBaseline =
				rdd.map( vi ->
				{
					final ViewId viewId = new ViewId( vi[ 0 ], vi[ 1 ] );
					final SpimData2 dataLocal = Spark.getSparkJobSpimData2( xmlURIf );

					final double[] baseline = computeCoarseBaseline(
							dataLocal, viewId, fieldsURIf, fieldsFormatf, granularity, percentile, coarseScale );

					return new ValuePair<>( viewKey( viewId ), baseline );
				} ).collect();

		// driver-side: map + per-group refMean
		final Map< String, double[] > baselines = new HashMap<>();
		for ( final ValuePair< String, double[] > e : perViewBaseline )
			baselines.put( e.getA(), e.getB() );

		// group views by (channel,illumination) to compute the temporal mean per group
		final SpimData2 dataGlobal = Spark.getSparkJobSpimData2( xmlURIf );
		final Map< String, List< ViewId > > groups = new java.util.LinkedHashMap<>();
		for ( final ViewId v : viewIds )
			groups.computeIfAbsent( groupKeyOf( dataGlobal, v ), k -> new java.util.ArrayList<>() ).add( v );

		final Map< String, Double > groupRefMean = new HashMap<>();
		for ( final Map.Entry< String, List< ViewId > > g : groups.entrySet() )
		{
			final List< double[] > groupBaselines = g.getValue().stream()
					.map( v -> baselines.get( viewKey( v ) ) )
					.filter( b -> b != null )
					.collect( Collectors.toList() );
			final double refMean =
					net.preibisch.bigstitcher.spark.flatfield.FlatfieldApply.groupRefMean( groupBaselines );
			groupRefMean.put( g.getKey(), refMean );

			// drift summary (min/mean/max over all baseline entries in the group)
			double min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY, sum = 0.0;
			long cnt = 0;
			for ( final double[] b : groupBaselines )
				for ( final double val : b )
				{
					min = Math.min( min, val );
					max = Math.max( max, val );
					sum += val;
					++cnt;
				}
			final double mean = ( cnt == 0 ) ? 0.0 : sum / cnt;
			System.out.printf( "  group %s: baseline min=%.3f mean=%.3f max=%.3f refMean=%.3f (%d view(s), %d entries)%n",
					g.getKey(), ( cnt == 0 ? 0.0 : min ), mean, ( cnt == 0 ? 0.0 : max ), refMean, g.getValue().size(), cnt );
		}

		// build per-view delta (per drift mode)
		final Map< String, double[] > deltas = new HashMap<>();
		for ( final ViewId v : viewIds )
		{
			final double[] bView = baselines.get( viewKey( v ) );
			final double refMean = groupRefMean.getOrDefault( groupKeyOf( dataGlobal, v ), 0.0 );
			final double[] delta =
					net.preibisch.bigstitcher.spark.flatfield.FlatfieldApply.baselineDelta( driftMode, bView, refMean );
			if ( delta != null )
				deltas.put( viewKey( v ), delta );
		}

		// QC / provenance in the output container
		try
		{
			n5Writer.setAttribute( "/", "BaselineDrift", driftMode );
			n5Writer.setAttribute( "/", "BaselineGranularity", granularity.name() );
			n5Writer.setAttribute( "/", "BaselinePercentile", percentile );
			n5Writer.setAttribute( "/", "BaselineScale", coarseScale );

			final Map< String, double[] > qcBaselines = new java.util.LinkedHashMap<>();
			for ( final ViewId v : viewIds )
			{
				final double[] b = baselines.get( viewKey( v ) );
				if ( b != null )
					qcBaselines.put( viewKey( v ), b );
			}
			n5Writer.setAttribute( "/", "BaselinePerView", qcBaselines );
			n5Writer.setAttribute( "/", "BaselineGroupRefMean", groupRefMean );
		}
		catch ( final Exception e )
		{
			System.out.println( "WARNING: could not write baseline QC attributes (" + e + ")." );
		}

		System.out.println( "Phase 0 done, took " + ( System.currentTimeMillis() - t0 ) + " ms." );
		return deltas;
	}

	/**
	 * Load a coarse-XY (down to {@code coarseScale}), full-z copy of a view, resize the
	 * group's flat/dark fields to the coarse X/Y, shading-correct, and compute the
	 * per-plane (SLICE) or whole-view (VIEW) percentile baseline. z is kept at full
	 * resolution. {@code coarseScale <= 0} keeps full XY.
	 */
	private static < T extends RealType< T > & NativeType< T > > double[] computeCoarseBaseline(
			final SpimData2 data,
			final ViewId viewId,
			final URI fieldsURI,
			final StorageFormat fieldsFormat,
			final net.preibisch.bigstitcher.spark.flatfield.FlatfieldApply.BaselineGranularity granularity,
			final double percentile,
			final int coarseScale )
	{
		final String groupKey = groupKeyOf( data, viewId );

		final SetupImgLoader< ? > imgLoader =
				data.getSequenceDescription().getImgLoader().getSetupImgLoader( viewId.getViewSetupId() );
		final RandomAccessibleInterval< T > img = Cast.unchecked( imgLoader.getImage( viewId.getTimePointId() ) );

		final int viewW = ( int ) img.dimension( 0 );
		final int viewH = ( int ) img.dimension( 1 );
		final int viewD = ( int ) img.dimension( 2 );

		// target coarse XY (keep aspect via the larger side; z full)
		final int coarseW, coarseH;
		if ( coarseScale <= 0 || ( viewW <= coarseScale && viewH <= coarseScale ) )
		{
			coarseW = viewW;
			coarseH = viewH;
		}
		else
		{
			final double scale = ( double ) coarseScale / Math.max( viewW, viewH );
			coarseW = Math.max( 1, ( int ) Math.round( viewW * scale ) );
			coarseH = Math.max( 1, ( int ) Math.round( viewH * scale ) );
		}

		// materialize the coarse-XY full-z volume (average-free bilinear XY resize per plane)
		final RandomAccessibleInterval< net.imglib2.type.numeric.real.FloatType > coarse =
				coarseXYVolume( img, viewW, viewH, viewD, coarseW, coarseH );

		// fields resized to the coarse XY (reuses the cached, resize-capable loader)
		final FlatfieldApply.Field2D field = net.preibisch.bigstitcher.spark.flatfield.FlatfieldApply.loadFieldCached(
				fieldsURI, fieldsFormat, groupKey, coarseW, coarseH );

		return net.preibisch.bigstitcher.spark.flatfield.FlatfieldApply.computeViewBaseline(
				coarse, field, granularity, percentile );
	}

	/**
	 * Build a coarse-XY, full-z {@code FloatType} copy of a 3D view by bilinearly
	 * resizing each z-plane independently to {@code coarseW x coarseH} (reusing
	 * {@link BasicFlatfield#resize}). Returns a zero-min {@code ArrayImg}.
	 */
	private static < T extends RealType< T > > RandomAccessibleInterval< net.imglib2.type.numeric.real.FloatType > coarseXYVolume(
			final RandomAccessibleInterval< T > img,
			final int viewW,
			final int viewH,
			final int viewD,
			final int coarseW,
			final int coarseH )
	{
		final float[] data = new float[ coarseW * coarseH * viewD ];
		final net.imglib2.RandomAccess< T > ra = img.randomAccess();
		final long minX = img.min( 0 ), minY = img.min( 1 ), minZ = img.min( 2 );

		final float[] plane = new float[ viewW * viewH ];
		for ( int z = 0; z < viewD; ++z )
		{
			ra.setPosition( minZ + z, 2 );
			for ( int y = 0; y < viewH; ++y )
			{
				ra.setPosition( minY + y, 1 );
				final int rowOff = y * viewW;
				for ( int x = 0; x < viewW; ++x )
				{
					ra.setPosition( minX + x, 0 );
					plane[ rowOff + x ] = ( float ) ra.get().getRealDouble();
				}
			}

			final float[] resized = ( coarseW == viewW && coarseH == viewH )
					? plane.clone()
					: BasicFlatfield.resize( plane, viewH, viewW, coarseH, coarseW );

			System.arraycopy( resized, 0, data, z * coarseW * coarseH, coarseW * coarseH );
		}

		return net.imglib2.img.array.ArrayImgs.floats( data, coarseW, coarseH, viewD );
	}

	// ─── resave of a corrected s0 block ──────────────────────────────────────────

	/**
	 * Crop one s0 grid block from an already-corrected full view (of the output data
	 * type) and write it to the container. For OME-ZARR the 3D block is expanded to
	 * 5D (X,Y,Z,1,1); {@code gridBlock[2]} is the grid offset in inner-block coords.
	 */
	private static < O extends RealType< O > & NativeType< O > > void resaveCorrectedBlock(
			final RandomAccessibleInterval< ? extends RealType< ? > > correctedFullView,
			final N5Writer n5Out,
			final StorageFormat storageType,
			final String dataset,
			final long[][] gridBlock )
	{
		final boolean isZarr = ( storageType == StorageFormat.ZARR || storageType == StorageFormat.ZARR2 );

		final long[] blockOffset3d = new long[] { gridBlock[ 0 ][ 0 ], gridBlock[ 0 ][ 1 ], gridBlock[ 0 ][ 2 ] };
		final long[] blockSize3d = new long[] { gridBlock[ 1 ][ 0 ], gridBlock[ 1 ][ 1 ], gridBlock[ 1 ][ 2 ] };
		final long[] gridOffset = isZarr
				? new long[] { gridBlock[ 2 ][ 0 ], gridBlock[ 2 ][ 1 ], gridBlock[ 2 ][ 2 ], 0, 0 }
				: gridBlock[ 2 ];

		final RandomAccessibleInterval< O > block =
				Cast.unchecked( Views.offsetInterval( correctedFullView, blockOffset3d, blockSize3d ) );
		final RandomAccessibleInterval< O > toWrite = isZarr
				? Views.addDimension( Views.addDimension( block, 0, 0 ), 0, 0 )
				: block;

		N5Utils.saveBlock( toWrite, n5Out, dataset, gridOffset );
	}

	// ─── Spark orchestration (mirrors SparkResaveN5) ─────────────────────────────

	@FunctionalInterface
	private interface GridBlockProcessor extends Serializable
	{
		void process( long[][] gridBlock ) throws Exception;
	}

	private static long processSNBlocks(
			final JavaSparkContext sc,
			final List< long[][] > blocks,
			final IntUnaryOperator partitionCount,
			final String retryDescription,
			final String completedMessage,
			final boolean printPartitionCount,
			final boolean printRetryCount,
			final GridBlockProcessor processBlock )
	{
		final long time = System.currentTimeMillis();

		final RetryTrackerSpark< long[][] > retryTracker =
				RetryTrackerSpark.forGridBlocks( retryDescription, blocks.size() );

		do
		{
			if ( !retryTracker.beginAttempt() )
			{
				System.out.println( "Stopping." );
				System.exit( 1 );
			}

			final int nPartitions = partitionCount.applyAsInt( blocks.size() );

			if ( printPartitionCount )
				System.out.printf( "Use %d partitions to process %d blocks%n", nPartitions, blocks.size() );

			final JavaRDD< long[][] > rdds = sc.parallelize( blocks, nPartitions );

			final JavaRDD< long[][] > rddsResult = rdds.map( gridBlock ->
			{
				processBlock.process( gridBlock );
				return gridBlock.clone();
			} );

			final Set< long[][] > failedBlocksSet = retryTracker.processResults( rddsResult.collect(), blocks );

			if ( !retryTracker.processFailures( failedBlocksSet ) )
			{
				System.out.println( "Stopping." );
				System.exit( 1 );
			}

			blocks.clear();
			if ( printRetryCount && !failedBlocksSet.isEmpty() )
				System.out.printf( "Retry %d failed blocks%n", failedBlocksSet.size() );
			blocks.addAll( failedBlocksSet );
		}
		while ( !blocks.isEmpty() );

		System.out.println( completedMessage + ( System.currentTimeMillis() - time ) + " ms." );

		return time;
	}

	private static void processDownsampledViews(
			final JavaSparkContext sc,
			final int[][] downsamplings,
			final List< ViewId > viewIdsGlobal,
			final Map< ViewId, MultiResolutionLevelInfo[] > viewIdToMrInfo,
			final StorageFormat storageFormat,
			final URI n5PathURI )
	{
		for ( int level = 1; level < downsamplings.length; ++level )
		{
			final int s = level;

			final List< long[][] > allBlocks =
					viewIdsGlobal.stream().map( viewId ->
							N5ApiTools.assembleJobs( viewId, viewIdToMrInfo.get( viewId )[ s ] ) )
							.flatMap( List::stream ).collect( Collectors.toList() );

			System.out.println( "Downsampling level " + ( storageFormat == StorageFormat.N5 ? "s" : "" ) + s + "... " );
			System.out.println( "Number of compute blocks: " + allBlocks.size() );

			processSNBlocks(
					sc,
					allBlocks,
					blockCount -> Math.min( Spark.maxPartitions, blockCount ),
					"s" + s + " flatfield downsampling",
					"Downsampled level " + s + ", took: ",
					false,
					false,
					gridBlock ->
					{
						final N5Writer n5Lcl = URITools.instantiateN5Writer( storageFormat, n5PathURI );

						if ( storageFormat == StorageFormat.N5 )
						{
							N5ApiTools.writeDownsampledBlock(
									n5Lcl,
									viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s ],
									viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s - 1 ],
									gridBlock );
						}
						else
						{
							N5ApiTools.writeDownsampledBlock5dOMEZARR(
									n5Lcl,
									viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s ],
									viewIdToMrInfo.get( N5ApiTools.gridBlockToViewId( gridBlock ) )[ s - 1 ],
									gridBlock,
									0,
									0 );
						}

						n5Lcl.close();
					} );
		}
	}

	public static void main( final String... args )
	{
		System.out.println( Arrays.toString( args ) );
		System.exit( new CommandLine( new SparkIlluminationAndLensCorrections() ).execute( args ) );
	}
}
