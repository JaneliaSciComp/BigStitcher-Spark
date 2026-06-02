package net.preibisch.bigstitcher.spark;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;

import mpicbg.spim.data.generic.base.Entity;
import mpicbg.spim.data.registration.ViewRegistrations;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import org.apache.commons.lang.StringUtils;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.hdf5.N5HDF5Writer;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import org.janelia.saalfeldlab.n5.universe.metadata.ome.ngff.OmeNgffMetadata;

import bdv.util.MipmapTransforms;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.Angle;
import mpicbg.spim.data.sequence.Channel;
import mpicbg.spim.data.sequence.FinalVoxelDimensions;
import mpicbg.spim.data.sequence.Illumination;
import mpicbg.spim.data.sequence.Tile;
import mpicbg.spim.data.sequence.TimePoint;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.ViewSetup;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.FinalDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Util;
import net.preibisch.bigstitcher.spark.SparkFusion.DataTypeFusion;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.bigstitcher.spark.util.Downsampling;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.legacy.io.IOFunctions;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBox;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.OMEZarrAttributes;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.AllenOMEZarrLoader.OMEZARREntry;
import net.preibisch.mvrecon.process.export.ExportN5Api;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import net.preibisch.mvrecon.process.n5api.N5ApiTools;
import net.preibisch.mvrecon.process.n5api.N5ApiTools.MultiResolutionLevelInfo;
import net.preibisch.mvrecon.process.n5api.SpimData2Tools;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

public class CreateFusionContainer extends AbstractBasic implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = -9140450542904228386L;

	@Option(names = { "-o", "--outputPath" }, required = true, description = "OME-ZARR/N5/HDF5 path for saving, e.g. -o /home/fused.zarr, file:/home/fused.n5 or e.g. s3://myBucket/data.zarr")
	private String outputPathURIString = null;

	@Option(names = {"-s", "--storage"}, defaultValue = "ZARR", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Dataset storage type: ZARR (=OME-ZARR v3, supports sharding), ZARR2 (=OME-ZARR v2, no sharding), N5, "
					+ "or HDF5 (ONLY for local, multithreaded Spark). Note: with the default ZARR (v3), sharding is auto-enabled "
					+ "unless --useSharding=false is set. Use ZARR2 for OME-ZARR v2 (no sharding). (default: ZARR / OME-ZARR v3)")
	private StorageFormat storageType = null;

	@Option(names = {"-c", "--compression"}, defaultValue = "Zstandard", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Dataset compression")
	private Compressions compressionType = null;

	@Option(names = {"-cl", "--compressionLevel" }, description = "compression level, if supported by the codec (default: gzip 1, Zstandard 3, xz 6)")
	private Integer compressionLevel = null;

	@Option(names = {"-ch", "--numChannels" }, description = "number of fused channels in the output container (default: as many as in the XML)")
	private Integer numChannels = null;

	@Option(names = {"-tp", "--numTimepoints" }, description = "number of fused timepoints in the output container (default: as many as in the XML)")
	private Integer numTimepoints = null;

	@Option(names = "--blockSize", description = "blockSize (default: 128,128,128)")
	private String blockSizeString = "128,128,128";

	@Option(names = {"-d", "--dataType"}, defaultValue = "FLOAT32", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Data type, UINT8 [0...255], UINT16 [0...65535] and FLOAT32 are supported, when choosing UINT8 or UINT16 you must define min and max intensity (default: FLOAT32)")
	private DataTypeFusion dataTypeFusion = null;

	@Option(names = { "--minIntensity" }, description = "optionally adjust min intensity for scaling values to the desired range for UINT8 and UINT16 output, (default: 0)")
	private Double minIntensity = null;

	@Option(names = { "--maxIntensity" }, description = "optionally adjust max intensity for scaling values to the desired range for UINT8 and UINT16 output, (default: 255 for UINT8, 65535 for UINT16)")
	private Double maxIntensity = null;

	@Option(names = { "--bdv" }, required = false, description = "Write a BigDataViewer-compatible dataset (default: false)")
	private boolean bdv = false;

	@Option(names = { "-xo", "--xmlout" }, required = false, description = "path to the new BigDataViewer xml project (only valid if --bdv was selected), "
			+ "e.g. -xo /home/project.xml or -xo s3://myBucket/project.xml (default: dataset.xml in basepath for H5, dataset.xml one directory level above basepath for N5)")
	private String xmlOutURIString = null;

	@Option(names = { "-b", "--boundingBox" }, description = "fuse a specific bounding box listed in the XML (default: fuse everything)")
	private String boundingBoxName = null;

	@Option(names = { "--multiRes" }, description = "Automatically create a multi-resolution pyramid (default: false)")
	private boolean multiRes = false;

	@Option(names = { "-ds", "--downsampling" }, split = ";", required = false, description = "Manually define steps to create a multi-resolution pyramid (e.g. -ds 1,1,1 -ds 2,2,1 -ds 4,4,2 -ds 8,8,4)")
	private List<String> downsampling = null;

	@Option(names = { "--preserveAnisotropy" }, description = "preserve the anisotropy of the data (default: false)")
	private boolean preserveAnisotropy = false;

	@Option(names = { "--anisotropyFactor" }, description = "define the anisotropy factor if preserveAnisotropy is set to true (default: compute from data)")
	private double anisotropyFactor = Double.NaN;

	@Option(names = { "--useSharding" },
		description = "Enable Zarr v3 sharding (only for ZARR format, not ZARR2, default: auto-detect - enabled for ZARR, disabled otherwise)")
	private Boolean useSharding = null; // null = auto-detect

	@Option(names = { "--shardSizeFactor" },
		description = "Shard size as multiple of block size (e.g. 4,4,2 means shard will be 4x4x2 blocks), default: 8,8,2")
	private String shardSizeFactorString = "8,8,2";

	URI outPathURI = null, xmlOutURI = null;

	// the purpose of this parameter is to allow me to create a round specific group under the main container, e.g.
	// stitched.ome.zarr
	//     |- Round0
	//         |- S0
	//         |- S1
	//     |- Round1
	//         |- S0
	//         |- S1
	@Option(names = { "--group" }, description = "Container group path")
	private String groupPath = "";

	private double[] cal = new double[] { 1, 1, 1 };
	private String calUnit = "micrometer";
	private double avgAnisotropy = Double.NaN;

	/**
	 * @return container group path always terminated with a '/'
	 */
	private String getContainerGroupPath()
	{
		if (!groupPath.endsWith("/")) {
			return groupPath + "/";
		} else {
			return groupPath;
		}
	}

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		if (dryRun)
		{
			System.out.println( "dry-run not supported for CreateFusionContainer.");
			System.exit( 0 );
		}

		if ( this.bdv && xmlOutURIString == null )
		{
			System.out.println( "Please specify the output XML for the BDV dataset: -xo");
			return null;
		}

		this.outPathURI =  URITools.toURI( outputPathURIString );
		System.out.println( "ZARR/N5/HDF5 container: " + outPathURI );

		if ( storageType == StorageFormat.HDF5 && !URITools.isFile( outPathURI ) )
		{
			System.out.println( "HDF5 only supports local storage, but --outputPath=" + outPathURI );
			return null;
		}

		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIdsGlobal = Import.getViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			return null;

		final int numTimepointsXML = dataGlobal.getSequenceDescription().getTimePoints().getTimePointsOrdered().size();
		final int numChannelsXML = dataGlobal.getSequenceDescription().getAllChannelsOrdered().size();

		System.out.println( "XML project contains " + numChannelsXML + " channels, " + numTimepointsXML + " timepoints." );

		if ( numChannels == null )
			numChannels = numChannelsXML;

		if ( numTimepoints == null )
			numTimepoints = numTimepointsXML;

		if ( numChannels < numChannelsXML )
			System.out.println( "WARNING: you selected to fuse LESS channels than present in the data. This works, but you will need specify the content manually.");
		else if ( numChannels > numChannelsXML )
			System.out.println( "WARNING: you selected to fuse MORE channels than present in the data. This works, but you will need specify the content manually.");

		if ( numTimepoints < numTimepointsXML )
			System.out.println( "WARNING: you selected to fuse LESS timepoints than present in the data. This works, but you will need specify the content manually.");
		else if ( numTimepoints > numTimepointsXML )
			System.out.println( "WARNING: you selected to fuse MORE timepoints than present in the data. This works, but you will need specify the content manually.");

		if ( this.bdv )
		{
			this.xmlOutURI = URITools.toURI( xmlOutURIString );
			System.out.println( "XML: " + xmlOutURI );
		}

		BoundingBox boundingBox = Import.getBoundingBox( dataGlobal, viewIdsGlobal, boundingBoxName );

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

			System.out.println( "Adjusted bounding box (anisotropy preserved): " + Util.printInterval( boundingBox ) );
		}

		final int[] blockSize = Import.csvStringToIntArray( blockSizeString );

		System.out.println( "Fusion target: " + boundingBox.getTitle() + ": " + Util.printInterval( boundingBox ) + " with blocksize " + Util.printCoordinates( blockSize ) );

		// Parse shard size factor
		final int[] shardSizeFactor = Import.csvStringToIntArray( shardSizeFactorString );

		if ( shardSizeFactor.length != 3 )
		{
			System.out.println( "ERROR: shardSizeFactor must have 3 values (x,y,z)" );
			return null;
		}

		// Auto-detect sharding based on storage type (enabled for ZARR v3, disabled otherwise)
		if ( useSharding == null )
			useSharding = (storageType == StorageFormat.ZARR);

		// Validate: sharding only for ZARR v3
		if ( useSharding && storageType != StorageFormat.ZARR )
		{
			System.out.println( "WARNING: Sharding only supported for ZARR v3. Disabling sharding." );
			useSharding = false;
		}

		// Calculate shard size
		final int[] shardSize;
		if ( useSharding )
		{
			shardSize = new int[] {
				blockSize[0] * shardSizeFactor[0],
				blockSize[1] * shardSizeFactor[1],
				blockSize[2] * shardSizeFactor[2]
			};
			System.out.println( "Sharding enabled. Shard size: " + Util.printCoordinates( shardSize ) + " (factor: " + Util.printCoordinates( shardSizeFactor ) + ")" );
			System.out.println( "Note: For Zarr v3 sharding, computeBlockSize will equal shardSize for shard-aware writing." );
		}
		else
		{
			shardSize = null;
			System.out.println( "Sharding disabled." );
		}

		// compression and data type
		final Compression compression = N5Util.getCompression( this.compressionType, this.compressionLevel );

		System.out.println( "Compression: " + this.compressionType );
		System.out.println( "Compression level: " + ( compressionLevel == null ? "default" : compressionLevel ) );

		final DataType dt;
		Double minIntensity, maxIntensity;

		if ( dataTypeFusion == DataTypeFusion.UINT8 )
		{
			dt = DataType.UINT8;
			minIntensity = (this.minIntensity == null) ? 0 : this.minIntensity;
			maxIntensity = (this.maxIntensity == null) ? 255 : this.maxIntensity;
		}
		else if ( dataTypeFusion == DataTypeFusion.UINT16 )
		{
			dt = DataType.UINT16;
			minIntensity = (this.minIntensity == null) ? 0 : this.minIntensity;
			maxIntensity = (this.maxIntensity == null) ? 65535 : this.maxIntensity;
		}
		else if ( dataTypeFusion == DataTypeFusion.FLOAT32 )
		{
			dt = DataType.FLOAT32;
			minIntensity = maxIntensity = null;
		}
		else
		{
			dt = null;
			minIntensity = maxIntensity = null;
		}

		System.out.println( "Data type: " + dt );

		if ( dt == null || compression == null )
			return null;

		//
		// set up downsampling
		//
		if ( !Downsampling.testDownsamplingParameters( this.multiRes, this.downsampling ) )
			return null;

		final int[][] downsamplings;

		if ( multiRes )
			downsamplings = ExportN5Api.estimateMultiResPyramid( new FinalDimensions( boundingBox ), anisotropyFactor );
		else if ( this.downsampling != null )
			downsamplings = Import.csvStringListToDownsampling( this.downsampling );
		else
			downsamplings = new int[][]{{ 1, 1, 1 }};

		if ( downsamplings == null )
			return null;

		System.out.println( "The following downsampling pyramid will be created:" );
		System.out.println( Arrays.deepToString( downsamplings ) );

		//
		// set up container and metadata
		//
		final N5Writer driverVolumeWriter;

		System.out.println();
		System.out.println( "Setting up container and metadata in '" + outPathURI + "' ... " );

		// init base folder and writer
		if ( storageType == StorageFormat.HDF5 )
		{
			final File dir = new File( URITools.fromURI( outPathURI ) ).getParentFile();
			if ( !dir.exists() )
				dir.mkdirs();
			driverVolumeWriter = new N5HDF5Writer( URITools.fromURI( outPathURI ) );
		}
		else if ( storageType == StorageFormat.N5 || storageType == StorageFormat.ZARR || storageType == StorageFormat.ZARR2 )
		{
			driverVolumeWriter = URITools.instantiateN5Writer( storageType, outPathURI );
		}
		else
		{
			System.out.println( "Unsupported format: " + storageType );
			return null;
		}

		// if there is a group different from the root, create it
		if ( ! getContainerGroupPath().equals("/") )
			driverVolumeWriter.createGroup( getContainerGroupPath() );

		driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/InputXML", xmlURI );

		driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/NumTimepoints", numTimepoints );
		driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/NumChannels", numChannels );

		driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/Boundingbox_min", boundingBox.minAsLongArray() );
		driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/Boundingbox_max", boundingBox.maxAsLongArray() );

		driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/PreserveAnisotropy", preserveAnisotropy );
		if (preserveAnisotropy) // cannot write Double.NaN into JSON
			driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/AnisotropyFactor", anisotropyFactor );
		driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/DataType", dt );
		driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/BlockSize", blockSize );

		driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/UseSharding", useSharding );
		if ( useSharding )
		{
			driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/ShardSize", shardSize );
			driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/ShardSizeFactor", shardSizeFactor );
		}

		if ( minIntensity != null && maxIntensity != null )
		{
			driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/MinIntensity", minIntensity );
			driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/MaxIntensity", maxIntensity );
		}

		// setup datasets and metadata
		MultiResolutionLevelInfo[][] mrInfos = null;

		// OME-Zarr export
		// this code needs refactoring some sort of refactoring. When exporting OME-ZARR, we first create the OME-ZARR container,
		// and if it is BDV-XML, we only create the XML in the next if statement. If it is N5/HDF5, there
		// is code that creates the N5/HDF5 container and the XML in one if statement. The reason is that
		// HDF5/N5 containers with XML may be different that OME-ZARR's; they are always the same no matter
		// if it is a BDV project or not
		if ( storageType == StorageFormat.ZARR || storageType == StorageFormat.ZARR2 )
		{
			System.out.println( "Creating 5D OME-ZARR metadata for '" + outPathURI + "' ... " );

			if ( !bdv )
				driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/FusionFormat", "OME-ZARR" );

			final long[] dim3d = boundingBox.dimensionsAsLongArray();

			final long[] dim = new long[] { dim3d[ 0 ], dim3d[ 1 ], dim3d[ 2 ], numChannels, numTimepoints };
			final int[] blockSize5d = new int[] { blockSize[ 0 ], blockSize[ 1 ], blockSize[ 2 ], 1, 1 };
			final int[][] ds = new int[ downsamplings.length ][];
			for ( int d = 0; d < ds.length; ++d )
				ds[ d ] = new int[] { downsamplings[ d ][ 0 ], downsamplings[ d ][ 1 ], downsamplings[ d ][ 2 ], 1, 1 };

			mrInfos = new MultiResolutionLevelInfo[ 1 ][];

			// all is 5d now
			// Convert shardSize to 5D if sharding is enabled
			final int[] shardSize5d = useSharding && shardSize != null
				? new int[] { shardSize[ 0 ], shardSize[ 1 ], shardSize[ 2 ], 1, 1 }
				: null;

			mrInfos[ 0 ] = N5ApiTools.setupMultiResolutionPyramid(
					driverVolumeWriter,
					(level) -> getContainerGroupPath() + level, // multiscale pyramid will be created for the entire provided group
					dt,
					dim, //5d
					compression,
					blockSize5d, //5d
					ds, // 5d
					useSharding,
					shardSize5d ); // 5d

			final MultiResolutionLevelInfo[] mrInfo = mrInfos[ 0 ];

			// Note: mrInfo contains 5D downsampling (x,y,z,c,t) but MipmapTransforms expects 3D
			final Function<Integer, AffineTransform3D> levelToMipmapTransform =
					(level) -> MipmapTransforms.getMipmapTransformDefault( Arrays.copyOf( mrInfo[level].absoluteDownsamplingDouble(), 3 ) );

			updateAnisotropyAndCalibration(dataGlobal, viewIdsGlobal);
			// extract the resolution of the s0 export
			final double[] resolutionS0 = OMEZarrAttributes.getResolutionS0( cal, avgAnisotropy, Double.NaN );

			System.out.println( "Resolution of level 0: " + Util.printCoordinates( resolutionS0 ) + " " + calUnit );

			// create metadata
			if (storageType == StorageFormat.ZARR2) {
				final OmeNgffMetadata meta = OMEZarrAttributes.createOMEZarrMetadata(
						5, // int n
						getContainerGroupPath(), // String name, I also saw "/",
						"0.4",
						resolutionS0, // double[] resolutionS0,
						calUnit, //vx.unit() might not be OME-ZARR compatible // String unitXYZ, // e.g micrometer
						mrInfos[ 0 ].length, // int numResolutionLevels,
						(level) -> "/" + level, // OME-ZARR metadata will be created relative to the provided group
						levelToMipmapTransform );

				driverVolumeWriter.setAttribute( getContainerGroupPath(), "multiscales", meta.multiscales );
			} else {
				// ZARRv3
				final OmeNgffMetadata meta = OMEZarrAttributes.createOMEZarrMetadata(
						5, // int n
						StringUtils.removeEnd(getContainerGroupPath(), "/"), // String name
						"0.5",
						resolutionS0, // double[] resolutionS0,
						calUnit, // vx.unit() might not be OME-ZARR compatible // String unitXYZ, // e.g micrometer
						mrInfos[ 0 ].length, // int numResolutionLevels,
						(level) -> "/" + level, // OME-ZARR metadata will be created relative to the provided group
						levelToMipmapTransform );
				driverVolumeWriter.setAttribute( getContainerGroupPath(), "ome", meta );
				// this is hacky until OmeNgffV05Metadata gets fixed to output version
				driverVolumeWriter.setAttribute( getContainerGroupPath(), "ome/version", "0.5" );
			}
		}

		if ( bdv )
		{
			System.out.println( "Creating BDV compatible container at '" + outPathURI + "' ... " );
			if ( storageType == StorageFormat.N5 )
				driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/FusionFormat", "BDV/N5" );
			else if ( storageType == StorageFormat.ZARR || storageType == StorageFormat.ZARR2)
				driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/FusionFormat", "BDV/OME-ZARR" );
			else
				driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/FusionFormat", "BDV/HDF5" );

			driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/OutputXML", xmlOutURI );

			final long[] bb = boundingBox.dimensionsAsLongArray();

			final ArrayList< ViewSetup > setups = new ArrayList<>();
			final ArrayList< TimePoint > tps = new ArrayList<>();

			for ( int t = 0; t < numTimepoints; ++t )
				tps.add( new TimePoint( t ) );

			// extract the resolution of the s0 export
			final double[] resolutionS0 = OMEZarrAttributes.getResolutionS0( cal, avgAnisotropy, Double.NaN );

			System.out.println( "Resolution of level 0: " + Util.printCoordinates( resolutionS0 ) + " " + calUnit );

			final VoxelDimensions vxNew = new FinalVoxelDimensions( calUnit, resolutionS0 );

			for ( int c = 0; c < numChannels; ++c )
			{
				setups.add(
						new ViewSetup(
								c,
								"setup " + c,
								new FinalDimensions( bb ),
								vxNew,
								new Tile( 0 ),
								new Channel( c, "Channel " + c ),
								new Angle( 0 ),
								new Illumination( 0 ) ) );
			}

			final Map< ViewId, OMEZARREntry > viewIdToPath;

			if ( storageType == StorageFormat.ZARR || storageType == StorageFormat.ZARR2 )
			{
				viewIdToPath = new HashMap<>();

				for ( int t = 0; t < numTimepoints; ++t )
					for ( int c = 0; c < numChannels; ++c )
					{
						final OMEZARREntry omeZarrEntry = new OMEZARREntry(
								mrInfos[ t ][ c ].dataset.substring(0, mrInfos[ t ][ c ].dataset.lastIndexOf( "/" ) ),
								new int[] { c, t } );

						viewIdToPath.put( new ViewId( t, c ), omeZarrEntry );
					}
			}
			else
			{
				viewIdToPath = null;
			}

			final SpimData2 dataFusion =
					SpimData2Tools.createNewSpimDataForFusion( storageType, outPathURI, xmlOutURI, viewIdToPath, setups, tps );

			new XmlIoSpimData2().save( dataFusion, xmlOutURI );

			if ( storageType != StorageFormat.ZARR && storageType != StorageFormat.ZARR2 )
			{
				final Collection<ViewDescription> vds = dataFusion.getSequenceDescription().getViewDescriptions().values();

				mrInfos = new MultiResolutionLevelInfo[ vds.size() ][];
				final MultiResolutionLevelInfo myMrInfo[][] = mrInfos;

				vds.stream().parallel().forEach( vd ->
				{
					final int c = vd.getViewSetup().getChannel().getId();
					final int t = vd.getTimePointId();

					if ( storageType == StorageFormat.N5 )
					{
						myMrInfo[ c + t*c  ] = N5ApiTools.setupBdvDatasetsN5(
								driverVolumeWriter, vd, dt, bb, compression, blockSize, downsamplings );

						driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/FusionFormat", "BDV/N5" );
					}
					else // HDF5
					{
						myMrInfo[ c + t*numChannels  ] = N5ApiTools.setupBdvDatasetsHDF5(
								driverVolumeWriter, vd, dt, bb, compression, blockSize, downsamplings);
					}
				});
			}
			// TODO: set extra attributes to load the state
		}
		else if ( storageType == StorageFormat.N5 || storageType == StorageFormat.HDF5 ) // simple (no bdv project) HDF5/N5 export
		{
			mrInfos = new MultiResolutionLevelInfo[ numChannels * numTimepoints ][];

			if ( storageType == StorageFormat.N5 )
				driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/FusionFormat", "N5" );
			else
				driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/FusionFormat", "HDF5" );

			for ( int c = 0; c < numChannels; ++c )
				for ( int t = 0; t < numTimepoints; ++t )
				{
					String title = "ch"+c+"tp"+t;

					IOFunctions.println( "Creating 3D " + storageType +" container '" + title + "' in '" + outPathURI + "' ... " );
		
					// setup multi-resolution pyramid
					mrInfos[ c + t*numChannels  ] = N5ApiTools.setupMultiResolutionPyramid(
							driverVolumeWriter,
							(level) -> title + "/s" + level,
							dt,
							boundingBox.dimensionsAsLongArray(),
							compression,
							blockSize,
							downsamplings,
							useSharding,
							shardSize );
				}
		}

		// TODO: set extra attributes to load the state
		driverVolumeWriter.setAttribute( getContainerGroupPath(), "Bigstitcher-Spark/MultiResolutionInfos", mrInfos );

		driverVolumeWriter.close();

		return null;
	}

	private void updateAnisotropyAndCalibration( SpimData2 dataGlobal, List<ViewId> viewIdsGlobal )
	{
		ViewRegistrations registrations = dataGlobal.getViewRegistrations();
		// get all view descriptions
		List<ViewDescription> vds = SpimData2.getAllViewDescriptionsSorted(dataGlobal, viewIdsGlobal);
		// group by timepoint and channel
		Set<Class<? extends Entity>> groupingFactors = new HashSet<>(Arrays.asList(TimePoint.class, Channel.class));
		List<Group<ViewDescription>> fusionGroups = Group.splitBy( vds, groupingFactors );
		Pair<double[], String> calAndUnit = fusionGroups.stream().findFirst()
				.map(group -> TransformationTools.computeAverageCalibration(group, registrations))
				.orElse(new ValuePair<>(new double[]{ 1, 1, 1 }, "micrometer"));
		cal = calAndUnit.getA();
		calUnit = calAndUnit.getB();

		if (preserveAnisotropy) {
			if (!Double.isNaN(this.anisotropyFactor)) {
				avgAnisotropy = this.anisotropyFactor;
			} else {
				avgAnisotropy = TransformationTools.getAverageAnisotropyFactor(dataGlobal, viewIdsGlobal);
			}
		} else {
			avgAnisotropy = Double.NaN;
		}
	}

	public static void main(final String... args) throws SpimDataException
	{
		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new CreateFusionContainer()).execute(args));
	}
}
