package net.preibisch.bigstitcher.spark;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.hdf5.N5HDF5Writer;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import org.janelia.saalfeldlab.n5.universe.metadata.ome.ngff.v04.OmeNgffMultiScaleMetadata;

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
import net.preibisch.bigstitcher.spark.SparkAffineFusion.DataTypeFusion;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.bigstitcher.spark.util.Downsampling;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.legacy.io.IOFunctions;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBox;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.OMEZarrAttibutes;
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

	public static enum Compressions { Lz4, Gzip, Zstandard, Blosc, Bzip2, Xz, Raw };

	@Option(names = { "-o", "--outputPath" }, required = true, description = "OME-ZARR/N5/HDF5 path for saving, e.g. -o /home/fused.zarr, file:/home/fused.n5 or e.g. s3://myBucket/data.zarr")
	private String outputPathURIString = null;

	@Option(names = {"-s", "--storage"}, defaultValue = "ZARR", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Dataset storage type, currently supported OME-ZARR, N5, and ONLY for local, multithreaded Spark HDF5 (default: OME-ZARR)")
	private StorageFormat storageType = null;

	@Option(names = {"-c", "--compression"}, defaultValue = "Zstandard", showDefaultValue = CommandLine.Help.Visibility.ALWAYS,
			description = "Dataset compression")
	private Compressions compression = null;

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

	URI outPathURI = null, xmlOutURI = null;

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

		// compression and data type
		final Compression compression = N5Util.getCompression( this.compression, this.compressionLevel );

		System.out.println( "Compression: " + this.compression );
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
		//MultiResolutionLevelInfo[] mrInfo;

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
		else if ( storageType == StorageFormat.N5 || storageType == StorageFormat.ZARR )
		{
			driverVolumeWriter = URITools.instantiateN5Writer( storageType, outPathURI );
		}
		else
		{
			System.out.println( "Unsupported format: " + storageType );
			return null;
			
		}

		driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/InputXML", xmlURI );

		driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/NumTimepoints", numTimepoints );
		driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/NumChannels", numChannels );

		driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/Boundingbox_min", boundingBox.minAsLongArray() );
		driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/Boundingbox_max", boundingBox.maxAsLongArray() );

		driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/PreserveAnisotropy", preserveAnisotropy );
		if (preserveAnisotropy) // cannot write Double.NaN into JSON
			driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/AnisotropyFactor", anisotropyFactor );
		driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/DataType", dt );
		driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/BlockSize", blockSize );

		if ( minIntensity != null && maxIntensity != null )
		{
			driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/MinIntensity", minIntensity );
			driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/MaxIntensity", maxIntensity );
		}

		// setup datasets and metadata
		MultiResolutionLevelInfo[][] mrInfos = null;

		// OME-Zarr export
		// this code needs refactoring some sort of refactoring. When exporting OME-ZARR, we first create the OME-ZARR container,
		// and if it is BDV-XML, we only create the XML in the next if statement. If it is N5/HDF5, there
		// is code that creates the N5/HDF5 container and the XML in one if statement. The reason is that
		// HDF5/N5 containers with XML may be different that OME-ZARR's; they are always the same no matter
		// if it is a BDV project or not
		if ( storageType == StorageFormat.ZARR )
		{
			System.out.println( "Creating 5D OME-ZARR metadata for '" + outPathURI + "' ... " );

			if ( !bdv )
				driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/FusionFormat", "OME-ZARR" );

			final long[] dim3d = boundingBox.dimensionsAsLongArray();

			final long[] dim = new long[] { dim3d[ 0 ], dim3d[ 1 ], dim3d[ 2 ], numChannels, numTimepoints };
			final int[] blockSize5d = new int[] { blockSize[ 0 ], blockSize[ 1 ], blockSize[ 2 ], 1, 1 };
			final int[][] ds = new int[ downsamplings.length ][];
			for ( int d = 0; d < ds.length; ++d )
				ds[ d ] = new int[] { downsamplings[ d ][ 0 ], downsamplings[ d ][ 1 ], downsamplings[ d ][ 2 ], 1, 1 };

			final Function<Integer, String> levelToName = (level) -> "/" + level;

			mrInfos = new MultiResolutionLevelInfo[ 1 ][];

			// all is 5d now
			mrInfos[ 0 ] = N5ApiTools.setupMultiResolutionPyramid(
					driverVolumeWriter,
					levelToName,
					dt,
					dim, //5d
					compression,
					blockSize5d, //5d
					ds ); // 5d

			final MultiResolutionLevelInfo[] mrInfo = mrInfos[ 0 ];

			final Function<Integer, AffineTransform3D> levelToMipmapTransform =
					(level) -> MipmapTransforms.getMipmapTransformDefault( mrInfo[level].absoluteDownsamplingDouble() );

			// extract the resolution of the s0 export
			// TODO: this is inaccurate, we should actually estimate it from the final transformn that is applied
			// TODO: this is a hack (returns 1,1,1) so the export downsampling pyramid is working
			final VoxelDimensions vx = new FinalVoxelDimensions( "micrometer", new double[] { 1, 1, 1 } );// dataGlobal.getSequenceDescription().getViewSetupsOrdered().iterator().next().getVoxelSize();
			final double[] resolutionS0 = OMEZarrAttibutes.getResolutionS0( vx, anisotropyFactor, Double.NaN );

			System.out.println( "Resolution of level 0: " + Util.printCoordinates( resolutionS0 ) + " " + "micrometer" ); //vx.unit() might not be OME-ZARR compatiblevx.unit() );

			// create metadata
			final OmeNgffMultiScaleMetadata[] meta = OMEZarrAttibutes.createOMEZarrMetadata(
					5, // int n
					"/", // String name, I also saw "/"
					resolutionS0, // double[] resolutionS0,
					"micrometer", //vx.unit() might not be OME-ZARR compatible // String unitXYZ, // e.g micrometer
					mrInfos[ 0 ].length, // int numResolutionLevels,
					levelToName,
					levelToMipmapTransform );

			// save metadata

			//org.janelia.saalfeldlab.n5.universe.metadata.ome.ngff.v04.OmeNgffMetadata
			// for this to work you need to register an adapter in the N5Factory class
			// final GsonBuilder builder = new GsonBuilder().registerTypeAdapter( CoordinateTransformation.class, new CoordinateTransformationAdapter() );
			driverVolumeWriter.setAttribute( "/", "multiscales", meta );
		}

		if ( bdv )
		{
			System.out.println( "Creating BDV compatible container at '" + outPathURI + "' ... " );

			if ( storageType == StorageFormat.N5 )
				driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/FusionFormat", "BDV/N5" );
			else if ( storageType == StorageFormat.ZARR )
				driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/FusionFormat", "BDV/OME-ZARR" );
			else
				driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/FusionFormat", "BDV/HDF5" );

			driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/OutputXML", xmlOutURI );

			final long[] bb = boundingBox.dimensionsAsLongArray();

			final ArrayList< ViewSetup > setups = new ArrayList<>();
			final ArrayList< TimePoint > tps = new ArrayList<>();

			for ( int t = 0; t < numTimepoints; ++t )
				tps.add( new TimePoint( t ) );

			// extract the resolution of the s0 export
			// TODO: this is inaccurate, we should actually estimate it from the final transformn that is applied
			// TODO: this is a hack (returns 1,1,1) so the export downsampling pyramid is working
			final VoxelDimensions vx = new FinalVoxelDimensions( "micrometer", new double[] { 1, 1, 1 } );// dataGlobal.getSequenceDescription().getViewSetupsOrdered().iterator().next().getVoxelSize();
			final double[] resolutionS0 = OMEZarrAttibutes.getResolutionS0( vx, anisotropyFactor, Double.NaN );

			System.out.println( "Resolution of level 0: " + Util.printCoordinates( resolutionS0 ) + " " + "m" ); //vx.unit() might not be OME-ZARR compatiblevx.unit() );

			final VoxelDimensions vxNew = new FinalVoxelDimensions( "micrometer", resolutionS0 );

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

			if ( storageType == StorageFormat.ZARR )
			{
				viewIdToPath = new HashMap<>();

				for ( int c = 0; c < numChannels; ++c )
					for ( int t = 0; t < numTimepoints; ++t )
					{
						final OMEZARREntry omeZarrEntry = new OMEZARREntry(
								mrInfos[ 0 ][ 0 ].dataset.substring(0, mrInfos[ 0 ][ 0 ].dataset.lastIndexOf( "/" ) ),
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

			if ( storageType != StorageFormat.ZARR )
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
								driverVolumeWriter, vd, dt, bb, compression, blockSize, downsamplings);
	
						driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/FusionFormat", "BDV/N5" );
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
				driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/FusionFormat", "N5" );
			else
				driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/FusionFormat", "HDF5" );

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
							downsamplings );
				}
		}

		// TODO: set extra attributes to load the state
		driverVolumeWriter.setAttribute( "/", "Bigstitcher-Spark/MultiResolutionInfos", mrInfos );

		driverVolumeWriter.close();

		return null;
	}

	public static void main(final String... args) throws SpimDataException
	{

		//final XmlIoSpimData io = new XmlIoSpimData();
		//final SpimData spimData = io.load( "/Users/preibischs/Documents/Microscopy/Stitching/Truman/standard/output/dataset.xml" );
		//BdvFunctions.show( spimData );
		//SimpleMultiThreading.threadHaltUnClean();

		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new CreateFusionContainer()).execute(args));
	}
}