package net.preibisch.bigstitcher.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Dimensions;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.util.Grid;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBox;
import net.preibisch.mvrecon.process.boundingbox.BoundingBoxTools;
import net.preibisch.mvrecon.process.fusion.FusionTools;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class AffineFusion implements Callable<Void>, Serializable 
{
	private static final long serialVersionUID = 2279327568867124470L;

	@Option(names = { "-o", "--n5Path" }, required = true, description = "N5 path for saving, e.g. /home/fused.n5")
	private String n5Path = null;

	@Option(names = { "-d", "--n5Dataset" }, required = true, description = "N5 dataset - it is highly recommended to add s0 to be able to compute a multi-resolution pyramid later, e.g. /ch488/s0")
	private String n5Dataset = null;

	@Option(names = { "-x", "--xml" }, required = true, description = "path to the BigStitcher xml, e.g. /home/project.xml")
	private String xmlPath = null;

	@Option(names = { "-b", "--boundingBox" }, required = false, description = "fuse a specific bounding box listed in the XML (default: fuse everything)")
	private String boundingBoxName = null;

	
	@Option(names = { "--angleId" }, required = false, description = "list the angle ids that should be fused into a single image, you can find them in the XML, e.g. --angleId '0,1,2' (default: all angles)")
	private String angleIds = null;

	@Option(names = { "--tileId" }, required = false, description = "list the tile ids that should be fused into a single image, you can find them in the XML, e.g. --tileId '0,1,2' (default: all tiles)")
	private String tileIds = null;

	@Option(names = { "--illuminationId" }, required = false, description = "list the illumination ids that should be fused into a single image, you can find them in the XML, e.g. --illuminationId '0,1,2' (default: all illuminations)")
	private String illuminationIds = null;

	@Option(names = { "--channelId" }, required = false, description = "list the channel ids that should be fused into a single image, you can find them in the XML (usually just ONE!), e.g. --channelId '0,1,2' (default: all channels)")
	private String channelIds = null;

	@Option(names = { "--timepointId" }, required = false, description = "list the timepoint ids that should be fused into a single image, you can find them in the XML (usually just ONE!), e.g. --timepointId '0,1,2' (default: all time points)")
	private String timepointIds = null;

	@Option(names = { "-vi" }, required = false, description = "specifically list the view ids (time point, view setup) that should be fused into a single image, e.g. -vi '0,0' -vi '0,1' (default: all view ids)")
	private String[] vi = null;

	
	@Option(names = { "--UINT16" }, required = false, description = "save as UINT16 [0...65535], if you choose it you must define min and max intensity (default: fuse as 32 bit float)")
	private boolean uint16 = false;

	@Option(names = { "--UINT8" }, required = false, description = "save as UINT8 [0...255], if you choose it you must define min and max intensity (default: fuse as 32 bit float)")
	private boolean uint8 = false;

	@Option(names = { "--minIntensity" }, required = false, description = "min intensity for scaling values to the desired range (required for UINT8 and UINT16), e.g. 0.0")
	private Double minIntensity = null;

	@Option(names = { "--maxIntensity" }, required = false, description = "max intensity for scaling values to the desired range (required for UINT8 and UINT16), e.g. 2048.0")
	private Double maxIntensity = null;

	public static ArrayList< ViewId > getViewIds( final SpimData2 data )
	{
		// select views to process
		final ArrayList< ViewId > viewIds = new ArrayList< ViewId >();
		viewIds.addAll( data.getSequenceDescription().getViewDescriptions().values() );

		// filter not present ViewIds
		SpimData2.filterMissingViews( data, viewIds );

		return viewIds;
	}

	public static ArrayList< ViewId > getViewIds( final SpimData2 data, final ArrayList<ViewId> vi )
	{
		// select views to process
		final ArrayList< ViewId > viewIds = new ArrayList< ViewId >();

		for ( final ViewDescription vd : data.getSequenceDescription().getViewDescriptions().values() )
		{
			for ( final ViewId v : vi )
				if ( vd.getTimePointId() == v.getTimePointId() && vd.getViewSetupId() == v.getViewSetupId() )
					viewIds.add( vd );
		}

		// filter not present ViewIds
		SpimData2.filterMissingViews( data, viewIds );

		return viewIds;
	}

	public static ArrayList< ViewId > getViewIds(
			final SpimData2 data,
			final HashSet<Integer> a,
			final HashSet<Integer> c,
			final HashSet<Integer> i,
			final HashSet<Integer> ti,
			final HashSet<Integer> tp )
	{
		// select views to process
		final ArrayList< ViewId > viewIds = new ArrayList< ViewId >();

		for ( final ViewDescription vd : data.getSequenceDescription().getViewDescriptions().values() )
		{
			if (
					( a == null || a.contains( vd.getViewSetup().getAngle().getId() )) &&
					( c == null || c.contains( vd.getViewSetup().getChannel().getId() )) &&
					( i == null || i.contains( vd.getViewSetup().getIllumination().getId() )) &&
					( ti == null || ti.contains( vd.getViewSetup().getTile().getId() )) &&
					( tp == null || tp.contains( vd.getTimePointId() )) )
			{
				viewIds.add( vd );
			}
		}

		// filter not present ViewIds
		SpimData2.filterMissingViews( data, viewIds );

		return viewIds;
	}

	public static HashSet< Integer > parseIdList( String idList )
	{
		if ( idList == null )
			return null;

		idList = idList.trim();

		if ( idList.length() == 0 )
			return null;

		final String[] ids = idList.split( "," );
		final HashSet< Integer > hash = new HashSet<>();

		for ( int i = 0; i < ids.length; ++i )
			hash.add( Integer.parseInt( ids[ i ].trim() ) );

		return hash;
	}

	public static ArrayList<ViewId> viewId( final String[] s )
	{
		final ArrayList<ViewId> viewIds = new ArrayList<>();
		for ( final String s0 : s )
			viewIds.add( viewId( s0 ) );
		return viewIds;
	}

	public static ViewId viewId( final String s )
	{
		final String[] e = s.trim().split( "," );
		return new ViewId( Integer.parseInt( e[0].trim()), Integer.parseInt( e[1].trim() ) );
	}

	@Override
	public Void call() throws Exception
	{
		if ( uint8 && uint16 )
		{
			System.err.println( "Please only select UINT8, UINT16 or nothing (FLOAT32)." );
			System.exit(0);
		}

		if ( ( uint8 || uint16 ) && (minIntensity == null || maxIntensity == null ) )
		{
			System.err.println( "When selecting UINT8 or UINT16 you need to specify minIntensity and maxIntensity." );
			System.exit(0);
		}

		if ( vi != null && ( angleIds != null || tileIds != null || illuminationIds != null || timepointIds != null || channelIds != null ) )
		{
			System.err.println( "You can only specify ViewIds (-vi) OR angles, channels, illuminations, tiles, timepoints." );
			System.exit(0);
		}

		final XmlIoSpimData2 io = new XmlIoSpimData2( "" );
		final SpimData2 data = io.load( xmlPath );

		// select views to process
		final ArrayList< ViewId > viewIds;

		if ( vi != null )
		{
			System.out.println( "Parsing selected ViewIds ... ");
			ArrayList<ViewId> parsedViews = viewId( vi );
			viewIds = getViewIds( data, parsedViews );
		}
		else if ( angleIds != null || tileIds != null || illuminationIds != null || timepointIds != null || channelIds != null )
		{
			System.out.print( "Parsing selected angle ids ... ");
			final HashSet<Integer> a = parseIdList( angleIds );
			System.out.println( a != null ? a : "all" );

			System.out.print( "Parsing selected channel ids ... ");
			final HashSet<Integer> c = parseIdList( channelIds );
			System.out.println( c != null ? c : "all" );

			System.out.print( "Parsing selected illumination ids ... ");
			final HashSet<Integer> i = parseIdList( illuminationIds );
			System.out.println( i != null ? i : "all" );

			System.out.print( "Parsing selected tile ids ... ");
			final HashSet<Integer> ti = parseIdList( tileIds );
			System.out.println( ti != null ? ti : "all" );

			System.out.print( "Parsing selected timepoint ids ... ");
			final HashSet<Integer> tp = parseIdList( timepointIds );
			System.out.println( tp != null ? tp : "all" );

			viewIds = getViewIds( data, a, c, i, ti, tp );
		}
		else
		{
			// get all
			viewIds = getViewIds( data );
		}

		if ( viewIds.size() == 0 )
		{
			System.err.println( "No views to fuse left." );
			System.exit(0);
		}

		System.out.println( "Following ViewIds will be fused: ");
		for ( final ViewId v : viewIds )
			System.out.print( "[" + v.getTimePointId() + "," + v.getViewSetupId() + "] " );
		System.out.println();

		BoundingBox bb = null;

		if ( boundingBoxName == null )
		{
			bb = BoundingBoxTools.maximalBoundingBox( data, viewIds, "All Views" );
		}
		else
		{
			final List<BoundingBox> boxes = BoundingBoxTools.getAllBoundingBoxes( data, null, false );

			for ( final BoundingBox box : boxes )
				if ( box.getTitle().equals( boundingBoxName ) )
					bb = box;

			if ( bb == null )
				throw new RuntimeException( "Bounding box '" + boundingBoxName + "' not present in XML." );
		}

		final int[] blockSize = new int[] { 128, 128, 128 };

		System.out.println( "Fusing: " + bb.getTitle() + ": " + Util.printInterval( bb )  + " with blocksize " + Util.printCoordinates( blockSize ) );

		final DataType dataType;

		if ( uint8 )
		{
			System.out.println( "Fusing to UINT8, min intensity = " + minIntensity + ", max intensity = " + maxIntensity );
			dataType = DataType.UINT8;
		}
		else if ( uint16)
		{
			System.out.println( "Fusing to UINT16, min intensity = " + minIntensity + ", max intensity = " + maxIntensity );
			dataType = DataType.UINT16;
		}
		else
		{
			System.out.println( "Fusing to FLOAT32" );
			dataType = DataType.FLOAT32;
		}

		final long[] dimensions = new long[ bb.numDimensions() ];
		bb.dimensions( dimensions );

		final long[] min = new long[ bb.numDimensions() ];
		bb.min( min );

		// display virtually
		//final RandomAccessibleInterval< FloatType > virtual = FusionTools.fuseVirtual( data, viewIds, bb, Double.NaN ).getA();
		//new ImageJ();
		//ImageJFunctions.show( virtual, Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() ) );
		//SimpleMultiThreading.threadHaltUnClean();

		// final variables for Spark
		final String n5Path = this.n5Path;
		final String n5Dataset = this.n5Dataset;
		final String xmlPath = this.xmlPath;
		final N5Writer n5 = new N5FSWriter(n5Path);
		final long[] minBB = new long[ bb.numDimensions() ];
		final long[] maxBB = new long[ bb.numDimensions() ];
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
		final int[][] serializedViewIds = new int[ viewIds.size() ][ 2 ];

		for ( int i = 0; i < viewIds.size(); ++i )
		{
			serializedViewIds[ i ][ 0 ] = viewIds.get( i ).getTimePointId();
			serializedViewIds[ i ][ 1 ] = viewIds.get( i ).getViewSetupId();
		}

		for ( int d = 0; d < minBB.length; ++d )
		{
			minBB[ d ] = bb.min( d );
			maxBB[ d ] = bb.max( d );
		}

		n5.createDataset(
				n5Dataset,
				dimensions,
				blockSize,
				dataType,
				new GzipCompression( 1 ) );

		n5.setAttribute( n5Dataset, "min", min);

		System.out.println( "numBlocks = " + Grid.create( dimensions, blockSize).size() );

		final SparkConf conf = new SparkConf().setAppName("AffineFusion");

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final JavaRDD<long[][]> rdd =
				sc.parallelize(
						Grid.create(
								dimensions,
								blockSize));

		final long time = System.currentTimeMillis();

		rdd.foreach(
				gridBlock -> {
					final SpimData2 dataLocal = new XmlIoSpimData2( "" ).load( xmlPath );

					// be smarter, test which ViewIds are actually needed for the block we want to fuse
					final Interval fusedBlock =
							Intervals.translate(
									Intervals.translate(
											new FinalInterval( gridBlock[1] ), // blocksize
											gridBlock[0] ), // block offset
									min ); // min of the randomaccessbileinterval

					// recover views to process
					final ArrayList< ViewId > viewIdsLocal = new ArrayList<>();

					for ( int i = 0; i < serializedViewIds.length; ++i )
					{
						final ViewId viewId =  new ViewId( serializedViewIds[i][0], serializedViewIds[i][1] );
						final Dimensions dim = dataLocal.getSequenceDescription().getImgLoader().getSetupImgLoader( viewId.getViewSetupId() ).getImageSize( viewId.getTimePointId() );

						final ViewRegistration reg = dataLocal.getViewRegistrations().getViewRegistration( viewId );
						reg.updateModel();
						final AffineTransform3D model = reg.getModel();

						// expand to be conservative ...
						final Interval bounds = Intervals.expand( Intervals.largestContainedInterval( model.estimateBounds( new FinalInterval( dim ) ) ), 2 );
						final Interval intersection = Intervals.intersect( fusedBlock, bounds );

						boolean overlaps = true;

						for ( int d = 0; d < intersection.numDimensions(); ++d )
							if ( intersection.dimension( d ) < 0 )
								overlaps = false;

						if ( overlaps )
							viewIdsLocal.add( viewId );
					}

					// nothing to save...
					if ( viewIdsLocal.size() == 0 )
						return;

					final RandomAccessibleInterval<FloatType> source =
									FusionTools.fuseVirtual(
											dataLocal,
											viewIdsLocal,
											new FinalInterval(minBB, maxBB),
											Double.NaN ).getA();

					final N5Writer n5Writer = new N5FSWriter(n5Path);

					if ( uint8 )
					{
						final RandomAccessibleInterval< UnsignedByteType > sourceUINT8 =
								Converters.convert(
										source,(i, o) -> o.setReal( ( i.get() - minIntensity ) / range ),
										new UnsignedByteType());

						final RandomAccessibleInterval<UnsignedByteType> sourceGridBlock = Views.offsetInterval(sourceUINT8, gridBlock[0], gridBlock[1]);
						N5Utils.saveBlock(sourceGridBlock, n5Writer, n5Dataset, gridBlock[2]);
					}
					else if ( uint16 )
					{
						final RandomAccessibleInterval< UnsignedShortType > sourceUINT16 =
								Converters.convert(
										source,(i, o) -> o.setReal( ( i.get() - minIntensity ) / range ),
										new UnsignedShortType());

						final RandomAccessibleInterval<UnsignedShortType> sourceGridBlock = Views.offsetInterval(sourceUINT16, gridBlock[0], gridBlock[1]);
						N5Utils.saveBlock(sourceGridBlock, n5Writer, n5Dataset, gridBlock[2]);
					}
					else
					{
						final RandomAccessibleInterval<FloatType> sourceGridBlock = Views.offsetInterval(source, gridBlock[0], gridBlock[1]);
						N5Utils.saveBlock(sourceGridBlock, n5Writer, n5Dataset, gridBlock[2]);
					}
				});

		sc.close();

		System.out.println( "Saved, e.g. view with './n5-view -i " + n5Path + " -d " + n5Dataset );
		System.out.println( "done, took: " + (System.currentTimeMillis() - time ) + " ms." );

		return null;
	}

	public static final void main(final String... args) {

		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new AffineFusion()).execute(args));
	}

}
