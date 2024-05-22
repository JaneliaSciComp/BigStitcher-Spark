package net.preibisch.bigstitcher.spark.cloud;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.N5Factory;

import bdv.ViewerImgLoader;
import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.volatiles.VolatileViews;
import ij.ImageJ;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.generic.sequence.BasicImgLoader;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Util;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.explorer.ViewSetupExplorer;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsN5;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPointLists;

public class TestN5Loading
{
	public static <T extends NativeType<T> & RealType<T>> void testBDV() throws IOException, URISyntaxException
	{
		final N5Reader n5 = new N5Factory().openReader("s3://janelia-bigstitcher-spark/Stitching/dataset.n5");

		String[] l = n5.list( "/" );

		for ( final String s : l )
			System.out.println(s);

		final String n5Dataset = "/setup0/timepoint0/s0/";
		final RandomAccessibleInterval<T> img = N5Utils.openVolatile(n5, n5Dataset);

		BdvFunctions.show(
				VolatileViews.wrapAsVolatile( img ),
				n5Dataset,
				BdvOptions.options()).setDisplayRange(0, 255);
	}

	public static <S extends NativeType<S> & IntegerType<S>, T extends NativeType<T> & RealType<T>> void testInterestPoints() throws IOException
	{
		final N5Reader n5 = new N5Factory().openReader("s3://janelia-bigstitcher-spark/Stitching/interestpoints.n5");

		String[] l = n5.list( "/" );

		for ( final String s : l )
			System.out.println( s );

		final RandomAccessibleInterval< S > id = N5Utils.open(n5, l[2] + "/beads/interestpoints/id" );
		final RandomAccessibleInterval< T > loc = N5Utils.open(n5, l[2] + "/beads/interestpoints/loc" );

		System.out.println( "id: " + Util.printInterval( id ));
		System.out.println( "loc: " + Util.printInterval( loc ));

		final long numPoints = id.dimension( 1 );

		final RandomAccess< S > idR = id.randomAccess();
		final RandomAccess< T > locR = loc.randomAccess();

		idR.setPosition( new long[] { 0, 0 } );
		locR.setPosition( new long[] { 0, 0 } );

		for ( long i = 0; i < numPoints; ++i )
		{
			System.out.print( "id=" + idR.setPositionAndGet( 0, i ) );

			System.out.println( ", [" + locR.setPositionAndGet( 0, i ) + ", " + locR.setPositionAndGet( 1, i ) + ", " + locR.setPositionAndGet( 2, i ) + "]");

		}
	}

	public static void testLoadInterestPoints() throws SpimDataException, IOException
	{
		final SpimData2 data = Spark.getSparkJobSpimData2( "s3://janelia-bigstitcher-spark/Stitching/dataset.xml" , false );

		System.out.println( "num viewsetups: " + data.getSequenceDescription().getViewSetupsOrdered().size() );

		final Map<ViewId, ViewInterestPointLists> ips = data.getViewInterestPoints().getViewInterestPoints();
		final ViewInterestPointLists ipl = ips.values().iterator().next();
		final InterestPoints ip = ipl.getHashMap().values().iterator().next();
		
		System.out.println("base dir: " + ip.getBaseDir() );
		System.out.println("base dir modified: " + InterestPointsN5.assembleURI( ip.getBaseDir(), InterestPointsN5.baseN5 ) );

		List<InterestPoint> ipList = ip.getInterestPointsCopy();

		System.out.println( "Loaded " + ipList.size() + " interest points.");

		System.out.println( "Saving s3://janelia-bigstitcher-spark/Stitching/dataset-save.xml ...");

		Spark.saveSpimData2( data, "s3://janelia-bigstitcher-spark/Stitching/dataset-save.xml" , false );

		System.out.println( "Done.");
	}

	public static void testBigStitcherGUI( final String xml ) throws SpimDataException
	{
		new ImageJ();

		final SpimData2 data = Spark.getSparkJobSpimData2( xml , false );

		final BasicImgLoader imgLoader = data.getSequenceDescription().getImgLoader();
		if (imgLoader instanceof ViewerImgLoader)
			((ViewerImgLoader) imgLoader).setNumFetcherThreads(-1);

		
		final ViewSetupExplorer< SpimData2 > explorer = new ViewSetupExplorer<>( data, xml, new XmlIoSpimData2("") );

		explorer.getFrame().toFront();
	}

	public static void main( String[] args ) throws IOException, URISyntaxException, SpimDataException
	{
		CloudUtil.parseCloudLink( "s3://janelia-bigstitcher-spark/Stitching/dataset.xml" );

		//testLoadInterestPoints();
		//testBigStitcherGUI( "s3://janelia-bigstitcher-spark/Stitching/dataset.xml" );
		testBigStitcherGUI( "/Users/preibischs/Documents/Janelia/Projects/BigStitcher/Allen/bigstitcher_emr_708369_2024-04-23_06-52-14_2.xml" );
		//s3://aind-open-data/exaSPIM_708369_2024-04-08_15-20-36_flatfield-correction_2024-04-16_20-33-12/SPIM.ome.zarr
		//testBDV();
		//testInterestPoints();
	}
}
