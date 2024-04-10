package net.preibisch.bigstitcher.spark.cloud;

import java.io.IOException;
import java.net.URISyntaxException;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.N5Factory;

import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.volatiles.VolatileViews;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Util;

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

		final RandomAccessibleInterval< S > id = N5Utils.open(n5, l[0] + "/points/interestpoints/id" );
		final RandomAccessibleInterval< T > loc = N5Utils.open(n5, l[0] + "/points/interestpoints/loc" );

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

	public static void main( String[] args ) throws IOException, URISyntaxException
	{
		testBDV();
		testInterestPoints();
	}
}
