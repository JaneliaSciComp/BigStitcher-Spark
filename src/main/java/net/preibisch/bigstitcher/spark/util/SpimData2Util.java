package net.preibisch.bigstitcher.spark.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.CorrespondingInterestPoints;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;

public class SpimData2Util
{
	public static void printCorrespondingInterestPoints(
			final String n5Dir,
			final String ipName,
			final int index ) throws IOException
	{
		final N5Reader n5 = new N5FSReader( n5Dir );

		String[] l = n5.list( "/" );

		for ( final String s : l )
			System.out.println( s );

		final String dataset = l[ index ] + "/" + ipName+ "/correspondences/";

		final String version = n5.getAttribute(dataset, "correspondences", String.class );
		final Map< String, Long > idMap = n5.getAttribute(dataset, "idMap", Map.class ); // to store ID (viewId.getTimePointId() + "," + viewId.getViewSetupId() + "," + label)

		System.out.println( version + ", " + idMap.size() + " correspondence codes" );

		if ( idMap.size() == 0 )
			return;

		final Map< Long, Pair<ViewId, String> > quickLookup = new HashMap<>();
		for ( final Entry<String, Long> entry : idMap.entrySet() )
		{
			final int firstComma = entry.getKey().indexOf( "," );
			final String tp = entry.getKey().substring( 0, firstComma );
			String remaining = entry.getKey().substring( firstComma + 1, entry.getKey().length() );
			final int secondComma = remaining.indexOf( "," );
			final String setup = remaining.substring( 0, secondComma );
			final String label = remaining.substring( secondComma + 1, remaining.length() );

			final int tpInt = Integer.parseInt(tp);
			final int setupInt = Integer.parseInt(setup);

			final long id;

			if ( Double.class.isInstance((Object)entry.getValue()))
				id = Math.round( (Double)(Object)entry.getValue() ); // TODO: bug, a long maybe loaded as a double
			else
				id = entry.getValue();

			final Pair<ViewId, String> value = new ValuePair<>( new ViewId( tpInt, setupInt ), label );
			quickLookup.put( id , value );
		}

		quickLookup.forEach( (s,i) -> System.out.println( s + " -> " + Group.pvid( i.getA() ) + ", " + i.getB() ) );

		final RandomAccessibleInterval< UnsignedLongType > data = N5Utils.open(n5, dataset + "/data" );
		System.out.println( "data: " + Util.printInterval( data ));

		final RandomAccess< UnsignedLongType > corrRA = data.randomAccess();

		corrRA.setPosition( 0, 0 );
		corrRA.setPosition( 0, 1 );

		for ( int i = 0; i < data.dimension( 1 ); ++ i )
		{
			final long idA = corrRA.get().get();
			corrRA.fwd(0);
			final long idB = corrRA.get().get();
			corrRA.fwd(0);
			final long id = corrRA.get().get();

			corrRA.bck(0);
			corrRA.bck(0);

			if ( i != data.dimension( 1 ) - 1 )
				corrRA.fwd( 1 );

			// final int detectionId, final ViewId correspondingViewId, final String correspondingLabel, final int correspondingDetectionId
			final Pair<ViewId, String> value = quickLookup.get( id );
			final CorrespondingInterestPoints cip = new CorrespondingInterestPoints( (int)idA, value.getA(), value.getB(), (int)idB );

			System.out.println( "id=" + idA + " ==> " + Group.pvid(value.getA()) + "; label=" + value.getB() + ", id=" + idB );
		}

		/*
		final long numCorrespondences = id.dimension( 1 );


		for ( long i = 0; i < numPoints; ++i )
			System.out.println( "id=" + idR.setPositionAndGet( 0, i ) + ", [" + locR.setPositionAndGet( 0, i ) + ", " + locR.setPositionAndGet( 1, i ) + ", " + locR.setPositionAndGet( 2, i ) + "]");*/
	}

	public static <S extends NativeType<S> & IntegerType<S>, T extends NativeType<T> & RealType<T>> void printInterestPoints(
			final String n5Dir,
			final String ipName,
			final int index ) throws IOException
	{
		final N5Reader n5 = new N5FSReader( n5Dir );

		String[] l = n5.list( "/" );

		for ( final String s : l )
			System.out.println( s );

		final RandomAccessibleInterval< S > id = N5Utils.open(n5, l[ index ] + "/" + ipName+ "/interestpoints/id" );
		final RandomAccessibleInterval< T > loc = N5Utils.open(n5, l[ index ] + "/" + ipName + "/interestpoints/loc" );

		System.out.println( "id: " + Util.printInterval( id ));
		System.out.println( "loc: " + Util.printInterval( loc ));

		final long numPoints = id.dimension( 1 );

		final RandomAccess< S > idR = id.randomAccess();
		final RandomAccess< T > locR = loc.randomAccess();

		idR.setPosition( new long[] { 0, 0 } );
		locR.setPosition( new long[] { 0, 0 } );

		for ( long i = 0; i < numPoints; ++i )
			System.out.println( "id=" + idR.setPositionAndGet( 0, i ) + ", [" + locR.setPositionAndGet( 0, i ) + ", " + locR.setPositionAndGet( 1, i ) + ", " + locR.setPositionAndGet( 2, i ) + "]");
	}

	public static void main( String[] args ) throws IOException
	{
		printInterestPoints( "/Users/preibischs/Downloads/public-archivedwl-747/interestpoints.n5", "beads", 0 );

		printCorrespondingInterestPoints( "/Users/preibischs/Downloads/public-archivedwl-747/interestpoints.n5", "beads", 0 );

	}
}
