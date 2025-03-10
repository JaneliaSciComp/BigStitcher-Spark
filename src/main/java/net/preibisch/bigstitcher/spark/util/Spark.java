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
package net.preibisch.bigstitcher.spark.util;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bdv.ViewerImgLoader;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.generic.sequence.BasicImgLoader;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.stitchingresults.PairwiseStitchingResult;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;

public class Spark {

	public static final int maxPartitions = 10_000;

	public static List< ViewId > deserializeViewIds( final int[][] serializedViewIds )
	{
		final List< ViewId > viewIds = new ArrayList<>( serializedViewIds.length );
		for ( int[] sid : serializedViewIds )
			viewIds.add( deserializeViewId( sid ) );
		return viewIds;
	}
	
	public static Pair<ViewId, ViewId> derserializeViewIdPairsForRDD( final int[][] serializedPair )
	{
		return new ValuePair<ViewId, ViewId>(deserializeViewId( serializedPair[ 0 ] ), deserializeViewId( serializedPair[ 1 ] ));
	}

	public static ViewId deserializeViewIds( final int[][] serializedViewIds, final int i )
	{
		return deserializeViewId( serializedViewIds[i] );
	}

	public static ViewId deserializeViewId( final int[] serializedViewIds )
	{
		return new ViewId( serializedViewIds[0], serializedViewIds[1] );
	}

	public static int[][] serializeViewIds( final List< ViewId > viewIds )
	{
		final int[][] serializedViewIds = new int[ viewIds.size() ][ 2 ];

		for ( int i = 0; i < viewIds.size(); ++i )
		{
			serializedViewIds[ i ][ 0 ] = viewIds.get( i ).getTimePointId();
			serializedViewIds[ i ][ 1 ] = viewIds.get( i ).getViewSetupId();
		}

		return serializedViewIds;
	}

	public static Pair<Group<ViewId>, Group<ViewId>> deserializeGroupedViewIdPairForRDD( final int[][][] serializedPair )
	{
		final ArrayList< ViewId > pairA = new ArrayList<>( serializedPair[ 0 ].length );
		final ArrayList< ViewId > pairB = new ArrayList<>( serializedPair[ 1 ].length );

		for ( int a = 0; a < serializedPair[ 0 ].length; ++a )
			pairA.add( deserializeViewId( serializedPair[ 0 ][ a ]) );

		for ( int b = 0; b < serializedPair[ 1 ].length; ++b )
			pairB.add( deserializeViewId( serializedPair[ 1 ][ b ]) );

		return new ValuePair<Group<ViewId>, Group<ViewId>>( new Group<>( pairA ), new Group<>( pairB ) );
	}

	public static ArrayList<int[][]> serializeViewIdPairsForRDD( final List< Pair<ViewId, ViewId> > pairs )
	{
		final ArrayList<int[][]> ser = new ArrayList<>();

		for ( final Pair<ViewId, ViewId> pair : pairs )
			ser.add( serializeViewIdPairForRDD( pair ) );

		return ser;
	}

	public static int[][] serializeViewIdPairForRDD( final Pair<ViewId, ViewId> pair )
	{
		final int[][] pairInt = new int[2][];

		pairInt[0] = serializeViewId( pair.getA() );
		pairInt[1] = serializeViewId( pair.getB() );

		return pairInt;
	}

	public static ArrayList<int[][][]> serializeGroupedViewIdPairsForRDD( final List< ? extends Pair<? extends Group<? extends ViewId>, ? extends Group<? extends ViewId>>> pairs )
	{
		final ArrayList<int[][][]> ser = new ArrayList<>();

		for ( final Pair<? extends Group<? extends ViewId>, ? extends Group<? extends ViewId>> pair : pairs )
			ser.add( serializeGroupedViewIdPairForRDD( pair ) );

		return ser;
	}

	public static int[][][] serializeGroupedViewIdPairForRDD( final Pair<? extends Group<? extends ViewId>, ? extends Group<? extends ViewId>> pair )
	{
		final int[][][] pairInt = new int[2][][];

		pairInt[0] = new int[ pair.getA().getViews().size() ][];
		pairInt[1] = new int[ pair.getB().getViews().size() ][];

		int i = 0;
		for ( final ViewId viewId : pair.getA().getViews() )
			pairInt[0][i++] = serializeViewId( viewId );

		i = 0;
		for ( final ViewId viewId : pair.getB().getViews() )
			pairInt[1][i++] = serializeViewId( viewId );

		return pairInt;
	}

	public static ArrayList<int[]> serializeViewIdsForRDD( final List< ViewId > viewIds )
	{
		final ArrayList<int[]> serializedViewIds = new ArrayList<>();

		for ( int i = 0; i < viewIds.size(); ++i )
			serializedViewIds.add( serializeViewId( viewIds.get( i ) ) );

		return serializedViewIds;
	}

	public static ArrayList< InterestPoint > deserializeInterestPoints( final double[][] points )
	{
		final ArrayList< InterestPoint > list = new ArrayList<>();
		
		for ( int i = 0; i < points.length; ++i )
			list.add( new InterestPoint(i, points[ i ] ));

		return list;
	}

	public static int[] serializeViewId( final ViewId viewId )
	{
		return new int[] { viewId.getTimePointId(), viewId.getViewSetupId() };
	}

	public static Interval deserializeInterval( final long[][] serializedInterval )
	{
		return new FinalInterval( serializedInterval[ 0 ], serializedInterval[ 1 ] );
	}

	public static long[][] serializeInterval( final Interval interval )
	{
		return new long[][]{ interval.minAsLongArray(), interval.maxAsLongArray() };
	}

	public static class SerializablePairwiseStitchingResult implements Serializable
	{
		private static final long serialVersionUID = -8920256594391301778L;

		final int[][][] pair; // Pair< Group<ViewId>, Group<ViewId> > pair;
		final double[][] matrix = new double[3][4]; //AffineTransform3D transform;
		final double[] min, max; //final RealInterval boundingBox;
		final double r;
		final double hash;

		public SerializablePairwiseStitchingResult( final PairwiseStitchingResult< ViewId> result )
		{
			this.r = result.r();
			this.hash = result.getHash();
			this.min = result.getBoundingBox().minAsDoubleArray();
			this.max = result.getBoundingBox().maxAsDoubleArray();
			this.pair = Spark.serializeGroupedViewIdPairForRDD( result.pair() );
			((AffineTransform3D)result.getTransform()).toMatrix( matrix );
		}

		public PairwiseStitchingResult< ViewId > deserialize()
		{
			final AffineTransform3D t = new AffineTransform3D();
			t.set( matrix );

			return new PairwiseStitchingResult<>(
					Spark.deserializeGroupedViewIdPairForRDD( pair ),
					new FinalRealInterval(min, max),
					t,
					r,
					hash );
		}
	}

	public static String getSparkExecutorId() {
		final SparkEnv sparkEnv = SparkEnv.get();
		return sparkEnv == null ? null : sparkEnv.executorId();
	}

	/**
	 * @return a new data instance optimized for use within single-threaded Spark tasks.
	 */
	public static SpimData2 getSparkJobSpimData2( final URI xmlPath ) throws SpimDataException
	{
		return getJobSpimData2( xmlPath, 0 );
	}

	/**
	 * @return a new data instance optimized for multi-threaded tasks.
	 */
	public static SpimData2 getJobSpimData2( final URI xmlPath, final int numFetcherThreads ) throws SpimDataException
	{
		final SpimData2 data = new XmlIoSpimData2().load(xmlPath);
		final SequenceDescription sequenceDescription = data.getSequenceDescription();

		// set number of fetcher threads to 0 for spark usage
		final BasicImgLoader imgLoader = sequenceDescription.getImgLoader();
		if (imgLoader instanceof ViewerImgLoader) {
			((ViewerImgLoader) imgLoader).setNumFetcherThreads( numFetcherThreads );
		}

		//LOG.info("getSparkJobSpimData2: loaded {}, xmlPath={} on executorId={}", data, xmlPath, getSparkExecutorId());

		return data;
	}

	private static final Logger LOG = LoggerFactory.getLogger(Spark.class);

	public static ArrayList< Pair<ViewId, ViewId> > toViewIds( final List<Pair<ViewId, ViewId>> pairList )
	{
		final ArrayList< Pair<ViewId, ViewId> > serializableList = new ArrayList<>();

		pairList.forEach( pair -> serializableList.add(
				new ValuePair<>(
						new ViewId(
								pair.getA().getTimePointId(),
								pair.getA().getViewSetupId()),
						new ViewId(
								pair.getB().getTimePointId(),
								pair.getB().getViewSetupId())
						)));

		return serializableList;
	}

	public static ArrayList< Pair<Group<ViewId>, Group<ViewId>> > toGroupViewIds( final List<Pair<Group<ViewId>, Group<ViewId>>> pairList )
	{
		final ArrayList< Pair<Group<ViewId>, Group<ViewId>> > serializableList = new ArrayList<>();

		pairList.forEach( pair -> serializableList.add(
				new ValuePair<>(
						toGroupViewIds( pair.getA() ),
						toGroupViewIds( pair.getB() ) )));

		return serializableList;
	}

	public static Group<ViewId> toGroupViewIds( final Group<ViewId> group )
	{
		return new Group<>(
				group.getViews().stream().map( viewId -> new ViewId(
						viewId.getTimePointId(),
						viewId.getViewSetupId()) ).collect( Collectors.toList() ) );
	}
}
