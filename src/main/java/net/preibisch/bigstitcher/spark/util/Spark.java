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
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.stitchingresults.PairwiseStitchingResult;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;

/**
 * Static helpers for running BigStitcher / multiview-reconstruction code inside Spark jobs: converting
 * {@link ViewId}s, {@link Group}s of views, view pairs and {@link Interval}s to and from plain primitive arrays
 * (which Spark can serialize cheaply), re-wrapping {@code ViewDescription}-backed objects as plain
 * {@link ViewId}s, and loading a {@link SpimData2} instance configured for use inside a Spark task.
 */
public class Spark {

	/** Maximum number of Spark partitions a job is split into; set from {@code --maxPartitions} (default 10000). */
	public static int maxPartitions = 10_000;

	/**
	 * Converts serialized view ids back into {@link ViewId}s.
	 *
	 * @param serializedViewIds one {@code {timepointId, viewSetupId}} pair per view, as created by
	 *        {@link #serializeViewIds(List)}
	 * @return the corresponding {@link ViewId}s, in the same order
	 */
	public static List< ViewId > deserializeViewIds( final int[][] serializedViewIds )
	{
		final List< ViewId > viewIds = new ArrayList<>( serializedViewIds.length );
		for ( int[] sid : serializedViewIds )
			viewIds.add( deserializeViewId( sid ) );
		return viewIds;
	}
	
	/**
	 * Converts a serialized pair of view ids back into a {@link Pair} of {@link ViewId}s.
	 *
	 * @param serializedPair two serialized view ids ({@code {timepointId, viewSetupId}} each), as created by
	 *        {@link #serializeViewIdPairForRDD(Pair)}
	 * @return the pair of {@link ViewId}s (entry 0 as {@code A}, entry 1 as {@code B})
	 */
	public static Pair<ViewId, ViewId> derserializeViewIdPairsForRDD( final int[][] serializedPair )
	{
		return new ValuePair<ViewId, ViewId>(deserializeViewId( serializedPair[ 0 ] ), deserializeViewId( serializedPair[ 1 ] ));
	}

	/**
	 * Converts the {@code i}-th entry of an array of serialized view ids into a {@link ViewId}.
	 *
	 * @param serializedViewIds one {@code {timepointId, viewSetupId}} pair per view
	 * @param i index of the entry to deserialize
	 * @return the {@link ViewId} at index {@code i}
	 */
	public static ViewId deserializeViewIds( final int[][] serializedViewIds, final int i )
	{
		return deserializeViewId( serializedViewIds[i] );
	}

	/**
	 * Converts a serialized view id back into a {@link ViewId}.
	 *
	 * @param serializedViewIds {@code {timepointId, viewSetupId}}, as created by {@link #serializeViewId(ViewId)}
	 * @return the corresponding {@link ViewId}
	 */
	public static ViewId deserializeViewId( final int[] serializedViewIds )
	{
		return new ViewId( serializedViewIds[0], serializedViewIds[1] );
	}

	/**
	 * Serializes {@link ViewId}s into a primitive array that Spark can ship to executors.
	 *
	 * @param viewIds the views to serialize
	 * @return one {@code {timepointId, viewSetupId}} pair per view, in list order
	 */
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

	/**
	 * Converts a serialized pair of view groups back into a {@link Pair} of {@link Group}s of {@link ViewId}s.
	 *
	 * @param serializedPair {@code [group][view]{timepointId, viewSetupId}} with exactly two groups, as created by
	 *        {@link #serializeGroupedViewIdPairForRDD(Pair)}
	 * @return the pair of groups (group 0 as {@code A}, group 1 as {@code B})
	 */
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

	/**
	 * Serializes a list of view pairs into primitive arrays suitable for {@code JavaSparkContext.parallelize}.
	 *
	 * @param pairs the view pairs to serialize
	 * @return one {@code int[2][2]} array per pair (see {@link #serializeViewIdPairForRDD(Pair)}), in list order
	 */
	public static ArrayList<int[][]> serializeViewIdPairsForRDD( final List< Pair<ViewId, ViewId> > pairs )
	{
		final ArrayList<int[][]> ser = new ArrayList<>();

		for ( final Pair<ViewId, ViewId> pair : pairs )
			ser.add( serializeViewIdPairForRDD( pair ) );

		return ser;
	}

	/**
	 * Serializes a pair of views into a primitive array.
	 *
	 * @param pair the view pair to serialize
	 * @return {@code {serialized A, serialized B}}, each entry being {@code {timepointId, viewSetupId}}
	 */
	public static int[][] serializeViewIdPairForRDD( final Pair<ViewId, ViewId> pair )
	{
		final int[][] pairInt = new int[2][];

		pairInt[0] = serializeViewId( pair.getA() );
		pairInt[1] = serializeViewId( pair.getB() );

		return pairInt;
	}

	/**
	 * Serializes a list of pairs of view groups into primitive arrays suitable for
	 * {@code JavaSparkContext.parallelize}.
	 *
	 * @param pairs the pairs of view groups to serialize
	 * @return one {@code int[2][][]} array per pair (see {@link #serializeGroupedViewIdPairForRDD(Pair)}), in list
	 *         order
	 */
	public static ArrayList<int[][][]> serializeGroupedViewIdPairsForRDD( final List< ? extends Pair<? extends Group<? extends ViewId>, ? extends Group<? extends ViewId>>> pairs )
	{
		final ArrayList<int[][][]> ser = new ArrayList<>();

		for ( final Pair<? extends Group<? extends ViewId>, ? extends Group<? extends ViewId>> pair : pairs )
			ser.add( serializeGroupedViewIdPairForRDD( pair ) );

		return ser;
	}

	/**
	 * Serializes a pair of view groups into a primitive array.
	 *
	 * @param pair the pair of view groups to serialize
	 * @return {@code [group][view]{timepointId, viewSetupId}}, with the views of {@code A} at index 0 and those of
	 *         {@code B} at index 1
	 */
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

	/**
	 * Serializes {@link ViewId}s into a list of primitive arrays suitable for {@code JavaSparkContext.parallelize}.
	 *
	 * @param viewIds the views to serialize
	 * @return one {@code {timepointId, viewSetupId}} array per view, in list order
	 */
	public static ArrayList<int[]> serializeViewIdsForRDD( final List< ViewId > viewIds )
	{
		final ArrayList<int[]> serializedViewIds = new ArrayList<>();

		for ( int i = 0; i < viewIds.size(); ++i )
			serializedViewIds.add( serializeViewId( viewIds.get( i ) ) );

		return serializedViewIds;
	}

	/**
	 * Reads interest points back from a 2D image of coordinates (e.g. the temporary N5 {@code points} dataset
	 * written by interest point detection): dimension 0 holds the coordinates of one point, dimension 1 indexes
	 * the points.
	 *
	 * @param points the {@code numDimensions x numPoints} image of point coordinates
	 * @return the interest points, using the running index along dimension 1 as point id
	 */
	public static ArrayList< InterestPoint > deserializeInterestPoints( final RandomAccessibleInterval<DoubleType> points )
	{
		final ArrayList< InterestPoint > list = new ArrayList<>();
		final Cursor< DoubleType > cursor = Views.flatIterable( points ).localizingCursor();

		for ( int i = 0; i < points.dimension( 1 ); ++i )
		{
			final double[] l = new double[ (int)points.dimension( 0 ) ];

			for ( int d = 0; d < points.dimension( 0 ); ++d )
				l[ d ] = cursor.next().get();

			list.add( new InterestPoint(i, l ));
		}

		return list;
	}

	/**
	 * Serializes a view id into a primitive array.
	 *
	 * @param viewId the view to serialize
	 * @return {@code {timepointId, viewSetupId}}
	 */
	public static int[] serializeViewId( final ViewId viewId )
	{
		return new int[] { viewId.getTimePointId(), viewId.getViewSetupId() };
	}

	/**
	 * Converts a serialized interval back into an {@link Interval}.
	 *
	 * @param serializedInterval {@code {min, max}}, as created by {@link #serializeInterval(Interval)}
	 * @return a {@link FinalInterval} spanning {@code min} to {@code max} (both inclusive)
	 */
	public static Interval deserializeInterval( final long[][] serializedInterval )
	{
		return new FinalInterval( serializedInterval[ 0 ], serializedInterval[ 1 ] );
	}

	/**
	 * Serializes an interval into a primitive array.
	 *
	 * @param interval the interval to serialize
	 * @return {@code {min, max}}, i.e. the interval's minimum and maximum coordinates (both inclusive)
	 */
	public static long[][] serializeInterval( final Interval interval )
	{
		return new long[][]{ interval.minAsLongArray(), interval.maxAsLongArray() };
	}

	/**
	 * A {@link Serializable} stand-in for a {@link PairwiseStitchingResult} over {@link ViewId}s that stores the
	 * view pair, affine transform and bounding box as primitive arrays, so the result of a pairwise stitching
	 * task can be returned from a Spark executor.
	 */
	public static class SerializablePairwiseStitchingResult implements Serializable
	{
		private static final long serialVersionUID = -8920256594391301778L;

		/** The compared pair of view groups, serialized as {@code [group][view]{timepointId, viewSetupId}}. */
		final int[][][] pair; // Pair< Group<ViewId>, Group<ViewId> > pair;
		/** The 3D affine transform mapping A to B, as a 3 by 4 matrix ({@code matrix[row][column]}). */
		final double[][] matrix = new double[3][4]; //AffineTransform3D transform;
		/** Minimum and maximum of the bounding box (in global space) in which the pair was compared. */
		final double[] min, max; //final RealInterval boundingBox;
		/** The cross-correlation of the pairwise comparison. */
		final double r;
		/** Hash of the view registrations at the time the relative pairwise shift was computed. */
		final double hash;

		/**
		 * Captures the given result in serializable form.
		 *
		 * @param result the stitching result to capture
		 */
		public SerializablePairwiseStitchingResult( final PairwiseStitchingResult< ViewId> result )
		{
			this.r = result.r();
			this.hash = result.getHash();
			this.min = result.getBoundingBox().minAsDoubleArray();
			this.max = result.getBoundingBox().maxAsDoubleArray();
			this.pair = Spark.serializeGroupedViewIdPairForRDD( result.pair() );
			((AffineTransform3D)result.getTransform()).toMatrix( matrix );
		}

		/**
		 * Rebuilds the result from the stored primitive arrays.
		 *
		 * @return a new {@link PairwiseStitchingResult} with the same content as the one this instance was created from
		 */
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

	/**
	 * Returns the id of the Spark executor (or driver) this code is running on.
	 *
	 * @return the executor id, or {@code null} if no Spark environment is active in this thread
	 */
	public static String getSparkExecutorId() {
		final SparkEnv sparkEnv = SparkEnv.get();
		return sparkEnv == null ? null : sparkEnv.executorId();
	}

	/**
	 * Loads the dataset XML with the image loader's fetcher threads set to 0, i.e. without background fetching.
	 *
	 * @param xmlPath URI of the dataset XML to load
	 * @return a new data instance optimized for use within single-threaded Spark tasks.
	 * @throws SpimDataException if the XML cannot be loaded
	 */
	public static SpimData2 getSparkJobSpimData2( final URI xmlPath ) throws SpimDataException
	{
		return getJobSpimData2( xmlPath, 0 );
	}

	/**
	 * Loads the dataset XML and, if its image loader is a {@link ViewerImgLoader}, sets the number of fetcher
	 * threads it uses to {@code numFetcherThreads}.
	 *
	 * @param xmlPath URI of the dataset XML to load
	 * @param numFetcherThreads number of background fetcher threads for the image loader; {@code 0} disables them
	 * @return a new data instance optimized for multi-threaded tasks.
	 * @throws SpimDataException if the XML cannot be loaded
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

	/**
	 * Copies view pairs into plain {@link ViewId}/{@link ValuePair} instances, dropping any subclass state
	 * (e.g. {@code ViewDescription}) so the list can be serialized by Spark.
	 *
	 * @param pairList the pairs to copy
	 * @return a new list of pairs of plain {@link ViewId}s, in the same order
	 */
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

	/**
	 * Copies pairs of view groups into plain {@link ViewId}/{@link ValuePair}/{@link Group} instances, dropping
	 * any subclass state (e.g. {@code ViewDescription}) so the list can be serialized by Spark.
	 *
	 * @param pairList the pairs of groups to copy
	 * @return a new list of pairs of groups of plain {@link ViewId}s, in the same order
	 */
	public static ArrayList< Pair<Group<ViewId>, Group<ViewId>> > toGroupViewIds( final List<Pair<Group<ViewId>, Group<ViewId>>> pairList )
	{
		final ArrayList< Pair<Group<ViewId>, Group<ViewId>> > serializableList = new ArrayList<>();

		pairList.forEach( pair -> serializableList.add(
				new ValuePair<>(
						toGroupViewIds( pair.getA() ),
						toGroupViewIds( pair.getB() ) )));

		return serializableList;
	}

	/**
	 * Copies a view group into a new {@link Group} of plain {@link ViewId}s, dropping any subclass state
	 * (e.g. {@code ViewDescription}).
	 *
	 * @param group the group to copy
	 * @return a new group containing a plain {@link ViewId} for each view of {@code group}
	 */
	public static Group<ViewId> toGroupViewIds( final Group<ViewId> group )
	{
		return new Group<>(
				group.getViews().stream().map( viewId -> new ViewId(
						viewId.getTimePointId(),
						viewId.getViewSetupId()) ).collect( Collectors.toList() ) );
	}
}
