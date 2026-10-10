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
import java.util.Collection;
import java.util.List;

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
 * Static helpers for running BigStitcher / multiview-reconstruction code inside Spark jobs: copying
 * {@link ViewId}s, {@link Group}s of views and view pairs into plain, serializable instances (dropping
 * {@code ViewDescription} state, which references the whole sequence description), converting
 * {@link Interval}s to and from primitive arrays, and loading a {@link SpimData2} instance configured for use
 * inside a Spark task.
 */
public class Spark {

	/** Maximum number of Spark partitions a job is split into; set from {@code --maxPartitions} (default 10000). */
	public static int maxPartitions = 10_000;

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
	 * view pair as plain {@link Group}s of {@link ViewId}s and the affine transform and bounding box as primitive
	 * arrays, so the result of a pairwise stitching task can be returned from a Spark executor.
	 */
	public static class SerializablePairwiseStitchingResult implements Serializable
	{
		private static final long serialVersionUID = -8920256594391301778L;

		/** The compared pair of view groups, as plain {@link ViewId}s. */
		final Group< ViewId > groupA, groupB; // Pair< Group<ViewId>, Group<ViewId> > pair;
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
			this.groupA = Spark.toGroupViewIds( result.pair().getA() );
			this.groupB = Spark.toGroupViewIds( result.pair().getB() );
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
					new ValuePair<>( groupA, groupB ),
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
	 * Copies a view id into a plain {@link ViewId}, dropping any subclass state (e.g. {@code ViewDescription},
	 * which references the whole sequence description and therefore cannot be serialized by Spark).
	 *
	 * @param viewId the view to copy
	 * @return a new plain {@link ViewId} with the same timepoint and view setup id
	 */
	public static ViewId toViewId( final ViewId viewId )
	{
		return new ViewId( viewId.getTimePointId(), viewId.getViewSetupId() );
	}

	/**
	 * Copies views into plain {@link ViewId}s (see {@link #toViewId(ViewId)}) so the list can be serialized by
	 * Spark.
	 *
	 * @param viewIds the views to copy
	 * @return a new list of plain {@link ViewId}s, in iteration order
	 */
	public static ArrayList< ViewId > toViewIds( final Collection< ? extends ViewId > viewIds )
	{
		final ArrayList< ViewId > serializableList = new ArrayList<>( viewIds.size() );

		for ( final ViewId viewId : viewIds )
			serializableList.add( toViewId( viewId ) );

		return serializableList;
	}

	/**
	 * Copies view pairs into plain {@link ViewId}/{@link ValuePair} instances (see {@link #toViewId(ViewId)}).
	 * Note that {@link ValuePair} itself is not {@link Serializable}.
	 *
	 * @param pairList the pairs to copy
	 * @return a new list of pairs of plain {@link ViewId}s, in the same order
	 */
	public static ArrayList< Pair<ViewId, ViewId> > toViewIdPairs( final List< ? extends Pair< ? extends ViewId, ? extends ViewId > > pairList )
	{
		final ArrayList< Pair<ViewId, ViewId> > serializableList = new ArrayList<>();

		pairList.forEach( pair -> serializableList.add( new ValuePair<>( toViewId( pair.getA() ), toViewId( pair.getB() ) ) ) );

		return serializableList;
	}

	/**
	 * Copies pairs of view groups into plain {@link ViewId}/{@link ValuePair}/{@link Group} instances, dropping
	 * any subclass state (e.g. {@code ViewDescription}) so the list can be serialized by Spark.
	 *
	 * @param pairList the pairs of groups to copy
	 * @return a new list of pairs of groups of plain {@link ViewId}s, in the same order
	 */
	public static ArrayList< Pair<Group<ViewId>, Group<ViewId>> > toGroupViewIds( final List< ? extends Pair< ? extends Group< ? extends ViewId >, ? extends Group< ? extends ViewId > > > pairList )
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
	public static Group<ViewId> toGroupViewIds( final Group< ? extends ViewId > group )
	{
		return new Group<>( toViewIds( group.getViews() ) );
	}
}
