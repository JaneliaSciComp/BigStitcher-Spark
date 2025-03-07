package net.preibisch.bigstitcher.spark.fusion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import mpicbg.spim.data.SpimData;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Interval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Intervals;
import net.preibisch.bigstitcher.spark.util.ViewUtil;

public class OverlappingViews
{
	/**
	 * Find all views among the given {@code viewIds} that overlap the given {@code interval}.
	 * The image interval of each view is transformed into world coordinates
	 * and checked for overlap with {@code interval}, with a conservative
	 * extension of 2 pixels in each direction.
	 *
	 * @param spimData contains bounds and registrations for all views
	 * @param viewIds which views to check
	 * @param interval interval in world coordinates
	 * @return views that overlap {@code interval}
	 */
	public static List<ViewId> findOverlappingViews(
			final SpimData spimData,
			final List<ViewId> viewIds,
			final Interval interval )
	{
		final List< ViewId > overlapping = new ArrayList<>();

		// expand to be conservative ...
		final Interval expandedInterval = Intervals.expand( interval, 2 );

		for ( final ViewId viewId : viewIds )
		{
			final Interval bounds = ViewUtil.getTransformedBoundingBox( spimData, viewId );
			if ( ViewUtil.overlaps( expandedInterval, bounds ) )
				overlapping.add( viewId );
		}

		return overlapping;
	}

	/**
	 * Find all views among the given {@code viewIds} that overlap the given {@code interval}.
	 * The image interval of each view is transformed into world coordinates
	 * and checked for overlap with {@code interval}, with a conservative
	 * extension of 2 pixels in each direction.
	 *
	 * @param spimData contains bounds and registrations for all views
	 * @param viewIds which views to check
	 * @param interval interval in world coordinates
	 * @return views that overlap {@code interval}
	 */
	public static List<ViewId> findOverlappingViews(
			final SpimData spimData,
			final List<ViewId> viewIds,
			final HashMap< ViewId, AffineTransform3D > registrations,
			final Interval interval )
	{
		final List< ViewId > overlapping = new ArrayList<>();

		// expand to be conservative ...
		final Interval expandedInterval = Intervals.expand( interval, 2 );

		for ( final ViewId viewId : viewIds )
		{
			final Interval bounds = ViewUtil.getTransformedBoundingBox( spimData, viewId, registrations.get( viewId ) );
			if ( ViewUtil.overlaps( expandedInterval, bounds ) )
				overlapping.add( viewId );
		}

		return overlapping;
	}

	public static ArrayList< ViewId > findAllOverlappingViewsFor(
			final ViewId viewIdA,
			final SpimData spimData,
			final List<ViewId> viewIds)
	{
		final ArrayList< ViewId > overlappingViews = new ArrayList<>();

		for ( final ViewId viewIdB : viewIds )
		{
			if ( viewIdA.equals( viewIdB ) )
				continue;

			final Interval bounds1 = ViewUtil.getTransformedBoundingBox( spimData, viewIdA );
			final Interval bounds2 = ViewUtil.getTransformedBoundingBox( spimData, viewIdB );

			if ( ViewUtil.overlaps( bounds1, bounds2 ) )
				overlappingViews.add( viewIdB );
		}

		return overlappingViews;
	}

}
