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

/**
 * Static helpers that decide which views of a dataset overlap a given world-space interval, or
 * overlap each other, based on the views' registered (transformed) bounding boxes. The fusion
 * tools use them to restrict loading and rendering to the views that actually contribute to a
 * block.
 */
public class OverlappingViews
{
	/**
	 * Default expansion (in pixels, per side) of a block interval when searching for overlapping
	 * views in affine fusion ({@code SparkFusion} with {@code FusionMethod.AFFINE}).
	 */
	public static final int defaultAffineExpansion = 2;

	/**
	 * Default expansion (in pixels, per side) of a block interval when searching for overlapping
	 * views in thin-plate-spline fusion; much larger than {@link #defaultAffineExpansion} because
	 * the non-rigid deformation can pull content in from outside the affinely transformed bounds.
	 */
	public static final int defaultTPSExpansion = 50;

	/**
	 * Find all views among the given {@code viewIds} that overlap the given {@code interval}.
	 * The image interval of each view is transformed into world coordinates
	 * and checked for overlap with {@code interval}, with a conservative
	 * extension of 2 pixels in each direction.
	 *
	 * @param spimData contains bounds and registrations for all views
	 * @param viewIds which views to check
	 * @param interval interval in world coordinates
	 * @param registrations registrations for each view, may be adjusted for anisotropy
	 * @param expansion how much the interval will be expanded to avoid accidental missed (e.g. affine could be 2)
	 * @return views that overlap {@code interval}
	 */
	public static List<ViewId> findOverlappingViews(
			final SpimData spimData,
			final List<ViewId> viewIds,
			final HashMap< ViewId, AffineTransform3D > registrations,
			final Interval interval,
			final int expansion )
	{
		final List< ViewId > overlapping = new ArrayList<>();

		// expand to be conservative ...
		final Interval expandedInterval = Intervals.expand( interval, expansion );

		for ( final ViewId viewId : viewIds )
		{
			final Interval bounds = ViewUtil.getTransformedBoundingBox( spimData, viewId, registrations.get( viewId ) );
			if ( ViewUtil.overlaps( expandedInterval, bounds ) )
				overlapping.add( viewId );
		}

		return overlapping;
	}

}
