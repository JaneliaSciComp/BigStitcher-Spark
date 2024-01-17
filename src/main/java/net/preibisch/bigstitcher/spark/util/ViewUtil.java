package net.preibisch.bigstitcher.spark.util;

import mpicbg.spim.data.SpimData;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.sequence.ImgLoader;
import mpicbg.spim.data.sequence.SetupImgLoader;
import mpicbg.spim.data.sequence.ViewId;

import net.imglib2.Dimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.util.Intervals;

public class ViewUtil {

	public static boolean overlaps( final Interval interval1, final Interval interval2 )
	{
		return !Intervals.isEmpty( Intervals.intersect( interval1, interval2 ) );
	}

	public static Dimensions getDimensions(final SpimData data, final ViewId viewId ) throws IllegalArgumentException
	{
		final ImgLoader imgLoader = data.getSequenceDescription().getImgLoader();
		final SetupImgLoader<?> setupImgLoader = imgLoader.getSetupImgLoader(viewId.getViewSetupId());
		if (setupImgLoader == null) {
			throw new IllegalArgumentException(
					"failed to find setupImgLoader for " + viewIdToString(viewId) + " in " + data);
		}
		return setupImgLoader.getImageSize(viewId.getTimePointId() );
	}

	public static ViewRegistration getViewRegistration(final SpimData data, final ViewId viewId ) throws IllegalArgumentException
	{
		final ViewRegistration reg = data.getViewRegistrations().getViewRegistration( viewId );
		if (reg == null) {
			throw new IllegalArgumentException(
					"failed to find viewRegistration for " + viewIdToString(viewId) + " in " + data);
		}

		reg.updateModel(); // TODO: This shouldn't be necessary, right?

		return reg;
	}

	/**
	 * Get the estimated bounding box of the specified view in world coordinates.
	 * This transforms the image dimension for {@code viewId} with the {@code
	 * ViewRegistration} for {@code viewId}, and takes the bounding box.
	 */
	public static Interval getTransformedBoundingBox( final SpimData data, final ViewId viewId ) throws IllegalArgumentException
	{
		final Dimensions dim = getDimensions( data, viewId );
		final ViewRegistration reg = getViewRegistration( data, viewId );

		return Intervals.smallestContainingInterval( reg.getModel().estimateBounds( new FinalInterval( dim ) ) );
	}

	public static String viewIdToString(final ViewId viewId) {
		return viewId == null ?
			   null : "{\"setupId\": " + viewId.getViewSetupId() + ", \"timePointId\": " + viewId.getTimePointId() + "}";
	}
}
