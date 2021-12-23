package net.preibisch.bigstitcher.spark.util;

import mpicbg.spim.data.SpimData;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Dimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.util.Intervals;

public class ViewUtil {

	public static boolean overlaps( final Interval interval1, final Interval interval2 )
	{
		final Interval intersection = Intervals.intersect( interval1, interval2 );

		for ( int d = 0; d < intersection.numDimensions(); ++d )
			if ( intersection.dimension( d ) < 0 )
				return false;

		return true;
	}

	public static Interval getTransformedBoundingBox( final SpimData data, final ViewId viewId )
	{
		final Dimensions dim = data.getSequenceDescription().getImgLoader().getSetupImgLoader( viewId.getViewSetupId() ).getImageSize( viewId.getTimePointId() );

		final ViewRegistration reg = data.getViewRegistrations().getViewRegistration( viewId );
		reg.updateModel();

		final Interval bounds = Intervals.largestContainedInterval( reg.getModel().estimateBounds( new FinalInterval( dim ) ) );

		return bounds;
	}
}
