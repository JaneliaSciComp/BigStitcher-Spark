package net.preibisch.bigstitcher.spark.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import mpicbg.spim.data.SpimData;
import mpicbg.spim.data.generic.sequence.BasicMultiResolutionSetupImgLoader;
import mpicbg.spim.data.generic.sequence.BasicSetupImgLoader;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewRegistrations;
import mpicbg.spim.data.sequence.ImgLoader;
import mpicbg.spim.data.sequence.SetupImgLoader;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Dimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.cell.AbstractCellImg;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.iterator.IntervalIterator;
import net.imglib2.iterator.LocalizingIntervalIterator;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Intervals;
import net.imglib2.util.LinAlgHelpers;
import net.imglib2.util.Util;

public class ViewUtil
{

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

	/**
	 * Find cells of the given {@code ViewId} required to produce the given
	 * {@code fusedBlock} interval in world coordinates. If {@code data} is
	 * multi-resolution, the best resolution is picked (the one that will be
	 * used for fusion, too).
	 *
	 * @param data
	 * 		has all images and transformations
	 * @param viewId
	 * 		which view to check
	 * @param fusedBlock
	 * 		the interval that will be processed (in world coordinates)
	 *
	 * @return a list of {@code PrefetchPixel} callables that will each prefetch one cell.
	 */
	public static List< PrefetchPixel< ? > > findOverlappingBlocks(
			final SpimData data,
			final ViewId viewId,
			final Interval fusedBlock )
	{
		final List< PrefetchPixel< ? > > prefetch = new ArrayList<>();

		final ImgLoader imgLoader = data.getSequenceDescription().getImgLoader();
		final SetupImgLoader< ? > setupImgLoader = imgLoader.getSetupImgLoader( viewId.getViewSetupId() );
		if ( setupImgLoader == null )
		{
			throw new IllegalArgumentException(
					"failed to find setupImgLoader for " + viewIdToString( viewId ) + " in " + data );
		}

		final ViewRegistrations registrations = data.getViewRegistrations();
		final AffineTransform3D model = registrations.getViewRegistration( viewId ).getModel();

		final ImgAndMipmapTransform< ? > best = ImgAndMipmapTransform.forBestResolution( setupImgLoader, viewId.getTimePointId(), model );
		final RandomAccessibleInterval< ? > img = best.img;
		final AffineTransform3D imgToWorld = model.copy();
		imgToWorld.concatenate( best.mipmapTransform );

		if ( ! ( img instanceof AbstractCellImg ) )
		{
			throw new IllegalArgumentException( "TODO. Handling source types other than CellImg is not implemented yet" );
		}

		// Brute force search for overlapping cells:
		//
		// For each grid cell, estimate its bounding box in world space and test
		// for intersection with fusedBlock
		//
		// TODO: BigVolumeViewer has a more sophisticated method for
		//       intersecting the View Frustum with the source grid and
		//       determining overlapping cells. This is similar and could be
		//       re-used here to make the search more efficient. It should
		//       provide more accurate results because it uses non-axis aligned
		//       planes for intersection. See class FindRequiredBlocks.

		// iterate all cells (intervals) in grid
		final CellGrid grid = ( ( AbstractCellImg< ?, ?, ?, ? > ) img ).getCellGrid();
		final IntervalIterator gridIter = new LocalizingIntervalIterator( grid.getGridDimensions() );
		final int n = grid.numDimensions();
		final long[] gridPos = new long[ n ];
		final long[] cellMin = new long[ n ];
		final long[] cellMax = new long[ n ];
		final Interval cellInterval = FinalInterval.wrap( cellMin, cellMax );
		while( gridIter.hasNext() )
		{
			gridIter.fwd();
			gridIter.localize( gridPos );
			grid.getCellInterval( gridPos, cellMin, cellMax );

			final Interval bounds =
					Intervals.smallestContainingInterval(
							imgToWorld.estimateBounds(
									Intervals.expand( cellInterval, 1 ) ) );

			if ( overlaps( bounds, fusedBlock ) )
				prefetch.add( new PrefetchPixel( img, cellMin.clone() ) );
		}

//		prefetch.forEach( System.out::println );
		return prefetch;
	}

	/**
	 * Callable that reads one pixel from a {@link RandomAccessible}, for
	 * prefetching. In a cached `CellImg`, this will trigger the loading of the
	 * containing `Cell`. Holding on to the pixel (of type 'T') returned by
	 * 'call()' prevents garbage-collection of the cached 'Cell'!
	 */
	public static class PrefetchPixel< T > implements Callable< Object >
	{
		private final RandomAccessible< T > img;

		private final long[] pos;

		PrefetchPixel( final RandomAccessible< T > img, final long[] pos )
		{
			this.pos = pos;
			this.img = img;
		}

		@Override
		public T call()
		{
			return img.getAt( pos );
		}

		@Override
		public String toString()
		{
			return "PrefetchPixel{" +
					"img=" + img +
					", pos=" + Arrays.toString( pos ) +
					'}';
		}
	}

	private static class ImgAndMipmapTransform< T >
	{
		final RandomAccessibleInterval< T > img;

		final AffineTransform3D mipmapTransform;

		private ImgAndMipmapTransform( final RandomAccessibleInterval< T > img, final AffineTransform3D mipmapTransform )
		{
			this.img = img;
			this.mipmapTransform = mipmapTransform;
		}

		/**
		 * Finds the  best resolution level using the same logic as {@code FusionTools.fuseVirtual()}
		 *
		 * TODO: Ideally, we would re-use the same code here. Refactor
		 *       multiview-reconstruction to make that possible.
		 */
		static < T > ImgAndMipmapTransform< T > forBestResolution( BasicSetupImgLoader< T > setupImgLoader, int timepointId, final AffineTransform3D sourceToWorld )
		{
			if ( setupImgLoader instanceof BasicMultiResolutionSetupImgLoader )
			{
				final BasicMultiResolutionSetupImgLoader< T > mrSetupImgLoader = ( BasicMultiResolutionSetupImgLoader< T > ) setupImgLoader;
				final double[][] mipmapResolutions = mrSetupImgLoader.getMipmapResolutions();
				final AffineTransform3D[] mipmapTransforms = mrSetupImgLoader.getMipmapTransforms();

				// Find the  best resolution level, using the same logic as
				// FusionTools.fuseVirtual()
				//

				float acceptedError = 0.02f;

				// assuming that this is the best one
				int bestLevel = 0;
				double bestScaling = 0;

				float[] sizeMaxResolution = null;

				// find the best level

				for ( int level = 0; level < mipmapTransforms.length; ++level )
				{
					final double[] factors = mipmapResolutions[ level ];
					AffineTransform3D levelToWorld = sourceToWorld.copy();
					levelToWorld.concatenate( mipmapTransforms[ level ] );
					final float[] size = getStepSize( levelToWorld );
					if ( level == 0 )
					{
						sizeMaxResolution = size;
						bestScaling = factors[ 0 ] * factors[ 1 ] * factors[ 2 ];
					}
					else
					{
						boolean isValid = true;
						for ( int d = 0; d < 3; ++d )
						{
							if ( !( size[ d ] < 1.0 + acceptedError || Util.isApproxEqual( size[ d ], sizeMaxResolution[ d ], acceptedError ) ) )
							{
								isValid = false;
								break;
							}
						}
						if ( isValid )
						{
							final double totalScale = factors[ 0 ] * factors[ 1 ] * factors[ 2 ];
							if ( totalScale > bestScaling )
							{
								bestScaling = totalScale;
								bestLevel = level;
							}
						}
					}
				}

				return new ImgAndMipmapTransform<>(
						mrSetupImgLoader.getImage( timepointId, bestLevel ),
						mipmapTransforms[ bestLevel ] );
			}
			else
			{
				// if setupImgLoader does not support not multi-resolution, use
				// the full resolution image and an identity mipmap transform
				return new ImgAndMipmapTransform<>(
						setupImgLoader.getImage( timepointId ),
						new AffineTransform3D() );
			}
		}

		// TODO: return double[] instead of float[]. We use float[], because
		//       multiview-reconstruction does, and we need to be compatible.
		private static float[] getStepSize( final AffineTransform3D model )
		{
			final float[] size = new float[ 3 ];
			final double[] tmp = new double[ 3 ];
			for ( int d = 0; d < 3; ++d )
			{
				for ( int i = 0; i < 3; ++i )
					tmp[ i ] = model.get( i, d );
				size[ d ] = ( float ) LinAlgHelpers.length( tmp );
			}
			return size;
		}
	}
}
