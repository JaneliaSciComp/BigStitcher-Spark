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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import mpicbg.spim.data.SpimData;
import mpicbg.spim.data.generic.sequence.BasicMultiResolutionSetupImgLoader;
import mpicbg.spim.data.generic.sequence.BasicSetupImgLoader;
import mpicbg.spim.data.registration.ViewRegistration;
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
import net.imglib2.transform.integer.BoundingBox;
import net.imglib2.transform.integer.MixedTransform;
import net.imglib2.util.Intervals;
import net.imglib2.util.LinAlgHelpers;
import net.imglib2.util.Util;
import net.imglib2.view.IntervalView;
import net.imglib2.view.MixedTransformView;
import util.Grid;

public class ViewUtil
{
	// code from: https://stackoverflow.com/questions/852665/command-line-progress-bar-in-java
	public static void progressPercentage(int remain, int total) {
		if (remain > total) {
			throw new IllegalArgumentException();
		}
		int maxBareSize = 10; // 10unit for 100%
		int remainProcent = ((100 * remain) / total) / maxBareSize;
		char defaultChar = '-';
		String icon = "*";
		String bare = new String(new char[maxBareSize]).replace('\0', defaultChar) + "]";
		StringBuilder bareDone = new StringBuilder();
		bareDone.append("[");
		for (int i = 0; i < remainProcent; i++) {
			bareDone.append(icon);
		}
		String bareRemain = bare.substring(remainProcent, bare.length());
		System.out.print("\r" + bareDone + bareRemain + " " + remainProcent * 10 + "%");
		if (remain == total) {
			System.out.print("\n");
		}
	}

	public static void main(String[] args) {
		for (int i = 0; i <= 200; i = i + 20) {
			progressPercentage(i, 200);
			try {
				Thread.sleep(500);
			} catch (Exception e) {
			}
		}
	}

	public static long size( final Interval interval )
	{
		if ( interval == null || interval.numDimensions() == 0 )
			return 0;

		long size = interval.dimension( 0 );

		for ( int d = 1; d < interval.numDimensions(); ++d )
			size *= interval.dimension( d );

		return size;
	}

	public static boolean overlaps( final Interval interval1, final Interval interval2 )
	{
		return !Intervals.isEmpty( Intervals.intersect( interval1, interval2 ) );
	}

	public static Dimensions getDimensions(final SpimData data, final ViewId viewId ) throws IllegalArgumentException
	{
		return data.getSequenceDescription().getViewDescription( viewId ).getViewSetup().getSize();
		/*
		final ImgLoader imgLoader = data.getSequenceDescription().getImgLoader();
		final SetupImgLoader<?> setupImgLoader = imgLoader.getSetupImgLoader(viewId.getViewSetupId());
		if (setupImgLoader == null) {
			throw new IllegalArgumentException(
					"failed to find setupImgLoader for " + viewIdToString(viewId) + " in " + data);
		}
		return setupImgLoader.getImageSize(viewId.getTimePointId() );
		*/
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
	/*
	public static Interval getTransformedBoundingBox( final SpimData data, final ViewId viewId ) throws IllegalArgumentException
	{
		final Dimensions dim = getDimensions( data, viewId );
		final ViewRegistration reg = getViewRegistration( data, viewId );

		return Intervals.smallestContainingInterval( reg.getModel().estimateBounds( new FinalInterval( dim ) ) );
	}
	*/

	/**
	 * Get the estimated bounding box of the specified view in world coordinates.
	 * This transforms the image dimension for {@code viewId} with the {@code
	 * ViewRegistration} for {@code viewId}, and takes the bounding box.
	 */
	public static Interval getTransformedBoundingBox( final SpimData data, final ViewId viewId, final AffineTransform3D t ) throws IllegalArgumentException
	{
		final Dimensions dim = getDimensions( data, viewId );

		return Intervals.smallestContainingInterval( t.estimateBounds( new FinalInterval( dim ) ) );
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
	 * @param model
	 * 		the transformation model applied to the View during fusion
	 * @return a list of {@code PrefetchPixel} callables that will each prefetch one cell.
	 */
	public static List< PrefetchPixel< ? > > findOverlappingBlocks(
			final SpimData data,
			final ViewId viewId,
			final Interval fusedBlock,
			final AffineTransform3D model )
	{
		return findOverlappingBlocks(data, viewId, fusedBlock, model, 1 );
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
	 * @param model
	 * 		the transformation model applied to the View during fusion
	 * @param fusedBlock
	 * 		the interval that will be processed (in world coordinates)
	 * @param expand
	 * 		how much to expand each block (to avoid rounding errors?)
	 *
	 * @return a list of {@code PrefetchPixel} callables that will each prefetch one cell.
	 */
	public static List< PrefetchPixel< ? > > findOverlappingBlocks(
			final SpimData data,
			final ViewId viewId,
			final Interval fusedBlock,
			final AffineTransform3D model,
			final int expand )
	{
		final List< PrefetchPixel< ? > > prefetch = new ArrayList<>();

		final ImgLoader imgLoader = data.getSequenceDescription().getImgLoader();
		final SetupImgLoader< ? > setupImgLoader = imgLoader.getSetupImgLoader( viewId.getViewSetupId() );
		if ( setupImgLoader == null )
		{
			throw new IllegalArgumentException(
					"failed to find setupImgLoader for " + viewIdToString( viewId ) + " in " + data );
		}

		//final ViewRegistrations registrations = data.getViewRegistrations();
		//final AffineTransform3D model = registrations.getViewRegistration( viewId ).getModel();

		final ImgAndMipmapTransform< ? > best = ImgAndMipmapTransform.forBestResolution( setupImgLoader, viewId.getTimePointId(), model );
		final RandomAccessibleInterval< ? > img = best.img;
		final AffineTransform3D imgToWorld = model.copy();

		imgToWorld.concatenate( best.mipmapTransform );

		RandomAccessible< ? > rai = best.img;

		// strip one level of IntervalView, if present
		if ( rai instanceof IntervalView )
		{
			rai = ( ( IntervalView< ? > ) rai ).getSource();
		}

		final MixedTransform transformToSource;
		if ( rai instanceof MixedTransformView )
		{
			transformToSource = ( ( MixedTransformView< ? > ) rai ).getTransformToSource();
			rai = ( ( MixedTransformView< ? > ) rai ).getSource();
		}
		else
		{
			transformToSource = null;
		}

		// this failed for the AllenOMEZarrLoader because the RandomAccessibleInterval< ? > img is .view().slice( 4, 0 ).slice( 3, 0 );
		// TODO: I added some copy of the code below that assumes a 128x128x64 blocksize, this should be fixed. But better this than a non-working code ...
		if ( ! ( rai instanceof AbstractCellImg ) )
		{
			// use some random blocksize and fetch for all
			final List<long[][]> grid = Grid.create( new FinalInterval( img ).dimensionsAsLongArray(), new int[] { 128, 128, 64 } );

			final int n = img.numDimensions();
			final BoundingBox cellBBox = new BoundingBox( n );
			final long[] cellMin = cellBBox.corner1;
			final long[] cellMax = cellBBox.corner2;

			final BoundingBox projectedCellBBox;
			final Interval projectedCellInterval;
			final int m = img.numDimensions(); // should be always ==3
			projectedCellBBox = new BoundingBox( m );
			projectedCellInterval = FinalInterval.wrap( projectedCellBBox.corner1, projectedCellBBox.corner2 );

			for ( final long[][] block : grid )
			{
				final long[] offset = block[ 0 ];
				final long[] size = block[ 1 ];

				for ( int d = 0; d < n; ++d )
				{
					cellMin[ d ] = offset[ d ];
					cellMax[ d ] = cellMin[ d ] + size[ d ] - 1;
				}

				if ( transformToSource == null )
				{
					expand( cellBBox, expand, projectedCellBBox );
				}
				else
				{
					transform( transformToSource, projectedCellBBox, cellBBox );
					expand( projectedCellBBox, expand );
				}

				final Interval bounds = Intervals.smallestContainingInterval(
						imgToWorld.estimateBounds( projectedCellInterval ) );

				if ( overlaps( bounds, fusedBlock ) )
				{
					System.out.println( Arrays.toString( offset ) + ", " + Arrays.toString( size ) );

					prefetch.add( new PrefetchPixel<>( rai, cellMin.clone() ) );
				}
			}

			//throw new IllegalArgumentException( "TODO. Handling source types other than CellImg is not implemented yet, rai is=" + rai.getClass().getName() );
		}
		else
		{
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
			//
			// TODO: The following works for the hyperslice views currently produced by ZarrImageLoader
			//       (from https://github.com/bigdataviewer/bigdataviewer-omezarr)
			//       However, for the general case, the logic should be inverted:
			//       Project the "fused" bounding box into source coordinates (see
			//       above), because that is well-defined.
			//       In contrast, the code below performs a projection onto the
			//       "fused" hyper-slice, which can lead to non-required blocks
			//       being loaded.
	
			// iterate all cells (intervals) in grid
			final CellGrid grid = ( ( AbstractCellImg< ?, ?, ?, ? > ) rai ).getCellGrid();
	
			final int n = grid.numDimensions();
			final long[] gridPos = new long[ n ];
			final BoundingBox cellBBox = new BoundingBox( n );
			final long[] cellMin = cellBBox.corner1;
			final long[] cellMax = cellBBox.corner2;
	
			final BoundingBox projectedCellBBox;
			final Interval projectedCellInterval;
			final int m = img.numDimensions(); // should be always ==3
			projectedCellBBox = new BoundingBox( m );
			projectedCellInterval = FinalInterval.wrap( projectedCellBBox.corner1, projectedCellBBox.corner2 );
	
			final IntervalIterator gridIter = new LocalizingIntervalIterator( grid.getGridDimensions() );
			while( gridIter.hasNext() )
			{
				gridIter.fwd();
				gridIter.localize( gridPos );
				grid.getCellInterval( gridPos, cellMin, cellMax );
	
				if ( transformToSource == null )
				{
					expand( cellBBox, expand, projectedCellBBox );
				}
				else
				{
					transform( transformToSource, projectedCellBBox, cellBBox );
					expand( projectedCellBBox, expand );
				}
	
				final Interval bounds = Intervals.smallestContainingInterval(
						imgToWorld.estimateBounds( projectedCellInterval ) );
	
				if ( overlaps( bounds, fusedBlock ) )
					prefetch.add( new PrefetchPixel<>( rai, cellMin.clone() ) );
			}
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

	/**
	 * Grow {@code BoundingBox} by {@code border} pixels on every side.
	 */
	private static void expand( final BoundingBox bbox, final long border )
	{
		expand( bbox, border, bbox );
	}

	/**
	 * Grow {@code BoundingBox} by {@code border} pixels on every side.
	 */
	private static void expand( final BoundingBox bbox, final long border, final BoundingBox expanded )
	{
		final int n = bbox.numDimensions();
		for ( int d = 0; d < n; ++d )
		{
			expanded.corner1[ d ] = bbox.corner1[ d ] - border;
			expanded.corner2[ d ] = bbox.corner2[ d ] + border;
		}
	}

	/**
	 * Reverse-apply {@code transform} to a target bounding box to obtain a
	 * source bounding box.
	 * <p>
	 * Note that {@code transform} might not be invertible. For example. if
	 * source is a hyper-slice of target, some dimensions of the target vector
	 * are ignored.
	 *
	 * @param transform
	 * 		the transform from target to source.
	 * @param sourceBoundingBox
	 * 		the source bounding box. <em>This is the output and is modified.</em>
	 * @param targetBoundingBox
	 * 		the target bounding box. <em>This is the input and is not modified.</em>
	 */
	private static void transform( final MixedTransform transform, final BoundingBox sourceBoundingBox, final BoundingBox targetBoundingBox )
	{
		assert sourceBoundingBox.numDimensions() == transform.numSourceDimensions();
		assert targetBoundingBox.numDimensions() == transform.numTargetDimensions();
		apply( transform, sourceBoundingBox.corner1, targetBoundingBox.corner1 );
		apply( transform, sourceBoundingBox.corner2, targetBoundingBox.corner2 );
		sourceBoundingBox.orderMinMax();
	}

	/**
	 * Reverse-apply {@code transform} to a target vector to obtain a source vector.
	 * <p>
	 * Note that {@code transform} might not be invertible. For example. if
	 * source is a hyper-slice of target, some dimensions of the target vector
	 * are ignored.
	 *
	 * @param transform
	 * 		transform from source to target.
	 * @param source
	 * 		set this to the source coordinates. <em>This is the output and is modified.</em>
	 * @param target
	 * 		target coordinates. <em>This is the input and is not modified.</em>
	 */
	private static void apply( MixedTransform transform, long[] source, long[] target )
	{
		assert source.length >= transform.numSourceDimensions();
		assert target.length >= transform.numTargetDimensions();
		for ( int d = 0; d < transform.numTargetDimensions(); ++d )
		{
			if ( !transform.getComponentZero( d ) )
			{
				long v = target[ d ] - transform.getTranslation( d );
				source[ transform.getComponentMapping( d ) ] = transform.getComponentInversion( d ) ? -v : v;
			}
		}
	}
}
