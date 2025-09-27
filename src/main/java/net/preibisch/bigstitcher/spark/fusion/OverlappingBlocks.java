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
package net.preibisch.bigstitcher.spark.fusion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import mpicbg.spim.data.SpimData;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Interval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Intervals;
import net.preibisch.bigstitcher.spark.util.ViewUtil;
import net.preibisch.bigstitcher.spark.util.ViewUtil.PrefetchPixel;

/**
 * Determine which input blocks overlap a given interval using {@link
 * OverlappingBlocks#find}. Then prefetch those blocks using {@link
 * OverlappingBlocks#prefetch}.
 */
public class OverlappingBlocks
{
	/**
	 * Determine which of the given {@code viewIds} have blocks that overlap
	 * {@code interval}.
	 *
	 * @param data
	 * 		has all image sizes
	 * @param registrations
	 * 		registrations, maybe updated to reflect anisotropy
	 * @param viewIds
	 * 		which views to check
	 * @param interval
	 * 		the interval that will be processed (in world coordinates)
	 * @return list of views with overlapping blocks, and a prefetcher for those blocks.
	 */
	public static OverlappingBlocks find(
			final SpimData data,
			final HashMap< ViewId, AffineTransform3D > registrations,
			final List<ViewId> viewIds,
			Interval interval )
	{
		final List< ViewId > overlapping = new ArrayList<>();
		final List< Callable< Object > > prefetch = new ArrayList<>();

		// expand to be conservative ...
		final Interval expandedInterval = Intervals.expand( interval, 2 );

		for ( final ViewId viewId : viewIds )
		{
			final Interval bounds = ViewUtil.getTransformedBoundingBox( data, viewId, registrations.get( viewId ) );
			if ( ViewUtil.overlaps( expandedInterval, bounds ) )
			{
				// determine which Cells exactly we need to compute the fused block
				final List< PrefetchPixel< ? > > blocks = ViewUtil.findOverlappingBlocks( data, viewId, interval, registrations.get( viewId ) );
				if ( !blocks.isEmpty() )
				{
					prefetch.addAll( blocks );
					overlapping.add( viewId );
				}
			}
		}

		return new OverlappingBlocks( overlapping, prefetch );
	}

	/**
	 * Get the list of views with overlapping blocks.
	 *
	 * @return list of views with overlapping blocks
	 */
	public List< ViewId > overlappingViews()
	{
		return overlappingViews;
	}

	/**
	 * Prefetch all overlapping blocks.
	 * <p>
	 * The returned {@code AutoCloseable} holds strong reference to all
	 * prefetched blocks (until it is closed), preventing those blocks
	 * from being garbage-collected.
	 *
	 * @param executor blocks are loaded in parallel using this executor
	 *
	 * @return {@code AutoCloseable} that holds strong reference to all prefetched blocks (until it is closed), preventing those blocks from being garbage-collected.
	 */
	public AutoCloseable prefetch( final ExecutorService executor ) throws InterruptedException
	{
		return new Prefetched( executor.invokeAll( prefetchBlocks ) );
	}

	private final List< ViewId > overlappingViews;

	private final List< Callable< Object > > prefetchBlocks;

	private OverlappingBlocks(
			final List< ViewId > overlappingViews,
			final List< Callable< Object > > prefetchBlocks )
	{
		this.overlappingViews = overlappingViews;
		this.prefetchBlocks = prefetchBlocks;
	}

	public int numPrefetchBlocks() { return prefetchBlocks.size(); }

	/**
	 * Result of {@link OverlappingBlocks#prefetch}. Holds strong
	 * references to prefetched data, until it is {@link #close()
	 * closed}.
	 */
	private static class Prefetched implements AutoCloseable
	{
		private final List< Future< Object > > prefetched;

		public Prefetched( final List< Future< Object > > prefetched )
		{
			this.prefetched = prefetched;
		}

		@Override
		public void close() throws Exception
		{
			// let go of references to the prefetched cells
			prefetched.clear();
		}
	}
}
