package net.preibisch.bigstitcher.spark.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import net.preibisch.legacy.io.IOFunctions;
import net.preibisch.mvrecon.process.export.RetryTracker;

public class RetryTrackerSpark<T> extends RetryTracker<T>
{

	public RetryTrackerSpark(
			Function<T, String> keyExtractor, String operationName, int maxRetries,
			boolean giveUpOnFailure, long retryDelayMs, boolean triggerGC, int totalBlocks)
	{
		super(keyExtractor, operationName, maxRetries, giveUpOnFailure, retryDelayMs, triggerGC, totalBlocks);
	}

	/**
	 * Convenience constructor for grid blocks with default settings
	 */
	public static RetryTrackerSpark<long[][]> forGridBlocks(final String operationName, final int totalBlocks)
	{
		return new RetryTrackerSpark<>(block -> Arrays.toString(block[0]), operationName, 5, true, 2000, false, totalBlocks);
	}

	/**
	 * Compare the results produced by a Spark job against the grid of submitted blocks and
	 * return the set of blocks that did not yield a successful result. Callers should pass
	 * already-collected results (e.g. {@code rdd.collect()}) so the underlying RDD is
	 * materialized exactly once and its side effects (block writes) are not re-executed.
	 *
	 * @param results - results returned by the Spark job (one per processed block)
	 * @param grid - blocks that were submitted for processing
	 * @return set of failed blocks
	 */
	public Set< T > processResults( final Collection< T > results, final Collection< T > grid )
	{
		// we add all blocks to the failedBlocksSet, and remove the ones that succeeded
		final HashMap< String, T > failedBlocksMap = createFailedBlocksMap( grid );

		for ( final T result : results )
		{
			try
			{
				if ( result != null )
					failedBlocksMap.remove( keyExtractor().apply( result ) );
			}
			catch ( Exception e )
			{
				IOFunctions.println( "block error s0 (will be re-tried): " + e );
			}
		}

		return new HashSet<>( failedBlocksMap.values() );
	}

}
