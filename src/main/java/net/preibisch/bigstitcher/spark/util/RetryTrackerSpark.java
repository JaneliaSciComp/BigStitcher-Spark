package net.preibisch.bigstitcher.spark.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import org.apache.spark.api.java.JavaRDD;

import net.preibisch.legacy.io.IOFunctions;
import net.preibisch.mvrecon.process.export.RetryTracker;

/**
 * {@link RetryTracker} variant for Spark jobs: determines the blocks that failed and must be re-run from
 * the collected results of a {@link JavaRDD} instead of a list of {@code Future}s.
 *
 * @param <T> type of the processed work items, e.g. {@code long[][]} grid blocks
 */
public class RetryTrackerSpark<T> extends RetryTracker<T>
{

	/**
	 * Creates a tracker; see {@link RetryTracker} for the retry semantics. The total number of attempts
	 * is capped at {@code totalBlocks * maxRetries}.
	 *
	 * @param keyExtractor derives a unique string key from a work item, used to match results to blocks
	 * @param operationName descriptive name of the operation for log messages
	 * @param maxRetries how often an individual block may fail before giving up on it
	 * @param giveUpOnFailure if {@code true}, stop everything once one block exceeded {@code maxRetries};
	 *        otherwise drop only that block and continue
	 * @param retryDelayMs pause in milliseconds before each retry cycle
	 * @param triggerGC whether to call {@code System.gc()} before each retry cycle
	 * @param totalBlocks number of blocks that will be processed
	 */
	public RetryTrackerSpark(
			Function<T, String> keyExtractor, String operationName, int maxRetries,
			boolean giveUpOnFailure, long retryDelayMs, boolean triggerGC, int totalBlocks)
	{
		super(keyExtractor, operationName, maxRetries, giveUpOnFailure, retryDelayMs, triggerGC, totalBlocks);
	}

	/**
	 * Convenience factory for {@code long[][]} grid blocks (keyed by their offset {@code block[0]}) with
	 * default settings: 5 retries per block, give up on failure, 2000 ms retry delay, no GC trigger.
	 *
	 * @param operationName descriptive name of the operation for log messages
	 * @param totalBlocks number of blocks that will be processed
	 * @return the tracker
	 */
	public static RetryTrackerSpark<long[][]> forGridBlocks(final String operationName, final int totalBlocks)
	{
		return new RetryTrackerSpark<>(block -> Arrays.toString(block[0]), operationName, 5, true, 2000, false, totalBlocks);
	}

	/**
	 * Method to process all results when running with a service that returns futures. When running with Spark,
	 * this method needs to be adjusted.
	 *
	 * @param rddResults - list of RDD's with results
	 * @param grid - list blocks that were processed
	 * @return set of failed blocks
	 */
	public Set< T > processWithSpark( final JavaRDD<T> rddResults, final Collection< T > grid )
	{
		// we add all blocks to the failedBlocksSet, and remove the ones that succeeded
		final HashMap< String, T > failedBlocksMap = createFailedBlocksMap( grid );

		for ( final T result : rddResults.collect() )
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

		// Convert to Set<long[][]> for RetryTracker
		return new HashSet<>( failedBlocksMap.values() );
	}

}
