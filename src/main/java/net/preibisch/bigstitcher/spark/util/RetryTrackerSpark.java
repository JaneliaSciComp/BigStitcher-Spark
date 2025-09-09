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
