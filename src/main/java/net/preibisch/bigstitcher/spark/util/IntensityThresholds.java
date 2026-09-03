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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.janelia.saalfeldlab.n5.N5Exception;

import mpicbg.spim.data.sequence.ViewId;
import util.URITools;

/**
 * The per-view intensity thresholds the gains were measured on, stored as
 * "thresholds.txt" ("timepointId setupId threshold" per line) next to the pairwise
 * matches, so the solve and the fusion can pick up what the matching used.
 */
public class IntensityThresholds
{
	private static final String FILENAME = "thresholds.txt";

	public static void write( final URI matchesURI, final Map< ViewId, Double > thresholds ) throws IOException
	{
		final URI fn = matchesURI.resolve( FILENAME );

		try ( final PrintWriter pw = URITools.openFileWriteCloudWriter( URITools.getKeyValueAccess( matchesURI ), fn ) )
		{
			thresholds.entrySet().stream()
					.sorted( Comparator.comparingInt( ( Map.Entry< ViewId, Double > e ) -> e.getKey().getTimePointId() )
							.thenComparingInt( e -> e.getKey().getViewSetupId() ) )
					.forEach( e -> pw.println( e.getKey().getTimePointId() + " " + e.getKey().getViewSetupId() + " " + e.getValue() ) );
		}

		System.out.println( "Wrote " + fn );
	}

	/**
	 * @return the stored thresholds, or null if the matching ran with a fixed threshold
	 *         (i.e. no thresholds.txt was written)
	 */
	public static Map< ViewId, Double > read( final URI matchesURI ) throws IOException
	{
		final URI fn = matchesURI.resolve( FILENAME );

		try (
				final InputStream is = URITools.openFileReadCloudStream( URITools.getKeyValueAccess( matchesURI ), fn );
				final BufferedReader br = new BufferedReader( new InputStreamReader( is ) ) )
		{
			final Map< ViewId, Double > thresholds = new HashMap<>();

			for ( String line = br.readLine(); line != null; line = br.readLine() )
			{
				final String[] tokens = line.trim().split( "\\s+" );

				if ( tokens.length != 3 )
					throw new IOException( "Cannot parse '" + line + "' in " + fn + ", expected 'timepointId setupId threshold'" );

				thresholds.put(
						new ViewId( Integer.parseInt( tokens[ 0 ] ), Integer.parseInt( tokens[ 1 ] ) ),
						Double.parseDouble( tokens[ 2 ] ) );
			}

			return thresholds;
		}
		catch ( final N5Exception.N5NoSuchKeyException e )
		{
			return null;
		}
	}
}
