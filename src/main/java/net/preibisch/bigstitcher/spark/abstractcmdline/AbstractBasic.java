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
package net.preibisch.bigstitcher.spark.abstractcmdline;

import java.io.Serializable;
import java.net.URI;
import java.util.concurrent.Callable;

import bdv.ViewerImgLoader;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.generic.sequence.BasicImgLoader;
import mpicbg.spim.data.sequence.SequenceDescription;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import picocli.CommandLine.Option;
import util.URITools;

/**
 * Base class of the command-line tools that operate on an existing BigStitcher project. It adds the required
 * {@code -x}/{@code --xml} option and loads the project ({@link SpimData2}) from that location, which can be
 * a local path or a URI such as {@code s3://bucket/dataset.xml}. Loading this class also turns off the
 * ImageJ log output of the legacy {@code IOFunctions}.
 */
public abstract class AbstractBasic extends AbstractInfrastructure implements Callable<Void>, Serializable
{
	static { net.preibisch.legacy.io.IOFunctions.printIJLog = false;  }

	private static final long serialVersionUID = -4916959775650710928L;

	@Option(names = { "-x", "--xml" }, required = true, description = "Path to the existing BigStitcher project xml, e.g. -x /home/project.xml or -x s3://mybucket/data/dataset.xml or -x file:/home/project.xml")
	protected String xmlURIString = null;

	protected URI xmlURI = null;

	/**
	 * Parses {@code --xml} into {@link #xmlURI} and loads the BigStitcher project from it, with the image
	 * loader configured for single-threaded Spark tasks (zero fetcher threads).
	 *
	 * @return the loaded project, or {@code null} if loading failed (a one-line error is printed to
	 *         {@code stderr} instead of a stack trace)
	 */
	public SpimData2 loadSpimData2()
	{
		System.out.println( "'" + xmlURIString + "'" );
		System.out.println( "xml: " + (xmlURI = URITools.toURI(xmlURIString)) );
		try
		{
			return Spark.getSparkJobSpimData2( xmlURI );
		}
		catch ( final SpimDataException e )
		{
			// Print a one-line friendly message for common failures (file not found,
			// malformed XML, ...) instead of dumping a full stack trace on the user.
			// Callers already check for null and exit cleanly.
			final Throwable cause = ( e.getCause() != null ) ? e.getCause() : e;
			final String detail = ( cause.getMessage() != null ) ? cause.getMessage() : cause.getClass().getSimpleName();
			System.err.println( "ERROR: failed to load BigStitcher XML '" + xmlURIString + "': " + detail );
			return null;
		}
	}

	/**
	 * Loads the project like {@link #loadSpimData2()} and then sets the number of fetcher threads of the
	 * image loader if it is a {@link ViewerImgLoader}, e.g. for multi-threaded local processing instead
	 * of the Spark default of {@code 0}.
	 *
	 * @param numFetcherThreads number of fetcher threads to use for loading image blocks
	 * @return the loaded project, or {@code null} if loading failed
	 */
	public SpimData2 loadSpimData2( final int numFetcherThreads )
	{
		final SpimData2 data = loadSpimData2();
		if ( data == null )
			return null;

		final SequenceDescription sequenceDescription = data.getSequenceDescription();

		// set number of fetcher threads (by default set to 0 for spark)
		final BasicImgLoader imgLoader = sequenceDescription.getImgLoader();
		if (imgLoader instanceof ViewerImgLoader)
			((ViewerImgLoader) imgLoader).setNumFetcherThreads( numFetcherThreads );

		return data;
	}
}
