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

public abstract class AbstractBasic extends AbstractInfrastructure implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = -4916959775650710928L;

	@Option(names = { "-x", "--xml" }, required = true, description = "Path to the existing BigStitcher project xml, e.g. -x /home/project.xml or -x s3://mybucket/data/dataset.xml or -x file:/home/project.xml")
	protected String xmlURIString = null;

	// will be assigned in loadSpimData2()
	protected URI xmlURI = null;

	public SpimData2 loadSpimData2() throws SpimDataException
	{
		System.out.println( "'" + xmlURIString + "'" );
		System.out.println( "xml: " + (xmlURI = URITools.toURI(xmlURIString)) );
		final SpimData2 dataGlobal = Spark.getSparkJobSpimData2( xmlURI );

		return dataGlobal;
	}

	public SpimData2 loadSpimData2( final int numFetcherThreads ) throws SpimDataException
	{
		final SpimData2 data = loadSpimData2();

		final SequenceDescription sequenceDescription = data.getSequenceDescription();

		// set number of fetcher threads (by default set to 0 for spark)
		final BasicImgLoader imgLoader = sequenceDescription.getImgLoader();
		if (imgLoader instanceof ViewerImgLoader)
			((ViewerImgLoader) imgLoader).setNumFetcherThreads( numFetcherThreads );

		return data;
	}
}
