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
package net.preibisch.bigstitcher.spark;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.janelia.saalfeldlab.n5.N5FSWriter;

import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsN5;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPointLists;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class ClearInterestPoints extends AbstractBasic
{
	@Option(names = { "--correspondencesOnly" }, description = "clear only corresponding interest points (default: false)")
	private boolean correspondencesOnly = false;

	private static final long serialVersionUID = -7892604354139919145L;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIdsGlobal = Import.getViewIds( dataGlobal );
		Collections.sort( viewIdsGlobal );

		final Map<ViewId, ViewInterestPointLists> ips = dataGlobal.getViewInterestPoints().getViewInterestPoints();

		// load all (shows debug stuff)
		for ( final Entry<ViewId, ViewInterestPointLists> ip : ips.entrySet() )
			for ( final Entry<String, InterestPoints> points : ip.getValue().getHashMap().entrySet() )
			{
				points.getValue().getInterestPointsCopy().size();
				points.getValue().getCorrespondingInterestPointsCopy().size();
			}

		final String file = new File( dataGlobal.getBasePath().getAbsolutePath(), InterestPointsN5.baseN5 ).getAbsolutePath();

		if ( correspondencesOnly)
			System.out.println( "The following correspondences will be removed in ('" + file + "'):");
		else
			System.out.println( "The following interest points and correspondences will be removed in ('" + file + "'):");

		// display all data
		for ( final Entry<ViewId, ViewInterestPointLists> ip : ips.entrySet() )
		{
			System.out.println( Group.pvid( ip.getKey() ) + ":" );

			for ( final Entry<String, InterestPoints> points : ip.getValue().getHashMap().entrySet() )
				System.out.println( "\t'" + points.getKey() + "' " + points.getValue().getInterestPointsCopy().size() + " interest points, " + points.getValue().getCorrespondingInterestPointsCopy().size() + " correspondences." );
		}

		if ( !dryRun )
		{
			if ( correspondencesOnly )
			{
				for ( final Entry<ViewId, ViewInterestPointLists> ip : ips.entrySet() )
				{
					for ( final Entry<String, InterestPoints> points : ip.getValue().getHashMap().entrySet() )
					{
						System.out.println( "Clearing " + Group.pvid( ip.getKey() ) + ", '" + points.getKey() + "' ... " );

						points.getValue().setCorrespondingInterestPoints( new ArrayList<>() );
						points.getValue().saveCorrespondingInterestPoints( true );
					}
				}
			}
			else
			{
				System.out.println( "Saving XML (metadata only) ..." );
	
				dataGlobal.getViewInterestPoints().getViewInterestPoints().clear();
				new XmlIoSpimData2().save( dataGlobal, xmlURI );
	
				System.out.println( "Removing interest point directory '" + file + "' ... " );
	
				final N5FSWriter n5Writer = new N5FSWriter( file );
				n5Writer.remove();
				n5Writer.close();
			}
		}

		System.out.println( "done" );

		return null;
	}

	public static void main(final String... args)
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new ClearInterestPoints()).execute(args));
	}

}
