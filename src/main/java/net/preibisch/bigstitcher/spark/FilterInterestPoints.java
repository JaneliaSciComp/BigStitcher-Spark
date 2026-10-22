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

import java.util.ArrayList;
import java.util.List;

import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.mvrecon.fiji.plugin.Filter_Detections;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointFilter;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointFilter.Axis;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointFilter.Limit;
import net.preibisch.mvrecon.process.interestpointdetection.InterestPointTools;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * Creates filtered labels of existing interest points without detecting again, as "Filter/Manage Interest Points" of the
 * BigStitcher GUI does (Filter_Detections, Limit_Detections, the interactive filter of the Interest Point Explorer): a
 * filtered label has no points of its own, it filters its base label on the fly by ranges of |DoG response|, image
 * intensity and sigma and/or keeps a fixed number of points per view, has its own correspondences and follows a
 * re-detection of its base; {@code --copy} stores the points that pass as a plain label instead. Several entries per
 * range (separated by ',') give one label per entry, like the dialog, named base label plus suffix (e.g. beads_r0.008).
 * Driver-only, no Spark job: the XML is edited and the markers of the filtered labels are written to interestpoints.n5.
 *
 * @author Stephan Preibisch
 */
public class FilterInterestPoints extends AbstractSelectableViews
{
	private static final long serialVersionUID = 1L;

	@Option(names = { "-l", "--label" }, required = true, description = "the interest points to filter (the base label), e.g. beads")
	protected String label = null;

	@Option(names = { "--newLabel" }, description = "the name of the filtered label (default: base label + suffix, e.g. beads_r0.008; only with a single filter)")
	protected String newLabel = null;

	@Option(names = { "--response" }, description = "keep |DoG response| (the image normalized to [0,1]) in a range: 'a' keeps >= a, 'a-b' or 'a..b' keeps a..b; several entries separated by ',' give one label each, e.g. '0.008,0.016,0.032'")
	protected String response = null;

	@Option(names = { "--intensity" }, description = "keep the image intensity at the detection (raw units, at the resolution of the detection) in a range, same grammar as --response")
	protected String intensity = null;

	@Option(names = { "--sigma" }, description = "keep the sigma (px at full resolution; varies for --method SCALE_SPACE detections) in a range, same grammar as --response")
	protected String sigma = null;

	@Option(names = { "--keepPoints" }, description = "keep a fixed number of points per view (after the ranges), e.g. 3000")
	protected Integer keepPoints = null;

	@Option(names = { "--keepBy" }, description = "the attribute --keepPoints ranks by: response, intensity or sigma (default: response)")
	protected String keepBy = Axis.RESPONSE.key;

	@Option(names = { "--keepMode" }, description = "which points --keepPoints keeps: highest, median (around the median) or lowest (default: highest)")
	protected String keepMode = Limit.HIGHEST.key;

	@Option(names = { "--copy" }, description = "store the points that pass as a plain label (a copy of the points) instead of a filtered label that follows its base (default: false)")
	protected boolean copy = false;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIds = this.loadViewIds( dataGlobal );

		if ( viewIds == null || viewIds.size() == 0 )
			return null;

		final List< InterestPointFilter > filters;

		try
		{
			filters = filters( response, intensity, sigma, keepPoints, keepBy, keepMode );
		}
		catch ( final IllegalArgumentException e )
		{
			System.err.println( "ERROR: " + e.getMessage() );
			return null;
		}

		if ( filters.isEmpty() )
		{
			System.err.println( "ERROR: nothing to filter by, set --response, --intensity, --sigma and/or --keepPoints." );
			return null;
		}

		if ( newLabel != null && filters.size() > 1 )
		{
			System.err.println( "ERROR: --newLabel names one label, but " + filters.size() + " labels would be created (one per entry); leave it out to name them base label + suffix." );
			return null;
		}

		// the base label must be there, with the attributes the filters need (a label stored before the attributes existed has none)
		final List< ViewId > views = InterestPointTools.viewsWithLabel( dataGlobal, label, viewIds );

		if ( views.isEmpty() )
		{
			System.err.println( "ERROR: none of the " + viewIds.size() + " selected view(s) has interest points '" + label + "'." );
			return null;
		}

		if ( views.size() < viewIds.size() )
			System.out.println( "NOTE: " + ( viewIds.size() - views.size() ) + " of the selected views have no interest points '" + label + "' and are skipped." );

		if ( !Filter_Detections.checkAttributes( dataGlobal, views, label, filters ) )
			return null; // logged

		final ArrayList< String > names = new ArrayList<>();

		for ( final InterestPointFilter filter : filters )
		{
			final String name = newLabel != null ? newLabel : label + filter.suffix();

			if ( !Filter_Detections.checkNewLabel( dataGlobal, views, label, name, copy ) )
				return null; // logged

			names.add( name );
			System.out.println( ( copy ? "Label '" : "Filtered label '" ) + name + "' in " + views.size() + " view(s): " + filter.describe( label ) );
		}

		if ( dryRun )
		{
			System.out.println( "dryRun: nothing is written." );
			return null;
		}

		for ( int i = 0; i < filters.size(); ++i )
			Filter_Detections.create( dataGlobal, views, label, names.get( i ), filters.get( i ), copy );

		// the counts per label (a filtered label counts its base on the fly)
		for ( final String name : names )
		{
			long total = 0;

			for ( final ViewId viewId : views )
				total += dataGlobal.getViewInterestPoints().getViewInterestPointLists( viewId ).getInterestPointList( name ).getInterestPointsCopy().size();

			System.out.println( "'" + name + "': " + total + " points in " + views.size() + " view(s)" );
		}

		System.out.println( "Saving XML and interest points ..." );

		new XmlIoSpimData2().save( dataGlobal, xmlURI );

		System.out.println( "Done ..." );

		return null;
	}

	/**
	 * The filters of the options: one per combination of the range entries (Filter_Detections.filtersFor, as the dialog),
	 * each with the count limit if one is given; a count limit alone is one filter
	 *
	 * @throws IllegalArgumentException if an entry, the attribute or the mode of the limit is malformed
	 */
	public static List< InterestPointFilter > filters( final String response, final String intensity, final String sigma, final Integer keepPoints, final String keepBy, final String keepMode )
	{
		final List< InterestPointFilter > ranges = Filter_Detections.filtersFor( dialogEntries( response, Axis.RESPONSE ), dialogEntries( intensity, Axis.INTENSITY ), dialogEntries( sigma, Axis.SIGMA ) );

		if ( ranges == null )
			throw new IllegalArgumentException( "malformed range entry, see the log." );

		if ( keepPoints == null )
			return ranges;

		if ( keepPoints < 1 )
			throw new IllegalArgumentException( "--keepPoints must be >= 1, but is " + keepPoints + "." );

		final Axis axis = Axis.forKey( keepBy.trim().toLowerCase() );
		final Limit limit = Limit.forKey( keepMode.trim().toLowerCase() );

		final ArrayList< InterestPointFilter > filters = new ArrayList<>();

		if ( ranges.isEmpty() )
			filters.add( InterestPointFilter.of( axis, limit, keepPoints ) );
		else
			for ( final InterestPointFilter range : ranges )
				filters.add( range.withLimit( axis, limit, keepPoints ) );

		return filters;
	}

	/**
	 * The entries of a range option in the grammar of the dialog fields ("a" or "a-b"): the canonical "a..b" of the action
	 * history is accepted too (InterestPointFilter.parseAnyRange), an open lower bound becomes 0 as in the dialog
	 *
	 * @return the field for Filter_Detections.filtersFor, "" for none
	 */
	protected static String dialogEntries( final String option, final Axis axis )
	{
		if ( option == null || option.trim().isEmpty() )
			return "";

		final StringBuilder field = new StringBuilder();

		for ( final String entry : option.split( "[,;]" ) )
		{
			if ( entry.trim().isEmpty() )
				continue;

			final double[] range;

			try
			{
				range = InterestPointFilter.parseAnyRange( entry );
			}
			catch ( final IllegalArgumentException e )
			{
				throw new IllegalArgumentException( "cannot parse the " + axis.description + " entry '" + entry.trim() + "': " + e.getMessage() );
			}

			if ( field.length() > 0 )
				field.append( "," );

			field.append( InterestPointFilter.plain( Double.isNaN( range[ 0 ] ) ? 0 : range[ 0 ] ) );

			if ( !Double.isNaN( range[ 1 ] ) )
				field.append( "-" ).append( InterestPointFilter.plain( range[ 1 ] ) );
		}

		return field.toString();
	}

	public static void main( final String... args )
	{
		System.out.println( java.util.Arrays.toString( args ) );
		System.exit( new CommandLine( new FilterInterestPoints() ).execute( args ) );
	}
}
