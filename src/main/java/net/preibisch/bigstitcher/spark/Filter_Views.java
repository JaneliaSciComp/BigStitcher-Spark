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

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mpicbg.models.AffineModel1D;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewRegistrations;
import mpicbg.spim.data.sequence.MissingViews;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.TimePoint;
import mpicbg.spim.data.sequence.TimePoints;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.ViewSetup;
import net.imglib2.util.Pair;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.intensityadjust.IntensityAdjustments;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPointLists;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.pointspreadfunctions.PointSpreadFunction;
import net.preibisch.mvrecon.fiji.spimdata.pointspreadfunctions.PointSpreadFunctions;
import net.preibisch.mvrecon.fiji.spimdata.stitchingresults.PairwiseStitchingResult;
import net.preibisch.mvrecon.fiji.spimdata.stitchingresults.StitchingResults;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

/**
 * Filter a BigStitcher / SpimData2 XML to keep only specific views.
 *
 * Java port of {@code multiview-reconstruction/scripts/filter_bigstitcher_xml.py}: load the input
 * XML, intersect AND-style attribute filters to determine which view setups to keep, optionally
 * apply a separate timepoint filter, then build a new {@link SpimData2} from the kept components
 * and write it out. Image data and interest-point N5 datasets are not touched (run
 * {@code clear-interestpoints --clearMode FIX_INTERESTPOINTS} afterwards if you want to drop the
 * orphan IP entries / dirs that the filtered XML no longer references).
 */
public class Filter_Views extends AbstractBasic
{
	private static final long serialVersionUID = 1L;

	@Option(names = { "-xo", "--xmlout" }, description = "path to the output BigStitcher xml. "
			+ "Default: input path with .xml replaced by .filtered.xml, or .filtered.xml appended if the input does not end in .xml.")
	private String xmlOutURIString = null;

	@Option(names = "--channels", description = "Channel IDs to keep, comma-separated; ranges with '-' allowed (e.g. '0', '0,2', '0-3'). AND-intersected with other attribute / view filters.")
	private String channelsSpec = null;

	@Option(names = "--tiles", description = "Tile IDs to keep, comma-separated; ranges allowed (e.g. '0-10,15').")
	private String tilesSpec = null;

	@Option(names = "--angles", description = "Angle IDs to keep, comma-separated; ranges allowed.")
	private String anglesSpec = null;

	@Option(names = "--illuminations", description = "Illumination IDs to keep, comma-separated; ranges allowed.")
	private String illuminationsSpec = null;

	@Option(names = "--views", description = "View setup IDs to keep, comma-separated; ranges allowed (e.g. '0,1,5,10', '0-99,200,300-305'). AND-intersected with attribute filters.")
	private String viewsSpec = null;

	@Option(names = "--timepoints", description = "Timepoint IDs to keep, comma-separated; ranges allowed (e.g. '0-5'). Independent dimension; not part of the view-setup intersection.")
	private String timepointsSpec = null;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		final boolean anyFilter =
				channelsSpec != null || tilesSpec != null || anglesSpec != null
				|| illuminationsSpec != null || viewsSpec != null || timepointsSpec != null;
		if ( !anyFilter )
		{
			System.err.println( "ERROR: at least one filter must be specified "
					+ "(--channels, --tiles, --angles, --illuminations, --views, or --timepoints)." );
			return null;
		}

		final SpimData2 dataGlobal = this.loadSpimData2();
		if ( dataGlobal == null )
			return null;

		final URI xmlOutURI = ( xmlOutURIString != null )
				? URITools.toURI( xmlOutURIString )
				: defaultFilteredXmlOut( xmlURI );
		System.out.println( "xmlout: " + xmlOutURI );

		final SequenceDescription oldSeq = dataGlobal.getSequenceDescription();
		final Collection< ? extends ViewSetup > allSetups = oldSeq.getViewSetups().values();

		// Compute keepSetupIds via AND-intersection of all attribute filters and the direct --views filter.
		Set< Integer > keepSetupIds = new HashSet<>();
		for ( final ViewSetup vs : allSetups )
			keepSetupIds.add( vs.getId() );

		keepSetupIds = intersectByAttribute( keepSetupIds, allSetups, "channel",      channelsSpec );
		keepSetupIds = intersectByAttribute( keepSetupIds, allSetups, "tile",         tilesSpec );
		keepSetupIds = intersectByAttribute( keepSetupIds, allSetups, "angle",        anglesSpec );
		keepSetupIds = intersectByAttribute( keepSetupIds, allSetups, "illumination", illuminationsSpec );

		final HashSet< Integer > viewsFilter = Import.parseIdRangeSet( viewsSpec );
		if ( viewsFilter != null )
		{
			keepSetupIds.retainAll( viewsFilter );
			System.out.println( "  views filter: " + keepSetupIds.size() + " after intersection" );
		}

		System.out.println( "Keeping " + keepSetupIds.size() + " view setup(s)" );

		final HashSet< Integer > keepTimepointIds = Import.parseIdRangeSet( timepointsSpec );
		if ( keepTimepointIds != null )
			System.out.println( "  timepoints filter: keeping " + keepTimepointIds.size() + " timepoint(s)" );

		if ( keepSetupIds.isEmpty() )
		{
			System.err.println( "ERROR: no matching view setups; nothing to write." );
			return null;
		}

		final SpimData2 filtered = buildFiltered( dataGlobal, keepSetupIds, keepTimepointIds );

		if ( !dryRun )
		{
			new XmlIoSpimData2().save( filtered, xmlOutURI );
			System.out.println( "Saved: " + xmlOutURI );
		}
		else
		{
			System.out.println( "Dry run: skipped writing XML." );
		}

		return null;
	}

	/** Intersect {@code current} with the IDs of view setups whose attribute matches the spec. */
	private static Set< Integer > intersectByAttribute(
			final Set< Integer > current,
			final Collection< ? extends ViewSetup > allSetups,
			final String attrName,
			final String spec )
	{
		final HashSet< Integer > filterIds = Import.parseIdRangeSet( spec );
		if ( filterIds == null )
			return current; // no filter on this attribute

		final HashSet< Integer > matching = new HashSet<>();
		for ( final ViewSetup vs : allSetups )
		{
			final int id = attributeId( vs, attrName );
			if ( id >= 0 && filterIds.contains( id ) )
				matching.add( vs.getId() );
		}

		final HashSet< Integer > intersected = new HashSet<>( current );
		intersected.retainAll( matching );
		System.out.println( "  " + attrName + " filter " + filterIds.size() + " value(s): "
				+ matching.size() + " matching, " + intersected.size() + " after intersection" );
		return intersected;
	}

	private static int attributeId( final ViewSetup vs, final String attrName )
	{
		switch ( attrName )
		{
			case "channel":      return ( vs.getChannel()      != null ) ? vs.getChannel().getId()      : -1;
			case "tile":         return ( vs.getTile()         != null ) ? vs.getTile().getId()         : -1;
			case "angle":        return ( vs.getAngle()        != null ) ? vs.getAngle().getId()        : -1;
			case "illumination": return ( vs.getIllumination() != null ) ? vs.getIllumination().getId() : -1;
			default: throw new IllegalArgumentException( "Unknown attribute '" + attrName + "'" );
		}
	}

	/** Same predicate the Python script uses (handles null timepoint filter as "keep all"). */
	private static boolean keepView(
			final ViewId viewId,
			final Set< Integer > keepSetupIds,
			final Set< Integer > keepTimepointIds )
	{
		if ( !keepSetupIds.contains( viewId.getViewSetupId() ) )
			return false;
		if ( keepTimepointIds != null && !keepTimepointIds.contains( viewId.getTimePointId() ) )
			return false;
		return true;
	}

	/**
	 * Construct a new SpimData2 from the kept components. Reuses the source dataset's ImgLoader and
	 * basePathURI so relative paths to image data continue to resolve.
	 */
	private static SpimData2 buildFiltered(
			final SpimData2 src,
			final Set< Integer > keepSetupIds,
			final Set< Integer > keepTimepointIds )
	{
		final SequenceDescription oldSeq = src.getSequenceDescription();

		// ViewSetups
		final ArrayList< ViewSetup > newSetups = new ArrayList<>();
		for ( final ViewSetup vs : oldSeq.getViewSetups().values() )
			if ( keepSetupIds.contains( vs.getId() ) )
				newSetups.add( vs );

		// TimePoints
		final TimePoints newTimepoints;
		if ( keepTimepointIds == null )
		{
			newTimepoints = oldSeq.getTimePoints();
		}
		else
		{
			final ArrayList< TimePoint > kept = new ArrayList<>();
			for ( final TimePoint tp : oldSeq.getTimePoints().getTimePointsOrdered() )
				if ( keepTimepointIds.contains( tp.getId() ) )
					kept.add( tp );
			newTimepoints = new TimePoints( kept );
		}

		// MissingViews
		final HashSet< ViewId > newMissing = new HashSet<>();
		final MissingViews oldMissing = oldSeq.getMissingViews();
		if ( oldMissing != null && oldMissing.getMissingViews() != null )
			for ( final ViewId mv : oldMissing.getMissingViews() )
				if ( keepView( mv, keepSetupIds, keepTimepointIds ) )
					newMissing.add( mv );

		// ViewRegistrations
		final HashMap< ViewId, ViewRegistration > newViewRegs = new HashMap<>();
		for ( final Entry< ViewId, ViewRegistration > e : src.getViewRegistrations().getViewRegistrations().entrySet() )
			if ( keepView( e.getKey(), keepSetupIds, keepTimepointIds ) )
				newViewRegs.put( e.getKey(), e.getValue() );

		// ViewInterestPoints
		final HashMap< ViewId, ViewInterestPointLists > newViewIPs = new HashMap<>();
		for ( final Entry< ViewId, ViewInterestPointLists > e : src.getViewInterestPoints().getViewInterestPoints().entrySet() )
			if ( keepView( e.getKey(), keepSetupIds, keepTimepointIds ) )
				newViewIPs.put( e.getKey(), e.getValue() );

		// PointSpreadFunctions
		final HashMap< ViewId, PointSpreadFunction > newPsfs = new HashMap<>();
		final PointSpreadFunctions oldPsfs = src.getPointSpreadFunctions();
		if ( oldPsfs != null )
			for ( final Entry< ViewId, PointSpreadFunction > e : oldPsfs.getPointSpreadFunctions().entrySet() )
				if ( keepView( e.getKey(), keepSetupIds, keepTimepointIds ) )
					newPsfs.put( e.getKey(), e.getValue() );

		// StitchingResults — keep only pairs where every ViewId in BOTH groups passes.
		final StitchingResults newStitching = new StitchingResults();
		final StitchingResults oldStitching = src.getStitchingResults();
		if ( oldStitching != null )
		{
			for ( final Entry< Pair< Group< ViewId >, Group< ViewId > >, PairwiseStitchingResult< ViewId > > e :
					oldStitching.getPairwiseResults().entrySet() )
			{
				if ( allInKeep( e.getKey().getA(), keepSetupIds, keepTimepointIds )
						&& allInKeep( e.getKey().getB(), keepSetupIds, keepTimepointIds ) )
					newStitching.setPairwiseResultForPair( e.getKey(), e.getValue() );
			}
		}

		// IntensityAdjustments
		final HashMap< ViewId, AffineModel1D > newIntensities = new HashMap<>();
		final IntensityAdjustments oldIntensities = src.getIntensityAdjustments();
		if ( oldIntensities != null )
			for ( final Entry< ViewId, AffineModel1D > e : oldIntensities.getIntensityAdjustments().entrySet() )
				if ( keepView( e.getKey(), keepSetupIds, keepTimepointIds ) )
					newIntensities.put( e.getKey(), e.getValue() );

		final SequenceDescription newSeq = new SequenceDescription(
				newTimepoints, newSetups, null, new MissingViews( newMissing ) );
		newSeq.setImgLoader( oldSeq.getImgLoader() );

		return new SpimData2(
				src.getBasePathURI(),
				newSeq,
				new ViewRegistrations( newViewRegs ),
				new ViewInterestPoints( newViewIPs ),
				src.getBoundingBoxes(),
				new PointSpreadFunctions( newPsfs ),
				newStitching,
				new IntensityAdjustments( newIntensities ) );
	}

	private static boolean allInKeep(
			final Group< ViewId > group,
			final Set< Integer > keepSetupIds,
			final Set< Integer > keepTimepointIds )
	{
		for ( final ViewId v : group.getViews() )
			if ( !keepView( v, keepSetupIds, keepTimepointIds ) )
				return false;
		return true;
	}

	/**
	 * Derive the default output XML path: replace a trailing ".xml" (case-insensitive) with
	 * ".filtered.xml", or append ".filtered.xml" if the input URI doesn't end in ".xml".
	 * Mirrors {@code SplitDatasets.defaultSplitXmlOut}.
	 */
	private static URI defaultFilteredXmlOut( final URI xmlURI )
	{
		final String in = xmlURI.toString();
		final String out = in.toLowerCase().endsWith( ".xml" )
				? in.substring( 0, in.length() - ".xml".length() ) + ".filtered.xml"
				: in + ".filtered.xml";
		return URITools.toURI( out );
	}

	public static void main( final String... args )
	{
		System.out.println( Arrays.toString( args ) );
		System.exit( new CommandLine( new Filter_Views() ).execute( args ) );
	}
}
