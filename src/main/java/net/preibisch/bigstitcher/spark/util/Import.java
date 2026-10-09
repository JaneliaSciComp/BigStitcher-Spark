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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import mpicbg.spim.data.SpimData;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.SparkFusion.DataTypeFusion;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBox;
import net.preibisch.mvrecon.process.boundingbox.BoundingBoxTools;

/**
 * Static helpers for turning command-line arguments of the BigStitcher-Spark tools into dataset objects:
 * selecting {@link ViewId}s by explicit id or by angle/channel/illumination/tile/timepoint ids, looking up
 * bounding boxes, and parsing comma-separated numbers, id lists and downsampling specifications.
 */
public class Import {

	/**
	 * Resolves the bounding box to process: the one titled {@code boundingBoxName} as stored in the dataset XML,
	 * or, if {@code boundingBoxName} is {@code null}, the maximal bounding box around {@code viewIds} (titled
	 * {@code "All Views"}).
	 *
	 * @param data the dataset
	 * @param viewIds the views whose extent defines the maximal bounding box when no name is given
	 * @param boundingBoxName title of a bounding box stored in the XML, or {@code null} for the maximal one
	 * @return the selected bounding box
	 * @throws IllegalArgumentException if a name is given but no bounding box with that title exists in the XML
	 */
	public static BoundingBox getBoundingBox(
			final SpimData2 data,
			final List< ViewId > viewIds,
			final String boundingBoxName )
			throws IllegalArgumentException
	{
		BoundingBox bb = null;

		if ( boundingBoxName == null )
		{
			bb = BoundingBoxTools.maximalBoundingBox( data, viewIds, "All Views" );
		}
		else
		{
			final List<BoundingBox> boxes = BoundingBoxTools.getAllBoundingBoxes( data, null, false );

			for ( final BoundingBox box : boxes )
				if ( box.getTitle().equals( boundingBoxName ) )
					bb = box;

			if ( bb == null )
			{
				throw new IllegalArgumentException( "Bounding box '" + boundingBoxName + "' not present in XML." );
			}
		}

		return bb;
	}

	/**
	 * Checks that an intensity range was given when fusing to an integer output type.
	 *
	 * @param datatype the output data type
	 * @param minIntensity the minimum input intensity to map to the output range, may be {@code null}
	 * @param maxIntensity the maximum input intensity to map to the output range, may be {@code null}
	 * @throws IllegalArgumentException if {@code datatype} is {@code UINT8} or {@code UINT16} and either intensity
	 *         is {@code null}
	 */
	public static void validateInputParameters(
			final DataTypeFusion datatype,
			final Double minIntensity,
			final Double maxIntensity )
			throws IllegalArgumentException
	{
		if ( ( datatype == DataTypeFusion.UINT8 || datatype == DataTypeFusion.UINT16 ) && (minIntensity == null || maxIntensity == null ) ) {
			throw new IllegalArgumentException( "When selecting UINT8 or UINT16 you need to specify minIntensity and maxIntensity." );
		}
	}

	/**
	 * Checks that views are selected either by explicit view ids or by attribute ids, but not both.
	 *
	 * @param vi explicit view ids ({@code -vi}), or {@code null}
	 * @param angleIds selected angle ids, or {@code null}
	 * @param channelIds selected channel ids, or {@code null}
	 * @param illuminationIds selected illumination ids, or {@code null}
	 * @param tileIds selected tile ids, or {@code null}
	 * @param timepointIds selected timepoint ids, or {@code null}
	 * @throws IllegalArgumentException if {@code vi} is given together with any of the attribute id lists
	 */
	public static void validateInputParameters(
			final String[] vi,
			final String angleIds, 
			final String channelIds,
			final String illuminationIds,
			final String tileIds,
			final String timepointIds )
			throws IllegalArgumentException
	{
		if ( vi != null &&
			 ( angleIds != null || tileIds != null || illuminationIds != null || timepointIds != null || channelIds != null ) ) {
			throw new IllegalArgumentException( "You can only specify ViewIds (-vi) OR angles, channels, illuminations, tiles, timepoints." );
		}
	}

	/**
	 * Selects the views to process from the command-line arguments: the explicitly listed view ids if {@code vi}
	 * is given, otherwise the views matching all given attribute id lists ({@code null} or empty lists match
	 * everything), otherwise all views. Views missing from the dataset are always filtered out. Progress and the
	 * number of requested view ids that are actually present are printed to {@code System.out}.
	 *
	 * @param data the dataset
	 * @param vi explicit view ids as {@code "timepointId,viewSetupId"} strings, or {@code null}
	 * @param angleIds comma-separated angle ids, or {@code null} for all
	 * @param channelIds comma-separated channel ids, or {@code null} for all
	 * @param illuminationIds comma-separated illumination ids, or {@code null} for all
	 * @param tileIds comma-separated tile ids, or {@code null} for all
	 * @param timepointIds comma-separated timepoint ids, or {@code null} for all
	 * @return the selected, present views (the dataset's {@code ViewDescription}s)
	 */
	public static ArrayList< ViewId > createViewIds(
			final SpimData data,
			final String[] vi,
			final String angleIds, 
			final String channelIds,
			final String illuminationIds,
			final String tileIds,
			final String timepointIds )
	{
		final ArrayList< ViewId > viewIds;

		if ( vi != null )
		{
			System.out.println( "Parsing selected ViewIds ... ");
			ArrayList<ViewId> parsedViews = Import.getViewIds( vi );
			viewIds = Import.getViewIds( data, parsedViews );
			System.out.println( "Warning: only " + viewIds.size() + " of " + parsedViews.size() + " that you specified exist and are present.");
		}
		else if ( angleIds != null || tileIds != null || illuminationIds != null || timepointIds != null || channelIds != null )
		{
			System.out.print( "Parsing selected angle ids ... ");
			final HashSet<Integer> a = Import.parseIdList( angleIds );
			System.out.println( a != null ? a : "all" );

			System.out.print( "Parsing selected channel ids ... ");
			final HashSet<Integer> c = Import.parseIdList( channelIds );
			System.out.println( c != null ? c : "all" );

			System.out.print( "Parsing selected illumination ids ... ");
			final HashSet<Integer> i = Import.parseIdList( illuminationIds );
			System.out.println( i != null ? i : "all" );

			System.out.print( "Parsing selected tile ids ... ");
			final HashSet<Integer> ti = Import.parseIdList( tileIds );
			System.out.println( ti != null ? ti : "all" );

			System.out.print( "Parsing selected timepoint ids ... ");
			final HashSet<Integer> tp = Import.parseIdList( timepointIds );
			System.out.println( tp != null ? tp : "all" );

			viewIds = Import.getViewIds( data, a, c, i, ti, tp );
		}
		else
		{
			// get all
			viewIds = Import.getViewIds( data );
		}

		return viewIds;
	}

	/**
	 * Returns all views of the dataset that are present (not marked as missing).
	 *
	 * @param data the dataset
	 * @return all present views
	 */
	public static ArrayList< ViewId > getViewIds( final SpimData data )
	{
		// select views to process
		final ArrayList<ViewId> viewIds = new ArrayList<>(data.getSequenceDescription().getViewDescriptions().values());

		// filter not present ViewIds
		SpimData2.filterMissingViews( data, viewIds );

		return viewIds;
	}

	/**
	 * Resolves the requested view ids against the dataset, keeping only those that exist and are present.
	 *
	 * @param data the dataset
	 * @param vi the requested view ids
	 * @return the dataset's views matching {@code vi}, in dataset order, with missing views removed
	 */
	public static ArrayList< ViewId > getViewIds( final SpimData data, final ArrayList<ViewId> vi )
	{
		// select views to process
		final ArrayList< ViewId > viewIds = new ArrayList<>();

		for ( final ViewDescription vd : data.getSequenceDescription().getViewDescriptions().values() )
		{
			for ( final ViewId v : vi )
				if ( vd.getTimePointId() == v.getTimePointId() && vd.getViewSetupId() == v.getViewSetupId() )
					viewIds.add( vd );
		}

		// filter not present ViewIds
		SpimData2.filterMissingViews( data, viewIds );

		return viewIds;
	}

	/**
	 * Selects the present views whose attributes match all of the given id sets.
	 *
	 * @param data the dataset
	 * @param a angle ids to accept, or {@code null} for all
	 * @param c channel ids to accept, or {@code null} for all
	 * @param i illumination ids to accept, or {@code null} for all
	 * @param ti tile ids to accept, or {@code null} for all
	 * @param tp timepoint ids to accept, or {@code null} for all
	 * @return the matching views, in dataset order, with missing views removed
	 */
	public static ArrayList< ViewId > getViewIds(
			final SpimData data,
			final HashSet<Integer> a,
			final HashSet<Integer> c,
			final HashSet<Integer> i,
			final HashSet<Integer> ti,
			final HashSet<Integer> tp )
	{
		// select views to process
		final ArrayList< ViewId > viewIds = new ArrayList<>();

		for ( final ViewDescription vd : data.getSequenceDescription().getViewDescriptions().values() )
		{
			if (
					( a == null || a.contains( vd.getViewSetup().getAngle().getId() )) &&
					( c == null || c.contains( vd.getViewSetup().getChannel().getId() )) &&
					( i == null || i.contains( vd.getViewSetup().getIllumination().getId() )) &&
					( ti == null || ti.contains( vd.getViewSetup().getTile().getId() )) &&
					( tp == null || tp.contains( vd.getTimePointId() )) )
			{
				viewIds.add( vd );
			}
		}

		// filter not present ViewIds
		SpimData2.filterMissingViews( data, viewIds );

		return viewIds;
	}

	/**
	 * Parses a comma-separated list of integer ids, e.g. {@code "0,1,3"} (whitespace around entries is ignored).
	 *
	 * @param idList the list to parse; may be {@code null}
	 * @return the set of ids, or {@code null} if {@code idList} is {@code null} or blank (callers treat this as
	 *         "no restriction")
	 */
	public static HashSet< Integer > parseIdList( String idList )
	{
		if ( idList == null )
			return null;

		idList = idList.trim();

		if ( idList.length() == 0 )
			return null;

		final String[] ids = idList.split( "," );
		final HashSet< Integer > hash = new HashSet<>();

		for (final String id : ids) {
			hash.add(Integer.parseInt(id.trim()));
		}

		return hash;
	}

	/**
	 * Like {@link #parseIdList} but also accepts inclusive range tokens of the form {@code "lo-hi"}.
	 * Tokens are comma-separated; each token is either a single integer ({@code "5"}) or a range
	 * ({@code "0-99"}). Returns {@code null} when the input is null or empty (matches the
	 * {@code parseIdList} convention).
	 *
	 * @param spec the comma-separated ids and/or ranges to parse; may be {@code null}
	 * @return the set of all ids covered, or {@code null} if {@code spec} is {@code null} or blank
	 * @throws IllegalArgumentException if a range's end is smaller than its start
	 */
	public static HashSet< Integer > parseIdRangeSet( String spec )
	{
		if ( spec == null )
			return null;

		spec = spec.trim();
		if ( spec.length() == 0 )
			return null;

		final HashSet< Integer > result = new HashSet<>();
		for ( final String raw : spec.split( "," ) )
		{
			final String token = raw.trim();
			if ( token.isEmpty() )
				continue;

			// "lo-hi" range, but only when the dash is internal (a leading dash means a negative number).
			final int dash = token.indexOf( '-', 1 );
			if ( dash > 0 )
			{
				final int lo = Integer.parseInt( token.substring( 0, dash ).trim() );
				final int hi = Integer.parseInt( token.substring( dash + 1 ).trim() );
				if ( hi < lo )
					throw new IllegalArgumentException( "Invalid range '" + token + "': end < start" );
				for ( int i = lo; i <= hi; i++ )
					result.add( i );
			}
			else
			{
				result.add( Integer.parseInt( token ) );
			}
		}
		return result;
	}

	/**
	 * Parses view ids given as {@code "timepointId,viewSetupId"} strings.
	 *
	 * @param s the strings to parse, one view per entry
	 * @return the parsed view ids, in the same order
	 */
	public static ArrayList<ViewId> getViewIds( final String[] s )
	{
		final ArrayList<ViewId> viewIds = new ArrayList<>();
		for ( final String s0 : s )
			viewIds.add( getViewId( s0 ) );
		return viewIds;
	}

	/**
	 * Parses a comma-separated list of integers, e.g. {@code "1, 2, 3"} (whitespace around entries is ignored).
	 *
	 * @param csvString the list to parse
	 * @return the parsed values, in order
	 */
	public static int[] csvStringToIntArray(final String csvString) {
		return Arrays.stream(csvString.split(",")).map( st -> st.trim() ).mapToInt(Integer::parseInt).toArray();
	}

	/**
	 * Parses a comma-separated list of doubles, e.g. {@code "0.5, 1, 2.5"} (whitespace around entries is ignored).
	 *
	 * @param csvString the list to parse
	 * @return the parsed values, in order
	 */
	public static double[] csvStringToDoubleArray(final String csvString) {
		return Arrays.stream(csvString.split(",")).map( st -> st.trim() ).mapToDouble(Double::parseDouble).toArray();
	}

	/**
	 * converts a String like '1,1,1; 2,2,1; 4,4,1; 8,8,2' to downsampling levels in int[][]
	 * @param csvString semicolon-separated downsampling levels, each a comma-separated list of per-dimension
	 *        factors
	 * @return one {@code int[]} of per-dimension factors per level, in order
	 */
	public static int[][] csvStringToDownsampling(final String csvString) {

		final String[] split = csvString.split(";");

		final int[][] downsampling = new int[split.length][];
		for ( int i = 0; i < split.length; ++i )
			downsampling[ i ] = Arrays.stream(split[ i ].split(",")).map( st -> st.trim() ).mapToInt(Integer::parseInt).toArray();

		return downsampling;
	}

	/**
	 * converts a List of Strings like '[1,1,1][ 2,2,1][ 4,4,1 ][ 8,8,2] to downsampling levels in int[][]
	 * @param csvString one comma-separated list of three per-dimension factors per downsampling level
	 * @return one {@code int[3]} per level, in order; {@code null} (with a message on {@code System.out}) if the
	 *         list is {@code null} or empty, an entry does not have exactly three values, or the first entry is
	 *         not {@code 1,1,1}
	 */
	public static int[][] csvStringListToDownsampling(final List<String> csvString)
	{
		if ( csvString == null || csvString.size() < 1 )
		{
			System.out.println( "List of strings for downsampling is empty/null.");
			return null;
		}
		
		final int[][] downsampling = new int[csvString.size()][];
		for ( int i = 0; i < csvString.size(); ++i )
		{
			downsampling[ i ] = Arrays.stream(csvString.get( i ).split(",")).map( st -> st.trim() ).mapToInt(Integer::parseInt).toArray();
			if ( downsampling[ i ].length != 3 )
			{
				System.out.println( "dimensions of downsampling entry is not 3: " + csvString.get( i ) );
				return null;
			}
		}

		if ( !Arrays.equals( downsampling[ 0 ], new int[] { 1, 1, 1 }))
		{
			System.out.println( "first entry is not [1,1,1], but must be: " + csvString.get( 0 ) );
			return null;
		}

		return downsampling;
	}

	/**
	 * tests that the first downsampling is [1,1,....1]
	 *
	 * @param downsampling the downsampling levels, one {@code int[]} of per-dimension factors per level
	 * @return {@code true} if there is at least one level and all factors of the first level are {@code 1}
	 */
	public static boolean testFirstDownsamplingIsPresent(final int[][] downsampling)
	{
		if ( downsampling.length > 0 && Arrays.stream(downsampling[0]).boxed().anyMatch( n -> n == 1 ) && Arrays.stream(downsampling[0]).boxed().distinct().count() == 1 )
			return true;
		else
			return false;
	}

	/**
	 * Parses a single view id given as {@code "timepointId,viewSetupId"} (whitespace is ignored).
	 *
	 * @param bdvString the string to parse
	 * @return the parsed view id
	 */
	public static ViewId getViewId(final String bdvString )
	{
		final String[] entries = bdvString.trim().split( "," );
		final int timepointId = Integer.parseInt( entries[ 0 ].trim() );
		final int viewSetupId = Integer.parseInt( entries[ 1 ].trim() );

		return new ViewId(timepointId, viewSetupId);
	}

	/*
	public static String createBDVPath(final String bdvString, final StorageFormat storageType)
	{
		final ViewId viewId = getViewId(bdvString);

		String path = null;

		if ( StorageFormat.N5.equals(storageType) )
		{
			path = "setup" + viewId.getViewSetupId() + "/" + "timepoint" + viewId.getTimePointId() + "/s0";
		}
		else if ( StorageFormat.HDF5.equals(storageType) )
		{
			path = "t" + String.format("%05d", viewId.getTimePointId()) + "/" + "s" + String.format("%02d", viewId.getViewSetupId()) + "/0/cells";
		}
		else
		{
			new RuntimeException( "BDV-compatible dataset cannot be written for " + storageType + " (yet).");
		}

		System.out.println( "Saving BDV-compatible " + storageType + " using ViewSetupId=" + viewId.getViewSetupId() + ", TimepointId=" + viewId.getTimePointId()  );
		System.out.println( "path=" + path );

		return path;
	}*/
}
