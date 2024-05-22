package net.preibisch.bigstitcher.spark.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import mpicbg.spim.data.SpimData;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBox;
import net.preibisch.mvrecon.process.boundingbox.BoundingBoxTools;
import net.preibisch.mvrecon.process.export.ExportN5API.StorageType;

public class Import {

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

	public static void validateInputParameters(
			final boolean uint8,
			final boolean uint16,
			final Double minIntensity,
			final Double maxIntensity )
			throws IllegalArgumentException
	{
		if ( uint8 && uint16 ) {
			throw new IllegalArgumentException( "Please only select UINT8, UINT16 or nothing (FLOAT32)." );
		}

		if ( ( uint8 || uint16 ) && (minIntensity == null || maxIntensity == null ) ) {
			throw new IllegalArgumentException( "When selecting UINT8 or UINT16 you need to specify minIntensity and maxIntensity." );
		}
	}

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

	public static ArrayList< ViewId > getViewIds( final SpimData data )
	{
		// select views to process
		final ArrayList<ViewId> viewIds = new ArrayList<>(data.getSequenceDescription().getViewDescriptions().values());

		// filter not present ViewIds
		SpimData2.filterMissingViews( data, viewIds );

		return viewIds;
	}

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

	public static ArrayList<ViewId> getViewIds( final String[] s )
	{
		final ArrayList<ViewId> viewIds = new ArrayList<>();
		for ( final String s0 : s )
			viewIds.add( getViewId( s0 ) );
		return viewIds;
	}

	public static int[] csvStringToIntArray(final String csvString) {
		return Arrays.stream(csvString.split(",")).map( st -> st.trim() ).mapToInt(Integer::parseInt).toArray();
	}

	/**
	 * converts a String like '1,1,1; 2,2,1; 4,4,1; 8,8,2' to downsampling levels in int[][]
	 * @param csvString
	 * @return
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
	 * @param csvString
	 * @return
	 */
	public static int[][] csvStringListToDownsampling(final List<String> csvString) {

		final int[][] downsampling = new int[csvString.size()][];
		for ( int i = 0; i < csvString.size(); ++i )
			downsampling[ i ] = Arrays.stream(csvString.get( i ).split(",")).map( st -> st.trim() ).mapToInt(Integer::parseInt).toArray();

		return downsampling;
	}

	/**
	 * tests that the first downsampling is [1,1,....1]
	 *
	 * @param downsampling
	 * @return
	 */
	public static boolean testFirstDownsamplingIsPresent(final int[][] downsampling)
	{
		if ( downsampling.length > 0 && Arrays.stream(downsampling[0]).boxed().anyMatch( n -> n == 1 ) && Arrays.stream(downsampling[0]).boxed().distinct().count() == 1 )
			return true;
		else
			return false;
	}

	public static ViewId getViewId(final String bdvString )
	{
		final String[] entries = bdvString.trim().split( "," );
		final int timepointId = Integer.parseInt( entries[ 0 ].trim() );
		final int viewSetupId = Integer.parseInt( entries[ 1 ].trim() );

		return new ViewId(timepointId, viewSetupId);
	}

	public static String createBDVPath(final String bdvString, final StorageType storageType)
	{
		final ViewId viewId = getViewId(bdvString);

		String path = null;

		if ( StorageType.N5.equals(storageType) )
		{
			path = "setup" + viewId.getViewSetupId() + "/" + "timepoint" + viewId.getTimePointId() + "/s0";
		}
		else if ( StorageType.HDF5.equals(storageType) )
		{
			path = "t" + String.format("%05d", viewId.getTimePointId()) + "/" + "s" + String.format("%02d", viewId.getViewSetupId()) + "/0/cells";
		}
		else
		{
			throw new RuntimeException( "BDV-compatible dataset cannot be written for " + storageType + " (yet).");
		}

		System.out.println( "Saving BDV-compatible " + storageType + " using ViewSetupId=" + viewId.getViewSetupId() + ", TimepointId=" + viewId.getTimePointId()  );
		System.out.println( "path=" + path );

		return path;
	}
}
