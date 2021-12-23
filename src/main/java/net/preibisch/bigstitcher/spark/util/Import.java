package net.preibisch.bigstitcher.spark.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import mpicbg.spim.data.SpimData;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBox;
import net.preibisch.mvrecon.process.boundingbox.BoundingBoxTools;

public class Import {

	public static BoundingBox getBoundingBox(
			final SpimData2 data,
			final List< ViewId > viewIds,
			final String boundingBoxName )
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
				System.out.println( "Bounding box '" + boundingBoxName + "' not present in XML." );
				return null;
			}
		}

		return bb;
	}

	public static boolean testInputParamters(
			final boolean uint8,
			final boolean uint16,
			final Double minIntensity,
			final Double maxIntensity,
			final String[] vi,
			final String angleIds, 
			final String channelIds,
			final String illuminationIds,
			final String tileIds,
			final String timepointIds )
	{
		if ( uint8 && uint16 )
		{
			System.err.println( "Please only select UINT8, UINT16 or nothing (FLOAT32)." );
			return false;
		}

		if ( ( uint8 || uint16 ) && (minIntensity == null || maxIntensity == null ) )
		{
			System.err.println( "When selecting UINT8 or UINT16 you need to specify minIntensity and maxIntensity." );
			return false;
		}

		if ( vi != null && ( angleIds != null || tileIds != null || illuminationIds != null || timepointIds != null || channelIds != null ) )
		{
			System.err.println( "You can only specify ViewIds (-vi) OR angles, channels, illuminations, tiles, timepoints." );
			return false;
		}

		return true;
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
			ArrayList<ViewId> parsedViews = Import.viewId( vi );
			viewIds = Import.getViewIds( data, parsedViews );
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
		final ArrayList< ViewId > viewIds = new ArrayList< ViewId >();
		viewIds.addAll( data.getSequenceDescription().getViewDescriptions().values() );

		// filter not present ViewIds
		SpimData2.filterMissingViews( data, viewIds );

		return viewIds;
	}

	public static ArrayList< ViewId > getViewIds( final SpimData data, final ArrayList<ViewId> vi )
	{
		// select views to process
		final ArrayList< ViewId > viewIds = new ArrayList< ViewId >();

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
		final ArrayList< ViewId > viewIds = new ArrayList< ViewId >();

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

		for ( int i = 0; i < ids.length; ++i )
			hash.add( Integer.parseInt( ids[ i ].trim() ) );

		return hash;
	}

	public static ArrayList<ViewId> viewId( final String[] s )
	{
		final ArrayList<ViewId> viewIds = new ArrayList<>();
		for ( final String s0 : s )
			viewIds.add( viewId( s0 ) );
		return viewIds;
	}

	public static ViewId viewId( final String s )
	{
		final String[] e = s.trim().split( "," );
		return new ViewId( Integer.parseInt( e[0].trim()), Integer.parseInt( e[1].trim() ) );
	}
}
