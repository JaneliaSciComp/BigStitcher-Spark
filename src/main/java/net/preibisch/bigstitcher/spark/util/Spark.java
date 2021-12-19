package net.preibisch.bigstitcher.spark.util;

import java.util.List;

import mpicbg.spim.data.sequence.ViewId;

public class Spark {

	public static ViewId deserializeViewIds( final int[][] serializedViewIds, final int i )
	{
		return new ViewId( serializedViewIds[i][0], serializedViewIds[i][1] );
	}

	public static int[][] serializeViewIds( final List< ViewId > viewIds )
	{
		final int[][] serializedViewIds = new int[ viewIds.size() ][ 2 ];

		for ( int i = 0; i < viewIds.size(); ++i )
		{
			serializedViewIds[ i ][ 0 ] = viewIds.get( i ).getTimePointId();
			serializedViewIds[ i ][ 1 ] = viewIds.get( i ).getViewSetupId();
		}

		return serializedViewIds;
	}

}
