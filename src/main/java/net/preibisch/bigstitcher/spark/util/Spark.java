package net.preibisch.bigstitcher.spark.util;

import java.util.ArrayList;
import java.util.List;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.generic.sequence.BasicImgLoader;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.ViewId;

import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bdv.ViewerImgLoader;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;

public class Spark {

	public static ViewId deserializeViewIds( final int[][] serializedViewIds, final int i )
	{
		return new ViewId( serializedViewIds[i][0], serializedViewIds[i][1] );
	}

	public static List< ViewId > deserializeViewIds( final int[][] serializedViewIds )
	{
		final List< ViewId > viewIds = new ArrayList<>( serializedViewIds.length );
		for ( int[] sid : serializedViewIds )
			viewIds.add( new ViewId( sid[ 0 ], sid[ 1 ] ) );
		return viewIds;
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

	public static String getSparkExecutorId() {
		final SparkEnv sparkEnv = SparkEnv.get();
		return sparkEnv == null ? null : sparkEnv.executorId();
	}

	/**
	 * @return a new data instance optimized for use within single-threaded Spark tasks.
	 */
	public static SpimData2 getSparkJobSpimData2(final String clusterExt,
												 final String xmlPath)
			throws SpimDataException {

		final SpimData2 data = new XmlIoSpimData2(clusterExt).load(xmlPath);
		final SequenceDescription sequenceDescription = data.getSequenceDescription();

		// set number of fetcher threads to 0 for spark usage
		final BasicImgLoader imgLoader = sequenceDescription.getImgLoader();
		if (imgLoader instanceof ViewerImgLoader) {
			((ViewerImgLoader) imgLoader).setNumFetcherThreads(0);
		}

		LOG.info("getSparkJobSpimData2: loaded {} for clusterExt={}, xmlPath={} on executorId={}",
				 data, clusterExt, xmlPath, getSparkExecutorId());

		return data;
	}

	private static final Logger LOG = LoggerFactory.getLogger(Spark.class);

}
