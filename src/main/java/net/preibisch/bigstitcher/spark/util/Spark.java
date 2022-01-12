package net.preibisch.bigstitcher.spark.util;

import java.util.List;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.generic.sequence.BasicImgLoader;
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
	 * @return the single common data instance for the current spark job (JVM).
	 *         This instance is optimized for use within single-threaded Spark tasks.
	 */
	public static SpimData2 getSparkJobSpimData2(final String clusterExt,
												 final String xmlPath)
			throws SpimDataException {


		final SpimData2 data;
		if (sparkJobSpimData2 == null) {
			data = loadSpimData2(clusterExt, xmlPath);
		} else {
			validateSpimData2Location(clusterExt, xmlPath);
			data = sparkJobSpimData2;
		}

		LOG.info("getSparkJobSpimData2: returning {} for clusterExt={}, xmlPath={} on executorId={}",
				 data, clusterExt, xmlPath, getSparkExecutorId());

		return data;
	}

	private static synchronized SpimData2 loadSpimData2(final String clusterExt,
														final String xmlPath)
			throws SpimDataException {

		if (sparkJobSpimData2 == null) {

			final SpimData2 data = new XmlIoSpimData2(clusterExt).load(xmlPath);
			// set number of fetcher threads to 0 for spark usage
			final BasicImgLoader imgLoader = data.getSequenceDescription().getImgLoader();
			if (imgLoader instanceof ViewerImgLoader) {
				((ViewerImgLoader) imgLoader).setNumFetcherThreads(0);
			}

			sparkJobSpimData2ClusterExt = clusterExt;
			sparkJobSpimData2XmlPath = xmlPath;
			sparkJobSpimData2 = data;

			LOG.info("loadSpimData2: loaded {} for clusterExt={}, xmlPath={} on executorId={}",
					 sparkJobSpimData2, clusterExt, xmlPath, getSparkExecutorId());

		} else {
			validateSpimData2Location(clusterExt, xmlPath);
		}

		return sparkJobSpimData2;
	}

	private static void validateSpimData2Location(final String clusterExt,
												  final String xmlPath) throws SpimDataException {
		if (! (clusterExt.equals(sparkJobSpimData2ClusterExt) && xmlPath.equals(sparkJobSpimData2XmlPath))) {
			throw new SpimDataException("attempted to load data with clusterExt " + clusterExt + " and xmlPath " +
										xmlPath + " after data was already loaded with clusterExt " +
										sparkJobSpimData2ClusterExt + " and xmlPath " + sparkJobSpimData2XmlPath);
		}
	}

	private static String sparkJobSpimData2ClusterExt;
	private static String sparkJobSpimData2XmlPath;
	private static SpimData2 sparkJobSpimData2;

	private static final Logger LOG = LoggerFactory.getLogger(Spark.class);

}
