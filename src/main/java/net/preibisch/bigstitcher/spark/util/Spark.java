package net.preibisch.bigstitcher.spark.util;

import static mpicbg.spim.data.XmlKeys.SPIMDATA_TAG;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.opendal.Operator;
import org.apache.spark.SparkEnv;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bdv.ViewerImgLoader;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.SpimDataIOException;
import mpicbg.spim.data.generic.sequence.BasicImgLoader;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.FinalInterval;
import net.imglib2.FinalRealInterval;
import net.imglib2.Interval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.stitchingresults.PairwiseStitchingResult;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;

public class Spark {

	public static String aws_region= "us-east-1";
	public static String aws_endpoint = "https://s3.amazonaws.com";

	public static List< ViewId > deserializeViewIds( final int[][] serializedViewIds )
	{
		final List< ViewId > viewIds = new ArrayList<>( serializedViewIds.length );
		for ( int[] sid : serializedViewIds )
			viewIds.add( deserializeViewId( sid ) );
		return viewIds;
	}
	
	public static Pair<ViewId, ViewId> derserializeViewIdPairsForRDD( final int[][] serializedPair )
	{
		return new ValuePair<ViewId, ViewId>(deserializeViewId( serializedPair[ 0 ] ), deserializeViewId( serializedPair[ 1 ] ));
	}

	public static ViewId deserializeViewIds( final int[][] serializedViewIds, final int i )
	{
		return deserializeViewId( serializedViewIds[i] );
	}

	public static ViewId deserializeViewId( final int[] serializedViewIds )
	{
		return new ViewId( serializedViewIds[0], serializedViewIds[1] );
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

	public static Pair<Group<ViewId>, Group<ViewId>> deserializeGroupedViewIdPairForRDD( final int[][][] serializedPair )
	{
		final ArrayList< ViewId > pairA = new ArrayList<>( serializedPair[ 0 ].length );
		final ArrayList< ViewId > pairB = new ArrayList<>( serializedPair[ 1 ].length );

		for ( int a = 0; a < serializedPair[ 0 ].length; ++a )
			pairA.add( deserializeViewId( serializedPair[ 0 ][ a ]) );

		for ( int b = 0; b < serializedPair[ 1 ].length; ++b )
			pairB.add( deserializeViewId( serializedPair[ 1 ][ b ]) );

		return new ValuePair<Group<ViewId>, Group<ViewId>>( new Group<>( pairA ), new Group<>( pairB ) );
	}

	public static ArrayList<int[][]> serializeViewIdPairsForRDD( final List< Pair<ViewId, ViewId> > pairs )
	{
		final ArrayList<int[][]> ser = new ArrayList<>();

		for ( final Pair<ViewId, ViewId> pair : pairs )
			ser.add( serializeViewIdPairForRDD( pair ) );

		return ser;
	}

	public static int[][] serializeViewIdPairForRDD( final Pair<ViewId, ViewId> pair )
	{
		final int[][] pairInt = new int[2][];

		pairInt[0] = serializeViewId( pair.getA() );
		pairInt[1] = serializeViewId( pair.getB() );

		return pairInt;
	}

	public static ArrayList<int[][][]> serializeGroupedViewIdPairsForRDD( final List< ? extends Pair<? extends Group<? extends ViewId>, ? extends Group<? extends ViewId>>> pairs )
	{
		final ArrayList<int[][][]> ser = new ArrayList<>();

		for ( final Pair<? extends Group<? extends ViewId>, ? extends Group<? extends ViewId>> pair : pairs )
			ser.add( serializeGroupedViewIdPairForRDD( pair ) );

		return ser;
	}

	public static int[][][] serializeGroupedViewIdPairForRDD( final Pair<? extends Group<? extends ViewId>, ? extends Group<? extends ViewId>> pair )
	{
		final int[][][] pairInt = new int[2][][];

		pairInt[0] = new int[ pair.getA().getViews().size() ][];
		pairInt[1] = new int[ pair.getB().getViews().size() ][];

		int i = 0;
		for ( final ViewId viewId : pair.getA().getViews() )
			pairInt[0][i++] = serializeViewId( viewId );

		i = 0;
		for ( final ViewId viewId : pair.getB().getViews() )
			pairInt[1][i++] = serializeViewId( viewId );

		return pairInt;
	}

	public static ArrayList<int[]> serializeViewIdsForRDD( final List< ViewId > viewIds )
	{
		final ArrayList<int[]> serializedViewIds = new ArrayList<>();

		for ( int i = 0; i < viewIds.size(); ++i )
			serializedViewIds.add( serializeViewId( viewIds.get( i ) ) );

		return serializedViewIds;
	}

	public static ArrayList< InterestPoint > deserializeInterestPoints( final double[][] points )
	{
		final ArrayList< InterestPoint > list = new ArrayList<>();
		
		for ( int i = 0; i < points.length; ++i )
			list.add( new InterestPoint(i, points[ i ] ));

		return list;
	}

	public static int[] serializeViewId( final ViewId viewId )
	{
		return new int[] { viewId.getTimePointId(), viewId.getViewSetupId() };
	}

	public static Interval deserializeInterval( final long[][] serializedInterval )
	{
		return new FinalInterval( serializedInterval[ 0 ], serializedInterval[ 1 ] );
	}

	public static long[][] serializeInterval( final Interval interval )
	{
		return new long[][]{ interval.minAsLongArray(), interval.maxAsLongArray() };
	}

	public static class SerializablePairwiseStitchingResult implements Serializable
	{
		private static final long serialVersionUID = -8920256594391301778L;

		final int[][][] pair; // Pair< Group<ViewId>, Group<ViewId> > pair;
		final double[][] matrix = new double[3][4]; //AffineTransform3D transform;
		final double[] min, max; //final RealInterval boundingBox;
		final double r;
		final double hash;

		public SerializablePairwiseStitchingResult( final PairwiseStitchingResult< ViewId> result )
		{
			this.r = result.r();
			this.hash = result.getHash();
			this.min = result.getBoundingBox().minAsDoubleArray();
			this.max = result.getBoundingBox().maxAsDoubleArray();
			this.pair = Spark.serializeGroupedViewIdPairForRDD( result.pair() );
			((AffineTransform3D)result.getTransform()).toMatrix( matrix );
		}

		public PairwiseStitchingResult< ViewId > deserialize()
		{
			final AffineTransform3D t = new AffineTransform3D();
			t.set( matrix );

			return new PairwiseStitchingResult<>(
					Spark.deserializeGroupedViewIdPairForRDD( pair ),
					new FinalRealInterval(min, max),
					t,
					r,
					hash );
		}
	}

	public static String getSparkExecutorId() {
		final SparkEnv sparkEnv = SparkEnv.get();
		return sparkEnv == null ? null : sparkEnv.executorId();
	}

	/**
	 * @return a new data instance optimized for use within single-threaded Spark tasks.
	 */
	public static SpimData2 getSparkJobSpimData2(final String xmlPath)
			throws SpimDataException {

		final SpimData2 data;

		if ( xmlPath.trim().startsWith( "s3:/" ) )
		{
			final File f = new File( xmlPath );
			String parent = f.getParent().replace( "//", "/" ); // new File cuts // already, but just to make sure
			parent = parent.substring(4, parent.length() );

			final String bucket, root;

			if (parent.contains( "/" ) )
			{
				// there is an extra path
				bucket = parent.substring(0,parent.indexOf( "/" ) );
				root = parent.substring(parent.indexOf( "/" ) + 1, parent.length() );
			}
			else
			{
				bucket = parent;
				root = "/";
			}

			final String xmlFile = f.getName();

			System.out.println( "Loading XML from s3:" );
			System.out.println( "bucket: '" + bucket + "'" );
			System.out.println( "root dir: '" + root + "'" );
			System.out.println( "xmlFile: '" + xmlFile + "'" );
			System.out.println( "region: '" + aws_region + "'" );
			System.out.println( "endpoint: '" + aws_endpoint + "'" );

			final Map<String, String> builder = new HashMap<>();

			builder.put("bucket", bucket );
			builder.put("root", root );
			builder.put("region", aws_region);
			builder.put("endpoint", aws_endpoint);

			final Operator op = Operator.of( "s3", builder );
			final byte[] decodedBytes = op.read( xmlFile ).join();
			op.close();

			final SAXBuilder sax = new SAXBuilder();
			Document doc;
			try
			{
				final InputStream is = new ByteArrayInputStream(decodedBytes);
				doc = sax.build( is );
			}
			catch ( final Exception e )
			{
				throw new SpimDataIOException( e );
			}

			final Element docRoot = doc.getRootElement();

			if ( docRoot.getName() != SPIMDATA_TAG )
				throw new RuntimeException( "expected <" + SPIMDATA_TAG + "> root element. wrong file?" );

			data = new XmlIoSpimData2("").fromXml( docRoot, new File( "s3://janelia-bigstitcher-spark/Stitching/dataset.xml" ) );
		}
		else
		{
			data = new XmlIoSpimData2("").load(xmlPath);
		}

		final SequenceDescription sequenceDescription = data.getSequenceDescription();

		// set number of fetcher threads to 0 for spark usage
		final BasicImgLoader imgLoader = sequenceDescription.getImgLoader();
		if (imgLoader instanceof ViewerImgLoader) {
			((ViewerImgLoader) imgLoader).setNumFetcherThreads(0);
		}

		LOG.info("getSparkJobSpimData2: loaded {} for xmlPath={} on executorId={}",
				 data, xmlPath, getSparkExecutorId());

		return data;
	}

	private static final Logger LOG = LoggerFactory.getLogger(Spark.class);

}
