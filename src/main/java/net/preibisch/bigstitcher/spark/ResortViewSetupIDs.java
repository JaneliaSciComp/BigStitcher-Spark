package net.preibisch.bigstitcher.spark;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

public class ResortViewSetupIDs extends AbstractBasic
{
	private static final long serialVersionUID = -8449058590348222341L;

	@Option(names = { "-xo", "--xmlout" }, description = "path to the output BigStitcher xml, e.g. /home/project-n5.xml or s3://myBucket/dataset.xml (default: overwrite input)")
	private String xmlOutURIString = null;

	private URI xmlOutURI = null;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		// if we want to display the result, we need to set the fetcher threads to anything but 0
		final SpimData2 data = this.loadSpimData2();

		if ( data == null )
			throw new IllegalArgumentException( "Couldn't load SpimData XML project." );

		final ArrayList< ViewId > viewIds = Import.getViewIds( data );

		if ( viewIds == null || viewIds.size() == 0 )
			return null;

		if ( xmlOutURIString == null )
			xmlOutURI = xmlURI;
		else
			xmlOutURI = URITools.toURI( xmlOutURIString );

		System.out.println( "xmlout: " + xmlOutURI );

		// we build a map from old to new ViewSetupId
		final SetupIDMapper mapper = new KellerMirrorScopeMapper( 1120/28, 28 );
		final Map< Integer, Integer > map = mapper.map( data );

		return null;
	}

	public static void main(final String... args) throws SpimDataException
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new ResortViewSetupIDs()).execute(args));
	}

	public static interface SetupIDMapper
	{
		Map< Integer, Integer > map( final SpimData2 data );
	}

	public static class KellerMirrorScopeMapper implements SetupIDMapper
	{
		final int rowCount, columnCount;

		public KellerMirrorScopeMapper( final int rowCount, final int columnCount )
		{
			this.rowCount = rowCount;
			this.columnCount = columnCount;
		}

		@Override
		public Map<Integer, Integer> map(SpimData2 data) {
			// TODO Auto-generated method stub
			return null;
		}
	}
}
