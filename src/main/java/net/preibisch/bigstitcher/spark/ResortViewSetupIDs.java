package net.preibisch.bigstitcher.spark;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.ViewSetup;
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
			Map<Integer, Integer> oldToNewMap = new HashMap<>();
			
			// Get all ViewSetupIds (assuming they correspond to tiles)
			ArrayList<Integer> oldIds = new ArrayList<>();
			for (ViewSetup setupId : data.getSequenceDescription().getViewSetupsOrdered()) {
				oldIds.add(setupId.getId());
			}
			
			// Current ordering: bottom-right has lowest ID, increasing row-first to left, then up
			// This means: for a grid of rowCount x columnCount
			// Position (row, col) has ID = row * columnCount + (columnCount - 1 - col)
			// where row=0 is bottom, row increases upward
			
			// Create acquisition time ordering: every 4th row acquired in parallel
			// First complete ALL parallel rows (0,4,8,12...) across ALL columns,
			// then move to next set of parallel rows (1,5,9,13...) across ALL columns, etc.
			
			int newId = 0;
			// Process each set of parallel rows separately
			for (int rowOffset = 0; rowOffset < 4; rowOffset++) {
				// For this set of parallel rows, process all columns from right to left
				for (int col = columnCount - 1; col >= 0; col--) {
					// Process all rows with this offset (rowOffset, rowOffset+4, rowOffset+8, etc.)
					for (int row = rowOffset; row < rowCount; row += 4) {
						// Convert from (row, col) position to current ViewSetupId
						// Current ID scheme: bottom-right lowest, row-first left, then up
						int oldId = row * columnCount + (columnCount - 1 - col);
						oldToNewMap.put(oldId, newId++);
					}
				}
			}

			LinkedList< String > output = new LinkedList<>();

			int maxViewSetupIds = rowCount * columnCount;
			int numDigits = String.valueOf(maxViewSetupIds - 1).length();
			String formatString = "%0" + numDigits + "d";

			int setupId = 0;
			for ( int row = 0; row < rowCount; ++row )
			{
				String line = "";
				for ( int col = 0; col < columnCount; ++col )
				{
					line = String.format(formatString, setupId) + ">" + String.format(formatString, oldToNewMap.get( setupId)) + " " + line;

					++setupId;
				}

				output.addFirst( line );

			}

			for ( final String s : output )
				System.out.println( s );
			return oldToNewMap;
		}
	}
}
