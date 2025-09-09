package net.preibisch.bigstitcher.spark.util;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.ViewSetup;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;

public interface SetupIDMapper
{
	Map< Integer, Integer > map( final SpimData2 data );

	public static class KellerMirrorScopeMapper implements SetupIDMapper
	{
		final int rowCount, columnCount;

		public KellerMirrorScopeMapper( final int rowCount, final int columnCount )
		{
			this.rowCount = rowCount;
			this.columnCount = columnCount;
		}

		public static void main( String args[] ) throws SpimDataException
		{
			final SpimData2 data = Spark.getSparkJobSpimData2( URI.create( "file:/Users/preibischs/Downloads/dataset_full.xml") );

			// we build a map from old to new ViewSetupId
			final Map< Integer, Integer > map = new KellerMirrorScopeMapper( 1120/28, 28 ).map( data );
		}

		@Override
		public HashMap<Integer, Integer> map( final SpimData2 data )
		{
			if ( data.getSequenceDescription().getAllAngles().size() > 1 )
				throw new RuntimeException( "multiple angles not yet supported." );

			if ( data.getSequenceDescription().getAllIlluminations().size() > 1 )
				throw new RuntimeException( "multiple illuminations not yet supported." );

			if ( data.getSequenceDescription().getAllChannels().size() > 1 )
				throw new RuntimeException( "multiple channels not yet supported." );

			final int maxViewSetupIds = rowCount * columnCount;

			if ( data.getSequenceDescription().getViewSetupsOrdered().size() != maxViewSetupIds )
				throw new RuntimeException( "number of viewsetups does not match rowCount * columnCount." );

			final HashMap<Integer, Integer> oldToNewMap = new HashMap<>();

			// Get all ViewSetupIds (assuming they correspond to tiles)
			final ArrayList<Integer> oldIds = new ArrayList<>();
			for (ViewSetup setupId : data.getSequenceDescription().getViewSetupsOrdered())
				oldIds.add(setupId.getId());

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

			final LinkedList< String > output = new LinkedList<>();

			final int numDigits = String.valueOf(maxViewSetupIds - 1).length();
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
