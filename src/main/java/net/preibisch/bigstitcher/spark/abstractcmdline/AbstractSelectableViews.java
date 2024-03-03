package net.preibisch.bigstitcher.spark.abstractcmdline;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Callable;

import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import picocli.CommandLine.Option;

public abstract class AbstractSelectableViews extends AbstractBasic implements Callable<Void>, Serializable //implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = 6343769494141756973L;

	@Option(names = { "--angleId" }, description = "list the angle ids that should be fused into a single image, you can find them in the XML, e.g. --angleId '0,1,2' (default: all angles)")
	protected String angleIds = null;

	@Option(names = { "--tileId" }, description = "list the tile ids that should be fused into a single image, you can find them in the XML, e.g. --tileId '0,1,2' (default: all tiles)")
	protected String tileIds = null;

	@Option(names = { "--illuminationId" }, description = "list the illumination ids that should be fused into a single image, you can find them in the XML, e.g. --illuminationId '0,1,2' (default: all illuminations)")
	protected String illuminationIds = null;

	@Option(names = { "--channelId" }, description = "list the channel ids that should be fused into a single image, you can find them in the XML (usually just ONE!), e.g. --channelId '0,1,2' (default: all channels)")
	protected String channelIds = null;

	@Option(names = { "--timepointId" }, description = "list the timepoint ids that should be fused into a single image, you can find them in the XML (usually just ONE!), e.g. --timepointId '0,1,2' (default: all time points)")
	protected String timepointIds = null;

	@Option(names = { "-vi" }, description = "specifically list the view ids (time point, view setup) that should be fused into a single image, e.g. -vi '0,0' -vi '0,1' (default: all view ids)")
	protected String[] vi = null;

	public ArrayList< ViewId > loadViewIds( final SpimData2 dataGlobal ) throws IllegalArgumentException
	{
		Import.validateInputParameters(vi, angleIds, channelIds, illuminationIds, tileIds, timepointIds);

		// select views to process
		ArrayList< ViewId > viewIdsGlobal =
				Import.createViewIds(
						dataGlobal, vi, angleIds, channelIds, illuminationIds, tileIds, timepointIds);

		if ( viewIdsGlobal.size() == 0 )
		{
			throw new IllegalArgumentException( "No views to be processed." );
		}
		else
		{
			System.out.println( "The following ViewIds will be processed: ");
			Collections.sort( viewIdsGlobal );
			for ( final ViewId v : viewIdsGlobal )
				System.out.print( "[" + v.getTimePointId() + "," + v.getViewSetupId() + "] " );
			System.out.println();
		}

		return viewIdsGlobal;
	}
}
