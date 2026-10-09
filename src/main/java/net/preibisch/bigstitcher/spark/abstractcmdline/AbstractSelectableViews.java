/*-
 * #%L
 * Spark-based parallel BigStitcher project.
 * %%
 * Copyright (C) 2021 - 2024 Developers.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */
package net.preibisch.bigstitcher.spark.abstractcmdline;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Callable;

import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import picocli.CommandLine.Option;

/**
 * Base class of the command-line tools that work on a subset of the views of a BigStitcher project. It adds
 * the view-selection options, either explicit view ids ({@code -vi}) or attribute filters ({@code --angleId},
 * {@code --tileId}, {@code --illuminationId}, {@code --channelId}, {@code --timepointId}), and resolves
 * them against the loaded dataset.
 */
public abstract class AbstractSelectableViews extends AbstractBasic implements Callable<Void>, Serializable //implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = 6343769494141756973L;

	@Option(names = { "--angleId" }, description = "list the angle ids that should be processed, you can find them in the XML, e.g. --angleId '0,1,2' (default: all angles)")
	protected String angleIds = null;

	@Option(names = { "--tileId" }, description = "list the tile ids that should be processed, you can find them in the XML, e.g. --tileId '0,1,2' (default: all tiles)")
	protected String tileIds = null;

	@Option(names = { "--illuminationId" }, description = "list the illumination ids that should be processed, you can find them in the XML, e.g. --illuminationId '0,1,2' (default: all illuminations)")
	protected String illuminationIds = null;

	@Option(names = { "--channelId" }, description = "list the channel ids that should be processed, you can find them in the XML (usually just one when fusing), e.g. --channelId '0,1,2' (default: all channels)")
	protected String channelIds = null;

	@Option(names = { "--timepointId" }, description = "list the timepoint ids that should be processed, you can find them in the XML (usually just one when fusing), e.g. --timepointId '0,1,2' (default: all time points)")
	protected String timepointIds = null;

	@Option(names = { "-vi" }, description = "specifically list the view ids (time point, view setup) that should be fused into a single image, e.g. -vi '0,0' -vi '0,1' (default: all view ids)")
	protected String[] vi = null;

	/**
	 * True when the user passed any view-selection flag (-vi, --angleId, --tileId, --illuminationId,
	 * --channelId, --timepointId). Intended for messaging only: whether a selection actually restricts
	 * anything is a set comparison against the dataset's views, not a question of which flags were typed.
	 *
	 * @return {@code true} if at least one view-selection option was specified
	 */
	protected boolean hasViewSelection()
	{
		return vi != null || angleIds != null || tileIds != null || illuminationIds != null || channelIds != null || timepointIds != null;
	}

	/**
	 * Resolves the view-selection options of this command against the given dataset; see
	 * {@link #loadViewIds(SpimData2, String[], String, String, String, String, String)}.
	 *
	 * @param dataGlobal the loaded dataset whose present views are selected from
	 * @return the sorted list of selected view ids, never empty
	 * @throws IllegalArgumentException if {@code -vi} is combined with attribute filters, or if no view
	 *         matches the selection
	 */
	public ArrayList< ViewId > loadViewIds( final SpimData2 dataGlobal ) throws IllegalArgumentException
	{
		return loadViewIds(dataGlobal, vi, angleIds, channelIds, illuminationIds, tileIds, timepointIds);
		/*Import.validateInputParameters(vi, angleIds, channelIds, illuminationIds, tileIds, timepointIds);

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

		return viewIdsGlobal;*/
	}

	/**
	 * Selects the views to process: with {@code vi} the explicitly listed view ids that exist and are present
	 * in the dataset; otherwise the present views matching all given attribute id lists, where {@code null}
	 * means "all" for that attribute; without any selection every present view. The result is sorted and
	 * printed to {@code stdout}.
	 *
	 * @param dataGlobal the loaded dataset
	 * @param vi explicit view ids as {@code "timepointId,viewSetupId"} strings, or {@code null}; must not be
	 *        combined with any of the attribute id lists
	 * @param angleIds comma-separated angle ids, or {@code null} for all
	 * @param channelIds comma-separated channel ids, or {@code null} for all
	 * @param illuminationIds comma-separated illumination ids, or {@code null} for all
	 * @param tileIds comma-separated tile ids, or {@code null} for all
	 * @param timepointIds comma-separated timepoint ids, or {@code null} for all
	 * @return the sorted list of selected view ids, never empty
	 * @throws IllegalArgumentException if {@code vi} is combined with attribute filters, or if no view
	 *         matches the selection
	 */
	public static ArrayList< ViewId > loadViewIds(
			final SpimData2 dataGlobal,
			final String[] vi,
			final String angleIds,
			final String channelIds,
			final String illuminationIds,
			final String tileIds,
			final String timepointIds ) throws IllegalArgumentException
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
