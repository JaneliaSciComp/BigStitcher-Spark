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
package net.preibisch.bigstitcher.spark;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;

import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.CorrespondenceTools;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.CorrespondingInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsN5;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsZarrStore;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPointLists;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.VirtualFilteredInterestPoints;
import net.preibisch.mvrecon.process.interestpointdetection.InterestPointTools;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

/**
 * Clear / repair interest points and correspondences of a BigStitcher project.
 *
 * All modes except {@link ClearMode#FIX_INTERESTPOINTS} honour the view-selection flags inherited from
 * {@link AbstractSelectableViews}. Whenever data is removed from the selected views, correspondences in
 * <em>unselected</em> views that point into the selection are removed as well, so the dataset stays
 * consistent in a single pass. Without selection flags every view is processed and the results are
 * identical to the previous, dataset-wide behaviour.
 */
public class ClearInterestPoints extends AbstractSelectableViews
{
	/** Operation selected with {@code --clearMode}. */
	public enum ClearMode
	{
		/**
		 * Remove all interest points and correspondences of the selected views (XML entries + their entries in the
		 * interest point store), plus correspondences in unselected views that point at them. Without a restricting
		 * view selection the whole store (interestpoints.zarr, and a legacy interestpoints.n5) is wiped.
		 */
		CLEAR_EVERYTHING,
		/**
		 * Keep interest-point detections; drop every correspondence link of the selected views, plus
		 * correspondences in unselected views that point at them.
		 */
		CLEAR_ALL_CORRESPONDENCES,
		/**
		 * Repair: drop IP entries (XML + store) for views no longer present in the SpimData,
		 * drop CorrespondingInterestPoints whose target view is no longer valid, and remove
		 * stale per-view groups of a legacy interestpoints.n5 whose name decodes to an invalid ViewId.
		 * Always covers the whole dataset; view-selection flags are ignored.
		 */
		FIX_INTERESTPOINTS,
		/**
		 * Add a label (--label) to every selected view that does not already have it.
		 * Creates an empty interest-point list and an empty correspondence list, both stored by the XML save
		 * so the entry is fully materialized. No-op for views that already have the label.
		 */
		ADD_LABEL,
		/**
		 * Remove a label (--label) from the selected views: drop the (view, label) entries from XML and store,
		 * then drop CorrespondingInterestPoints that point at one of the removed (view, label) lists from
		 * all remaining lists (selected and unselected views alike).
		 */
		REMOVE_LABEL
	}

	@Option(names = { "--clearMode" }, description = "what to clear: "
			+ "CLEAR_EVERYTHING removes all interest points and correspondences of the selected views (without view selection: wipes the whole interest point store); "
			+ "CLEAR_ALL_CORRESPONDENCES keeps detections but drops all correspondence links of the selected views; "
			+ "FIX_INTERESTPOINTS removes IP entries (XML + store) for views no longer in the SpimData and drops correspondences pointing to those views (ignores view selection); "
			+ "ADD_LABEL adds an empty interest-point list with the given --label to every selected view that does not already have it; "
			+ "REMOVE_LABEL drops every (selected view, --label) entry and every correspondence pointing at one of them. "
			+ "All modes except FIX_INTERESTPOINTS honour the view-selection flags (--angleId, --tileId, --channelId, --illuminationId, --timepointId, -vi); "
			+ "correspondences in unselected views that point into the selection are removed as well so the dataset stays consistent "
			+ "(default: CLEAR_EVERYTHING)")
	private ClearMode clearMode = ClearMode.CLEAR_EVERYTHING;

	@Option(names = { "--label" }, description = "interest-point label name. Required for --clearMode ADD_LABEL and REMOVE_LABEL.")
	private String label = null;

	@Option(names = { "--silent" }, description = "skip the per-view listing that would otherwise lazy-load every selected view's interest points and correspondences (file://, s3://, gs://) just to print counts. Recommended for large datasets, especially in the cloud (default: false)")
	private boolean silent = false;

	private static final long serialVersionUID = -7892604354139919145L;

	/** Top-level group name layout of a legacy interestpoints.n5, produced by {@code InterestPointsN5.createN5datasetPath(tpId, vsId, label)}. */
	private static final Pattern VIEW_GROUP_PATTERN = Pattern.compile( "^tpId_(\\d+)_viewSetupId_(\\d+)$" );

	/** Decode a top-level N5 group name like {@code tpId_3_viewSetupId_42} into a ViewId; null if the name doesn't match. */
	private static ViewId parseViewIdFromGroupName( final String name )
	{
		final Matcher m = VIEW_GROUP_PATTERN.matcher( name );
		if ( !m.matches() )
			return null;
		try
		{
			return new ViewId( Integer.parseInt( m.group( 1 ) ), Integer.parseInt( m.group( 2 ) ) );
		}
		catch ( final NumberFormatException e )
		{
			return null;
		}
	}

	/** Plain {@link ViewId} copies (drops ViewDescription subclasses) so set membership is purely id-based. */
	private static Set< ViewId > normalize( final Collection< ? extends ViewId > views )
	{
		final HashSet< ViewId > set = new HashSet<>();
		for ( final ViewId v : views )
			set.add( new ViewId( v.getTimePointId(), v.getViewSetupId() ) );
		return set;
	}

	/** {@code restriction == null} means "no restriction" (every view is selected). */
	private static boolean isSelected( final Set< ViewId > restriction, final ViewId viewId )
	{
		return restriction == null || restriction.contains( viewId );
	}

	/**
	 * Deletes the points and correspondences of every label of a view in the store (committed by the next XML save).
	 *
	 * @return the number of deleted lists
	 */
	private static int deleteLists( final ViewInterestPointLists lists, final String message )
	{
		if ( lists == null )
			return 0;

		if ( message != null )
			System.out.println( message );

		for ( final InterestPoints list : lists.getHashMap().values() )
		{
			list.deleteInterestPoints();
			list.deleteCorrespondingInterestPoints();
		}

		return lists.getHashMap().size();
	}

	/**
	 * Removes the per-view groups ({@code tpId_X_viewSetupId_Y}) of a legacy {@code interestpoints.n5} whose view matches.
	 * Never creates a legacy container.
	 *
	 * @return the number of removed groups
	 */
	private static int removeLegacyViewGroups( final URI containerUri, final Predicate< ViewId > shouldRemove, final boolean silent )
	{
		final List< String > groupsToRemove = new ArrayList<>();

		if ( !URITools.getKeyValueAccess( containerUri ).exists( URITools.getKeyValueAccess( containerUri ).compose( containerUri, "attributes.json" ) ) )
			return 0; // no legacy container

		try ( final N5Reader n5Reader = URITools.instantiateN5Reader( StorageFormat.N5, containerUri ) )
		{
			for ( final String groupName : n5Reader.list( "" ) )
			{
				final ViewId viewId = parseViewIdFromGroupName( groupName );
				if ( viewId != null && shouldRemove.test( viewId ) )
					groupsToRemove.add( groupName );
			}
		}

		if ( groupsToRemove.isEmpty() )
			return 0;

		try ( final N5Writer n5Writer = URITools.instantiateN5Writer( StorageFormat.N5, containerUri ) )
		{
			for ( final String groupName : groupsToRemove )
			{
				if ( !silent )
					System.out.println( "  removing legacy N5 group '" + groupName + "'" );
				n5Writer.remove( groupName );
			}
		}

		return groupsToRemove.size();
	}

	/**
	 * Drop every correspondence that points at ({@code view in views}, {@code label}) from all lists in
	 * {@code vip}. Set-based counterpart of {@link CorrespondenceTools#removeCorrespondencesToLabel}
	 * (which is label-wide, regardless of target view) and
	 * {@link CorrespondenceTools#removeCorrespondencesForViewLabel} (which handles a single view and must
	 * run before that view's list is removed). Multithreaded, one task per source view.
	 * Candidate for upstreaming to mvr's CorrespondenceTools.
	 *
	 * @return number of correspondence entries removed
	 */
	private static int removeCorrespondencesToViewsWithLabel(
			final ViewInterestPoints vip,
			final Set< ViewId > views,
			final String label,
			final int numThreads )
	{
		final List< ViewId > viewsToClean = new ArrayList<>( vip.getViewInterestPoints().keySet() );

		if ( viewsToClean.isEmpty() )
			return 0;

		final ExecutorService exec = Executors.newFixedThreadPool( Math.max( 1, numThreads ) );
		final List< Future< Integer > > futures = new ArrayList<>();

		for ( final ViewId vid : viewsToClean )
		{
			futures.add( exec.submit( () ->
			{
				int removed = 0;
				final ViewInterestPointLists vipl = vip.getViewInterestPoints().get( vid );
				if ( vipl == null || vipl.getHashMap() == null )
					return 0;

				for ( final InterestPoints ips : vipl.getHashMap().values() )
				{
					final List< CorrespondingInterestPoints > corrs = new ArrayList<>( ips.getCorrespondingInterestPointsCopy() );
					final int sizeBefore = corrs.size();
					corrs.removeIf( c -> label.equals( c.getCorrespodingLabel() ) && views.contains( c.getCorrespondingViewId() ) );

					if ( corrs.size() < sizeBefore )
					{
						ips.setCorrespondingInterestPoints( corrs );
						removed += sizeBefore - corrs.size();
					}
				}
				return removed;
			} ) );
		}

		exec.shutdown();
		int totalRemoved = 0;
		for ( final Future< Integer > f : futures )
		{
			try
			{
				totalRemoved += f.get();
			}
			catch ( final Exception e )
			{
				e.printStackTrace();
			}
		}
		return totalRemoved;
	}

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		if ( ( clearMode == ClearMode.ADD_LABEL || clearMode == ClearMode.REMOVE_LABEL )
				&& ( label == null || label.isEmpty() ) )
		{
			System.err.println( "ERROR: --label is required for --clearMode " + clearMode + "." );
			return null;
		}

		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		final ViewInterestPoints vip = dataGlobal.getViewInterestPoints();
		final Map<ViewId, ViewInterestPointLists> ips = vip.getViewInterestPoints();
		final int numThreads = Runtime.getRuntime().availableProcessors();

		// All present, non-missing views of the dataset ...
		final Set< ViewId > allPresent = normalize( Import.getViewIds( dataGlobal ) );

		// ... and the ones the user selected. 'restriction' is null when the selection covers every
		// present view (no flags, or flags that happen to match everything) — the legacy dataset-wide
		// code paths are taken in that case, so outcomes are unchanged for existing callers.
		final Set< ViewId > selected;
		final Set< ViewId > restriction;

		if ( clearMode == ClearMode.FIX_INTERESTPOINTS )
		{
			// The repair pass is only meaningful over the whole dataset. Don't call loadViewIds() here:
			// it would print a misleading "will be processed" list, and a -vi pointing at a missing view
			// would abort a repair whose selection is ignored anyway.
			if ( hasViewSelection() )
				System.out.println( "NOTE: view-selection flags are ignored for --clearMode FIX_INTERESTPOINTS; the repair always covers the whole dataset." );
			selected = allPresent;
			restriction = null;
		}
		else
		{
			try
			{
				selected = normalize( this.loadViewIds( dataGlobal ) );
			}
			catch ( final IllegalArgumentException e )
			{
				System.err.println( "ERROR: " + e.getMessage() );
				return null;
			}
			// Set comparison, not flag sniffing: '--tileId 0' on a single-tile dataset is still "everything".
			restriction = selected.containsAll( allPresent ) ? null : selected;
		}

		// URI form so we work with any backend (file://, s3://, gs://) — not just the local FS.
		final URI containerUri = URITools.toURI( URITools.appendName( dataGlobal.getBasePathURI(), InterestPointsN5.baseN5 ) );

		final String scope = ( restriction == null ) ? "" : " (restricted to " + restriction.size() + " of " + allPresent.size() + " views)";

		switch ( clearMode )
		{
			case CLEAR_ALL_CORRESPONDENCES:
				System.out.println( "The following correspondences will be removed in ('" + containerUri + "')" + scope + ":");
				break;
			case FIX_INTERESTPOINTS:
				System.out.println( "Repairing interest-point map and correspondences in ('" + containerUri + "') against present views in the XML.");
				break;
			case ADD_LABEL:
				System.out.println( "Adding label '" + label + "' to every selected view that does not already have it (container '" + containerUri + "')" + scope + ".");
				break;
			case REMOVE_LABEL:
				System.out.println( "Removing label '" + label + "' from every selected view, with the filtered labels of it and the correspondences that reference them (container '" + containerUri + "')" + scope + ".");
				break;
			case CLEAR_EVERYTHING:
			default:
				System.out.println( "The following interest points and correspondences will be removed in ('" + containerUri + "')" + scope + ":");
				break;
		}

		// display the selected data (lazy-loads via getInterestPointsCopy/getCorrespondingInterestPointsCopy)
		if ( !silent )
		{
			for ( final Entry<ViewId, ViewInterestPointLists> ip : ips.entrySet() )
			{
				if ( !isSelected( restriction, ip.getKey() ) )
					continue;

				System.out.println( Group.pvid( ip.getKey() ) + ":" );

				for ( final Entry<String, InterestPoints> points : ip.getValue().getHashMap().entrySet() )
				{
					String counts;

					try
					{
						counts = points.getValue().getInterestPointsCopy().size() + " interest points, " + points.getValue().getCorrespondingInterestPointsCopy().size() + " correspondences.";
					}
					catch ( final RuntimeException e )
					{
						// e.g. a filtered label whose base label is gone, which FIX_INTERESTPOINTS removes
						counts = "cannot be read (" + e.getMessage() + ")";
					}

					System.out.println( "\t'" + points.getKey() + "' " + ( points.getValue() instanceof VirtualFilteredInterestPoints ? "(filtered from '" + ( (VirtualFilteredInterestPoints)points.getValue() ).getBaseLabel() + "') " : "" ) + counts );
				}
			}

			// Unselected views are deliberately not loaded here just to preview counts.
			if ( restriction != null )
				System.out.println( "Correspondences in unselected views that point into the selection will be removed as well; counts are reported after processing." );
		}

		// FIX_INTERESTPOINTS classifies orphans regardless of dryRun so the user always sees the count;
		// the actual mutations + saves are gated on !dryRun below.
		final Set< ViewId > validViewIds;
		final List< ViewId > orphanViewIds;
		if ( clearMode == ClearMode.FIX_INTERESTPOINTS )
		{
			validViewIds = allPresent;
			orphanViewIds = new ArrayList<>();
			for ( final ViewId viewId : ips.keySet() )
				if ( !validViewIds.contains( viewId ) )
					orphanViewIds.add( viewId );

			System.out.println( "Found " + orphanViewIds.size() + " orphan view(s) with IP entries to remove." );
		}
		else
		{
			validViewIds = null;
			orphanViewIds = null;
		}

		if ( !dryRun )
		{
			switch ( clearMode )
			{
				case CLEAR_ALL_CORRESPONDENCES:
				{
					// one batch of the store for the whole run, committed at the end; the XML itself does not change in this mode
					final InterestPointsZarrStore store = InterestPointsZarrStore.get( dataGlobal.getBasePathURI() );
					store.beginBatch();
					int listsCleared = 0;
					try
					{
						for ( final Entry<ViewId, ViewInterestPointLists> ip : ips.entrySet() )
						{
							if ( !isSelected( restriction, ip.getKey() ) )
								continue;

							for ( final Entry<String, InterestPoints> points : ip.getValue().getHashMap().entrySet() )
							{
								if ( !silent )
									System.out.println( "Clearing " + Group.pvid( ip.getKey() ) + ", '" + points.getKey() + "' ... " );

								// Empty list directly — does not require loading existing correspondences first.
								points.getValue().setCorrespondingInterestPoints( new ArrayList<>() );
								points.getValue().saveCorrespondingInterestPoints( true );
								listsCleared++;
							}
						}
						System.out.println( "Cleared correspondences of " + listsCleared + " (view, label) list(s)." );

						if ( restriction != null )
						{
							// Unselected views may still reference points in the selection — drop those links too
							// and persist exactly the lists that changed.
							final int correspondencesDropped = CorrespondenceTools.removeCorrespondencesToViews( vip, restriction, numThreads );
							if ( correspondencesDropped > 0 )
								for ( final Entry<ViewId, ViewInterestPointLists> ip : ips.entrySet() )
									if ( !restriction.contains( ip.getKey() ) )
										for ( final InterestPoints points : ip.getValue().getHashMap().values() )
											if ( points.hasModifiedCorrespondingInterestPoints() )
												points.saveCorrespondingInterestPoints( true );
							System.out.println( "Removed " + correspondencesDropped + " correspondence(s) in unselected views that pointed into the selection." );
						}

						store.commit();
					}
					finally
					{
						store.endBatch();
					}
					break;
				}
				case FIX_INTERESTPOINTS:
				{
					// (a) Drop every correspondence whose target ViewId is no longer in the SpimData's
					// set of present, non-missing views. Filtering against the canonical valid set
					// (rather than against the orphan-IP-map subset) catches both:
					//   - refs to orphan views (still have IP map entries, no ViewSetup), and
					//   - refs to views deleted entirely from the XML (no IP map entry left at all).
					final int correspondencesDropped = CorrespondenceTools.removeCorrespondencesNotToViews(
							vip,
							validViewIds,
							numThreads );
					System.out.println( "Filtered " + correspondencesDropped + " correspondence(s) referencing views no longer in the SpimData." );

					// (b) Delete the entries of the orphan views in the store (committed by the XML save below), then the
					// per-view groups of a legacy interestpoints.n5 whose decoded ViewId is not in the valid set (also
					// stale directories left over after a view was edited out of the XML entirely).
					int n5GroupsRemoved = 0;
					for ( final ViewId viewId : orphanViewIds )
						n5GroupsRemoved += deleteLists( ips.get( viewId ), silent ? null : "  removing the lists of orphan " + Group.pvid( viewId ) );
					n5GroupsRemoved += removeLegacyViewGroups( containerUri, viewId -> !validViewIds.contains( viewId ), silent );
					System.out.println( "Removed " + n5GroupsRemoved + " stale entry/entries for views no longer in the SpimData." );

					// (c) Drop orphan in-memory XML map entries.
					for ( final ViewId viewId : orphanViewIds )
						ips.remove( viewId );

					// (d) Filtered labels whose base label is gone from their view cannot be read; drop them (and the
					// labels filtered from those), plus the correspondences that point at them.
					final int orphanFilteredRemoved = removeOrphanFilteredLabels( vip, numThreads, silent );

					if ( !orphanViewIds.isEmpty() || correspondencesDropped > 0 || n5GroupsRemoved > 0 || orphanFilteredRemoved > 0 )
					{
						// Save XML — also persists the in-memory correspondence edits via
						// XmlIoSpimData2.saveInterestPointsInParallel.
						System.out.println( "Saving XML ..." );
						new XmlIoSpimData2().save( dataGlobal, xmlURI );
					}
					break;
				}
				case ADD_LABEL:
				{
					// Iterate the selected (present, non-missing) views — never add labels to orphans.
					final List< ViewId > targetViews = new ArrayList<>( selected );
					Collections.sort( targetViews );
					int addedTo = 0;
					final URI baseDir = dataGlobal.getBasePathURI();
					for ( final ViewId viewId : targetViews )
					{
						final ViewInterestPointLists vipl = vip.getViewInterestPointLists( viewId );
						if ( vipl.contains( label ) )
							continue; // already has it — no-op

						if ( !silent )
							System.out.println( "  adding '" + label + "' to " + Group.pvid( viewId ) );

						final InterestPoints newIps = InterestPoints.newInstance( baseDir, viewId, label );
						newIps.setInterestPoints( new ArrayList<>() );
						newIps.setCorrespondingInterestPoints( new ArrayList<>() );
						newIps.setParameters( "Added by clear-interestpoints --clearMode ADD_LABEL --label " + label );

						// the XML save below stores the new (modified) list, as an empty entry of the store
						vipl.addInterestPointList( label, newIps );
						addedTo++;
					}
					System.out.println( "Added label '" + label + "' to " + addedTo + " view(s); skipped " + ( targetViews.size() - addedTo ) + " that already had it." );

					if ( addedTo > 0 )
					{
						System.out.println( "Saving XML ..." );
						new XmlIoSpimData2().save( dataGlobal, xmlURI );
					}
					break;
				}
				case REMOVE_LABEL:
				{
					// (a) For every selected view that has the label: delete the (view, label) entry of the store (committed by
					// the XML save below) AND the in-memory XML map entry. Without a restriction this also covers orphan IP-map
					// entries, as before.
					final List< ViewId > viewsWithLabel = new ArrayList<>();
					for ( final Entry< ViewId, ViewInterestPointLists > entry : ips.entrySet() )
						if ( isSelected( restriction, entry.getKey() ) && entry.getValue().contains( label ) )
							viewsWithLabel.add( entry.getKey() );
					Collections.sort( viewsWithLabel );

					// the filtered labels of the label (and of those) go with it, as in the BigStitcher GUI: without their base they cannot be read
					final TreeSet< String > dependants = new TreeSet<>();
					int dependantsRemoved = 0;

					for ( final ViewId viewId : viewsWithLabel )
					{
						for ( final String dependant : InterestPointTools.dependentLabels( ips.get( viewId ), label ) )
						{
							if ( !silent )
								System.out.println( "  removing '" + dependant + "' (filtered from '" + label + "') from " + Group.pvid( viewId ) );

							removeLabel( ips.get( viewId ), dependant );
							dependants.add( dependant );
							++dependantsRemoved;
						}

						if ( !silent )
							System.out.println( "  removing '" + label + "' from " + Group.pvid( viewId ) );

						removeLabel( ips.get( viewId ), label );
					}
					System.out.println( "Removed label '" + label + "' from " + viewsWithLabel.size() + " view(s)" + ( dependants.isEmpty() ? "." : ", with the filtered label(s) " + dependants + " of it (" + dependantsRemoved + " list(s))." ) );

					// (b) Drop correspondences pointing AT the just-removed (view, label) lists from every
					// remaining list. Label-wide when unrestricted (legacy path); otherwise only links into
					// the selected views — other views keeping the label must stay linked to each other.
					int correspondencesDropped = ( restriction == null )
							? CorrespondenceTools.removeCorrespondencesToLabel( vip, label, numThreads )
							: removeCorrespondencesToViewsWithLabel( vip, restriction, label, numThreads );
					for ( final String dependant : dependants )
						correspondencesDropped += ( restriction == null )
								? CorrespondenceTools.removeCorrespondencesToLabel( vip, dependant, numThreads )
								: removeCorrespondencesToViewsWithLabel( vip, restriction, dependant, numThreads );
					System.out.println( "Filtered " + correspondencesDropped + " correspondence(s) referencing label '" + label + "'" + ( dependants.isEmpty() ? "" : " or " + dependants ) + ( restriction == null ? "" : " in the selected views" ) + "." );

					// (c) Save XML if anything changed.
					if ( !viewsWithLabel.isEmpty() || correspondencesDropped > 0 )
					{
						System.out.println( "Saving XML ..." );
						new XmlIoSpimData2().save( dataGlobal, xmlURI );
					}
					break;
				}
				case CLEAR_EVERYTHING:
				default:
				{
					if ( restriction == null )
					{
						// Legacy fast path: wipe everything, including orphan entries.
						System.out.println( "Saving XML (metadata only) ..." );

						ips.clear();
						new XmlIoSpimData2().save( dataGlobal, xmlURI );

						final URI storeUri = URITools.toURI( URITools.appendName( dataGlobal.getBasePathURI(), InterestPointsZarrStore.ZARR_CONTAINER ) );
						System.out.println( "Removing interest point store '" + storeUri + "' ... " );

						try ( final N5Writer n5Writer = URITools.instantiateN5Writer( StorageFormat.ZARR, storeUri ) )
						{
							n5Writer.remove();
						}

						if ( URITools.getKeyValueAccess( containerUri ).exists( URITools.getKeyValueAccess( containerUri ).compose( containerUri, "attributes.json" ) ) )
						{
							System.out.println( "Removing legacy interest point directory '" + containerUri + "' ... " );

							try ( final N5Writer n5Writer = URITools.instantiateN5Writer( StorageFormat.N5, containerUri ) )
							{
								n5Writer.remove();
							}
						}
					}
					else
					{
						// (a) Drop links from unselected views into the selection.
						final int correspondencesDropped = CorrespondenceTools.removeCorrespondencesToViews( vip, restriction, numThreads );
						System.out.println( "Removed " + correspondencesDropped + " correspondence(s) in unselected views that pointed into the selection." );

						// (b) Delete all entries of the selected views in the store (committed by the XML save below), then
						// the per-view groups of a legacy interestpoints.n5 of selected views without an XML entry.
						int n5GroupsRemoved = 0;
						for ( final Entry< ViewId, ViewInterestPointLists > ip : ips.entrySet() )
							if ( restriction.contains( ip.getKey() ) )
								n5GroupsRemoved += deleteLists( ip.getValue(), silent ? null : "  removing the lists of " + Group.pvid( ip.getKey() ) );
						n5GroupsRemoved += removeLegacyViewGroups( containerUri, viewId -> restriction.contains( viewId ) && !ips.containsKey( viewId ), silent );
						System.out.println( "Removed " + n5GroupsRemoved + " (view, label) entry/entries." );

						// (c) Drop the XML map entries of the selected views.
						int entriesRemoved = 0;
						for ( final ViewId viewId : restriction )
							if ( ips.remove( viewId ) != null )
								entriesRemoved++;
						System.out.println( "Removed " + entriesRemoved + " view entry/entries from the interest-point map." );

						if ( correspondencesDropped > 0 || n5GroupsRemoved > 0 || entriesRemoved > 0 )
						{
							// Save XML — also persists the pruned correspondence lists of the unselected views.
							System.out.println( "Saving XML ..." );
							new XmlIoSpimData2().save( dataGlobal, xmlURI );
						}
						else
						{
							System.out.println( "Nothing to remove for the selected views." );
						}
					}
					break;
				}
			}
		}

		System.out.println( "done" );

		return null;
	}

	public static void main(final String... args)
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new ClearInterestPoints()).execute(args));
	}

	/** Removes one (view, label): its entry in the store (points and correspondences, committed by the next XML save, and a legacy N5 group now) and the entry of the XML map */
	private static void removeLabel( final ViewInterestPointLists vipl, final String label )
	{
		final InterestPoints list = vipl.getInterestPointList( label );

		if ( list != null )
		{
			list.deleteInterestPoints();
			list.deleteCorrespondingInterestPoints();
		}

		vipl.getHashMap().remove( label );
	}

	/**
	 * Drops every filtered label (VirtualFilteredInterestPoints) whose base label is missing in its view, with the labels
	 * filtered from it, and the correspondences of other lists that point at the removed (view, label)s
	 *
	 * @return the number of (view, label) lists removed
	 */
	private static int removeOrphanFilteredLabels( final ViewInterestPoints vip, final int numThreads, final boolean silent )
	{
		int removed = 0;
		int correspondencesDropped = 0;

		for ( final Entry< ViewId, ViewInterestPointLists > entry : vip.getViewInterestPoints().entrySet() )
		{
			final ViewInterestPointLists vipl = entry.getValue();
			final TreeSet< String > orphans = new TreeSet<>();

			for ( final Entry< String, InterestPoints > list : vipl.getHashMap().entrySet() )
				if ( list.getValue() instanceof VirtualFilteredInterestPoints && !vipl.contains( ( (VirtualFilteredInterestPoints)list.getValue() ).getBaseLabel() ) )
				{
					orphans.add( list.getKey() );
					orphans.addAll( InterestPointTools.dependentLabels( vipl, list.getKey() ) );
				}

			for ( final String orphan : orphans )
			{
				if ( !silent )
					System.out.println( "  removing '" + orphan + "' from " + Group.pvid( entry.getKey() ) + " (filtered from '" + ( (VirtualFilteredInterestPoints)vipl.getInterestPointList( orphan ) ).getBaseLabel() + "', which is gone)" );

				// the links to the orphan first (the removal needs the list to still be there), then the list
				correspondencesDropped += CorrespondenceTools.removeCorrespondencesForViewLabel( vip, entry.getKey(), orphan, numThreads );
				removeLabel( vipl, orphan );
				++removed;
			}
		}

		System.out.println( "Removed " + removed + " filtered label list(s) whose base label is gone, and " + correspondencesDropped + " correspondence(s) referencing them." );

		return removed;
	}
}
