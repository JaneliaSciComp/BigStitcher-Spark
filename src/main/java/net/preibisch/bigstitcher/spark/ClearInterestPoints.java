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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;

import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.CorrespondenceTools;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsN5;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPointLists;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

public class ClearInterestPoints extends AbstractBasic
{
	public enum ClearMode
	{
		/** Remove all interest points and correspondences; wipe the interestpoints.n5 directory. */
		CLEAR_EVERYTHING,
		/** Keep interest-point detections; drop every correspondence link. */
		CLEAR_ALL_CORRESPONDENCES,
		/**
		 * Repair: drop IP entries (XML + N5) for views no longer present in the SpimData,
		 * drop CorrespondingInterestPoints whose target view is no longer valid, and remove
		 * stale per-view N5 directories whose name decodes to an invalid ViewId.
		 */
		FIX_INTERESTPOINTS,
		/**
		 * Add a label (--label) to every present, non-missing view that does not already have it.
		 * Creates an empty interest-point list and an empty correspondence list, both written to N5
		 * so the entry is fully materialized. No-op for views that already have the label.
		 */
		ADD_LABEL,
		/**
		 * Remove a label (--label) entirely: drop the (view, label) entries from XML and N5 for every
		 * view that has it, then drop CorrespondingInterestPoints whose correspondingLabel equals
		 * --label from the remaining (view, otherLabel) lists.
		 */
		REMOVE_LABEL
	}

	@Option(names = { "--clearMode" }, description = "what to clear: "
			+ "CLEAR_EVERYTHING removes all interest points and correspondences and wipes the interestpoints.n5 directory; "
			+ "CLEAR_ALL_CORRESPONDENCES keeps detections but drops all correspondence links; "
			+ "FIX_INTERESTPOINTS removes IP entries (XML + N5) for views no longer in the SpimData and drops correspondences pointing to those views; "
			+ "ADD_LABEL adds an empty interest-point list with the given --label to every present, non-missing view that does not already have it; "
			+ "REMOVE_LABEL drops every (view, --label) entry and every correspondence whose correspondingLabel equals --label "
			+ "(default: CLEAR_EVERYTHING)")
	private ClearMode clearMode = ClearMode.CLEAR_EVERYTHING;

	@Option(names = { "--label" }, description = "interest-point label name. Required for --clearMode ADD_LABEL and REMOVE_LABEL.")
	private String label = null;

	@Option(names = { "--silent" }, description = "skip the per-view preamble and listing that would otherwise lazy-load every view's interest points and correspondences from N5 (file://, s3://, gs://) just to print counts. Recommended for large datasets, especially in the cloud (default: false)")
	private boolean silent = false;

	private static final long serialVersionUID = -7892604354139919145L;

	/** Top-level group name layout produced by {@code InterestPointsN5.createN5datasetPath(tpId, vsId, label)}. */
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

		final ArrayList< ViewId > viewIdsGlobal = Import.getViewIds( dataGlobal );
		Collections.sort( viewIdsGlobal );

		final Map<ViewId, ViewInterestPointLists> ips = dataGlobal.getViewInterestPoints().getViewInterestPoints();

		// Force-load every view's interest points and correspondences just to print counts below.
		// Skipped under --silent because each call lazy-reads from N5 — expensive on cloud / NFS.
		if ( !silent )
		{
			for ( final Entry<ViewId, ViewInterestPointLists> ip : ips.entrySet() )
				for ( final Entry<String, InterestPoints> points : ip.getValue().getHashMap().entrySet() )
				{
					points.getValue().getInterestPointsCopy().size();
					points.getValue().getCorrespondingInterestPointsCopy().size();
				}
		}

		// URI form so we work with any backend (file://, s3://, gs://) — not just the local FS.
		final URI containerUri = URITools.toURI( URITools.appendName( dataGlobal.getBasePathURI(), InterestPointsN5.baseN5 ) );

		switch ( clearMode )
		{
			case CLEAR_ALL_CORRESPONDENCES:
				System.out.println( "The following correspondences will be removed in ('" + containerUri + "'):");
				break;
			case FIX_INTERESTPOINTS:
				System.out.println( "Repairing interest-point map and correspondences in ('" + containerUri + "') against present views in the XML.");
				break;
			case ADD_LABEL:
				System.out.println( "Adding label '" + label + "' to every present view that does not already have it (container '" + containerUri + "').");
				break;
			case REMOVE_LABEL:
				System.out.println( "Removing label '" + label + "' from every view, plus correspondences that reference it (container '" + containerUri + "').");
				break;
			case CLEAR_EVERYTHING:
			default:
				System.out.println( "The following interest points and correspondences will be removed in ('" + containerUri + "'):");
				break;
		}

		// display all data (also lazy-loads via getInterestPointsCopy/getCorrespondingInterestPointsCopy)
		if ( !silent )
		{
			for ( final Entry<ViewId, ViewInterestPointLists> ip : ips.entrySet() )
			{
				System.out.println( Group.pvid( ip.getKey() ) + ":" );

				for ( final Entry<String, InterestPoints> points : ip.getValue().getHashMap().entrySet() )
					System.out.println( "\t'" + points.getKey() + "' " + points.getValue().getInterestPointsCopy().size() + " interest points, " + points.getValue().getCorrespondingInterestPointsCopy().size() + " correspondences." );
			}
		}

		// FIX_INTERESTPOINTS classifies orphans regardless of dryRun so the user always sees the count;
		// the actual mutations + saves are gated on !dryRun below.
		final Set< ViewId > validViewIds;
		final List< ViewId > orphanViewIds;
		if ( clearMode == ClearMode.FIX_INTERESTPOINTS )
		{
			validViewIds = new HashSet<>( Import.getViewIds( dataGlobal ) );
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
					System.out.println( "Clearing correspondences for " + ips.entrySet().size() + " views ... " );
					for ( final Entry<ViewId, ViewInterestPointLists> ip : ips.entrySet() )
					{
						for ( final Entry<String, InterestPoints> points : ip.getValue().getHashMap().entrySet() )
						{
							if ( !silent )
								System.out.println( "Clearing " + Group.pvid( ip.getKey() ) + ", '" + points.getKey() + "' ... " );

							// Empty list directly — does not require loading existing correspondences first.
							points.getValue().setCorrespondingInterestPoints( new ArrayList<>() );
							points.getValue().saveCorrespondingInterestPoints( true );
						}
					}
					break;
				case FIX_INTERESTPOINTS:
				{
					// (a) Drop every correspondence whose target ViewId is no longer in the SpimData's
					// set of present, non-missing views. Filtering against the canonical valid set
					// (rather than against the orphan-IP-map subset) catches both:
					//   - refs to orphan views (still have IP map entries, no ViewSetup), and
					//   - refs to views deleted entirely from the XML (no IP map entry left at all).
					final int correspondencesDropped = CorrespondenceTools.removeCorrespondencesNotToViews(
							dataGlobal.getViewInterestPoints(),
							validViewIds,
							Runtime.getRuntime().availableProcessors() );
					System.out.println( "Filtered " + correspondencesDropped + " correspondence(s) referencing views no longer in the SpimData." );

					// (b) Walk the interestpoints.n5 container and remove any per-view group whose
					// decoded ViewId is not in the valid set. This catches BOTH orphan-IP-map views
					// AND stale N5 directories left over after a view was edited out of the XML
					// entirely (no IP map entry to drive the GUI-style per-instance delete path).
					int n5GroupsRemoved = 0;
					try ( final N5Writer n5Writer = URITools.instantiateN5Writer( StorageFormat.N5, containerUri ) )
					{
						final String[] topLevel = n5Writer.list( "" );
						if ( topLevel != null )
						{
							for ( final String name : topLevel )
							{
								final ViewId vid = parseViewIdFromGroupName( name );
								if ( vid == null )
									continue; // unrecognized name — leave untouched
								if ( validViewIds.contains( vid ) )
									continue; // valid view — keep its data

								if ( !silent )
									System.out.println( "  removing stale N5 group '" + name + "'" );
								n5Writer.remove( name );
								n5GroupsRemoved++;
							}
						}
					}
					System.out.println( "Removed " + n5GroupsRemoved + " stale N5 group(s) for views no longer in the SpimData." );

					// (c) Drop orphan in-memory XML map entries. (b) already wiped any matching N5
					// dirs, so no per-instance N5 delete is needed here.
					for ( final ViewId viewId : orphanViewIds )
						ips.remove( viewId );

					if ( !orphanViewIds.isEmpty() || correspondencesDropped > 0 || n5GroupsRemoved > 0 )
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
					// Iterate present, non-missing views — never add labels to orphans.
					final List< ViewId > presentViews = Import.getViewIds( dataGlobal );
					int addedTo = 0;
					try ( final N5Writer n5Writer = URITools.instantiateN5Writer( StorageFormat.N5, containerUri ) )
					{
						final URI baseDir = dataGlobal.getBasePathURI();
						for ( final ViewId viewId : presentViews )
						{
							final ViewInterestPointLists vipl = dataGlobal.getViewInterestPoints().getViewInterestPointLists( viewId );
							if ( vipl.contains( label ) )
								continue; // already has it — no-op

							if ( !silent )
								System.out.println( "  adding '" + label + "' to " + Group.pvid( viewId ) );

							final InterestPoints newIps = InterestPoints.newInstance( baseDir, viewId, label );
							newIps.setInterestPoints( new ArrayList<>() );
							newIps.setCorrespondingInterestPoints( new ArrayList<>() );
							newIps.setParameters( "Added by clear-interestpoints --clearMode ADD_LABEL --label " + label );

							// Materialize the empty datasets in N5 so the entry isn't half-empty
							// metadata-without-data.
							final InterestPointsN5 n5ips = ( InterestPointsN5 ) newIps;
							n5ips.saveInterestPoints( true, n5Writer );
							n5ips.saveCorrespondingInterestPoints( true, n5Writer );

							vipl.addInterestPointList( label, newIps );
							addedTo++;
						}
					}
					System.out.println( "Added label '" + label + "' to " + addedTo + " view(s); skipped " + ( presentViews.size() - addedTo ) + " that already had it." );

					if ( addedTo > 0 )
					{
						System.out.println( "Saving XML ..." );
						new XmlIoSpimData2().save( dataGlobal, xmlURI );
					}
					break;
				}
				case REMOVE_LABEL:
				{
					// (a) For every view that has the label: drop the per-label N5 group AND the
					// in-memory XML map entry. One open writer for all per-(view, label) removals.
					final List< ViewId > viewsWithLabel = new ArrayList<>();
					for ( final Entry< ViewId, ViewInterestPointLists > entry : ips.entrySet() )
						if ( entry.getValue().contains( label ) )
							viewsWithLabel.add( entry.getKey() );

					try ( final N5Writer n5Writer = URITools.instantiateN5Writer( StorageFormat.N5, containerUri ) )
					{
						for ( final ViewId viewId : viewsWithLabel )
						{
							if ( !silent )
								System.out.println( "  removing '" + label + "' from " + Group.pvid( viewId ) );

							// Remove the per-label group (drops both /interestpoints and /correspondences).
							final String groupPath = "tpId_" + viewId.getTimePointId() +
									"_viewSetupId_" + viewId.getViewSetupId() + "/" + label;
							if ( n5Writer.exists( groupPath ) )
								n5Writer.remove( groupPath );

							ips.get( viewId ).getHashMap().remove( label );
						}
					}
					System.out.println( "Removed label '" + label + "' from " + viewsWithLabel.size() + " view(s)." );

					// (b) Drop correspondences pointing AT the just-removed label from every
					// remaining (view, otherLabel) list.
					final int correspondencesDropped = CorrespondenceTools.removeCorrespondencesToLabel(
							dataGlobal.getViewInterestPoints(),
							label,
							Runtime.getRuntime().availableProcessors() );
					System.out.println( "Filtered " + correspondencesDropped + " correspondence(s) referencing label '" + label + "'." );

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
					System.out.println( "Saving XML (metadata only) ..." );

					dataGlobal.getViewInterestPoints().getViewInterestPoints().clear();
					new XmlIoSpimData2().save( dataGlobal, xmlURI );

					System.out.println( "Removing interest point directory '" + containerUri + "' ... " );

					try ( final N5Writer n5Writer = URITools.instantiateN5Writer( StorageFormat.N5, containerUri ) )
					{
						n5Writer.remove();
					}
					break;
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

}
