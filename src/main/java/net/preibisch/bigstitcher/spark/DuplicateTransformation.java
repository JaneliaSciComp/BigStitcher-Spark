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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewRegistrations;
import mpicbg.spim.data.registration.ViewTransform;
import mpicbg.spim.data.sequence.Angle;
import mpicbg.spim.data.sequence.Channel;
import mpicbg.spim.data.sequence.Illumination;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.Tile;
import mpicbg.spim.data.sequence.TimePoint;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.legacy.io.IOFunctions;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * Duplicates view transformations across one dimension (timepoints, channels,
 * illuminations, angles, or tiles) of a BigStitcher dataset.
 *
 * Equivalent to the Duplicate_Transformation Fiji plugin but usable from the
 * command line without a GUI.
 */
public class DuplicateTransformation extends AbstractBasic implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = 3818906481255152528L;

	public enum DuplicateMode { TIMEPOINTS, CHANNELS, ILLUMINATIONS, ANGLES, TILES }

	public enum TransformationsMode
	{
		/** Reset the target registration to identity and copy all transforms from the source. */
		REPLACE_ALL,
		/** Prepend only the most-recently-applied (first in list) transform from the source. */
		ADD_LAST,
		/** Prepend the first N transforms from the source (see --numTransformations). */
		ADD_N
	}

	@Option(names = { "--duplicateMode" }, required = true,
			description = "which dimension to duplicate across: TIMEPOINTS, CHANNELS, ILLUMINATIONS, ANGLES, or TILES")
	protected DuplicateMode duplicateMode = null;

	@Option(names = { "--sourceId" }, required = true,
			description = "integer id of the source timepoint/channel/illumination/angle/tile (the view whose transforms will be copied)")
	protected int sourceId;

	@Option(names = { "--targetIds" },
			description = "comma-separated integer ids of the target timepoints/channels/etc to receive the transforms (default: all entities of that dimension except the source)")
	protected String targetIdsString = null;

	@Option(names = { "--transformationsMode" },
			description = "which transforms to copy: REPLACE_ALL, ADD_LAST, or ADD_N (default: REPLACE_ALL)")
	protected TransformationsMode transformationsMode = TransformationsMode.REPLACE_ALL;

	@Option(names = { "--numTransformations" },
			description = "number of transforms to copy when --transformationsMode is ADD_N (default: 2)")
	protected int numTransformations = 2;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		final SpimData2 data = loadSpimData2();

		if ( data == null )
		{
			System.out.println( "Could not load SpimData XML. Stopping." );
			return null;
		}

		// Convert TransformationsMode to the int convention used by the core method:
		//   0 = REPLACE_ALL, 1 = ADD_LAST, n>1 = ADD_N
		final int transformations;

		if ( transformationsMode == TransformationsMode.REPLACE_ALL )
			transformations = 0;
		else if ( transformationsMode == TransformationsMode.ADD_LAST )
			transformations = 1;
		else
			transformations = numTransformations;

		// Get all present ViewIds from the dataset
		final List<ViewId> allViewIds = data.getSequenceDescription().getViewDescriptions().entrySet().stream()
				.filter( e -> e.getValue().isPresent() )
				.map( e -> (ViewId) e.getKey() )
				.collect( Collectors.toList() );

		final SequenceDescription seq = data.getSequenceDescription();
		int countApplied = 0;

		switch ( duplicateMode )
		{
			case TIMEPOINTS:
			{
				final List< TimePoint > allTps = SpimData2.getAllTimePointsSorted( data, allViewIds );
				final TimePoint source = findById( allTps, sourceId, "timepoint" );
				if ( source == null ) return null;

				final List< TimePoint > targets = buildTargets( allTps, source, sourceId, "timepoint" );
				if ( targets == null ) return null;

				final List< Channel >      channels = SpimData2.getAllChannelsSorted( data, allViewIds );
				final List< Illumination > illums   = SpimData2.getAllIlluminationsSorted( data, allViewIds );
				final List< Angle >        angles   = SpimData2.getAllAnglesSorted( data, allViewIds );
				final List< Tile >         tiles    = SpimData2.getAllTilesSorted( data, allViewIds );

				for ( final TimePoint target : targets )
				{
					IOFunctions.println( "Applying timepoint " + source.getName() + " >>> " + target.getName() );
					++countApplied;

					for ( final Channel c : channels )
						for ( final Illumination i : illums )
							for ( final Angle a : angles )
								for ( final Tile x : tiles )
								{
									final ViewId src = SpimData2.getViewId( seq, source, c, a, i, x );
									final ViewId tgt = SpimData2.getViewId( seq, target, c, a, i, x );
									if ( src == null || tgt == null ) continue;
									duplicateTransformations( transformations, src, tgt, data );
								}
				}
				break;
			}

			case CHANNELS:
			{
				final List< Channel > allChannels = SpimData2.getAllChannelsSorted( data, allViewIds );
				final Channel source = findById( allChannels, sourceId, "channel" );
				if ( source == null ) return null;

				final List< Channel > targets = buildTargets( allChannels, source, sourceId, "channel" );
				if ( targets == null ) return null;

				final List< TimePoint >    tps    = SpimData2.getAllTimePointsSorted( data, allViewIds );
				final List< Illumination > illums  = SpimData2.getAllIlluminationsSorted( data, allViewIds );
				final List< Angle >        angles  = SpimData2.getAllAnglesSorted( data, allViewIds );
				final List< Tile >         tiles   = SpimData2.getAllTilesSorted( data, allViewIds );

				for ( final Channel target : targets )
				{
					IOFunctions.println( "Applying channel " + source.getName() + " >>> " + target.getName() );
					++countApplied;

					for ( final TimePoint t : tps )
						for ( final Illumination i : illums )
							for ( final Angle a : angles )
								for ( final Tile x : tiles )
								{
									final ViewId src = SpimData2.getViewId( seq, t, source, a, i, x );
									final ViewId tgt = SpimData2.getViewId( seq, t, target, a, i, x );
									if ( src == null || tgt == null ) continue;
									duplicateTransformations( transformations, src, tgt, data );
								}
				}
				break;
			}

			case ILLUMINATIONS:
			{
				final List< Illumination > allIllums = SpimData2.getAllIlluminationsSorted( data, allViewIds );
				final Illumination source = findById( allIllums, sourceId, "illumination" );
				if ( source == null ) return null;

				final List< Illumination > targets = buildTargets( allIllums, source, sourceId, "illumination" );
				if ( targets == null ) return null;

				final List< TimePoint > tps      = SpimData2.getAllTimePointsSorted( data, allViewIds );
				final List< Channel >   channels = SpimData2.getAllChannelsSorted( data, allViewIds );
				final List< Angle >     angles   = SpimData2.getAllAnglesSorted( data, allViewIds );
				final List< Tile >      tiles    = SpimData2.getAllTilesSorted( data, allViewIds );

				for ( final Illumination target : targets )
				{
					IOFunctions.println( "Applying illumination " + source.getName() + " >>> " + target.getName() );
					++countApplied;

					for ( final TimePoint t : tps )
						for ( final Channel c : channels )
							for ( final Angle a : angles )
								for ( final Tile x : tiles )
								{
									final ViewId src = SpimData2.getViewId( seq, t, c, a, source, x );
									final ViewId tgt = SpimData2.getViewId( seq, t, c, a, target, x );
									if ( src == null || tgt == null ) continue;
									duplicateTransformations( transformations, src, tgt, data );
								}
				}
				break;
			}

			case ANGLES:
			{
				final List< Angle > allAngles = SpimData2.getAllAnglesSorted( data, allViewIds );
				final Angle source = findById( allAngles, sourceId, "angle" );
				if ( source == null ) return null;

				final List< Angle > targets = buildTargets( allAngles, source, sourceId, "angle" );
				if ( targets == null ) return null;

				final List< TimePoint >    tps    = SpimData2.getAllTimePointsSorted( data, allViewIds );
				final List< Channel >      channels = SpimData2.getAllChannelsSorted( data, allViewIds );
				final List< Illumination > illums   = SpimData2.getAllIlluminationsSorted( data, allViewIds );
				final List< Tile >         tiles    = SpimData2.getAllTilesSorted( data, allViewIds );

				for ( final Angle target : targets )
				{
					IOFunctions.println( "Applying angle " + source.getName() + " >>> " + target.getName() );
					++countApplied;

					for ( final TimePoint t : tps )
						for ( final Channel c : channels )
							for ( final Illumination i : illums )
								for ( final Tile x : tiles )
								{
									final ViewId src = SpimData2.getViewId( seq, t, c, source, i, x );
									final ViewId tgt = SpimData2.getViewId( seq, t, c, target, i, x );
									if ( src == null || tgt == null ) continue;
									duplicateTransformations( transformations, src, tgt, data );
								}
				}
				break;
			}

			case TILES:
			{
				final List< Tile > allTiles = SpimData2.getAllTilesSorted( data, allViewIds );
				final Tile source = findById( allTiles, sourceId, "tile" );
				if ( source == null ) return null;

				final List< Tile > targets = buildTargets( allTiles, source, sourceId, "tile" );
				if ( targets == null ) return null;

				final List< TimePoint >    tps      = SpimData2.getAllTimePointsSorted( data, allViewIds );
				final List< Channel >      channels = SpimData2.getAllChannelsSorted( data, allViewIds );
				final List< Illumination > illums   = SpimData2.getAllIlluminationsSorted( data, allViewIds );
				final List< Angle >        angles   = SpimData2.getAllAnglesSorted( data, allViewIds );

				for ( final Tile target : targets )
				{
					IOFunctions.println( "Applying tile " + source.getName() + " >>> " + target.getName() );
					++countApplied;

					for ( final TimePoint t : tps )
						for ( final Channel c : channels )
							for ( final Illumination i : illums )
								for ( final Angle a : angles )
								{
									final ViewId src = SpimData2.getViewId( seq, t, c, a, i, source );
									final ViewId tgt = SpimData2.getViewId( seq, t, c, a, i, target );
									if ( src == null || tgt == null ) continue;
									duplicateTransformations( transformations, src, tgt, data );
								}
				}
				break;
			}
		}

		if ( countApplied == 0 )
		{
			System.out.println( "No transformations were applied (source equals target or no valid pairs found). Stopping." );
			return null;
		}

		if ( !dryRun )
		{
			System.out.println( "Saving XML ..." );
			new XmlIoSpimData2().save( data, xmlURI );
		}

		System.out.println( "Done." );
		return null;
	}

	/**
	 * Core transformation duplication logic (GUI-free equivalent of
	 * Duplicate_Transformation.duplicateTransformations).
	 *
	 * @param transformations
	 *            0 = replace all; 1 = add last; n &gt; 1 = add first n
	 */
	protected static void duplicateTransformations(
			final int transformations,
			final ViewId sourceViewId,
			final ViewId targetViewId,
			final SpimData2 spimData )
	{
		final ViewDescription sourceVD = spimData.getSequenceDescription().getViewDescription(
				sourceViewId.getTimePointId(), sourceViewId.getViewSetupId() );

		final ViewDescription targetVD = spimData.getSequenceDescription().getViewDescription(
				targetViewId.getTimePointId(), targetViewId.getViewSetupId() );

		if ( !sourceVD.isPresent() || !targetVD.isPresent() )
		{
			if ( !sourceVD.isPresent() )
				IOFunctions.println( "  Source viewId " + sourceViewId + " is NOT present, skipping." );
			if ( !targetVD.isPresent() )
				IOFunctions.println( "  Target viewId " + targetViewId + " is NOT present, skipping." );
			return;
		}

		final ViewRegistrations vrs = spimData.getViewRegistrations();
		final ViewRegistration vrSource = vrs.getViewRegistration( sourceViewId );
		final ViewRegistration vrTarget = vrs.getViewRegistration( targetViewId );

		if ( transformations == 0 )
		{
			// Replace all: reset target to identity, then copy every transform from source
			vrTarget.identity();
			for ( final ViewTransform vt : vrSource.getTransformList() )
			{
				IOFunctions.println( "  Concatenating model '" + vt.getName() + "': " + vt.asAffine3D() );
				vrTarget.concatenateTransform( vt );
			}
		}
		else
		{
			// Add last n transforms from source (index 0 is most-recently applied)
			final int n = Math.min( transformations, vrSource.getTransformList().size() );
			final ArrayList< ViewTransform > vts = new ArrayList<>();
			for ( int k = 0; k < n; ++k )
				vts.add( vrSource.getTransformList().get( k ) );

			// Prepend in reverse order so the overall concatenation matches the source
			for ( int k = vts.size() - 1; k >= 0; --k )
			{
				final ViewTransform vt = vts.get( k );
				IOFunctions.println( "  Adding model '" + vt.getName() + "': " + vt.asAffine3D() );
				vrTarget.preconcatenateTransform( vt );
			}
		}

		vrTarget.updateModel();
	}

	// -----------------------------------------------------------------------
	// Helpers
	// -----------------------------------------------------------------------

	/** Find an entity whose {@code getId()} matches {@code id}, or print an error and return null. */
	private static < E extends mpicbg.spim.data.generic.base.Entity > E findById(
			final List< E > entities, final int id, final String label )
	{
		for ( final E e : entities )
			if ( e.getId() == id )
				return e;

		System.out.println( "No " + label + " with id=" + id + " found in dataset. Available ids: "
				+ entities.stream().map( e -> String.valueOf( e.getId() ) ).collect( Collectors.joining( ", " ) ) );
		return null;
	}

	/**
	 * Build the target entity list.
	 * If {@code --targetIds} was specified, parse and filter that list (excluding the source).
	 * Otherwise return all entities except the source.
	 */
	private < E extends mpicbg.spim.data.generic.base.Entity > List< E > buildTargets(
			final List< E > all, final E source, final int sourceId, final String label )
	{
		final List< E > targets = new ArrayList<>();

		if ( targetIdsString != null && !targetIdsString.isEmpty() )
		{
			final List< Integer > requestedIds;
			try
			{
				requestedIds = Arrays.stream( targetIdsString.split( "," ) )
						.map( String::trim )
						.map( Integer::parseInt )
						.collect( Collectors.toList() );
			}
			catch ( NumberFormatException e )
			{
				System.out.println( "--targetIds must be a comma-separated list of integers, got: '" + targetIdsString + "'" );
				return null;
			}

			for ( final int tid : requestedIds )
			{
				if ( tid == sourceId )
				{
					System.out.println( "  Skipping target " + label + " id=" + tid + " because it equals the source." );
					continue;
				}
				final E t = findById( all, tid, label );
				if ( t == null ) return null;
				targets.add( t );
			}
		}
		else
		{
			for ( final E e : all )
				if ( !e.equals( source ) )
					targets.add( e );
		}

		if ( targets.isEmpty() )
		{
			System.out.println( "No target " + label + "s found. Stopping." );
			return null;
		}

		System.out.println( "Source " + label + ": id=" + sourceId );
		System.out.println( "Target " + label + "s: " + targets.stream()
				.map( e -> "id=" + e.getId() )
				.collect( Collectors.joining( ", " ) ) );
		System.out.println( "Transformations mode: " + transformationsMode
				+ ( transformationsMode == TransformationsMode.ADD_N ? " (n=" + numTransformations + ")" : "" ) );

		return targets;
	}

	public static void main( final String... args ) throws SpimDataException
	{
		System.out.println( Arrays.toString( args ) );
		System.exit( new CommandLine( new DuplicateTransformation() ).execute( args ) );
	}
}
