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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewTransform;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * Command-line tool that removes view transformations from the registrations of the selected views of a
 * BigStitcher project: the last {@code --remove N} transformations, everything beyond the first {@code --keep N},
 * the ones at the given {@code --index} positions, or the ones whose {@code --name} matches a pattern with
 * {@code *} and {@code ?} as wildcards. Indices count in the order the transforms are applied (0 = the first
 * applied, usually the calibration); the current chain of every view is printed with these indices. Exactly one
 * of the four options has to be given. The removed transforms are printed and the XML is saved unless
 * {@code --dryRun} is set. Not a Spark job.
 */
public class ClearRegistrations extends AbstractSelectableViews
{
	private static final long serialVersionUID = -526317635920954010L;

	@Option(names = { "--keep" }, description = "keep only the first N transfomations (in human order of transform application, e.g. first calibration, then tile locations, then affine transformation)")
	private Integer keep = null;

	@Option(names = { "--remove" }, description = "remove the last N transfomations (in human order of transform application, e.g. first calibration, then tile locations, then affine transformation)")
	private Integer remove = null;

	@Option(names = { "--index" }, split = ",", description = "remove the transformations at these indices, counted in order of application from 0 (0 = the first applied, usually the calibration; the indices are printed for every view), e.g. --index 2 or --index 1,3")
	private List< Integer > indices = null;

	@Option(names = { "--name" }, description = "remove the transformations whose name matches this pattern, with * (any text) and ? (one character) as wildcards, e.g. --name 'Mirror *' or --name 'Manually defined*'; repeat the option for several patterns")
	private List< String > names = null;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		int given = 0;
		for ( final Object o : new Object[] { keep, remove, indices, names } )
			if ( o != null )
				++given;

		if ( given != 1 || keep != null && keep < 0 || remove != null && remove < 0 || indices != null && indices.stream().anyMatch( i -> i < 0 ) )
		{
			System.out.println( "Please specify exactly one of --keep, --remove (both >= 0), --index (>= 0) or --name." );
			return null;
		}

		final List< Pattern > patterns = new ArrayList<>();
		if ( names != null )
			for ( final String n : names )
				patterns.add( globToPattern( n ) );

		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIdsGlobal = this.loadViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			return null;

		final Map<ViewId, ViewRegistration> regs = dataGlobal.getViewRegistrations().getViewRegistrations();

		// Only touch the views the user selected (all present views when no selection flags are given).
		final Set< ViewId > selected = new HashSet<>( viewIdsGlobal );

		System.out.println( "Transformations of the " + selected.size() + " selected view(s), [index in order of application] name, matrix; the removed ones marked with 'REMOVED':");

		int numRemoved = 0;

		for ( final Entry<ViewId, ViewRegistration> reg : regs.entrySet() )
		{
			if ( !selected.contains( reg.getKey() ) )
				continue;

			System.out.println( Group.pvid( reg.getKey() ) + ":" );

			final ViewRegistration r = reg.getValue();
			final List< ViewTransform > list = r.getTransformList(); // position 0 is applied last
			final int n = list.size();

			// which list positions go
			final Set< Integer > positions = new HashSet<>();

			if ( remove != null )
			{
				for ( int pos = 0; pos < Math.min( remove, n ); ++pos )
					positions.add( pos );
			}
			else if ( keep != null )
			{
				for ( int pos = 0; pos < n - keep; ++pos )
					positions.add( pos );
			}
			else if ( indices != null )
			{
				for ( final int index : indices )
				{
					if ( index < n )
						positions.add( n - 1 - index );
					else
						System.out.println( "\tno transformation with index " + index + " (the view has " + n + ")" );
				}
			}
			else
			{
				for ( int pos = 0; pos < n; ++pos )
				{
					final String name = list.get( pos ).getName() == null ? "" : list.get( pos ).getName();
					for ( final Pattern pattern : patterns )
						if ( pattern.matcher( name ).matches() )
						{
							positions.add( pos );
							break;
						}
				}
			}

			for ( int pos = 0; pos < n; ++pos )
			{
				final ViewTransform t = list.get( pos );
				System.out.println( "\t[" + ( n - 1 - pos ) + "] " + t.getName() + ", " + t.asAffine3D() + ( positions.contains( pos ) ? "  REMOVED" : "" ) );
			}

			// remove from the back so that the positions stay valid
			final List< Integer > sorted = new ArrayList<>( positions );
			Collections.sort( sorted, Collections.reverseOrder() );
			for ( final int pos : sorted )
				list.remove( pos );

			numRemoved += sorted.size();
			r.updateModel();
		}

		System.out.println( numRemoved + " transformation(s) removed." );

		if ( numRemoved == 0 )
			return null;

		if ( !dryRun )
		{
			System.out.println( "Saving XML ..." );
			new XmlIoSpimData2().save( dataGlobal, xmlURI );
		}

		return null;
	}

	/** a glob with {@code *} (any text) and {@code ?} (one character) as a pattern that must match the whole name */
	public static Pattern globToPattern( final String glob )
	{
		final StringBuilder sb = new StringBuilder( "^" );
		for ( final char c : glob.toCharArray() )
		{
			if ( c == '*' )
				sb.append( ".*" );
			else if ( c == '?' )
				sb.append( '.' );
			else
				sb.append( Pattern.quote( String.valueOf( c ) ) );
		}
		return Pattern.compile( sb.append( '$' ).toString(), Pattern.DOTALL );
	}

	public static void main(final String... args)
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new ClearRegistrations()).execute(args));
	}

}
