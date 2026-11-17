/*-
 * #%L
 * Spark-based parallel BigStitcher project.
 * %%
 * Copyright (C) 2021 - 2026 Developers.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import mpicbg.spim.data.registration.ViewTransform;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.mvrecon.SimulateUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import picocli.CommandLine;

/**
 * {@code clear-registrations} by index and by name pattern, on the simulated 3-view dataset with three named
 * transforms planted on view (0,1) by {@code apply-transformation}: calibration (index 0), "Mirror z" (1),
 * "Manually defined transformation (Rotation around y-axis by -57.0 degrees)" (2), "start" (3).
 */
public class TestClearRegistrations
{
	static final ViewId V0 = new ViewId( 0, 0 );
	static final ViewId V1 = new ViewId( 0, 1 );
	static final ViewId V2 = new ViewId( 0, 2 );
	static final String ROTATION = "Manually defined transformation (Rotation around y-axis by -57.0 degrees)";

	@TempDir
	Path tempDir;
	URI xml;
	int baseSize;

	@BeforeEach
	void buildFixture() throws Exception
	{
		final SpimData2 sd = SimulateUtil.setUp();
		sd.setBasePathURI( tempDir.toUri() );
		xml = tempDir.resolve( "dataset.xml" ).toUri();
		new XmlIoSpimData2().save( sd, xml );
		baseSize = names( load(), V1 ).size();
		assertEquals( 0, apply( "--mirror", "z", "-vi", "0,1" ) );
		assertEquals( 0, apply( "--rotate", "y,-57", "-vi", "0,1" ) );
		assertEquals( 0, apply( "--translate", "1,2,3", "--name", "start", "-vi", "0,1" ) );
		final List< String > n = names( load(), V1 );
		assertEquals( baseSize + 3, n.size() );
		assertEquals( "start", n.get( 0 ) );
		assertEquals( ROTATION, n.get( 1 ) );
		assertEquals( "Mirror z", n.get( 2 ) );
	}

	SpimData2 load() throws Exception
	{
		return new XmlIoSpimData2().load( xml );
	}

	int apply( final String... args )
	{
		return new CommandLine( new ApplyTransformation() ).execute( withXml( args ) );
	}

	int clear( final String... args )
	{
		return new CommandLine( new ClearRegistrations() ).execute( withXml( args ) );
	}

	String[] withXml( final String... args )
	{
		final String[] all = new String[ args.length + 2 ];
		all[ 0 ] = "-x";
		all[ 1 ] = tempDir.resolve( "dataset.xml" ).toString();
		System.arraycopy( args, 0, all, 2, args.length );
		return all;
	}

	/** the names in list order: position 0 is applied last */
	static List< String > names( final SpimData2 d, final ViewId v )
	{
		final List< String > n = new ArrayList<>();
		for ( final ViewTransform t : d.getViewRegistrations().getViewRegistration( v ).getTransformList() )
			n.add( t.getName() );
		return n;
	}

	@Test
	public void byIndexInOrderOfApplication() throws Exception
	{
		// index 1 = the second applied = "Mirror z" (the base chain of the fixture has baseSize transforms before it)
		final int mirrorIndex = baseSize;
		assertEquals( 0, clear( "--index", Integer.toString( mirrorIndex ), "-vi", "0,1" ) );
		List< String > n = names( load(), V1 );
		assertEquals( baseSize + 2, n.size() );
		assertFalse( n.contains( "Mirror z" ) );
		assertEquals( "start", n.get( 0 ) );
		assertEquals( ROTATION, n.get( 1 ) );

		// two indices at once: the rotation (now at mirrorIndex) and "start" (the last applied)
		assertEquals( 0, clear( "--index", mirrorIndex + "," + ( baseSize + 1 ), "-vi", "0,1" ) );
		n = names( load(), V1 );
		assertEquals( baseSize, n.size() );
		assertFalse( n.contains( ROTATION ) );
		assertFalse( n.contains( "start" ) );

		// an index beyond the chain removes nothing
		assertEquals( 0, clear( "--index", "50", "-vi", "0,1" ) );
		assertEquals( baseSize, names( load(), V1 ).size() );
	}

	@Test
	public void byNameWithWildcards() throws Exception
	{
		assertEquals( 0, clear( "--name", "Mirror ?", "-vi", "0,1" ) );
		List< String > n = names( load(), V1 );
		assertEquals( baseSize + 2, n.size() );
		assertFalse( n.contains( "Mirror z" ) );

		// a pattern that matches nothing changes nothing; a literal without wildcards must match the whole name
		assertEquals( 0, clear( "--name", "Rotation around y-axis", "-vi", "0,1" ) );
		assertEquals( baseSize + 2, names( load(), V1 ).size() );
		assertEquals( 0, clear( "--name", "*nothing like this*", "-vi", "0,1" ) );
		assertEquals( baseSize + 2, names( load(), V1 ).size() );

		// two patterns: the rotation and "start"
		assertEquals( 0, clear( "--name", "*Rotation around y-axis by -57.0 degrees)", "--name", "st?rt", "-vi", "0,1" ) );
		n = names( load(), V1 );
		assertEquals( baseSize, n.size() );
		assertFalse( n.contains( ROTATION ) );
		assertFalse( n.contains( "start" ) );
	}

	@Test
	public void selectionKeepRemoveAndRefusals() throws Exception
	{
		final List< String > v0 = names( load(), V0 ), v2 = names( load(), V2 );

		// the pattern matches on (0,1) only; without a selection all views are candidates but only (0,1) has it
		assertEquals( 0, clear( "--name", "Manually defined*" ) );
		assertEquals( baseSize + 2, names( load(), V1 ).size() );
		assertEquals( v0, names( load(), V0 ) );
		assertEquals( v2, names( load(), V2 ) );

		// --keep and --remove still work
		assertEquals( 0, clear( "--remove", "1", "-vi", "0,1" ) );
		assertEquals( baseSize + 1, names( load(), V1 ).size(), "the last applied ('start') removed" );
		assertFalse( names( load(), V1 ).contains( "start" ) );
		assertEquals( 0, clear( "--keep", Integer.toString( baseSize ), "-vi", "0,1" ) );
		assertEquals( baseSize, names( load(), V1 ).size() );

		// refusals: none or two modes, negative values; a dry run
		assertEquals( 0, apply( "--mirror", "z", "-vi", "0,1" ) );
		assertEquals( 0, clear( "-vi", "0,1" ) );
		assertEquals( baseSize + 1, names( load(), V1 ).size() );
		assertEquals( 0, clear( "--keep", "1", "--index", "0", "-vi", "0,1" ) );
		assertEquals( baseSize + 1, names( load(), V1 ).size() );
		assertEquals( 0, clear( "--index", "-1", "-vi", "0,1" ) );
		assertEquals( baseSize + 1, names( load(), V1 ).size() );
		assertEquals( 0, clear( "--name", "Mirror*", "-vi", "0,1", "--dryRun" ) );
		assertEquals( baseSize + 1, names( load(), V1 ).size() );
		assertTrue( names( load(), V1 ).contains( "Mirror z" ) );
	}

	@Test
	public void globs()
	{
		assertTrue( ClearRegistrations.globToPattern( "Mirror *" ).matcher( "Mirror z" ).matches() );
		assertTrue( ClearRegistrations.globToPattern( "*" ).matcher( "" ).matches() );
		assertTrue( ClearRegistrations.globToPattern( "a?c" ).matcher( "abc" ).matches() );
		assertFalse( ClearRegistrations.globToPattern( "a?c" ).matcher( "abbc" ).matches() );
		assertFalse( ClearRegistrations.globToPattern( "Mirror" ).matcher( "Mirror z" ).matches() );
		// regex metacharacters in names are literal
		assertTrue( ClearRegistrations.globToPattern( "*(Translation [1.0, 2.0, 3.0])" ).matcher( "Manually defined transformation (Translation [1.0, 2.0, 3.0])" ).matches() );
		assertFalse( ClearRegistrations.globToPattern( "a.c" ).matcher( "abc" ).matches() );
	}
}
