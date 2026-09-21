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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import picocli.CommandLine;

/** The {@code java -jar} dispatcher: registers every command under its shim name, help and version work. */
public class TestBigStitcherSpark
{
	/** Mirrors the install_command lines of the {@code install} script. */
	static final Set< String > SHIM_NAMES = new LinkedHashSet<>( Arrays.asList(
			"resave", "detect-interestpoints", "match-interestpoints", "stitching", "solver",
			"match-intensities", "solve-intensities", "create-fusion-container", "fusion", "nonrigid-fusion",
			"split-images", "filter-views", "downsample", "clear-interestpoints", "clear-registrations",
			"transform-points", "overlay-landmarks" ) );

	@Test
	public void everyShimCommandIsRegistered()
	{
		final CommandLine cl = BigStitcherSpark.commandLine();
		assertEquals( SHIM_NAMES, cl.getSubcommands().keySet() );
		assertEquals( 17, SHIM_NAMES.size() );
	}

	@Test
	public void helpAndVersionWork()
	{
		assertEquals( 0, BigStitcherSpark.commandLine().execute( "--version" ) );
		assertEquals( 0, BigStitcherSpark.commandLine().execute( "--help" ) );
		assertEquals( 0, BigStitcherSpark.commandLine().execute( "filter-views", "--help" ) );
		assertEquals( 0, BigStitcherSpark.commandLine().execute( "detect-interestpoints", "--version" ) );
	}

	@Test
	public void missingSubcommandIsAUsageError()
	{
		// picocli's exit code for ParameterException
		assertEquals( 2, BigStitcherSpark.commandLine().execute() );
		assertEquals( 2, BigStitcherSpark.commandLine().execute( "no-such-command" ) );
	}

	@Test
	public void subcommandUsageListsItsOwnOptions()
	{
		final String usage = BigStitcherSpark.commandLine().getSubcommands().get( "clear-registrations" ).getUsageMessage();
		assertTrue( usage.contains( "--keep" ) && usage.contains( "--remove" ) && usage.contains( "-vi" ), usage );
	}
}
