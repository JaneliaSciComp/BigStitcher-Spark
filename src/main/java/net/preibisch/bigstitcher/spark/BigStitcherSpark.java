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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

/**
 * Single entry point of the shaded jars: {@code java -Xmx64g -jar BigStitcher-Spark-<version>-local.jar <command> [options]}.
 *
 * Every command keeps its own {@code main} (used by the {@code install} shims and by
 * {@code spark-submit --class}); this class only registers them under the command names the shims use and
 * applies the defaults a plain {@code java -jar} start would otherwise lack: Spark local mode on all cores,
 * plus the two system properties Spark's launcher sets on Java 17/21. The module-access flags
 * ({@code --add-opens}) come from the jar manifest ({@code Add-Opens}), which the JVM honours for
 * {@code java -jar}.
 *
 * Registering the commands makes picocli reflect over their option fields, which links Spark types, so this
 * class runs only where Spark is on the classpath (the local jar, or spark-submit). From the Spark-free
 * cluster jar, call a command class directly: {@code java -cp BigStitcher-Spark-<version>-cluster.jar net.preibisch.bigstitcher.spark.Filter_Views ...}.
 */
@Command(
		name = "bigstitcher-spark",
		mixinStandardHelpOptions = true,
		versionProvider = BigStitcherSpark.Version.class,
		synopsisSubcommandLabel = "COMMAND",
		description = "Distributed BigStitcher processing with Apache Spark. Spark runs in local mode on all cores "
				+ "(spark.master=local[*]) unless -Dspark.master is given, e.g. -Dspark.master='local[8]'; "
				+ "set the memory with -Xmx, e.g. java -Xmx64g -jar BigStitcher-Spark-local.jar resave -x dataset.xml",
		footer = "%nRun 'COMMAND --help' for the options of a command." )
public class BigStitcherSpark implements Runnable
{
	/** Command names as used by the {@code install} shims, in display order, mapped to their classes. */
	public static final Map< String, Class< ? > > COMMANDS = new LinkedHashMap<>();

	static
	{
		// workflow tools
		COMMANDS.put( "resave", SparkResaveN5.class );
		COMMANDS.put( "detect-interestpoints", SparkInterestPointDetection.class );
		COMMANDS.put( "match-interestpoints", SparkGeometricDescriptorMatching.class );
		COMMANDS.put( "stitching", SparkPairwiseStitching.class );
		COMMANDS.put( "solver", Solver.class );
		COMMANDS.put( "match-intensities", SparkIntensityMatching.class );
		COMMANDS.put( "solve-intensities", IntensitySolver.class );
		COMMANDS.put( "create-fusion-container", CreateFusionContainer.class );
		COMMANDS.put( "fusion", SparkFusion.class );
		COMMANDS.put( "nonrigid-fusion", SparkNonRigidFusion.class );
		// utils
		COMMANDS.put( "split-images", SplitDatasets.class );
		COMMANDS.put( "filter-views", Filter_Views.class );
		COMMANDS.put( "downsample", SparkDownsample.class );
		COMMANDS.put( "clear-interestpoints", ClearInterestPoints.class );
		COMMANDS.put( "clear-registrations", ClearRegistrations.class );
		COMMANDS.put( "transform-points", TransformPoints.class );
		COMMANDS.put( "overlay-landmarks", OverlayLandmarks.class );
	}

	@Spec
	CommandSpec spec;

	@Override
	public void run()
	{
		throw new ParameterException( spec.commandLine(), "Missing required subcommand." );
	}

	/** The fully wired command line; commands are instantiated lazily, only when invoked. */
	public static CommandLine commandLine()
	{
		final CommandLine top = new CommandLine( new BigStitcherSpark() );
		final Version version = new Version();

		for ( final Map.Entry< String, Class< ? > > e : COMMANDS.entrySet() )
		{
			final CommandLine sub = new CommandLine( e.getValue() );
			sub.getCommandSpec().mixinStandardHelpOptions( true ).versionProvider( version );
			top.addSubcommand( e.getKey(), sub );
		}

		return top;
	}

	public static void main( final String... args )
	{
		// What the install shims and spark-submit's launcher pass as -D. Set before anything touches Spark,
		// netty or (for the reflection switch) creates the first reflective accessor.
		final Properties p = System.getProperties();
		p.putIfAbsent( "jdk.reflect.useDirectMethodHandle", "false" );
		p.putIfAbsent( "io.netty.tryReflectionSetAccessible", "true" );
		p.putIfAbsent( "spark.master", "local[*]" ); // spark-submit sets its own before calling main

		System.exit( commandLine().execute( args ) );
	}

	/** Reads the version from the shaded jar's manifest ({@code Implementation-Version}). */
	public static class Version implements IVersionProvider
	{
		@Override
		public String[] getVersion()
		{
			final String v = BigStitcherSpark.class.getPackage().getImplementationVersion();
			return new String[] {
					"BigStitcher-Spark " + ( v == null ? "(development build)" : v ),
					"Java " + System.getProperty( "java.version" ) + " (" + System.getProperty( "java.vendor" ) + ")" };
		}
	}
}
