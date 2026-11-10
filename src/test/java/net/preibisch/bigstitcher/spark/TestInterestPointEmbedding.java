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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.mvrecon.SimulateUtil;
import net.preibisch.mvrecon.fiji.plugin.Embed_Interest_Points;
import net.preibisch.mvrecon.fiji.plugin.Filter_Detections;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.actionhistory.ActionRecord;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointArrays;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointArrays.Vectors;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointFilter;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPointLists;
import net.preibisch.mvrecon.process.interestpointembedding.ModelHub;
import net.preibisch.mvrecon.tests.TestInterestPointDetection;
import net.preibisch.mvrecon.tests.TestModelHub;
import net.preibisch.mvrecon.tests.TestOnnxModels;
import picocli.CommandLine;

/**
 * {@code embed-interestpoints} end to end on the simulated beads with a tiny ONNX model written by mvrecon's test
 * {@code TestOnnxModels}: one Spark task per view writes the view's rows into the per-point array of the label, which the
 * matcher's reader accepts by its count rule; a filtered label is embedded through its base; a dry run writes nothing; a hub
 * model ({@code --model hf://...}, against mvrecon's {@code TestModelHub.FakeHub}) is pinned, downloaded into {@code --modelCache}
 * and recorded by its pinned URI.
 */
public class TestInterestPointEmbedding
{
	@TempDir
	Path tempDir;

	static final String LABEL = "beads";

	@Test
	public void embedsEveryView() throws Exception
	{
		System.setProperty( "spark.master", "local[*]" );

		final File xmlFile = tempDir.resolve( "dataset.xml" ).toFile();
		final SpimData2 simulated = SimulateUtil.setUp();
		new XmlIoSpimData2().save( simulated, xmlFile.toURI() );
		TestInterestPointDetection.testDoG( simulated, LABEL );
		// the detection scale the embedding needs: a parameters string as the GUI writes it
		for ( final ViewInterestPointLists lists : simulated.getViewInterestPoints().getViewInterestPoints().values() )
			if ( lists.contains( LABEL ) )
				lists.getInterestPointList( LABEL ).setParameters( "DOG s=" + TestInterestPointDetection.STANDARD_SIGMA + " t=" + TestInterestPointDetection.STANDARD_THRESHOLD + " min=false max=true downsampleX=2 downsampleY=2 downsampleZ=1 anisotropy=1.0" );
		new XmlIoSpimData2().save( simulated, xmlFile.toURI() );
		final String xml = xmlFile.getAbsolutePath();

		final Path onnx = tempDir.resolve( "tiny.onnx" );
		final TestOnnxModels.Tiny tiny = TestOnnxModels.write( onnx, new double[] { 3, 6 }, 4, 3, 5, 42 );

		// a dry run writes nothing
		run( new SparkInterestPointEmbedding(), "-x", xml, "-l", LABEL, "--model", onnx.toString(), "--dryRun", "--localSparkBindAddress" );
		SpimData2 data = new XmlIoSpimData2().load( xmlFile.toURI() );
		final URI base = data.getBasePathURI();
		assertTrue( InterestPointArrays.list( base, LABEL, 0 ).isEmpty() );

		run( new SparkInterestPointEmbedding(), "-x", xml, "-l", LABEL, "--model", onnx.toString(), "--blockSize", "37", "--localSparkBindAddress" );

		data = new XmlIoSpimData2().load( xmlFile.toURI() );
		final List< ViewId > views = new ArrayList<>();
		for ( final ViewId v : data.getViewInterestPoints().getViewInterestPoints().keySet() )
			views.add( new ViewId( v.getTimePointId(), v.getViewSetupId() ) );
		assertEquals( 3, views.size() );

		int total = 0;
		for ( final ViewId v : views )
		{
			final ViewInterestPointLists lists = data.getViewInterestPoints().getViewInterestPointLists( v );
			final int points = lists.getInterestPointList( LABEL ).getInterestPointsCopy().size();
			final Vectors vectors = InterestPointArrays.vectors( lists, LABEL, InterestPointArrays.EMBEDDING );
			assertEquals( points, vectors.size() + vectors.missing() );
			assertEquals( 5, vectors.columns() );
			assertEquals( tiny.fingerprint(), vectors.attributes().fingerprint() );
			assertEquals( SparkInterestPointEmbedding.WRITER, vectors.attributes().meta().get( "writer" ).getAsString() );
			total += points;
		}
		assertTrue( total > 30 );

		// recorded in the XML
		final List< ActionRecord > records = data.getActionHistory().getRecords();
		final ActionRecord last = records.get( records.size() - 1 );
		assertEquals( Embed_Interest_Points.ACTION_ID, last.getActionId() );
		assertEquals( LABEL, last.getParams().get( "label" ) );
		assertEquals( "37", last.getParams().get( "blockSize" ) );

		// a filtered label is embedded through its base, under another array name: all points of the base, read by the filtered label
		final ViewId first = views.get( 0 );
		final ViewInterestPointLists lists = data.getViewInterestPoints().getViewInterestPointLists( first );
		final String filtered = LABEL + "_i500";
		Filter_Detections.create( data, List.of( first ), LABEL, filtered, InterestPointFilter.fromString( "intensity=500.." ), false );
		new XmlIoSpimData2().save( data, xmlFile.toURI() );
		final int basePoints = lists.getInterestPointList( LABEL ).getInterestPointsCopy().size();
		final int filteredPoints = lists.getInterestPointList( filtered ).getInterestPointsCopy().size();
		assertTrue( filteredPoints > 0 && filteredPoints < basePoints );

		run( new SparkInterestPointEmbedding(), "-x", xml, "-l", filtered, "--model", onnx.toString(), "--arrayName", "other", "-vi", "0," + first.getViewSetupId(), "--localSparkBindAddress" );

		data = new XmlIoSpimData2().load( xmlFile.toURI() );
		final ViewInterestPointLists reloaded = data.getViewInterestPoints().getViewInterestPointLists( first );
		final Vectors viaFiltered = InterestPointArrays.vectors( reloaded, filtered, "other" );
		assertEquals( filteredPoints, viaFiltered.size() + viaFiltered.missing() );
		assertEquals( basePoints, InterestPointArrays.read( base, LABEL, 0, first.getViewSetupId(), "other" ).size() );
		assertNull( InterestPointArrays.read( base, LABEL, 0, views.get( 1 ).getViewSetupId(), "other" ) ); // only the selected view
		assertEquals( 2, InterestPointArrays.list( base, LABEL, 0 ).size() );

		// a model of the hub: pinned on the driver, downloaded into the cache (once per JVM here), recorded by its pinned URI
		final String hub = ModelHub.hub;
		try ( final TestModelHub.FakeHub fake = new TestModelHub.FakeHub() )
		{
			ModelHub.hub = fake.url();
			final Path cache = tempDir.resolve( "cache" );
			final TestOnnxModels.Tiny hubTiny = fake.addModel( "lab/stack-tiny", "0123456789abcdef0123456789abcdef01234567", "tiny.onnx", true, tempDir.resolve( "hub" ), 7 );

			run( new SparkInterestPointEmbedding(), "-x", xml, "-l", LABEL, "--model", "hf://lab/stack-tiny", "--modelCache", cache.toString(), "--arrayName", "hub", "--localSparkBindAddress" );

			data = new XmlIoSpimData2().load( xmlFile.toURI() );
			for ( final ViewId v : views )
				assertEquals( hubTiny.fingerprint(), InterestPointArrays.vectors( data.getViewInterestPoints().getViewInterestPointLists( v ), LABEL, "hub" ).attributes().fingerprint() );
			final List< ActionRecord > hubRecords = data.getActionHistory().getRecords();
			assertEquals( "hf://lab/stack-tiny@0123456789abcdef0123456789abcdef01234567/tiny.onnx", hubRecords.get( hubRecords.size() - 1 ).getParams().get( "model" ) );
			assertEquals( 1, ModelHub.cached( cache ).size() );
		}
		finally
		{
			ModelHub.hub = hub;
		}
	}

	static void run( final Object command, final String... args )
	{
		final int exit = new CommandLine( command ).execute( args );
		assertEquals( 0, exit, "exit code of " + command.getClass().getSimpleName() + " " + String.join( " ", args ) );
	}
}
