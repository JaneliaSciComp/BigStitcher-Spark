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

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.plugin.Embed_Interest_Points;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.actionhistory.ActionHistoryRecorder;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointArrays;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPointLists;
import net.preibisch.mvrecon.process.interestpointdetection.InterestPointTools;
import net.preibisch.mvrecon.process.interestpointembedding.EmbeddingModel;
import net.preibisch.mvrecon.process.interestpointembedding.InterestPointEmbedding;
import net.preibisch.mvrecon.process.interestpointembedding.ModelHub;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import scala.Tuple3;

/**
 * Computes the embeddings of a label's interest points with a model and stores them as the per-point array of the label
 * in {@code interestpoints.zarr}, what the BigStitcher GUI's "Compute interest point embeddings (ONNX)" and {@code pcmatch embed} do: one unit
 * vector per point of the (base) label, which {@code match-interestpoints -m EMBEDDINGS} matches on. One Spark task per
 * view: it loads the dataset, the model (pcmatch's ONNX export with its {@code .onnx.json} sidecar, evaluated in Java,
 * {@code mvrecon.process.interestpointembedding}; a local file, or a model of the Hugging Face Hub, {@code hf://...}, which the
 * driver pins to its commit and every executor downloads once into its cache, {@link ModelHub}) and the view's points, samples the cubes from the image pyramid, runs the
 * network block by block and writes the view's rows into the array ({@code setup<S>}); the driver creates the array
 * (replacing one of another model) before the tasks run and records the action in the XML afterwards. The arrays are not
 * part of the store's generations, so the tasks need no staging and no commit.
 *
 * @author Stephan Preibisch
 */
public class SparkInterestPointEmbedding extends AbstractSelectableViews implements Callable< Void >, Serializable
{
	private static final long serialVersionUID = 1L;

	/** the {@code meta.writer} of the arrays this command writes */
	public static final String WRITER = "BigStitcher-Spark embed-interestpoints (Java ONNX evaluator)";

	@Option(names = { "-l", "--label" }, required = true, description = "the interest points to embed, e.g. beads; a filtered label is embedded through its base label (all points of the base, stored under the base)")
	protected String label = null;

	@Option(names = { "--model" }, required = true, description = "the model: pcmatch's 'export-onnx' file (.onnx) with its '.onnx.json' sidecar next to it (a path every executor can read), or a model of the Hugging Face Hub, hf://<namespace>/<name>[@<revision>][/<file>] (e.g. hf://JaneliaSciComp/bigstitcher-embedding-translation-stack64; the revision defaults to main and is pinned to its commit, every executor downloads the model once into --modelCache)")
	protected String model = null;

	@Option(names = { "--modelCache" }, description = "the directory hub models are downloaded to (default: the system property " + ModelHub.CACHE_PROPERTY + ", else ~/.bigstitcher/models on every node); a shared directory on a cluster, or download the model once and pass its path as --model")
	protected String modelCache = null;

	@Option(names = { "--arrayName" }, description = "the per-point array the vectors are stored in (default: " + InterestPointArrays.EMBEDDING + "); an array of another model under that name is replaced")
	protected String arrayName = InterestPointArrays.EMBEDDING;

	@Option(names = { "--blockSize" }, description = "points per inference call (default: " + 128 + ")")
	protected int blockSize = InterestPointEmbedding.defaultBlockSize;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIdsGlobal = this.loadViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			return null;

		if ( blockSize < 1 )
		{
			System.err.println( "ERROR: --blockSize must be positive, got " + blockSize + "." );
			return null;
		}

		if ( arrayName == null || arrayName.isEmpty() || arrayName.contains( "/" ) )
		{
			System.err.println( "ERROR: --arrayName '" + arrayName + "' is empty or contains '/'." );
			return null;
		}

		// the model once on the driver: a hub model is pinned to its commit here, its sidecar and graph are checked, every task loads it again
		final Path cacheDir = modelCache == null ? ModelHub.defaultCacheDir() : Paths.get( modelCache );
		final ModelHub.Local driverLocal;
		final EmbeddingModel driverModel;
		try
		{
			driverLocal = ModelHub.localModel( model, cacheDir, ModelHub.defaultTimeout );
			driverModel = EmbeddingModel.load( driverLocal.path() );
		}
		catch ( final IOException e )
		{
			System.err.println( "ERROR: cannot load the model '" + model + "': " + e.getMessage() );
			return null;
		}
		final String modelSpec = driverLocal.spec(); // the pinned hf:// URI, or the absolute path

		final List< ViewId > views = InterestPointTools.viewsWithLabel( dataGlobal, label, viewIdsGlobal );

		if ( views.isEmpty() )
		{
			System.err.println( "ERROR: none of the " + viewIdsGlobal.size() + " selected view(s) has interest points '" + label + "'." );
			return null;
		}

		if ( views.size() < viewIdsGlobal.size() )
			System.out.println( "NOTE: " + ( viewIdsGlobal.size() - views.size() ) + " of the selected views have no interest points '" + label + "' and are skipped." );

		System.out.println( "label: " + label );
		System.out.println( "model: " + modelSpec + " = " + driverModel );
		System.out.println( "arrayName: " + arrayName );
		System.out.println( "blockSize: " + blockSize );
		System.out.println( "views: " + views.size() );

		// the arrays of the (base label, timepoint) groups, created or replaced once, before the tasks write their setups
		if ( !dryRun )
		{
			final HashSet< String > prepared = new HashSet<>();
			for ( final ViewId view : views )
			{
				final ViewInterestPointLists lists = dataGlobal.getViewInterestPoints().getViewInterestPointLists( view );
				final String base = InterestPointArrays.baseLabel( lists, label );
				final URI baseDir = lists.getInterestPointList( base ).getBasePath();
				if ( prepared.add( baseDir + "|" + base + "|" + view.getTimePointId() ) )
					InterestPointEmbedding.prepareArray( baseDir, base, view.getTimePointId(), arrayName, driverModel.contract(), WRITER );
			}
		}

		final SparkConf conf = new SparkConf().setAppName( "SparkInterestPointEmbedding" );

		if ( localSparkBindAddress )
		{
			conf.set( "spark.driver.bindAddress", "127.0.0.1" );
			conf.set( "spark.driver.host", "localhost" );
			org.apache.spark.util.Utils.setCustomHostname( "localhost" );
		}

		final JavaSparkContext sc = new JavaSparkContext( conf );
		sc.setLogLevel( "ERROR" );

		final URI xmlURI = this.xmlURI;
		final String label = this.label, model = modelSpec, arrayName = this.arrayName, cache = cacheDir.toString();
		final int blockSize = this.blockSize;
		final boolean dryRun = this.dryRun;

		final long time = System.currentTimeMillis();

		final List< Tuple3< ViewId, Integer, Double > > results = sc.parallelize( Spark.toViewIds( views ), Math.min( Spark.maxPartitions, views.size() ) ).map( view ->
		{
			final SpimData2 data = Spark.getSparkJobSpimData2( xmlURI );
			final EmbeddingModel taskModel = EmbeddingModel.load( ModelHub.localModel( model, Paths.get( cache ), ModelHub.defaultTimeout ).path() );

			final InterestPointEmbedding.Result result = InterestPointEmbedding.embedView( data, view, label, taskModel, blockSize, null );

			if ( !dryRun )
			{
				final ViewInterestPointLists lists = data.getViewInterestPoints().getViewInterestPointLists( view );
				InterestPointEmbedding.write( lists.getInterestPointList( result.baseLabel() ).getBasePath(), arrayName, result );
			}

			return new Tuple3<>( view, result.size(), result.seconds() );
		} ).collect();

		long points = 0;
		for ( final Tuple3< ViewId, Integer, Double > r : results )
		{
			System.out.println( Group.pvid( r._1() ) + ": " + r._2() + " points (" + String.format( "%.1f", r._3() ) + " s)" );
			points += r._2();
		}

		System.out.println( "Embedded " + points + " points of " + results.size() + " view(s), label '" + label + "', as array '" + arrayName + "' in " + ( System.currentTimeMillis() - time ) / 1000 + " s." );

		sc.close();

		if ( !dryRun )
		{
			final LinkedHashMap< String, String > params = ActionHistoryRecorder.params();
			ActionHistoryRecorder.put( params, "label", label );
			ActionHistoryRecorder.put( params, "model", model );
			ActionHistoryRecorder.put( params, "arrayName", arrayName );
			ActionHistoryRecorder.put( params, "blockSize", blockSize );
			ActionHistoryRecorder.put( params, "fingerprint", driverModel.contract().fingerprint() );
			ActionHistoryRecorder.record( dataGlobal, Embed_Interest_Points.ACTION_ID, SparkInterestPointEmbedding.class.getName(), params, new ArrayList<>( views ), "interestpoints:" + label );

			System.out.println( "Saving XML (action history) ..." );
			new XmlIoSpimData2().save( dataGlobal, xmlURI );
		}

		System.out.println( "Done ..." );

		return null;
	}

	public static void main( final String... args ) throws Exception
	{
		System.out.println( java.util.Arrays.toString( args ) );
		System.exit( new CommandLine( new SparkInterestPointEmbedding() ).execute( args ) );
	}
}
