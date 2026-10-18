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
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import mpicbg.models.AbstractModel;
import mpicbg.models.Affine3D;
import mpicbg.models.Model;
import mpicbg.models.RigidModel3D;
import mpicbg.models.Tile;
import mpicbg.models.TranslationModel3D;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.multithreading.SimpleMultiThreading;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractRegistration;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.legacy.mpicbg.PointMatchGeneric;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.global.GlobalOptimizationParameters;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.global.GlobalOptimizationParameters.GlobalOptType;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.AdvancedRegistrationParameters;
import net.preibisch.mvrecon.fiji.plugin.interestpointregistration.parameters.BasicRegistrationParameters.RegistrationType;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.CorrespondingInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.stitchingresults.PairwiseStitchingResult;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import net.preibisch.mvrecon.process.interestpointregistration.global.GlobalOpt;
import net.preibisch.mvrecon.process.interestpointregistration.global.GlobalOptIterative;
import net.preibisch.mvrecon.process.interestpointregistration.global.GlobalOptTwoRound;
import net.preibisch.mvrecon.process.interestpointregistration.global.convergence.ConvergenceStrategy;
import net.preibisch.mvrecon.process.interestpointregistration.global.convergence.SimpleIterativeConvergenceStrategy;
import net.preibisch.mvrecon.process.interestpointregistration.global.linkremoval.MaxErrorLinkRemoval;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.PointMatchCreator;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.strong.ImageCorrelationPointMatchCreator;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.strong.InterestPointMatchCreator;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.weak.MetaDataWeakLinkFactory;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.PairwiseResult;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.PairwiseSetup;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.Subset;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.overlap.SimpleBoundingBoxOverlap;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * Command-line tool that runs the global optimization ("solve") of a BigStitcher dataset, turning pairwise
 * links between views into one transformation per view. The links come either from corresponding interest
 * points ({@code -s IP}, one or more labels with weights) or from the pairwise stitching results stored in
 * the XML ({@code -s STITCHING}). Views can be grouped (illuminations, channels, tiles) or split per
 * timepoint; the independent registration subsets and the default fixed views are determined exactly like
 * in the GUI, and one of the {@link GlobalOpt}, {@link GlobalOptIterative} or {@link GlobalOptTwoRound}
 * variants is run. Optionally the result of every subset is mapped back onto a reference view
 * ({@code --enableMapbackViews}, requires {@code --disableFixedViews}). The solved transformations are
 * appended to the views' {@link ViewRegistration}s and, unless {@code --dryRun} is set, the XML is saved.
 */
public class Solver extends AbstractRegistration
{
	/*
	-x '/Users/preibischs/Documents/Microscopy/SPIM/HisYFP-SPIM/Spark_test_tp/dataset.xml'
	-l beadsSpark3
	--method ONE_ROUND_SIMPLE
	-rtp ALL_TO_ALL
	-s IP
	--splitTimepoints
	--dryRun
	*/
	private static final long serialVersionUID = 5220898723968914742L;

	/** Where the pairwise links for the solve come from. */
	public enum SolverSource
	{
		/** Corresponding interest points of the given {@code --label}(s). */
		IP,
		/** Pairwise stitching results (image-correlation shifts) stored in the XML. */
		STITCHING
	};

	/** Whether the views are pre-aligned before the global optimization starts. */
	public enum PreAlign { PREALIGN, NO_PREALIGN };

	public ArrayList< ViewId > fixedViewIds;

	/** Type of transformation model used to map the solved result back onto a reference view. */
	public enum MapbackModel { TRANSLATION, RIGID };

	public ArrayList< ViewId > mapBackViewIds;

	public Model<?> mapBackModel;

	@Option(names = { "-s", "--sourcePoints" }, required = true, description = "which source to use for the solve, IP (interest points) or STITCHING")
	protected SolverSource sourcePoints = null;

	@Option(names = { "--groupIllums" }, description = "group all illumination directions that belong to the same angle/channel/tile/timepoint together as one view, e.g. to stitch illums as one (default: false for IP, true for stitching)")
	protected Boolean groupIllums = null;

	@Option(names = { "--groupChannels" }, description = "group all channels that belong to the same angle/illumination/tile/timepoint together as one view, e.g. to stitch channels as one (default: false for IP, true for stitching)")
	protected Boolean groupChannels = null;

	@Option(names = { "--groupTiles" }, description = "group all tiles that belong to the same angle/channel/illumination/timepoint together as one view, e.g. to align across angles (default: false)")
	protected Boolean groupTiles = null;

	@Option(names = { "--splitTimepoints" }, description = "group all angles/channels/illums/tiles that belong to the same timepoint as one View, e.g. for stabilization across time (default: false)")
	protected Boolean splitTimepoints = null;

	@Option(names = { "-l", "--label" }, required = false, description = "label(s) of the interest points used for registration (e.g. -l beads -l nuclei)")
	protected ArrayList<String> labels = null;

	@Option(names = { "-lw", "--labelweights" }, description = "weights of label(s) of the interest points used for registration (e.g. -l 1.0 -l 0.1, default: 1.0)")
	protected ArrayList<Double> labelweights = null;

	@Option(names = { "--method" }, description = "global optimization method; ONE_ROUND_SIMPLE, ONE_ROUND_ITERATIVE, TWO_ROUND_SIMPLE or TWO_ROUND_ITERATIVE. Two round handles unconnected tiles, iterative handles wrong links (default: ONE_ROUND_SIMPLE)")
	protected GlobalOptType globalOptType = GlobalOptType.ONE_ROUND_SIMPLE;

	@Option(names = { "-pa", "--preAlign" }, description = "whether to pre-align before solving (PREALIGN) or to initialize with the current transformations (NO_PREALIGN), (default: NO_PREALIGN)")
	protected PreAlign preAlign = PreAlign.NO_PREALIGN;

	@Option(names = { "--relativeThreshold" }, description = "relative error threshold for iterative solvers, how many times worse than the average error a link needs to be (default: 3.5)")
	protected double relativeThreshold = 3.5;

	@Option(names = { "--absoluteThreshold" }, description = "absoluted error threshold for iterative solver to drop a link in pixels (default: 7.0)")
	protected double absoluteThreshold = 7.0;

	@Option(names = { "--maxError" }, description = "max error for the solve (default: 5.0)")
	protected Double maxError = 5.0;

	@Option(names = { "--maxIterations" }, description = "max number of iterations for solve (default: 10,000)")
	protected Integer maxIterations = 10000;

	@Option(names = { "--maxPlateauwidth" }, description = "max plateau width for solve (default: 200)")
	protected Integer maxPlateauwidth = 200;

	@Option(names = { "--disableFixedViews" }, description = "disable fixing of views (see --fixedViews)")
	protected boolean disableFixedViews = false;

	@Option(names = { "-fv", "--fixedViews" }, description = "define a list of (or a single) fixed view ids (time point, view setup), e.g. -fv '0,0' -fv '0,1' (default: first view id)")
	protected String[] fixedViews = null;

	@Option(names = { "--enableMapbackViews" }, description = "enable mapping back of views (see --mapbackViews and --mapbackModel), requires --disableFixedViews.")
	protected boolean enableMapbackViews = false;

	@Option(names = { "--mapbackViews" }, description = "define a view id (time point, view setup) onto which the registration result is mapped back onto, it needs to be one per independent registration subset (e.g. timepoint) (only works if no views are fixed), e.g. --mapbackViews '0,0' (default: first view id)")
	protected String[] mapbackViews = null;

	@Option(names = { "--mapbackModel" }, description = "which transformation model to use for mapback if it is activated; TRANSLATION or RIGID (default: RIGID)")
	protected MapbackModel mapbackModelEntry = MapbackModel.RIGID;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();
		initRegistrationParameters();

		if ( !this.setupParameters( dataGlobal ) )
			return null;

		final HashMap< ViewId, HashMap< String, Double > > labelMapGlobal;

		// setup specific things for Interestpoints or Stitching as a source
		if ( sourcePoints == SolverSource.IP )
		{
			if ( labels == null || labels.isEmpty() )
			{
				System.out.println( "No labels specified. Stopping." );
				return null;
			}

			if ( labelweights == null || labelweights.isEmpty() )
			{
				labelweights = new ArrayList<>();
				labels.forEach( label -> labelweights.add( 1.0 ));
			}

			if ( labelweights.size() != labels.size() )
			{
				System.out.println( "You need to specify as many weights as labels, or do not specify weights at all" );
				return null;
			}

			final HashMap< String, Double > map = new HashMap<>();

			for ( int i = 0; i < labels.size(); ++i )
				map.put( labels.get( i ), labelweights.get( i ) ); // weights are used for the solve

			System.out.println( "labels & weights: " + map);

			labelMapGlobal = SparkGeometricDescriptorMatching.buildLabelMap( dataGlobal, viewIdsGlobal, map );

			if ( groupIllums == null )
				groupIllums = false;

			if ( groupChannels == null )
				groupChannels = false;

			if ( groupTiles == null )
				groupTiles = false;

			if ( splitTimepoints == null )
				splitTimepoints = false;
		}
		else
		{
			System.out.println( "Using stitching results as source for solve." );

			labelMapGlobal = null;

			if ( groupIllums == null )
				groupIllums = true;

			if ( groupChannels == null )
				groupChannels = true;

			if ( groupTiles == null )
				groupTiles = false;

			if ( splitTimepoints == null )
				splitTimepoints = false;
		}

		System.out.println("The following grouping/splitting modes are set: ");
		System.out.println("groupIllums: " + groupIllums);
		System.out.println("groupChannels: " + groupChannels);
		System.out.println("groupTiles: " + groupTiles);
		System.out.println("splitTimepoints: " + splitTimepoints);

		System.out.println("Other parameters: ");
		System.out.println("transformationModel: " + transformationModel );
		System.out.println("regularizationModel: " + regularizationModel + ( regularizationModel == RegularizationModel.NONE ? "" : " (lambda=" + regularizationLambda + ")" ) );
		System.out.println("method: " + globalOptType );
		System.out.println("preAlign: " + preAlign );
		System.out.println("sourcePoints: " + sourcePoints );
		System.out.println("relativeThreshold: " + relativeThreshold );
		System.out.println("absoluteThreshold: " + absoluteThreshold );
		System.out.println("maxError: " + maxError );
		System.out.println("maxIterations: " + maxIterations );
		System.out.println("maxPlateauwidth: " + maxPlateauwidth );
		System.out.println("disableFixedViews: " + disableFixedViews );
		System.out.println("fixedViews: " + ( fixedViews == null ? "null" : Arrays.toString( fixedViews ) ) );
		System.out.println("labels: " + ( labels == null ? "null" : Arrays.toString( labels.toArray() ) ));
		System.out.println("labelweights: " + ( labelweights == null ? "null" : Arrays.toString( labelweights.toArray() ) ));

		// identify subsets exactly like the GUI (strategy + bounding-box overlap + connected components)
		final PairwiseSetup< ViewId > setup = setupGroups( viewReg, AdvancedRegistrationParameters.getGroups( dataGlobal, viewIdsGlobal, groupTiles, groupIllums, groupChannels, splitTimepoints ) );
		final ArrayList< Subset< ViewId > > subsets = setup.getSubsets();

		// assemble fixed views (GUI: strategy defaults + first view per subset / user list / nothing)
		final HashSet< ViewId > fixedViewIds = new HashSet<>( setup.getDefaultFixedViews() );

		if ( !this.disableFixedViews )
		{
			if ( this.fixedViewIds == null || this.fixedViewIds.size() == 0 )
				fixedViewIds.addAll( assembleFixed( subsets ) );
			else
				fixedViewIds.addAll( this.fixedViewIds );
		}

		System.out.println("The following ViewIds are used as fixed views: ");
		fixedViewIds.forEach( vid -> System.out.print( Group.pvid( vid ) + ", ") );
		System.out.println();

		// run global optimization
		final Collection< Group< ViewId > > groups;

		if ( !groupTiles && !groupIllums && !groupChannels && !splitTimepoints )
			groups = new ArrayList<>();
		else // for grouping all we need here is the set of groups
			groups = setup.getGroups();

		// parse view setup ID comparison pairs from file (null = all-to-all)
		final HashSet< Pair< Integer, Integer > > vsComparisonPairs = parseVsComparisonsFile( vsComparisonsFile );

		final PointMatchCreator pmc;

		if ( sourcePoints == SolverSource.IP )
		{
			final SparkConf conf = new SparkConf().setAppName("SparkSolver");

			if ( localSparkBindAddress )
			{
				conf.set("spark.driver.bindAddress", "127.0.0.1");
				conf.set("spark.driver.host", "localhost");
				org.apache.spark.util.Utils.setCustomHostname("localhost");
			}

			final JavaSparkContext sc = new JavaSparkContext(conf);
			sc.setLogLevel("INFO");

			try
			{
				pmc = setupPointMatchesFromInterestPoints( xmlURI, sc, viewIdsGlobal, labelMapGlobal, groups, fixedViewIds, vsComparisonPairs );
			}
			finally
			{
				sc.close();
			}
		}
		else
			pmc = setupPointMatchesStitching(dataGlobal, viewIdsGlobal, vsComparisonPairs);

		if ( pmc == null )
		{
			System.out.println( "No views are connected, stopping." );
			return null;
		}

		final GlobalOptimizationParameters globalOptParameters = new GlobalOptimizationParameters(relativeThreshold, absoluteThreshold, globalOptType, preAlign == PreAlign.PREALIGN, false );
		final Collection< Pair< Group< ViewId >, Group< ViewId > > > removedInconsistentPairs = new ArrayList<>();
		final HashMap<ViewId, Tile > models;
		final Model<?> model = createModelInstance(transformationModel, regularizationModel, regularizationLambda);

		if ( globalOptParameters.method == GlobalOptType.ONE_ROUND_SIMPLE )
		{
			final ConvergenceStrategy cs = new ConvergenceStrategy( maxError, maxIterations, maxPlateauwidth );

			models = (HashMap<ViewId, Tile>) GlobalOpt.computeTiles(
							(Model)model,
							globalOptParameters.preAlign,
							pmc,
							cs,
							fixedViewIds,
							groups,
							false );
		}
		else if ( globalOptParameters.method == GlobalOptType.ONE_ROUND_ITERATIVE )
		{
			models = (HashMap<ViewId, Tile>) GlobalOptIterative.computeTiles(
							(Model)model,
							globalOptParameters.preAlign,
							pmc,
							new SimpleIterativeConvergenceStrategy( Double.MAX_VALUE, maxIterations, maxPlateauwidth, globalOptParameters.relativeThreshold, globalOptParameters.absoluteThreshold ),
							new MaxErrorLinkRemoval(),
							removedInconsistentPairs,
							fixedViewIds,
							groups,
							false );
		}
		else //if ( globalOptParameters.method == GlobalOptType.TWO_ROUND_SIMPLE || globalOptParameters.method == GlobalOptType.TWO_ROUND_ITERATIVE )
		{
			if ( globalOptParameters.method == GlobalOptType.TWO_ROUND_SIMPLE )
				globalOptParameters.relativeThreshold = globalOptParameters.absoluteThreshold  = Double.MAX_VALUE;

			models = (HashMap<ViewId, Tile>)GlobalOptTwoRound.computeTiles(
					(Model & Affine3D)model,
					globalOptParameters.preAlign,
					pmc,
					new SimpleIterativeConvergenceStrategy( Double.MAX_VALUE, maxIterations, maxPlateauwidth, globalOptParameters.relativeThreshold, globalOptParameters.absoluteThreshold ), // if it's simple, both will be Double.MAX
					new MaxErrorLinkRemoval(),
					removedInconsistentPairs,
					new MetaDataWeakLinkFactory(
							dataGlobal.getViewRegistrations().getViewRegistrations(),
							new SimpleBoundingBoxOverlap<>(
									dataGlobal.getSequenceDescription().getViewSetups(),
									dataGlobal.getViewRegistrations().getViewRegistrations() ) ),
					new ConvergenceStrategy( Double.MAX_VALUE, maxIterations, maxPlateauwidth ),
					fixedViewIds,
					groups,
					true );
		}

		// update models in ViewRegistration
		if ( models == null || models.isEmpty() )
		{
			System.out.println( "No transformations could be found, stopping." );
			return null;
		}

		SimpleMultiThreading.threadWait( 100 );

		System.out.println( "\nFinal models: " + models.size() );

		// mapback (same as GUI: one model per registration subset, computed from the reference view's
		// registration BEFORE the new transform is stored and its solved model), then pre-concatenated to all views
		final HashMap< ViewId, AffineTransform3D > mapBackPerView = new HashMap<>();

		if ( enableMapbackViews )
		{
			final HashMap< ViewId, ViewId > mapBackViewFor = assembleMapBack( subsets, this.mapBackViewIds );
			final HashMap< ViewId, AffineTransform3D > mapBackPerRef = new HashMap<>();

			for ( final ViewId ref : new HashSet<>( mapBackViewFor.values() ) )
			{
				final Tile< ? > refTile = models.get( ref );

				if ( refTile == null )
				{
					System.out.println( "WARNING: mapback view " + Group.pvid( ref ) + " has no model (not connected), views of this subset are NOT mapped back." );
					continue;
				}

				final AffineTransform3D mapBack = TransformationTools.computeMapBackModel(
						dataGlobal.getSequenceDescription().getViewDescription( ref ).getViewSetup().getSize(),
						dataGlobal.getViewRegistrations().getViewRegistration( ref ).getModel(),
						(AbstractModel< ? >)refTile.getModel(),
						mapBackModel.copy() );

				System.out.println( "Mapback model (" + mapBackModel.getClass().getSimpleName() + ", pre-concatenated to all views of this subset) for " + Group.pvid( ref ) + ": " + mapBack );

				if ( mapBack != null )
					mapBackPerRef.put( ref, mapBack );
			}

			mapBackViewFor.forEach( ( viewId, ref ) -> mapBackPerView.put( viewId, mapBackPerRef.get( ref ) ) );
		}

		for ( final ViewId viewId : viewIdsGlobal )
		{
			final Tile< ? extends AbstractModel< ? > > tile = models.get( viewId );
			final ViewRegistration vr = dataGlobal.getViewRegistrations().getViewRegistration( viewId );
			TransformationTools.storeTransformation( vr, viewId, tile, mapBackPerView.get( viewId ), model.getClass().getSimpleName() );
		}

		// print per-view transformations + identity-vs-non-identity summary,
		// gated by TransformationTools.maxPerViewTransformLog
		if ( mapBackPerView.isEmpty() )
		{
			TransformationTools.printAndSummarizeTransformations( viewIdsGlobal, models );
		}
		else
		{
			// print what is actually stored: mapback composed with the solved model
			System.out.println( "Final transformation models (mapback model pre-concatenated):" );
			final HashMap< ViewId, Tile< ? > > stored = new HashMap<>();

			for ( final ViewId viewId : viewIdsGlobal )
			{
				final Tile< ? > tile = models.get( viewId );
				final AffineTransform3D t = tile == null ? new AffineTransform3D() : TransformationTools.getAffineTransform( (Affine3D< ? >)tile.getModel() );
				final AffineTransform3D mapBack = mapBackPerView.get( viewId );

				if ( mapBack != null )
					t.preConcatenate( mapBack );

				stored.put( viewId, new Tile<>( TransformationTools.getModel( t ) ) );
			}

			TransformationTools.printAndSummarizeTransformations( viewIdsGlobal, stored );
		}

		if (!dryRun)
		{
			System.out.println( "Saving resulting XML ... ");
			new XmlIoSpimData2().save( dataGlobal, xmlURI );
		}

		System.out.println( "Done.");
		return null;
	}

	/**
	 * Builds the point-match creator for a solve from the pairwise stitching results stored in the dataset.
	 * Only results between selected views are used, i.e. all views of both groups of a result must be in
	 * {@code viewIdsGlobal}. If {@code vsComparisonPairs} is given, only results whose two view setup IDs
	 * form an allowed pair are kept. Results whose stored hash does not match the current
	 * {@link ViewRegistration}s of the two views (i.e. the views were transformed after stitching) are
	 * dropped with an explanatory message. Selected views without any remaining result are reported and
	 * still take part in the solve (without links), so that the two-round methods can place them using
	 * the metadata weak links, like {@link #setupPointMatchesFromInterestPoints} does for interest points.
	 *
	 * @param dataGlobal the dataset holding the stitching results and view registrations
	 * @param viewIdsGlobal the views being solved; results involving any other view are ignored
	 * @param vsComparisonPairs allowed pairs of view setup IDs as {@code (min, max)}, or {@code null} to
	 *             allow all pairs
	 * @return a creator over the remaining stitching results (plus the unconnected selected views), or
	 *         {@code null} if no link remains
	 */
	public static PointMatchCreator setupPointMatchesStitching(
			final SpimData2 dataGlobal,
			final List<ViewId> viewIdsGlobal,
			final Set< Pair< Integer, Integer > > vsComparisonPairs )
	{
		Collection< PairwiseStitchingResult< ViewId > > results = dataGlobal.getStitchingResults().getPairwiseResults().values();

		// only process links between selected views (as the GUI does), i.e. drop every result in which
		// one of the two groups contains a view that is not part of this solve
		final HashSet< ViewId > selectedViews = new HashSet<>( viewIdsGlobal );
		final int numAll = results.size();
		results = results.stream().filter( psr ->
				selectedViews.containsAll( psr.pair().getA().getViews() ) && selectedViews.containsAll( psr.pair().getB().getViews() ) )
			.collect( Collectors.toList() );
		System.out.println( "Selected views filter: kept " + results.size() + " of " + numAll + " stitching pairs." );

		if ( vsComparisonPairs != null )
		{
			final int before = results.size();
			results = results.stream().filter( psr ->
			{
				final int vsA = psr.pair().getA().getViews().iterator().next().getViewSetupId();
				final int vsB = psr.pair().getB().getViews().iterator().next().getViewSetupId();
				return vsComparisonPairs.contains( new ValuePair<>( Math.min(vsA,vsB), Math.max(vsA,vsB) ) );
			}).collect( Collectors.toList() );
			System.out.println( "vsComparisons filter: kept " + results.size() + " of " + before + " stitching pairs." );
		}

		// filter bad hashes here
		final int numLinksBefore = results.size();
		results = results.stream().filter( psr -> 
		{
			final ViewId firstVidA = psr.pair().getA().getViews().iterator().next();
			final ViewId firstVidB = psr.pair().getB().getViews().iterator().next();
			final ViewRegistration vrA = dataGlobal.getViewRegistrations().getViewRegistration( firstVidA );
			final ViewRegistration vrB = dataGlobal.getViewRegistrations().getViewRegistration( firstVidB );
			final double hash = PairwiseStitchingResult.calculateHash( vrA, vrB );
			return psr.getHash() == hash;
		}).collect( Collectors.toList() );
		final int numLinksAfter = results.size();

		if (numLinksAfter != numLinksBefore)
		{
			System.out.println("Removed " + ( numLinksBefore - numLinksAfter ) + " of " + numLinksBefore + 
					" pairwise results because the underlying view registrations have changed.");
			System.out.println("Did you try to re-run the global optimization after aligning the dataset?");
			System.out.println("In that case, you can remove the latest transformation and try again.");
		}

		if (numLinksAfter < 1)
		{
			System.out.println( new Date(System.currentTimeMillis()) + ": no links remaining, stopping.");
			return null;
		}

		final ImageCorrelationPointMatchCreator pmc = new ImageCorrelationPointMatchCreator( results );

		// ImageCorrelationPointMatchCreator only knows the views of its results, so a selected view without
		// any (remaining) stitching result would get no tile in the global optimization: one-round methods
		// store identity for it anyway, but the two-round methods could not place it using the metadata weak
		// links either. Add such views to the set of views (without links), as the interest point path does.
		final HashSet< ViewId > connectedViews = pmc.getAllViews();
		final List< ViewId > unconnectedViews =
				viewIdsGlobal.stream().filter( v -> !connectedViews.contains( v ) ).sorted().collect( Collectors.toList() );

		System.out.println( "Total number of pairs of views that are connected: " + results.size() );
		System.out.println( "Total number of views: " + viewIdsGlobal.size() );
		System.out.println( "Total number of connected views: " + connectedViews.size() + " (" + unconnectedViews.size() + " missing that are not connected)");

		if ( unconnectedViews.isEmpty() )
			return pmc;

		System.out.println( "The following views have no stitching result and are added to the global optimization without links:" );
		unconnectedViews.forEach( v -> System.out.println( "\t" + Group.pvid( v ) ) );

		final HashSet< ViewId > allViews = new HashSet<>( connectedViews );
		allViews.addAll( unconnectedViews );

		return new PointMatchCreator()
		{
			@Override
			public HashSet< ViewId > getAllViews()
			{
				return new HashSet<>( allViews ); // GlobalOpt modifies the returned set
			}

			@Override
			public < M extends Model< M > > void assignWeights(
					final HashMap< ViewId, Tile< M > > tileMap,
					final ArrayList< Group< ViewId > > groups,
					final Collection< ViewId > fixedViews )
			{
				pmc.assignWeights( tileMap, groups, fixedViews );
			}

			@Override
			public < M extends Model< M > > void assignPointMatches(
					final HashMap< ViewId, Tile< M > > tileMap,
					final ArrayList< Group< ViewId > > groups,
					final Collection< ViewId > fixedViews )
			{
				pmc.assignPointMatches( tileMap, groups, fixedViews );
			}
		};
	}

	/**
	 * Builds the point-match creator for a solve from the stored corresponding interest points, using Spark.
	 * Every pair of views (optionally restricted by {@code vsComparisonPairs}) is one Spark task; pairs in which
	 * both views are fixed, or both belong to the same group, are skipped. A task loads the dataset XML itself
	 * and, for every combination of the two views' labels, turns the correspondences of view A that point to
	 * view B into point matches whose coordinates are transformed into global space with the views' current
	 * registration models. They are returned as inliers of a {@link PairwiseResult} that carries both labels
	 * (used to assign the label weights). Views without any connection are added as a self-pair without inliers
	 * so that they still take part in the solve.
	 *
	 * @param xmlURI the dataset XML, loaded by every Spark task
	 * @param sc the Spark context the tasks run on
	 * @param viewIdsGlobal the views to connect
	 * @param labelMap for every view, the interest point labels to use mapped to their weights
	 * @param groups groups of views that are treated as one view; pairs within a group are not connected
	 * @param fixedViewIds views held fixed during the solve; pairs of two fixed views are not connected
	 * @param vsComparisonPairs allowed pairs of view setup IDs as {@code (min, max)}, or {@code null} to
	 *             allow all pairs
	 * @return a creator over all connected pairs (plus the unconnected views), or {@code null} if there is no
	 *         pair of views to process or no connection was found
	 */
	public static InterestPointMatchCreator setupPointMatchesFromInterestPoints(
			final URI xmlURI,
			final JavaSparkContext sc,
			final List< ViewId > viewIdsGlobal,
			final Map< ViewId, ? extends Map< String, Double > > labelMap,
			final Collection< Group< ViewId > > groups,
			final Set< ViewId > fixedViewIds,
			final Set< Pair< Integer, Integer > > vsComparisonPairs )
	{
		System.out.println( "Setting up corresponding interest points with Spark ... " );

		// plain serializable data for the Spark tasks (ViewId subclasses such as ViewDescription are not)
		final Map< String, ArrayList< String > > labelsByView = new HashMap<>();
		final Set< String > fixedViewKeys = new HashSet<>();
		final Set< String > sameGroupPairs = new HashSet<>();
		final List< PairTask > tasks = new ArrayList<>();

		for ( final ViewId viewId : viewIdsGlobal )
			labelsByView.put( viewIdKey( viewId ), new ArrayList<>( labelMap.get( viewId ).keySet() ) );

		for ( final ViewId fixedViewId : fixedViewIds )
			fixedViewKeys.add( viewIdKey( fixedViewId ) );

		for ( final Group< ViewId > group : groups )
		{
			final List< ViewId > groupViews = new ArrayList<>( group.getViews() );

			for ( int i = 0; i < groupViews.size() - 1; ++i )
				for ( int j = i + 1; j < groupViews.size(); ++j )
				{
					final String a = viewIdKey( groupViews.get( i ) );
					final String b = viewIdKey( groupViews.get( j ) );
					sameGroupPairs.add( pairKey( a, b ) );
					sameGroupPairs.add( pairKey( b, a ) );
				}
		}

		for ( int i = 0; i < viewIdsGlobal.size() - 1; ++i )
			for ( int j = i + 1; j < viewIdsGlobal.size(); ++j )
			{
				final ViewId vA = viewIdsGlobal.get( i );
				final ViewId vB = viewIdsGlobal.get( j );

				if ( vsComparisonPairs != null )
				{
					final Pair< Integer, Integer > key = new ValuePair<>( Math.min( vA.getViewSetupId(), vB.getViewSetupId() ), Math.max( vA.getViewSetupId(), vB.getViewSetupId() ) );
					if ( !vsComparisonPairs.contains( key ) )
						continue;
				}

				// both are fixed, no need to connect them
				if ( fixedViewKeys.contains( viewIdKey( vA ) ) && fixedViewKeys.contains( viewIdKey( vB ) ) )
				{
					System.out.println( "Not assigning " + Group.pvid( vA ) + " <> " + Group.pvid( vB ) + " because they are both fixed." );
					continue;
				}

				// both are part of the same group, no need to connect them
				if ( sameGroupPairs.contains( pairKey( viewIdKey( vA ), viewIdKey( vB ) ) ) )
				{
					System.out.println( "Not assigning " + Group.pvid( vA ) + " <> " + Group.pvid( vB ) + " because they are part of the same group." );
					continue;
				}

				tasks.add( new PairTask( vA.getTimePointId(), vA.getViewSetupId(), vB.getTimePointId(), vB.getViewSetupId() ) ); // order doesn't matter, saved symmetrically
			}

		if ( tasks.isEmpty() )
		{
			System.out.println( "No view-pair tasks to process, stopping." );
			return null;
		}

		System.out.println( "Spark correspondence setup tasks: " + tasks.size() );

		final JavaRDD< PairTask > rdd = sc.parallelize( tasks, Math.min( Spark.maxPartitions, tasks.size() ) );

		final List< ArrayList< SerializedIPPairwiseResult > > sparkResults = rdd.map( pairTask ->
		{
			// every task loads its own SpimData2, so updateModel()/getModel() are never shared between threads
			final SpimData2 data = Spark.getSparkJobSpimData2( xmlURI );

			System.out.println( "Start collecting correspondences for " + pairTask );

			final ViewId vA = pairTask.viewId();
			final ViewId vB = pairTask.correspondingViewId();
			final ArrayList< String > labelsA = labelsByView.get( viewIdKey( vA ) );
			final ArrayList< String > labelsB = labelsByView.get( viewIdKey( vB ) );
			final ArrayList< SerializedIPPairwiseResult > results = new ArrayList<>();

			if ( labelsA == null || labelsB == null )
			{
				System.out.println( "Either " + Group.pvid( vA ) + " or " + Group.pvid( vB ) + " has no labels." );
				return results;
			}

			final ViewRegistration vRegA = data.getViewRegistrations().getViewRegistration( vA );
			final ViewRegistration vRegB = data.getViewRegistrations().getViewRegistration( vB );

			vRegA.updateModel();
			vRegB.updateModel();

			final AffineTransform3D mA = vRegA.getModel();
			final AffineTransform3D mB = vRegB.getModel();

			// iterate over all pairs of labels
			for ( final String labelA : labelsA )
			{
				final InterestPoints interestPointsA = data.getViewInterestPoints().getViewInterestPointLists( vA ).getInterestPointList( labelA );

				if ( interestPointsA == null )
					continue;

				final Map< Integer, InterestPoint > ipListA = interestPointsA.getInterestPointsCopy();
				final Collection< CorrespondingInterestPoints > cpA = interestPointsA.getCorrespondingInterestPointsCopy();

				for ( final String labelB : labelsB )
				{
					final InterestPoints interestPointsB = data.getViewInterestPoints().getViewInterestPointLists( vB ).getInterestPointList( labelB );

					if ( interestPointsB == null )
						continue;

					final Map< Integer, InterestPoint > ipListB = interestPointsB.getInterestPointsCopy();

					final SerializedIPPairwiseResult result = new SerializedIPPairwiseResult(
							pairTask.timePointA, pairTask.viewSetupA, pairTask.timePointB, pairTask.viewSetupB, labelA, labelB );

					for ( final CorrespondingInterestPoints p : cpA )
					{
						if ( !p.getCorrespodingLabel().equals( labelB ) || !p.getCorrespondingViewId().equals( vB ) )
							continue;

						final InterestPoint ipA = ipListA.get( p.getDetectionId() ); // now that it is a hashmap and not a list, it is no bug anymore
						final InterestPoint ipB = ipListB.get( p.getCorrespondingDetectionId() );

						if ( ipA == null || ipB == null )
							continue;

						// transform into fresh arrays (never in place: AffineTransform3D.apply(src, tgt) is not
						// alias-safe for non-diagonal matrices), and create new points because the lists are
						// not bijective (one point may correspond to several in the other view)
						final double[] lA = new double[ 3 ], lB = new double[ 3 ];
						mA.apply( ipA.getL(), lA );
						mB.apply( ipB.getL(), lB );

						result.inliers.add( new PointMatchGeneric<>( new InterestPoint( ipA.getId(), lA ), new InterestPoint( ipB.getId(), lB ) ) );
					}

					if ( !result.inliers.isEmpty() )
						results.add( result );
				}
			}

			System.out.println( "Done collecting correspondences for " + pairTask );

			return results;
		}).collect();

		final List< Pair< Pair< ViewId, ViewId >, PairwiseResult< ? > > > pairs = new ArrayList<>();
		final Set< ViewId > connectedViews = new HashSet<>();

		for ( final ArrayList< SerializedIPPairwiseResult > taskResults : sparkResults )
			for ( final SerializedIPPairwiseResult serializedResult : taskResults )
			{
				final Pair< Pair< ViewId, ViewId >, PairwiseResult< InterestPoint > > pair = serializedResult.deserialize();
				final int numInliers = pair.getB().getInliers().size();

				if ( numInliers > 0 )
				{
					System.out.println( Group.pvid( pair.getA().getA() ) + " <-> " + Group.pvid( pair.getA().getB() ) + ": " + numInliers + " correspondences added." );
					pairs.add( new ValuePair<>( pair.getA(), (PairwiseResult<?>)pair.getB() ) );
					connectedViews.add( pair.getA().getA() );
					connectedViews.add( pair.getA().getB() );
				}
			}

		final int missing = viewIdsGlobal.size() - connectedViews.size();

		System.out.println( "Total number of pairs of views that are connected: " + pairs.size() );
		System.out.println( "Total number of views: " + viewIdsGlobal.size() );
		System.out.println( "Total number of connected views: " + connectedViews.size() + " (" + missing + " missing that are not connected)");

		// add all ViewIds that are not connected
		if ( missing > 0 )
			for ( final ViewId viewId : viewIdsGlobal )
				if ( !connectedViews.contains( viewId ) )
				{
					final PairwiseResult< InterestPoint > pairResult = new PairwiseResult<>( false );

					final String randomLabel = labelMap.get( viewId ).keySet().iterator().next();

					pairResult.setLabelA( randomLabel );
					pairResult.setLabelB( randomLabel );

					pairResult.setInliers( new ArrayList<>(), 0.0 );
					pairs.add( new ValuePair<>( new ValuePair<>( viewId, viewId ), pairResult)) ; // we are connecting it to itself with no inliers so it is in the list
				}

		if ( !pairs.isEmpty() )
			return new InterestPointMatchCreator( pairs, labelMap );
		else
			return null;
	}

	private static String viewIdKey( final ViewId viewId )
	{
		return viewId.getTimePointId() + "," + viewId.getViewSetupId();
	}

	private static String pairKey( final String viewA, final String viewB )
	{
		return viewA + ">" + viewB;
	}

	/** one pair of views (plain ints, so it can be sent to the Spark tasks) */
	private static class PairTask implements Serializable
	{
		private static final long serialVersionUID = 1L;

		final int timePointA;
		final int viewSetupA;
		final int timePointB;
		final int viewSetupB;

		PairTask( final int timePointA, final int viewSetupA, final int timePointB, final int viewSetupB )
		{
			this.timePointA = timePointA;
			this.viewSetupA = viewSetupA;
			this.timePointB = timePointB;
			this.viewSetupB = viewSetupB;
		}

		ViewId viewId()
		{
			return new ViewId( timePointA, viewSetupA );
		}

		ViewId correspondingViewId()
		{
			return new ViewId( timePointB, viewSetupB );
		}

		@Override
		public String toString()
		{
			return new ToStringBuilder( this )
					.append( "timePointA", timePointA )
					.append( "viewSetupA", viewSetupA )
					.append( "timePointB", timePointB )
					.append( "viewSetupB", viewSetupB )
					.toString();
		}
	}

	/** the inliers (already in global coordinates) of one pair of views and labels, returned by a Spark task */
	private static class SerializedIPPairwiseResult implements Serializable
	{
		private static final long serialVersionUID = 1L;

		final int timePointA;
		final int viewSetupA;
		final int timePointB;
		final int viewSetupB;
		final String labelA;
		final String labelB;
		final List< PointMatchGeneric< InterestPoint > > inliers = new ArrayList<>();

		SerializedIPPairwiseResult( final int timePointA, final int viewSetupA, final int timePointB, final int viewSetupB, final String labelA, final String labelB )
		{
			this.timePointA = timePointA;
			this.viewSetupA = viewSetupA;
			this.timePointB = timePointB;
			this.viewSetupB = viewSetupB;
			this.labelA = labelA;
			this.labelB = labelB;
		}

		Pair< Pair< ViewId, ViewId >, PairwiseResult< InterestPoint > > deserialize()
		{
			final PairwiseResult< InterestPoint > pairResult = new PairwiseResult<>( false );

			// set labels (required by InterestPointMatchCreator to assign weights)
			pairResult.setLabelA( labelA );
			pairResult.setLabelB( labelB );
			pairResult.setInliers( inliers, 0.0 );

			return new ValuePair<>( new ValuePair<>( new ViewId( timePointA, viewSetupA ), new ViewId( timePointB, viewSetupB ) ), pairResult );
		}
	}

	/**
	 * GUI "Fix first view": the first (sorted) view of every subset.
	 *
	 * @param subsets the independent registration subsets; empty subsets are skipped
	 * @return the first view of every non-empty subset
	 */
	public static HashSet< ViewId > assembleFixed( final List< Subset< ViewId > > subsets )
		{
		final HashSet< ViewId > fixed = new HashSet<>();

		for ( final Subset< ViewId > subset : subsets )
			if ( !subset.getViews().isEmpty() )
				fixed.add( Subset.getViewsSorted( subset.getViews() ).get( 0 ) );

		return fixed;
	}

	/**
	 * GUI "Map back to first/user defined view": one reference view per subset. For each subset the first
	 * view of {@code mapBackViewIds} that it contains is used; if there is none, the first (sorted) view of
	 * the subset. The chosen reference views are printed.
	 *
	 * @param subsets the independent registration subsets; empty subsets are skipped
	 * @param mapBackViewIds user-defined reference views, or {@code null} to use the first view of every
	 *             subset
	 * @return for every view the mapback (reference) view of its subset
	 */
	public static HashMap< ViewId, ViewId > assembleMapBack(
			final List< Subset< ViewId > > subsets,
			final ArrayList< ViewId > mapBackViewIds )
	{
		final HashMap< ViewId, ViewId > map = new HashMap<>();

		for ( final Subset< ViewId > subset : subsets )
		{
			if ( subset.getViews().size() == 0 )
				continue;

			// user-specified view in this subset, otherwise the first one
			ViewId mapBackView = Subset.getViewsSorted( subset.getViews() ).get( 0 );

			if ( mapBackViewIds != null && !mapBackViewIds.isEmpty())
			{
				for (final ViewId mbv : mapBackViewIds)
					if ( subset.contains(mbv) )
					{
						mapBackView = mbv;
						break;
					}
			}

			for ( final ViewId viewId : subset.getViews() )
				map.put( viewId, mapBackView );
		}

		System.out.println("The following ViewIds are used for mapback: ");
		new HashSet<>( map.values() ).forEach( vid -> System.out.print( Group.pvid( vid ) + ", ") );
		System.out.println();

		return map;
	}

	/**
	 * Validates the fixed-view and mapback options and resolves them against the dataset. With
	 * {@code --disableFixedViews}, {@link #fixedViewIds} is set to {@code null}; if
	 * {@code --enableMapbackViews} is also set, {@code --mapbackViews} is parsed into {@link #mapBackViewIds}
	 * ({@code null} if not given, meaning the first view of every subset) and {@link #mapBackModel} is
	 * instantiated according to {@code --mapbackModel}. Otherwise {@code --fixedViews} is parsed into
	 * {@link #fixedViewIds} ({@code null} if not given). View IDs that do not exist in the dataset are
	 * dropped with a warning.
	 *
	 * @param dataGlobal the dataset against which the given view IDs are validated
	 * @return always {@code true}
	 * @throws IllegalArgumentException if {@code --enableMapbackViews} is used without
	 *             {@code --disableFixedViews} or together with {@code -rtp TO_REFERENCE_TIMEPOINT}, or if
	 *             none of the given fixed or mapback views exists in the dataset
	 */
	public boolean setupParameters( final SpimData2 dataGlobal )
	{
		if ( !disableFixedViews && enableMapbackViews )
			throw new IllegalArgumentException("You cannot use '--enableMapbackViews' without '--disableFixedViews'.");

		if ( enableMapbackViews && registrationTP == RegistrationType.TO_REFERENCE_TIMEPOINT )
			throw new IllegalArgumentException("'--enableMapbackViews' is not supported with '-rtp TO_REFERENCE_TIMEPOINT' (the GUI does not map back in this mode either).");

		// fixed views and mapping back to original view
		if ( disableFixedViews )
		{
			if ( enableMapbackViews )
			{
				if ( mapbackViews == null || mapbackViews.length == 0 )
				{
					System.out.println( "First ViewId(s) will be used as mapback view for each respective registration subset (e.g. timepoint) ... ");

					this.mapBackViewIds = null;
				}
				else
				{
					// load mapback view
					System.out.println( "Parsing mapback ViewIds ... ");
	
					final ArrayList<ViewId> parsedViews = Import.getViewIds( mapbackViews ); // all views
					this.mapBackViewIds = Import.getViewIds( dataGlobal, parsedViews );

					if ( parsedViews.size() != mapBackViewIds.size() )
						System.out.println( "Warning: only " + mapBackViewIds.size() + " of " + parsedViews.size() + " that you specified for mapback views exist and are present.");

					if ( this.mapBackViewIds == null || this.mapBackViewIds.size() == 0 )
						throw new IllegalArgumentException( "Mapback views couldn't be parsed. Please provide valid mapback views." );
				}

				// load mapback model
				this.mapBackModel = (mapbackModelEntry == MapbackModel.TRANSLATION) ? new TranslationModel3D() : new RigidModel3D();

				System.out.println( "Mapback model=" + mapBackModel.getClass().getSimpleName() );
			}
			else
			{
				System.out.println( "No views are fixed and no mapping back is selected, i.e. the Views will more or less float in space (might be fine if desired)." );

				this.mapBackViewIds = null;
			}

			this.fixedViewIds = null;
		}
		else
		{
			// set/load fixed views
			if ( fixedViews == null || fixedViews.length == 0 )
			{
				System.out.println( "First ViewId(s) will be used as fixed for each respective registration subset (e.g. timepoint) ... ");

				this.fixedViewIds = null;
			}
			else
			{
				System.out.println( "Parsing fixed ViewIds ... ");

				final ArrayList<ViewId> parsedViews = Import.getViewIds( fixedViews ); // all views
				this.fixedViewIds = Import.getViewIds( dataGlobal, parsedViews );

				if ( parsedViews.size() != fixedViewIds.size() )
					System.out.println( "Warning: only " + fixedViewIds.size() + " of " + parsedViews.size() + " that you specified as fixed views exist and are present.");

				if ( this.fixedViewIds == null || this.fixedViewIds.size() == 0 )
					throw new IllegalArgumentException( "Fixed views couldn't be parsed. Please provide a valid fixed view." );

				System.out.println("The following ViewIds are fixed: ");
				fixedViewIds.forEach( vid -> System.out.print( Group.pvid( vid ) + ", ") );
				System.out.println();
			}

		}

		return true;
	}

	public static void main(final String... args)
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new Solver()).execute(args));
	}

}
