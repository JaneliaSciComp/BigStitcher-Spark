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
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.generic.base.Entity;
import mpicbg.spim.data.registration.ViewRegistrations;
import mpicbg.spim.data.registration.ViewTransformAffine;
import mpicbg.spim.data.sequence.Angle;
import mpicbg.spim.data.sequence.Channel;
import mpicbg.spim.data.sequence.Illumination;
import mpicbg.spim.data.sequence.Tile;
import mpicbg.spim.data.sequence.TimePoint;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.RealInterval;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.bigstitcher.spark.util.Spark.SerializablePairwiseStitchingResult;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.stitchingresults.PairwiseStitchingResult;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import net.preibisch.stitcher.algorithm.FilteredStitchingResults;
import net.preibisch.stitcher.algorithm.GroupedViewAggregator;
import net.preibisch.stitcher.algorithm.GroupedViewAggregator.ActionType;
import net.preibisch.stitcher.algorithm.PairwiseStitching;
import net.preibisch.stitcher.algorithm.lucaskanade.LucasKanadeParameters;
import net.preibisch.stitcher.algorithm.SpimDataFilteringAndGrouping;
import net.preibisch.stitcher.algorithm.TransformTools;
import net.preibisch.stitcher.algorithm.globalopt.TransformationTools;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import scala.Tuple2;

public class SparkLucasKanadePairwiseStitching extends AbstractSelectableViews
{
	private static final long serialVersionUID = -1;

	@Option(names = { "-ds", "--downsampling" }, required = false, description = "Define the downsampling at which the stitching should be performed, e.g. -ds 4,4,1 (default: 2,2,1)")
	String downsampling = "2,2,1";

	@Option(names = { "--maxIterations" }, description = "Maximum number of Lucas-Kanade iterations (default: 100)")
	int maxIterations = 100;

	@Option(names = { "--minParameterChange" }, description = "Convergence threshold: minimum L2 norm of parameter update to continue iterating (default: 0.01)")
	double minParameterChange = 0.01;

	@Option(names = { "--modelType" }, description = "Warp function / transformation model type: TRANSLATION, RIGID, or AFFINE (default: TRANSLATION)")
	LucasKanadeParameters.WarpFunctionType modelType = LucasKanadeParameters.WarpFunctionType.TRANSLATION;

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

		System.out.println( "\nDebugging ViewDescriptions ... " );
		for ( final ViewId v : viewIdsGlobal )
		{
			System.out.println( "ViewId=" + Group.pvid( v ) );
			ViewDescription vd = dataGlobal.getSequenceDescription().getViewDescription( v );
			System.out.println( "ViewDescription=" + Group.pvid( vd ) );
			System.out.println( "ViewSetup=" + vd.getViewSetup() );
			System.out.println( "ViewSetup.getAttributes()=" + vd.getViewSetup().getAttributes() );
			System.out.println( "ViewSetup.getAttributes().size()=" + vd.getViewSetup().getAttributes().size() );
		}

		final long[] ds = Arrays.stream(downsampling.split(",")).map(String::trim).mapToLong(Long::parseLong).toArray();

		System.out.println( "(" + new Date( System.currentTimeMillis() ) + "): Downsampling used for stitching: " + Util.printCoordinates( ds ) );

		// get pairs to compare
		System.out.println( "(" + new Date( System.currentTimeMillis() ) + "): Finding pairs to compute overlap ... " );

		final SpimDataFilteringAndGrouping< SpimData2 > grouping = new SpimDataFilteringAndGrouping<>( dataGlobal );
		grouping.addFilters( viewIdsGlobal.stream().map( vid -> dataGlobal.getSequenceDescription().getViewDescription( vid ) ).collect( Collectors.toList() ) );

		// Defaults for grouping
		// the default grouping by channels and illuminations
		final HashSet< Class <? extends Entity> > defaultGroupingFactors = new HashSet<>();
		defaultGroupingFactors.add( Illumination.class );
		defaultGroupingFactors.add( Channel.class );
		// the default comparision by tiles
		final HashSet< Class <? extends Entity> > defaultComparisonFactors = new HashSet<>();
		defaultComparisonFactors.add(Tile.class);
		// the default application along time points and angles
		final HashSet< Class <? extends Entity> > defaultApplicationFactors = new HashSet<>();
		defaultApplicationFactors.add( TimePoint.class );
		defaultApplicationFactors.add( Angle.class );

		grouping.getAxesOfApplication().addAll( defaultApplicationFactors );
		grouping.getGroupingFactors().addAll( defaultGroupingFactors );
		grouping.getAxesOfComparison().addAll( defaultComparisonFactors );

		List< ? extends Pair< ? extends Group< ? extends ViewId >, ? extends Group< ? extends ViewId > > > groupedPairs =  grouping.getComparisons();

		// remove non-overlapping comparisons
		final List< Pair< Group< ViewId >, Group< ViewId > > > removedPairs = TransformationTools.filterNonOverlappingPairs( (List)grouping.getComparisons(), dataGlobal.getViewRegistrations(), dataGlobal.getSequenceDescription() );
		System.out.println( new Date( System.currentTimeMillis() ) + ": Removed " + removedPairs.size() + " non-overlapping view-pairs for computing." );

		System.out.println( "(" + new Date( System.currentTimeMillis() ) + "): For the following pairs pairwise stitching will be computed:");
		groupedPairs.forEach( pair -> System.out.println( "\t" + pair.getA() + " <=> " + pair.getB() ) );
		System.out.println( "(" + new Date( System.currentTimeMillis() ) + "): In total: " + groupedPairs.size() + " pair(s).");

		if ( groupedPairs.size() == 0 )
		{
			System.out.println( "no pairs to compare, stopping.");
			return null;
		}


		final SparkConf conf = new SparkConf().setAppName("SparkLucasKanadePairwiseStitching");

		if (localSparkBindAddress)
		{
			conf.set("spark.driver.bindAddress", "127.0.0.1");
			conf.set("spark.driver.host", "localhost");
			org.apache.spark.util.Utils.setCustomHostname("localhost");
		}

		final JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");

		final JavaRDD<int[][][]> rdd = sc.parallelize( Spark.serializeGroupedViewIdPairsForRDD( groupedPairs ), Math.min( Spark.maxPartitions, groupedPairs.size() ) );

		final JavaRDD<Tuple2<int[][][], SerializablePairwiseStitchingResult>> rddResults = rdd.map( serializedGroupPair ->
		{
			final SpimData2 data = Spark.getSparkJobSpimData2( xmlURI );
			final Pair<Group<ViewId>, Group<ViewId>> pair = Spark.deserializeGroupedViewIdPairForRDD( serializedGroupPair );
			final ViewRegistrations vrs = data.getViewRegistrations();

			final LucasKanadeParameters params = new LucasKanadeParameters(
					modelType,
					maxIterations,
					minParameterChange,
					false,
					false,
					0);

			final GroupedViewAggregator gva = new GroupedViewAggregator();
			final ExecutorService serviceLocal = Executors.newFixedThreadPool( 1 );

			// TODO: do non-equal transformation registration when views within a group have differing transformations
			final ViewId firstVdA = pair.getA().iterator().next();
			final ViewId firstVdB = pair.getB().iterator().next();

			boolean nonTranslationsEqual = TransformTools.nonTranslationsEqual( vrs.getViewRegistration( firstVdA ), vrs.getViewRegistration( firstVdB ) );

			final Pair<Pair< AffineGet, Double >, RealInterval> result;

			// debug code for https://github.com/JaneliaSciComp/BigStitcher-Spark/issues/43
			System.out.println( "\nDebugging ViewDescriptions ... " );
			for ( final ViewId v : pair.getA() )
			{
				System.out.println( "ViewId=" + Group.pvid( v ) );
				ViewDescription vd = data.getSequenceDescription().getViewDescription( v );
				System.out.println( "ViewDescription=" + Group.pvid( vd ) );
				System.out.println( "ViewSetup=" + vd.getViewSetup() );
				System.out.println( "ViewSetup.getAttributes()=" + vd.getViewSetup().getAttributes() );
				System.out.println( "ViewSetup.getAttributes().size()=" + vd.getViewSetup().getAttributes().size() );
			}

			for ( final ViewId v : pair.getB() )
			{
				System.out.println( "ViewId=" + Group.pvid( v ) );
				ViewDescription vd = data.getSequenceDescription().getViewDescription( v );
				System.out.println( "ViewDescription=" + Group.pvid( vd ) );
				System.out.println( "ViewSetup=" + vd.getViewSetup() );
				System.out.println( "ViewSetup.getAttributes()=" + vd.getViewSetup().getAttributes() );
				System.out.println( "ViewSetup.getAttributes().size()=" + vd.getViewSetup().getAttributes().size() );
			}


			if (nonTranslationsEqual)
			{
				result = TransformationTools.computeStitchingLucasKanade(
						pair.getA(),
						pair.getB(),
						vrs,
						params,
						data.getSequenceDescription(),
						gva,
						ds,
						serviceLocal );
			}
			else
			{
				result = TransformationTools.computeStitchingNonEqualTransformationsLucasKanade(
						pair.getA(),
						pair.getB(),
						vrs,
						params,
						data.getSequenceDescription(),
						gva,
						ds,
						serviceLocal );
			}

			serviceLocal.shutdown();

			if ( result == null )
			{
				System.out.println( new Date( System.currentTimeMillis() ) + ": Compute pairwise: " + pair.getA() + " <> " + pair.getB() + ": No shift found." );

				return new Tuple2<>(serializedGroupPair, null);
			}
			else
			{
				System.out.println( new Date( System.currentTimeMillis() ) + ": Compute pairwise: " + pair.getA() + " <> " + pair.getB() + ": r=" + result.getA().getB() );

				AffineTransform3D resT = new AffineTransform3D();
				resT.preConcatenate( result.getA().getA() );

				final double oldTransformHash = PairwiseStitchingResult.calculateHash(
						vrs.getViewRegistration( pair.getA().getViews().iterator().next() ),
						vrs.getViewRegistration( pair.getB().getViews().iterator().next() ) );

				final PairwiseStitchingResult<ViewId> pairwiseStitchingResult =
						new PairwiseStitchingResult<>(
								new ValuePair<>(
										pair.getA(),
										pair.getB()),
								result.getB(),
								resT,
								result.getA().getB(),
								oldTransformHash );

				return new Tuple2<>( serializedGroupPair, new SerializablePairwiseStitchingResult( pairwiseStitchingResult ) );
			}
		});

		rddResults.cache();
		rddResults.count();

		final ArrayList< PairwiseStitchingResult< ViewId > > results = new ArrayList<>();

		System.out.println( "\nCollecting results\n" );

		for ( final Tuple2<int[][][], SerializablePairwiseStitchingResult> result : rddResults.collect() )
		{
			final Pair<Group<ViewId>, Group<ViewId>> pair = Spark.deserializeGroupedViewIdPairForRDD( result._1() );

			if (result._2() != null )
			{
				final PairwiseStitchingResult< ViewId > r = result._2().deserialize();
				results.add( r );
				System.out.println( new Date( System.currentTimeMillis() ) + ": Compute pairwise: " + pair.getA() + " <> " + pair.getB() + ": r=" + r.r() );
			}
			else
			{
				System.out.println( new Date( System.currentTimeMillis() ) + ": Compute pairwise: " + pair.getA() + " <> " + pair.getB() + ": No shift found, skipping pair." );
			}

			// try to remove a -> b and b -> a, just to make sure
			dataGlobal.getStitchingResults().getPairwiseResults().remove( pair );
			dataGlobal.getStitchingResults().getPairwiseResults().remove( new ValuePair<>( pair.getB(), pair.getA() ) );
		}

		System.out.println( new Date( System.currentTimeMillis() ) + ": Remaining pairs: " + results.size() );

		// update StitchingResults with Results
		for ( final PairwiseStitchingResult< ViewId > psr : results )
		{
			if (psr == null)
				continue;

			// update the registrations transformations
			psr.pair().getA().getViews().forEach( viewId -> {
				dataGlobal.getViewRegistrations().getViewRegistration(viewId)
						.preconcatenateTransform(new ViewTransformAffine(
								"Stitching Transform",
								new AffineTransform3D().concatenate(psr.getInverseTransform())));
			});
			dataGlobal.getStitchingResults().setPairwiseResultForPair(psr.pair(), psr );
		}

		final FilteredStitchingResults fsr = new FilteredStitchingResults( dataGlobal.getStitchingResults(), null );

		fsr.applyToWrappedAll();

		System.out.println( new Date( System.currentTimeMillis() ) + ": Remaining pairs after applying filters: " + fsr.getPairwiseResults().size() );

		sc.close();

		if (!dryRun)
		{
			System.out.println( "Saving resulting XML ... ");
			new XmlIoSpimData2().save( dataGlobal, xmlURI );
		}

		return null;
	}

	public static void main(final String... args) throws SpimDataException
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new SparkLucasKanadePairwiseStitching()).execute(args));
	}
}
