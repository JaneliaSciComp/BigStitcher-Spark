/*-
 * #%L
 * Spark-based parallel BigStitcher project.
 * %%
 * Copyright (C) 2021 - 2025 Developers.
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
package net.preibisch.bigstitcher.spark.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;

import mpicbg.models.Model;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.legacy.mpicbg.PointMatchGeneric;
import net.preibisch.mvrecon.fiji.ImgLib2Temp.Pair;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.CorrespondingInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPoint;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsN5;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.MatcherPairwise;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.PairwiseResult;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.GroupedInterestPoint;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.ransac.RANSAC;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.methods.ransac.RANSACParameters;
import util.URITools;

/**
 * A {@link MatcherPairwise} that does NOT compute descriptor matches itself. Instead it loads
 * pre-computed correspondence CANDIDATES (e.g. from an external / learned matcher) from an N5
 * store and only runs BigStitcher's RANSAC (single or multi-consensus) on them.
 *
 * The candidates store uses exactly the layout of interestpoints.n5 correspondences:
 * group {@code tpId_<tp>_viewSetupId_<setup>/<label>/correspondences} with attributes
 * {@code correspondences="2.0.0"} and {@code idMap} ({"tp,setup,label" -> index}) and a
 * {@code data} dataset of shape 4 x N (uint64) with columns
 * [detectionId, correspondingDetectionId, idMapIndex, consensusSetId]. The consensusSetId
 * column is ignored (candidates have no set yet). Entries are expected to be stored
 * symmetrically (as BigStitcher does), so only view A's group is read for a pair. Reading uses
 * {@link InterestPointsN5#readCorrespondences(N5Reader, String)}, the exact parser BigStitcher
 * uses for its own correspondences (v1 3xN and v2 4xN layouts).
 *
 * Candidates whose detection ids are not present in the interest point lists handed to
 * {@link #match} are dropped (this honours e.g. OVERLAPPING_ONLY filtering upstream).
 *
 * @param <I> InterestPoint (grouped interest points are not supported)
 */
public class LoadCandidatesPairwise< I extends InterestPoint > implements MatcherPairwise< I >
{
	final RANSACParameters rp;
	final Model< ? > model;
	final String candidatesPath;

	/**
	 * Creates a matcher that reads the candidates of each view pair from {@code candidatesPath} and fits
	 * {@code model} to them with RANSAC.
	 *
	 * @param rp RANSAC parameters (epsilon, min inlier ratio, min matches, iterations, multi-consensus)
	 * @param model the transformation model to fit
	 * @param candidatesPath path or URI of the N5 store (the store itself, e.g. /data/candidates.n5)
	 */
	public LoadCandidatesPairwise( final RANSACParameters rp, final Model< ? > model, final String candidatesPath )
	{
		this.rp = rp;
		this.model = model;
		this.candidatesPath = candidatesPath;
	}

	@Override
	public < V > PairwiseResult< I > match(
			final Collection< I > listAIn,
			final Collection< I > listBIn,
			final V viewsA,
			final V viewsB,
			final String labelA,
			final String labelB )
	{
		final PairwiseResult< I > result = new PairwiseResult< I >( true );

		if ( Group.class.isInstance( viewsA ) || Group.class.isInstance( viewsB ) ||
			 !( viewsA instanceof ViewId ) || !( viewsB instanceof ViewId ) ||
			 ( !listAIn.isEmpty() && GroupedInterestPoint.class.isInstance( listAIn.iterator().next() ) ) ||
			 ( !listBIn.isEmpty() && GroupedInterestPoint.class.isInstance( listBIn.iterator().next() ) ) )
		{
			throw new RuntimeException( "LOAD_CANDIDATES does not support grouped views (--groupTiles/--groupIllums/--groupChannels/--splitTimepoints)." );
		}

		final ViewId vA = (ViewId)viewsA;
		final ViewId vB = (ViewId)viewsB;
		final int minNumCandidates = Math.max( model.getMinNumMatches(), rp.getMinNumMatches() );

		if ( listAIn.size() < minNumCandidates || listBIn.size() < minNumCandidates )
		{
			result.setCandidates( new ArrayList< PointMatchGeneric< I > >() );
			result.setInliers( new ArrayList< PointMatchGeneric< I > >(), Double.NaN );
			result.setResult( System.currentTimeMillis(), "LOAD_CANDIDATES: not enough interest points (" + listAIn.size() + "/" + listBIn.size() + ", need >= " + minNumCandidates + ")." );
			return result;
		}

		// id -> point from the lists we were GIVEN (they may have been filtered, e.g. overlapping only)
		final HashMap< Integer, I > mapA = new HashMap<>();
		final HashMap< Integer, I > mapB = new HashMap<>();
		for ( final I ip : listAIn )
			mapA.putIfAbsent( ip.getId(), ip );
		for ( final I ip : listBIn )
			mapB.putIfAbsent( ip.getId(), ip );

		// read view A's candidates with BigStitcher's own correspondence reader and keep those pointing to (viewB, labelB)
		final String dataset = InterestPointsN5.corrDataset( InterestPointsN5.createN5datasetPath( vA.getTimePointId(), vA.getViewSetupId(), labelA ) );
		final ArrayList< CorrespondingInterestPoints > stored;

		try ( final N5Reader n5 = URITools.instantiateN5Reader( StorageFormat.N5, URITools.toURI( candidatesPath ) ) )
		{
			if ( n5.exists( dataset ) )
			{
				stored = InterestPointsN5.readCorrespondences( n5, dataset );
			}
			else
			{
				System.out.println( "LOAD_CANDIDATES: no group '" + dataset + "' in " + candidatesPath + ", assuming 0 candidates." );
				stored = new ArrayList<>();
			}
		}

		final ArrayList< PointMatchGeneric< I > > candidates = new ArrayList<>();
		final HashSet< Long > seen = new HashSet<>();
		int forPair = 0, notInLists = 0, duplicates = 0;

		for ( final CorrespondingInterestPoints c : stored )
		{
			if ( !c.getCorrespodingLabel().equals( labelB ) || !c.getCorrespondingViewId().equals( vB ) )
				continue; // (consensusSetId is deliberately ignored)

			++forPair;

			final I a = mapA.get( c.getDetectionId() );
			final I b = mapB.get( c.getCorrespondingDetectionId() );

			if ( a == null || b == null )
			{
				++notInLists;
				continue;
			}

			if ( !seen.add( ( (long)c.getDetectionId() << 32 ) | ( c.getCorrespondingDetectionId() & 0xffffffffL ) ) )
			{
				++duplicates;
				continue;
			}

			candidates.add( new PointMatchGeneric< I >( a, b ) );
		}

		result.setCandidates( candidates );

		final String prefix = "LOAD_CANDIDATES: " + forPair + " candidates in store, " + candidates.size() + " used"
				+ ( notInLists > 0 ? " (" + notInLists + " not in the interest point lists, e.g. filtered as non-overlapping)" : "" )
				+ ( duplicates > 0 ? " (" + duplicates + " duplicates removed)" : "" ) + " -> ";

		if ( candidates.size() < minNumCandidates )
		{
			result.setInliers( new ArrayList< PointMatchGeneric< I > >(), Double.NaN );
			result.setResult( System.currentTimeMillis(), prefix + "not enough candidates (need >= " + minNumCandidates + ")." );
			return result;
		}

		final ArrayList< PointMatchGeneric< I > > inliers = new ArrayList<>();
		final ArrayList< Integer > setIds = new ArrayList<>();

		final Pair< String, Double > ransacResult = RANSAC.computeRANSAC(
				candidates, inliers, setIds, model,
				rp.getMaxEpsilon(), rp.getMinInlierRatio(), rp.getMinNumMatches(), rp.getNumIterations(),
				rp.multiConsensus(), rp.getMaxTrust(), rp.getFilterRansac() );

		result.setInliers( inliers, ransacResult.getB(), setIds );
		result.setResult( System.currentTimeMillis(), prefix + ransacResult.getA() );

		return result;
	}

	@Override
	public boolean requiresInterestPointDuplication() { return false; } // RANSAC clones into LinkedPoints itself
}
