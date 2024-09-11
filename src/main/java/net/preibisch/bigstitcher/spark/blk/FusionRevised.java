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
package net.preibisch.bigstitcher.spark.blk;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import mpicbg.spim.data.generic.AbstractSpimData;
import mpicbg.spim.data.generic.sequence.BasicImgLoader;
import mpicbg.spim.data.generic.sequence.BasicViewDescription;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.blocks.BlockAlgoUtils;
import net.imglib2.algorithm.blocks.BlockSupplier;
import net.imglib2.algorithm.blocks.ClampType;
import net.imglib2.algorithm.blocks.convert.Convert;
import net.imglib2.algorithm.blocks.transform.Transform;
import net.imglib2.algorithm.blocks.transform.Transform.Interpolation;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Cast;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.mvrecon.process.downsampling.DownsampleTools;
import net.preibisch.mvrecon.process.fusion.FusionTools;

public class FusionRevised
{
	public static < T extends NativeType< T > > RandomAccessibleInterval< T > fuseVirtual(
			final AbstractSpimData< ? > spimData,
			final Collection< ? extends ViewId > views,
			final Interval boundingBox,
			final T type,
			final double minIntensity,
			final double range )
	{
		final BasicImgLoader imgLoader = spimData.getSequenceDescription().getImgLoader();

		final HashMap< ViewId, AffineTransform3D > registrations = new HashMap<>();

		for ( final ViewId viewId : views )
		{
			final ViewRegistration vr = spimData.getViewRegistrations().getViewRegistration( viewId );
			vr.updateModel();
			registrations.put( viewId, vr.getModel().copy() );
		}

		final Map< ViewId, ? extends BasicViewDescription< ? > > viewDescriptions = spimData.getSequenceDescription().getViewDescriptions();

		return fuseVirtual( imgLoader, registrations, viewDescriptions, views, boundingBox, type, minIntensity, range );
	}

	public static < T extends NativeType< T > > RandomAccessibleInterval< T > fuseVirtual(
			final BasicImgLoader imgloader,
			final Map< ViewId, ? extends AffineTransform3D > registrations, // now contain the downsampling already
			final Map< ViewId, ? extends BasicViewDescription< ? > > viewDescriptions,
			final Collection< ? extends ViewId > views,
			final Interval boundingBox, // is already downsampled
			final T type,
			final double minIntensity,
			final double range )
	{
//		System.out.println( "Fusion.fuseVirtual" );
//		System.out.println( "  boundingBox = " + Intervals.toString(boundingBox) );

		// SIMPLIFIED:
		// assuming:
		// 	final boolean is2d = false;

		// SIMPLIFIED:
		// we already filtered the overlapping views
		// which views to process (use un-altered bounding box and registrations)
		// (sorted to be able to use the "lowest ViewId" wins strategy)
		final List< ViewId > viewIdsToProcess = views.stream().sorted().collect( Collectors.toList() );

		final List< BlockSupplier< FloatType > > images = new ArrayList<>();
		final List< BlockSupplier< FloatType > > blendings = new ArrayList<>();

		for ( final ViewId viewId : viewIdsToProcess )
		{
			final AffineTransform3D model = registrations.get( viewId ).copy();

			// this modifies the model so it maps from a smaller image to the global coordinate space,
			// which applies for the image itself as well as the weights since they also use the smaller
			// input image as reference
			final double[] usedDownsampleFactors = new double[ 3 ];
			RandomAccessibleInterval inputImg = DownsampleTools.openDownsampled( imgloader, viewId, model, usedDownsampleFactors );

			final int interpolation = 1; // TODO

			final AffineTransform3D transformFromSource = concatenateBoundingBoxOffset( model, boundingBox );

			final Interpolation interpolate = ( interpolation == 1 )
					? Interpolation.NLINEAR
					: Interpolation.NEARESTNEIGHBOR;
			final BlockSupplier< FloatType > viewBlocks = floatBlocks( Cast.unchecked( inputImg ) )
					.andThen( Transform.affine( transformFromSource, interpolate ) );
			images.add( viewBlocks );

			// SIMPLIFIED
			// add all (or no) weighting schemes
			// assuming:
			// 	final boolean useBlending = true;
			// 	final boolean useContentBased = false;

			// instantiate blending if necessary
			final float[] blending = Util.getArrayFromValue( FusionTools.defaultBlendingRange, 3 );
			final float[] border = Util.getArrayFromValue( FusionTools.defaultBlendingBorder, 3 );

			// adjust both for z-scaling (anisotropy), downsampling, and registrations itself
			FusionTools.adjustBlending( viewDescriptions.get( viewId ), blending, border, model );

			final BlockSupplier< FloatType > blend = new BlendingBlockSupplier( inputImg, border, blending, transformFromSource );
			blendings.add( blend );
		}

		return getFusedRandomAccessibleInterval( boundingBox, images, blendings, type, minIntensity, range );
	}

	private static < T extends NativeType< T > > BlockSupplier< FloatType > floatBlocks( RandomAccessibleInterval< T > inputImg )
	{
		return BlockSupplier.of( Views.extendBorder( ( inputImg ) ) )
				.andThen( Convert.convert( new FloatType() ) );
	}

	private static AffineTransform3D concatenateBoundingBoxOffset(
			final AffineTransform3D transformFromSource,
			final Interval boundingBoxInSource )
	{
		final AffineTransform3D t = new AffineTransform3D();
		t.setTranslation(
				-boundingBoxInSource.min( 0 ),
				-boundingBoxInSource.min( 1 ),
				-boundingBoxInSource.min( 2 ) );
		t.concatenate( transformFromSource );
		return t;
	}

	private static < T extends NativeType< T > > RandomAccessibleInterval< T > getFusedRandomAccessibleInterval(
			final Interval boundingBox,
			final List< BlockSupplier< FloatType > > images,
			final List< BlockSupplier< FloatType > > blendings,
			final T type,
			final double minIntensity,
			final double range )
	{
		BlockSupplier< FloatType > fblocks = new WeightedAverageBlockSupplier( images, blendings );  // TODO should be WeightedAverage.of( images, blendings )
		if ( ! (type instanceof FloatType ) )
			fblocks = fblocks.andThen( LinearRange.scaleAndOffset( range, minIntensity ) );
		final BlockSupplier< T > blocks = fblocks.andThen( Convert.convert( type, ClampType.CLAMP ) );
		return BlockAlgoUtils.cellImg( blocks, boundingBox.dimensionsAsLongArray(), new int[] { 64 } );
	}
}
