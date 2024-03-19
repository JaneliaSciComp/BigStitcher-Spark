package net.preibisch.bigstitcher.spark.blk;

import java.text.NumberFormat;
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
import net.imglib2.Dimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.algorithm.blocks.BlockAlgoUtils;
import net.imglib2.algorithm.blocks.UnaryBlockOperator;
import net.imglib2.algorithm.blocks.transform.Transform;
import net.imglib2.blocks.PrimitiveBlocks;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.realtransform.RealViews;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.mvrecon.process.downsampling.DownsampleTools;
import net.preibisch.mvrecon.process.fusion.transformed.FusedRandomAccessibleInterval;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;

public class Fusion
{

	public static RandomAccessibleInterval< FloatType > fuseVirtual_blk(
			final AbstractSpimData< ? > spimData,
			final Collection< ? extends ViewId > views,
			final Interval boundingBox )
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

		return fuseVirtual_blk( imgLoader, registrations, viewDescriptions, views, boundingBox );
	}

	public static RandomAccessibleInterval< FloatType > fuseVirtual_blk(
			final BasicImgLoader imgloader,
			final Map< ViewId, ? extends AffineTransform3D > registrations, // now contain the downsampling already
			final Map< ViewId, ? extends BasicViewDescription< ? > > viewDescriptions,
			final Collection< ? extends ViewId > views,
			final Interval boundingBox // is already downsampled
	)
	{
		System.out.println( "Fusion.fuseVirtual_blk" );
		System.out.println( "  boundingBox = " + Intervals.toString(boundingBox) );







		// SIMPLIFIED:
		// assuming:
		// 	final boolean is2d = true;

		// SIMPLIFIED:
		// we already filtered the opvelapping view
		// which views to process (use un-altered bounding box and registrations)
		// (sorted to be able to use the "lowest ViewId" wins strategy)
		final List< ViewId > viewIdsToProcess = views.stream().sorted().collect( Collectors.toList() );

		final ArrayList< RandomAccessibleInterval< FloatType > > images = new ArrayList<>();
		final ArrayList< RandomAccessibleInterval< FloatType > > weights = new ArrayList<>();

		for ( final ViewId viewId : viewIdsToProcess )
		{
			final AffineTransform3D model = registrations.get( viewId ).copy();

			// this modifies the model so it maps from a smaller image to the global coordinate space,
			// which applies for the image itself as well as the weights since they also use the smaller
			// input image as reference
			final double[] usedDownsampleFactors = new double[ 3 ];
			RandomAccessibleInterval inputImg = DownsampleTools.openDownsampled( imgloader, viewId, model, usedDownsampleFactors );

			Object inputImgType = imgloader.getSetupImgLoader( viewId.getViewSetupId() ).getImageType();

			final int interpolation = 1;
//			final RandomAccessibleInterval transformedInputImg = TransformView.transformView( inputImg, model, boundingBox, 0, interpolation );
			final RandomAccessibleInterval transformedInputImg = transformView( inputImg, inputImgType, model, boundingBox );
			images.add( transformedInputImg );



			// SIMPLIFIED
			// add all (or no) weighting schemes
			// assuming:
			// 	final boolean useBlending = true;
			// 	final boolean useContentBased = false;


			// instantiate blending if necessary
			final float[] blending = Util.getArrayFromValue( defaultBlendingRange, 3 );
			final float[] border = Util.getArrayFromValue( defaultBlendingBorder, 3 );

			// adjust both for z-scaling (anisotropy), downsampling, and registrations itself
			adjustBlending( viewDescriptions.get( viewId ), blending, border, model );

			final RandomAccessibleInterval< FloatType > transformedBlending = transformBlending(
					new FinalInterval( inputImg ),
					border,
					blending,
					model,
					boundingBox );

			weights.add( transformedBlending );
		}

		return new FusedRandomAccessibleInterval( new FinalInterval( getFusedZeroMinInterval( boundingBox ) ), images, weights );
	}

	private static RandomAccessibleInterval< FloatType > transformBlending(
			final Interval inputImgInterval,
			final float[] border,
			final float[] blending,
			final AffineTransform3D transform,
			final Interval boundingBox )
	{
		final RealRandomAccessible< FloatType > rra = new BlendingRealRandomAccessible( new FinalInterval( inputImgInterval ), border, blending );

		final AffineTransform3D t = new AffineTransform3D();
		t.setTranslation(
				-boundingBox.min( 0 ),
				-boundingBox.min( 1 ),
				-boundingBox.min( 2 ) );
		t.concatenate( transform );
		final Interval bb = Intervals.zeroMin( boundingBox );
		return Views.interval(
				RealViews.affine( rra, t ),
				bb );
	}

	private static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< T > transformView(
			final RandomAccessibleInterval< T > input,
			final Object type, // TODO: resolve generics... this should be T
			final AffineTransform3D transform,
			final Interval boundingBox )
	{
		final AffineTransform3D t = new AffineTransform3D();
		t.setTranslation(
				-boundingBox.min( 0 ),
				-boundingBox.min( 1 ),
				-boundingBox.min( 2 ) );
		t.concatenate( transform );

		final PrimitiveBlocks< T > blocks = PrimitiveBlocks.of( Views.extendBorder( input ) );
		final UnaryBlockOperator< T, T > affine = Transform.affine( ( T ) type, t, Transform.Interpolation.NLINEAR );
		return BlockAlgoUtils.cellImg( blocks, affine, ( T ) type, boundingBox.dimensionsAsLongArray(), new int[] { 64, 64, 64 } );
	}































	// ------------------------------------------------------------------------
	//
	//  unmodified from FusionTools
	//
	// ------------------------------------------------------------------------

	public static Interval getFusedZeroMinInterval( final Interval bbDS )
	{
		final long[] dim = new long[ bbDS.numDimensions() ];
		bbDS.dimensions( dim );
		return new FinalInterval( dim );
	}

	public static float defaultBlendingRange = 40;
	public static float defaultBlendingBorder = 0;

	/**
	 * Compute how much blending in the input has to be done so the target values blending and border are achieved in the fused image
	 *
	 * @param vd - which view
	 * @param blending - the target blending range, e.g. 40
	 * @param border - the target blending border, e.g. 0
	 * @param transformationModel - the transformation model used to map from the (downsampled) input to the output
	 */
	// NOTE (TP) blending and border are modified
	public static void adjustBlending( final BasicViewDescription< ? > vd, final float[] blending, final float[] border, final AffineTransform3D transformationModel )
	{
		adjustBlending( vd.getViewSetup().getSize(), Group.pvid( vd ), blending, border, transformationModel );
	}

	public static void adjustBlending( final Dimensions dim, final String name, final float[] blending, final float[] border, final AffineTransform3D transformationModel )
	{
		final double[] scale = TransformationTools.scaling( dim, transformationModel ).getA();

		final NumberFormat f = TransformationTools.f;

		//System.out.println( "View " + name + " is currently scaled by: (" +
		//		f.format( scale[ 0 ] ) + ", " + f.format( scale[ 1 ] ) + ", " + f.format( scale[ 2 ] ) + ")" );

		for ( int d = 0; d < blending.length; ++d )
		{
			blending[ d ] /= ( float )scale[ d ];
			border[ d ] /= ( float )scale[ d ];
		}
	}
}
