package net.preibisch.bigstitcher.spark.blk;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
import net.imglib2.Cursor;
import net.imglib2.Dimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealPoint;
import net.imglib2.RealRandomAccessible;
import net.imglib2.algorithm.blocks.BlockAlgoUtils;
import net.imglib2.algorithm.blocks.UnaryBlockOperator;
import net.imglib2.algorithm.blocks.transform.Transform;
import net.imglib2.blocks.PrimitiveBlocks;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
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

			final RandomAccessibleInterval< FloatType > transformedBlending = transformBlendingRender(
					inputImg,
					border,
					blending,
					model,
					boundingBox );

//			final RandomAccessibleInterval< FloatType > transformedBlendingRender = transformBlendingRender(
//					inputImg,
//					border,
//					blending,
//					model,
//					boundingBox );

//			if ( __first.getAndSet( false ) )
//			{
//				final BdvSource bdv1 = BdvFunctions.show( transformedBlending, "transformBlending" );
//				final BdvSource bdv2 = BdvFunctions.show( transformedBlendingRender, "transformedBlendingRender", Bdv.options().addTo( bdv1 ) );
//				bdv1.setDisplayRange( 0, 1 );
//				bdv1.setColor( new ARGBType( 0xff00ff ) );
//				bdv2.setDisplayRange( 0, 1 );
//				bdv2.setColor( new ARGBType( 0x00ff00 ) );
//			}

			weights.add( transformedBlending );
		}

		return new FusedRandomAccessibleInterval( new FinalInterval( getFusedZeroMinInterval( boundingBox ) ), images, weights );
	}

//	static AtomicBoolean __first = new AtomicBoolean( true );

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

	private static RandomAccessibleInterval< FloatType > transformBlendingRender(
			final Interval inputImgInterval,
			final float[] border,
			final float[] blending,
			final AffineTransform3D transform,
			final Interval boundingBox )
	{
		final AffineTransform3D t1 = new AffineTransform3D();
		t1.setTranslation(
				-boundingBox.min( 0 ),
				-boundingBox.min( 1 ),
				-boundingBox.min( 2 ) );
		t1.concatenate( transform );

		final AffineTransform3D t = new AffineTransform3D();
		t.translate( inputImgInterval.minAsDoubleArray() );
		t.preConcatenate( t1 );

		final long[] dim = boundingBox.dimensionsAsLongArray();
		final float[] weights = new float[ ( int ) Intervals.numElements( dim ) ];
		final Img< FloatType > rai = ArrayImgs.floats( weights, dim );

		final int n = 3;
		final int[] dimMinus1 = {
				( int ) inputImgInterval.dimension( 0 ) - 1,
				( int ) inputImgInterval.dimension( 1 ) - 1,
				( int ) inputImgInterval.dimension( 2 ) - 1 };

		final float[] b0 = new float[ n ];
		final float[] b1 = new float[ n ];
		final float[] b2 = new float[ n ];
		final float[] b3 = new float[ n ];
		for ( int d = 0; d < n; ++d )
		{
			b0[ d ] = border[ d ];
			b1[ d ] = border[ d ] + blending[ d ];
			b2[ d ] = dimMinus1[ d ] - border[ d ] - blending[ d ];
			b3[ d ] = dimMinus1[ d ] - border[ d ];
			// TODO handle b1 > b2
			// TODO handle (b1+b2)/2 < a
		}

		final double[] p = new double[ n ];
		final double[] location = new double[ n ];
		final double[] d0 = t.inverse().d( 0 ).positionAsDoubleArray();
		final int sx = ( int ) dim[ 0 ];
		final int sy = ( int ) dim[ 1 ];
		final int sz = ( int ) dim[ 2 ];
		for ( int z = 0; z < sz; ++z )
		{
			p[ 2 ] = z;
			for ( int y = 0; y < sy; ++y )
			{
				p[ 1 ] = y;
				p[ 0 ] = 0;
				t.applyInverse( location, p );
				final int offset = ( z * sy + y ) * sx;
				for ( int x = 0; x < sx; ++x )
				{
					weights[ offset + x ] = 1;
				}
				for ( int d = 0; d < 3; ++d )
				{
					final float l0 = ( float ) location[ d ];
					final float dd = ( float ) d0[ d ];
					if ( Math.abs( dd ) < 0.0001f )
					{
						float weight = computeWeight( l0, blending[ d ], b0[ d ], b1[ d ], b2[ d ], b3[ d ] );
						for ( int x = 0; x < sx; ++x )
						{
							weights[ offset + x ] *= weight;
						}
					}
					else
					{
						final float blend = blending[ d ] / dd;
						final float b0d = ( b0[ d ] - l0 ) / dd;
						final float b1d = ( b1[ d ] - l0 ) / dd;
						final float b2d = ( b2[ d ] - l0 ) / dd;
						final float b3d = ( b3[ d ] - l0 ) / dd;

						// TODO: next: loop x over 0 .. b0d .. b1d .. b2d .. b3d .. sx
						//       with bounds checking and floored loop bounds

						for ( int x = 0; x < sx; ++x )
						{
							float weight = computeWeight( x, blend, b0d, b1d, b2d, b3d );
							weights[ offset + x ] *= weight;
						}
					}
				}
			}
		}

		return rai;
	}

	private static float computeWeight( final float l, final float blending, final float b0, final float b1, final float b2, final float b3 )
	{
		if ( l < b0 )
		{
			return 0;
		}
		else if ( l < b1 )
		{
			return SmallLookup.fn( ( l - b0 ) / blending );
		}
		else if ( l < b2 )
		{
			return 1;
		}
		else if ( l < b3 )
		{
			return SmallLookup.fn( ( b3 - l ) / blending );
		}
		else
		{
			return 0;
		}
	}

	static class SmallLookup
	{
		// [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ]
		//   0                             1
		//   n = 10
		//   d = (double) i / n

		private static final int n = 30;

		// static lookup table for the blending function
		private static final float[] lookUp = createLookup( n );

		private static float[] createLookup( final int n )
		{
			final float[] lookup = new float[ n + 2 ];
			for ( int i = 0; i <= n; i++ )
			{
				final double d = ( double ) i / n;
				lookup[ i ] = ( float ) ( ( Math.cos( ( 1 - d ) * Math.PI ) + 1 ) / 2 );
			}
			lookup[ n + 1 ] = lookup[ n ];
			return lookup;
		}

		static float fn( final float d )
		{
			final int i = ( int ) ( d * n );
			final float s = ( d * n ) - i;
			return lookUp[ i ] * (1.0f - s) + lookUp[ i + 1 ] * s;
		}

		static float computeWeight(
				final double[] location,
				final int[] dimMinus1,
				final float[] border,
				final float[] blending,
				final float[] tmp, // holds dist, if any of it is zero we can stop
				final int n )
		{
			for ( int d = 0; d < n; ++d )
			{
				// the position in the image relative to the boundaries and the border
				final float l = ( float ) location[ d ];

				// the distance to the border that is closer
				tmp[ d ] = Math.min( l - border[ d ], dimMinus1[ d ] - l - border[ d ] );

				// if this is smaller or equal to 0, the total result will be 0, independent of the number of dimensions
				if ( tmp[ d ] <= 0 )
					return 0;
			}

			// compute multiplicative distance to the respective borders [0...1]
			float minDistance = 1;

			for ( int d = 0; d < n; ++d )
			{
				final float relDist = tmp[ d ] / blending[ d ];

				// within the range where we blend from 0 to 1
				if ( relDist < 1 )
					minDistance *= fn( relDist ); //( Math.cos( ( 1 - relDist ) * Math.PI ) + 1 ) / 2;
			}

			return minDistance;
		}

		static float computeWeight2(
				final double[] location,
				final int[] dimMinus1,
				final float[] border,
				final float[] blending,
				final float[] tmp, // holds dist, if any of it is zero we can stop
				final int n )
		{
			float[] b0 = new float[ n ];
			float[] b1 = new float[ n ];
			float[] b2 = new float[ n ];
			float[] b3 = new float[ n ];
			for ( int d = 0; d < n; ++d )
			{
				b0[ d ] = border[ d ];
				b1[ d ] = border[ d ] + blending[ d ];
				b2[ d ] = dimMinus1[ d ] - border[ d ] - blending[ d ];
				b3[ d ] = dimMinus1[ d ] - border[ d ];
				// TODO handle b1 > b2
				// TODO handle (b1+b2)/2 < a
			}

			return computeWeight3( location, blending, n, b0, b1, b2, b3 );
		}

		private static float computeWeight3( final double[] location, final float[] blending, final int n, final float[] b0, final float[] b1, final float[] b2, final float[] b3 )
		{
			float w = 1;
			for ( int d = 0; d < n; ++d )
			{
				// the position in the image relative to the boundaries and the border
				final float l = ( float ) location[ d ];

				if ( l < b0[ d ] )
				{
					return 0;
				}
				else if ( l < b1[ d ] )
				{
					w *= fn( ( l - b0[ d ] ) / blending[ d ] );
				}
				else if ( l < b2[ d ] )
				{
//					w *= 1;
				}
				else if ( l < b3[ d ] )
				{
					w *= fn( ( b3[ d ] - l ) / blending[ d ] );
				}
				else
				{
					return 0;
				}
			}
			return w;
		}
	}


	public static void main( String[] args )
	{

		AffineTransform3D transform = new AffineTransform3D();
		transform.rotate( 0, 0.2 );
		transform.rotate( 1, 1.2 );
		transform.rotate( 2, -2.4 );


		final double[] min = { 10, 10, 10 };

		final double[] c = { 1, 2, 3 };
		final double[] location = new double[ 3 ];
		transform.applyInverse( location, c );


		final double[] l = new double[ 3 ];
		l[ 0 ] = location[ 0 ] - min[ 0 ];
		l[ 1 ] = location[ 1 ] - min[ 1 ];
		l[ 2 ] = location[ 2 ] - min[ 2 ];
		System.out.println( "l = " + Arrays.toString( l ) );


		AffineTransform3D t = new AffineTransform3D();
		t.translate( min );
		t.preConcatenate( transform );
		t.applyInverse( l, c );
		System.out.println( "l = " + Arrays.toString( l ) );

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
