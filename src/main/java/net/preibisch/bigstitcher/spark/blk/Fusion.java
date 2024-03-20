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
import net.imglib2.Dimensions;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.blocks.BlockAlgoUtils;
import net.imglib2.algorithm.blocks.UnaryBlockOperator;
import net.imglib2.algorithm.blocks.convert.ClampType;
import net.imglib2.algorithm.blocks.transform.Transform;
import net.imglib2.algorithm.blocks.transform.Transform.ComputationType;
import net.imglib2.algorithm.blocks.transform.Transform.Interpolation;
import net.imglib2.blocks.PrimitiveBlocks;
import net.imglib2.blocks.TempArray;
import net.imglib2.cache.img.ReadOnlyCachedCellImgFactory;
import net.imglib2.cache.img.ReadOnlyCachedCellImgOptions;
import net.imglib2.cache.img.SingleCellArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.PrimitiveType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.CloseableThreadLocal;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.mvrecon.process.downsampling.DownsampleTools;
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

		final List< RandomAccessibleInterval< FloatType > > images = new ArrayList<>();
		final List< Blending > blendings = new ArrayList<>();

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
			final RandomAccessibleInterval< FloatType > transformedInputImg = transformView( inputImg, inputImgType, model, boundingBox );
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

			blendings.add( new Blending( inputImg, border, blending, model ) );
		}

		return getFusedRandomAccessibleInterval_blk_lazy( boundingBox, images, blendings );
	}

	private static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< FloatType > transformView(
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
		final UnaryBlockOperator< T, FloatType > operator = Transform.affine( ( T ) type, t, Interpolation.NLINEAR, ComputationType.FLOAT ).adaptTargetType( new FloatType(), ClampType.NONE );
		return BlockAlgoUtils.cellImg(
				blocks,
				operator,
				new FloatType(),
				boundingBox.dimensionsAsLongArray(),
				new int[] { 64, 64, 64 } );
	}


	static class Fuser
	{
		private final List< PrimitiveBlocks< FloatType > > imgBlocks;
		private final List< TempArray< float[] > > imgBuffers;
		private final long[] boundingBox_min;
		private final List< Blending > blendings;

		private final long[] cell_min;
		private int[] cell_dims;
		private final long[] bb_min;

		private final TempArray< float[] > weightsTempArray = TempArray.forPrimitiveType( PrimitiveType.FLOAT );
		private final TempArray< float[] > sumWeightsTempArray = TempArray.forPrimitiveType( PrimitiveType.FLOAT );
		private final TempArray< float[] > sumIntensityTempArray = TempArray.forPrimitiveType( PrimitiveType.FLOAT );

		private final double[] pos = new double[ 3 ];

		Fuser(
				final Interval boundingBox,
				final List< RandomAccessibleInterval< FloatType > > images,
				final List< Blending > blendings )
		{
			final int n = boundingBox.numDimensions();
			cell_min = new long[ n ];
			cell_dims = new int[ n ];
			bb_min = new long[ n ];

			this.blendings = blendings;
			imgBlocks = new ArrayList<>();
			imgBuffers = new ArrayList<>();
			for ( RandomAccessibleInterval< FloatType > image : images )
			{
				imgBlocks.add( PrimitiveBlocks.of( image ) );
				imgBuffers.add( TempArray.forPrimitiveType( PrimitiveType.FLOAT ) );
			}
			boundingBox_min = boundingBox.minAsLongArray();
		}

		void load( SingleCellArrayImg< FloatType, ? > cell ) throws Exception
		{
			Arrays.setAll( cell_min, cell::min );
			Arrays.setAll( cell_dims, d -> ( int ) cell.dimension( d ) );
			final int cell_size = ( int ) Intervals.numElements( cell_dims );

			// TODO pre-alloc
			final float[][] imgFloats = new float[ imgBlocks.size() ][];
			for ( int i = 0; i < imgFloats.length; ++i )
			{
				final PrimitiveBlocks< FloatType > blocks = imgBlocks.get( i );
				final float[] floats = imgBuffers.get( i ).get( cell_size );
				blocks.copy( cell_min, floats, cell_dims );
				imgFloats[ i ] = floats;
			}

			Arrays.setAll( bb_min, d -> cell_min[ d ] + boundingBox_min[ d ] );
			final float[] output = ( float[] ) cell.getStorageArray();

			final int sx = cell_dims[ 0 ];
			final int sy = cell_dims[ 1 ];
			final int sz = cell_dims[ 2 ];

			final float[] tmpW = weightsTempArray.get( sx );
			final float[] sumW = sumWeightsTempArray.get( sx );
			final float[] sumI = sumIntensityTempArray.get( sx );

			pos[ 0 ] = bb_min[ 0 ];
			for ( int z = 0; z < sz; ++z )
			{
				pos[ 2 ] = z + bb_min[ 2 ];
				for ( int y = 0; y < sy; ++y )
				{
					pos[ 1 ] = y + bb_min[ 1 ];
					final int offset = ( z * sy + y ) * sx;
					Arrays.fill( sumW, 0 );
					Arrays.fill( sumI, 0 );
					for ( int i = 0; i < imgFloats.length; i++ )
					{
						final float[] imageFloats = imgFloats[ i ];
						blendings.get( i ).fill_range( tmpW, 0, sx, pos );
						for ( int x = 0; x < sx; x++ )
						{
							final float weight = tmpW[ x ];
							final float intensity = imageFloats[ offset + x ];
							sumW[ x ] += weight;
							sumI[ x ] += weight * intensity;
						}
					}
					for ( int x = 0; x < sx; x++ )
					{
						final float w = sumW[ x ];
						output[ offset + x ] = ( w > 0 ) ? sumI[ x ] / w : 0;
					}
				}
			}
		}
	}

	private static RandomAccessibleInterval< FloatType > getFusedRandomAccessibleInterval_blk_lazy(
			final Interval boundingBox,
			final List< RandomAccessibleInterval< FloatType > > images,
			final List< Blending > blendings )
	{
		final CloseableThreadLocal< Fuser > fuserThreadLocal = CloseableThreadLocal.withInitial( () -> new Fuser( boundingBox, images, blendings ) );
		return new ReadOnlyCachedCellImgFactory().create(
				boundingBox.dimensionsAsLongArray(),
				new FloatType(),
				cell -> fuserThreadLocal.get().load( cell ),
				ReadOnlyCachedCellImgOptions.options().cellDimensions( 64, 64, 64 ) );
	}




	// ------------------------------------------------------------------------
	//
	//  unmodified from FusionTools
	//
	// ------------------------------------------------------------------------

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
