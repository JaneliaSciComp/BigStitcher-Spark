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
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.blocks.BlockProcessor;
import net.imglib2.algorithm.blocks.ComputationType;
import net.imglib2.algorithm.blocks.UnaryBlockOperator;
import net.imglib2.algorithm.blocks.ClampType;
import net.imglib2.algorithm.blocks.transform.Transform;
import net.imglib2.algorithm.blocks.transform.Transform.Interpolation;
import net.imglib2.blocks.PrimitiveBlocks;
import net.imglib2.blocks.TempArray;
import net.imglib2.cache.img.ReadOnlyCachedCellImgFactory;
import net.imglib2.cache.img.ReadOnlyCachedCellImgOptions;
import net.imglib2.cache.img.SingleCellArrayImg;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.PrimitiveType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.CloseableThreadLocal;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.mvrecon.process.downsampling.DownsampleTools;
import net.preibisch.mvrecon.process.fusion.FusionTools;

public class FusionFirstWins
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
		// TODO: make sorted(Comparator) configurable
		final List< ViewId > viewIdsToProcess = views.stream().sorted().collect( Collectors.toList() );

		final List< TransformedViewBlocks< ? > > images = new ArrayList<>();
		final List< Masking > maskings = new ArrayList<>();

		for ( final ViewId viewId : viewIdsToProcess )
		{
			final AffineTransform3D model = registrations.get( viewId ).copy();

			// this modifies the model so it maps from a smaller image to the global coordinate space,
			// which applies for the image itself as well as the weights since they also use the smaller
			// input image as reference
			final double[] usedDownsampleFactors = new double[ 3 ];
			RandomAccessibleInterval inputImg = DownsampleTools.openDownsampled( imgloader, viewId, model, usedDownsampleFactors );

			Object inputImgType = imgloader.getSetupImgLoader( viewId.getViewSetupId() ).getImageType();

			final int interpolation = 1; // TODO
			final TransformedViewBlocks< ? > viewBlocks = new TransformedViewBlocks( inputImg, ( NativeType ) inputImgType, model, boundingBox );
			images.add( viewBlocks );

			// SIMPLIFIED
			// add all (or no) weighting schemes
			// assuming:
			// 	final boolean useBlending = true;
			// 	final boolean useContentBased = false;

			// instantiate blending if necessary
			final float[] border = Util.getArrayFromValue( FusionTools.defaultBlendingBorder, 3 );

			// adjust both for z-scaling (anisotropy), downsampling, and registrations itself
			final float[] dummyBlending = new float[ 3 ];
			FusionTools.adjustBlending( viewDescriptions.get( viewId ), dummyBlending, border, model );

			maskings.add( new Masking( inputImg, border, model ) );
		}

		return getFusedRandomAccessibleInterval( boundingBox, images, maskings, type, minIntensity, range );
	}

	private static class TransformedViewBlocks< T extends NativeType< T > >
	{
		private final PrimitiveBlocks< T > threadSafeBlocks;

		private final UnaryBlockOperator< T, FloatType > threadSafeOperator;

		TransformedViewBlocks(
				final RandomAccessibleInterval< T > input,
				final T type, // TODO: resolve generics... this should be T
				final AffineTransform3D transform,
				final Interval boundingBox )
		{
			final AffineTransform3D t = new AffineTransform3D();
			t.setTranslation(
					-boundingBox.min( 0 ),
					-boundingBox.min( 1 ),
					-boundingBox.min( 2 ) );
			t.concatenate( transform );
			threadSafeBlocks = PrimitiveBlocks.of( Views.extendBorder( input ) )
					.threadSafe();
			threadSafeOperator = Transform.createAffineOperator( new FloatType(), t, Interpolation.NLINEAR, ComputationType.FLOAT, ClampType.NONE )
					.adaptSourceType( type, ClampType.NONE )
					.threadSafe();
		}

		void compute( final float[] dest, final Interval interval )
		{
			final BlockProcessor< Object, Object > processor = threadSafeOperator.blockProcessor();
			processor.setTargetInterval( interval );
			final Object src = processor.getSourceBuffer();
			threadSafeBlocks.copy( processor.getSourcePos(), src, processor.getSourceSize() );
			processor.compute( src, dest );
		}
	}

	private static < T extends NativeType< T > > RandomAccessibleInterval< T > getFusedRandomAccessibleInterval(
			final Interval boundingBox,
			final List< TransformedViewBlocks< ? > > images,
			final List< Masking > maskings,
			final T type,
			final double minIntensity,
			final double range )
	{
		final CloseableThreadLocal< FuserConverter< T > > fuserThreadLocal = CloseableThreadLocal.withInitial( () -> new FuserConverter<>( boundingBox, images, maskings, type, minIntensity, range ) );
		return new ReadOnlyCachedCellImgFactory().create(
				boundingBox.dimensionsAsLongArray(),
				type,
				cell -> fuserThreadLocal.get().load( cell ),
				ReadOnlyCachedCellImgOptions.options().cellDimensions( 128, 64, 64 ) );
//				ReadOnlyCachedCellImgOptions.options().cellDimensions( 32, 32, 32 ) );
//				ReadOnlyCachedCellImgOptions.options().cellDimensions( 32, 128, 128 ) );
//				ReadOnlyCachedCellImgOptions.options().cellDimensions( 64, 64, 64 ) );
	}

	private static class FuserConverter< T extends NativeType< T > >
	{
		// TODO: use arrays instead of lists?
		private final List< TransformedViewBlocks< ? > > views;

		private final TempArray< float[] >[] imgTempArrays;

		private final float[][] imgBuffers;

		private final long[] boundingBox_min;

		private final List< Masking > maskings;

		private final long[] cell_min;

		private int[] cell_dims;

		private final long[] bb_min;

		private final TempArray< byte[] > maskTempArray = TempArray.forPrimitiveType( PrimitiveType.BYTE );

		private final TempArray< float[] > intensitiesTempArray = TempArray.forPrimitiveType( PrimitiveType.FLOAT );

		private final TempArray< byte[] > accMaskTempArray = TempArray.forPrimitiveType( PrimitiveType.BYTE );

		private final TempArray< float[] > accIntensityTempArray = TempArray.forPrimitiveType( PrimitiveType.FLOAT );

		private final double[] pos = new double[ 3 ];

		private final FillOutputLine fillOutputLine;

		FuserConverter(
				final Interval boundingBox,
				final List< TransformedViewBlocks< ? > > views,
				final List< Masking > maskings,
				final T type, // output type
				final double minIntensity, // only used if output type is uint8 or uint16
				final double range ) // only used if output type is uint8 or uint16
		{
			final int n = boundingBox.numDimensions();
			cell_min = new long[ n ];
			cell_dims = new int[ n ];
			bb_min = new long[ n ];

			this.views = views;
			this.maskings = maskings;
			final int numImages = maskings.size();
			imgTempArrays = new TempArray[ numImages ];
			Arrays.setAll( imgTempArrays, i -> TempArray.forPrimitiveType( PrimitiveType.FLOAT ) );
			imgBuffers = new float[ numImages ][];
			boundingBox_min = boundingBox.minAsLongArray();

			fillOutputLine = FillOutputLine.of( type, minIntensity, range );
		}

		void load( SingleCellArrayImg< T, ? > cell ) throws Exception
		{
			final int numImages = imgBuffers.length;

			Arrays.setAll( cell_min, cell::min );
			Arrays.setAll( cell_dims, d -> ( int ) cell.dimension( d ) );
			final int cell_size = ( int ) Intervals.numElements( cell_dims );

			for ( int i = 0; i < numImages; ++i )
			{
				final float[] floats = imgTempArrays[ i ].get( cell_size );
				views.get( i ).compute( floats, cell );
				imgBuffers[ i ] = floats;
			}

			Arrays.setAll( bb_min, d -> cell_min[ d ] + boundingBox_min[ d ] );

			final int sx = cell_dims[ 0 ];
			final int sy = cell_dims[ 1 ];
			final int sz = cell_dims[ 2 ];

			final byte[] tmpW = maskTempArray.get( sx );
			final float[] tmpI = intensitiesTempArray.get( sx );
			final byte[] accW = accMaskTempArray.get( sx );
			final float[] accI = accIntensityTempArray.get( sx );

			final Object output = cell.getStorageArray();
			pos[ 0 ] = bb_min[ 0 ];
			for ( int z = 0; z < sz; ++z )
			{
				pos[ 2 ] = z + bb_min[ 2 ];
				for ( int y = 0; y < sy; ++y )
				{
					pos[ 1 ] = y + bb_min[ 1 ];
					final int offset = ( z * sy + y ) * sx;
					Arrays.fill( accW, ( byte ) 0 );
					Arrays.fill( accI, 0 );
					for ( int i = 0; i < numImages; i++ )
					{
						System.arraycopy( imgBuffers[ i ], offset, tmpI, 0, sx );
						maskings.get( i ).fill_range( tmpW, 0, sx, pos );
						if ( acc( sx, tmpW, accW, tmpI, accI ) )
							break;
					}
					fillOutputLine.compute( accI, output, offset, sx );
				}
			}
		}

		// TODO
		//   if true is returned we don't have to go on to the next image
		private static boolean acc(
				final int sx,
				final byte[] tmpM,
				final byte[] accM,
				final float[] tmpI,
				final float[] accI )
		{
			boolean done = true;
			for ( int x = 0; x < sx; ++x )
				if ( accM[ x ] == 0 )
					if ( tmpM[ x ] == 1 )
					{
						accM[ x ] = 1;
						accI[ x ] = tmpI[ x ];
					}
					else
						done = false;
			return done;
		}

	}

	@FunctionalInterface
	interface FillOutputLine
	{
		void compute( float[] sumI, Object output, int offset, int length );

		static FillOutputLine of(
				final Object type, // output type
				final double minIntensity, // only used if output type is uint8 or uint16
				final double range ) // only used if output type is uint8 or uint16
		{
			if ( type instanceof FloatType )
			{
				return ( sumI, output, offset, length ) -> copy( sumI, offset, length, ( float[] ) output );
			}
			else if ( type instanceof UnsignedByteType )
			{
				final float a = ( float ) ( 1 / range );
				final float b = ( float ) ( 0.5 - minIntensity / range );
				return ( sumI, output, offset, length ) -> convert_uint8( sumI, offset, length, ( byte[] ) output, a, b );
			}
			else if ( type instanceof UnsignedShortType )
			{
				final float a = ( float ) ( 1 / range );
				final float b = ( float ) ( 0.5 - minIntensity / range );
				return ( sumI, output, offset, length ) -> convert_uint16( sumI, offset, length, ( short[] ) output, a, b );
			}
			else
				throw new IllegalArgumentException();
		}

	}

	//
	private static void copy( final float[] sumI, final int offset, final int length, final float[] out )
	{
		for ( int x = 0; x < length; ++x )
			out[ offset + x ] = sumI[ x ];
	}

	private static void convert_uint8( final float[] sumI, final int offset, final int length, final byte[] out, final float a, final float b )
	{
		for ( int x = 0; x < length; ++x )
			out[ offset + x ] = ( byte ) ( sumI[ x ] * a + b );
	}

	private static void convert_uint16( final float[] sumI, final int offset, final int length, final short[] out, final float a, final float b )
	{
		for ( int x = 0; x < length; ++x )
			out[ offset + x ] = ( short ) ( sumI[ x ] * a + b );
	}
}
