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
package net.preibisch.bigstitcher.spark.render;

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferInt;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import bdv.BigDataViewer;
import bdv.cache.CacheControl;
import bdv.spimdata.WrapBasicImgLoader;
import bdv.tools.brightness.ConverterSetup;
import bdv.viewer.BasicViewerState;
import bdv.viewer.DisplayMode;
import bdv.viewer.Interpolation;
import bdv.viewer.Source;
import bdv.viewer.SourceAndConverter;
import bdv.viewer.render.MultiResolutionRenderer;
import bdv.viewer.render.RenderTarget;
import bdv.viewer.render.awt.BufferedImageRenderResult;
import mpicbg.spim.data.generic.AbstractSpimData;
import mpicbg.spim.data.generic.sequence.BasicViewSetup;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.IterableInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.util.Intervals;
import net.imglib2.display.screenimage.awt.ARGBScreenImage;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

/**
 * Renders what BigDataViewer would show for a SpimData without any window: the view setups become sources exactly
 * as BigDataViewer sets them up ({@link BigDataViewer#initSetups}), a {@link BasicViewerState} holds which of them
 * are shown, their colours and display ranges, the timepoint and the viewer transform (global to image pixels), and
 * a {@link MultiResolutionRenderer} with non-volatile sources renders one complete frame per call. A maximum
 * projection steps the viewer transform along screen z and keeps the per-channel maximum, like BigDataViewer's
 * "Record Max-Projection Movie". The rendered image keeps its viewer transform so overlays can be drawn later.
 */
public class HeadlessBdvRenderer implements AutoCloseable
{
	/** a rendered image (ARGB, transparent black where no source has data) with the viewer transform that made it */
	public record Frame( BufferedImage image, AffineTransform3D viewerTransform, int width, int height, int frames ) {}

	private final AbstractSpimData< ? > data;
	private final boolean wrappedLoader;
	private final List< SourceAndConverter< ? > > sources = new ArrayList<>();
	private final List< ConverterSetup > converterSetups = new ArrayList<>();
	private final Map< Integer, Integer > indexOfSetup = new HashMap<>();
	private final BasicViewerState state = new BasicViewerState();
	private final MaxProjectionTarget target = new MaxProjectionTarget();
	private final MultiResolutionRenderer renderer;

	/**
	 * @param data the dataset; an image loader that is not a BigDataViewer loader is wrapped (and unwrapped in {@link #close()})
	 * @param numRenderingThreads threads of the renderer
	 * @param pinnedLevel a resolution level every source is restricted to (0 = full resolution), or null to let the
	 *        renderer pick the level from the screen scale
	 */
	public HeadlessBdvRenderer( final AbstractSpimData< ? > data, final int numRenderingThreads, final Integer pinnedLevel )
	{
		this.data = data;
		this.wrappedLoader = WrapBasicImgLoader.wrapImgLoaderIfNecessary( data );
		BigDataViewer.initSetups( data, converterSetups, sources );

		if ( pinnedLevel != null )
			for ( int i = 0; i < sources.size(); ++i )
				sources.set( i, pin( sources.get( i ), pinnedLevel ) );

		int i = 0;
		for ( final BasicViewSetup vs : data.getSequenceDescription().getViewSetupsOrdered() )
			indexOfSetup.put( vs.getId(), i++ );

		state.setNumTimepoints( Math.max( 1, data.getSequenceDescription().getTimePoints().size() ) );
		state.addSources( sources );
		state.setSourcesActive( sources, false );
		state.setDisplayMode( DisplayMode.FUSED );
		state.setInterpolation( Interpolation.NLINEAR );

		renderer = new MultiResolutionRenderer( target, () -> {}, new double[] { 1 }, 0, Math.max( 1, numRenderingThreads ), null, false, new CacheControl.Dummy() );
	}

	private static < T > SourceAndConverter< T > pin( final SourceAndConverter< T > soc, final int level )
	{
		return new SourceAndConverter<>( new SingleLevelSource<>( soc.getSpimSource(), level ), soc.getConverter() );
	}

	private int index( final int setupId )
	{
		final Integer i = indexOfSetup.get( setupId );
		if ( i == null )
			throw new IllegalArgumentException( "no view setup " + setupId );
		return i;
	}

	/** the index into {@code getTimePointsOrdered()}, not the timepoint id */
	public void setTimepointIndex( final int index )
	{
		state.setCurrentTimepoint( index );
	}

	/** shows exactly these setups */
	public void setActiveSetups( final Collection< Integer > setupIds )
	{
		state.setSourcesActive( sources, false );
		for ( final int id : setupIds )
			state.setSourceActive( sources.get( index( id ) ), true );
	}

	/** @param argb the colour with alpha 255, e.g. {@code ARGBType.rgba( 255, 0, 255, 255 )} */
	public void setColor( final int setupId, final int argb )
	{
		final ConverterSetup cs = converterSetups.get( index( setupId ) );
		if ( cs.supportsColor() )
			cs.setColor( new ARGBType( argb ) );
	}

	public void setDisplayRange( final int setupId, final double min, final double max )
	{
		converterSetups.get( index( setupId ) ).setDisplayRange( min, max );
	}

	public Source< ? > source( final int setupId )
	{
		return sources.get( index( setupId ) ).getSpimSource();
	}

	/** a region is sampled at the coarsest resolution level that still holds this many voxels of it */
	public static final long MIN_RANGE_SAMPLES = 20_000;

	/**
	 * Display range from the percentiles of a source at a timepoint, sampled with at most {@code maxSamples} voxels
	 * of the coarsest resolution level (of the whole image, or of the part inside {@code globalBox} at the coarsest
	 * level that still holds {@link #MIN_RANGE_SAMPLES} voxels of it).
	 *
	 * @param globalBox the region in global coordinates, or null for the whole image
	 * @return [min, max], or null if the source is absent at the timepoint, not a real-valued type, or misses the box
	 */
	public double[] estimateDisplayRange( final int setupId, final int timepointIndex, final double lowerPercentile, final double upperPercentile, final long maxSamples, final RealInterval globalBox )
	{
		final Source< ? > src = source( setupId );
		if ( !src.isPresent( timepointIndex ) || !( src.getType() instanceof RealType ) )
			return null;

		IterableInterval< ? extends RealType< ? > > ii = null;
		for ( int level = src.getNumMipmapLevels() - 1; level >= 0 && ii == null; --level )
		{
			@SuppressWarnings( "unchecked" )
			final RandomAccessibleInterval< ? extends RealType< ? > > rai = ( RandomAccessibleInterval< ? extends RealType< ? > > ) src.getSource( timepointIndex, level );
			Interval region = rai;
			if ( globalBox != null )
			{
				final AffineTransform3D sourceToGlobal = new AffineTransform3D();
				src.getSourceTransform( timepointIndex, level, sourceToGlobal );
				region = Intervals.intersect( Intervals.smallestContainingInterval( sourceToGlobal.inverse().estimateBounds( globalBox ) ), rai );
				if ( Intervals.isEmpty( region ) )
					return null;
			}
			if ( Intervals.numElements( region ) >= MIN_RANGE_SAMPLES || level == 0 )
				ii = Views.iterable( Views.interval( rai, region ) );
		}
		final long n = ii.size();
		if ( n == 0 )
			return null;
		final long stride = Math.max( 1, n / Math.max( 1, maxSamples ) );
		final int m = (int) ( n / stride );
		final float[] v = new float[ m ];
		final Cursor< ? extends RealType< ? > > c = ii.cursor();
		for ( int i = 0; i < m; ++i )
		{
			c.jumpFwd( stride );
			v[ i ] = c.get().getRealFloat();
		}
		Arrays.sort( v );
		double min = v[ (int) Math.floor( lowerPercentile * ( m - 1 ) ) ];
		double max = v[ (int) Math.ceil( upperPercentile * ( m - 1 ) ) ];
		if ( max <= min )
		{
			min = v[ 0 ];
			max = v[ m - 1 ];
		}
		if ( max <= min )
			max = min + 1;
		return new double[] { min, max };
	}

	/** {@link #estimateDisplayRange(int, int, double, double, long, RealInterval)} over the whole image */
	public double[] estimateDisplayRange( final int setupId, final int timepointIndex, final double lowerPercentile, final double upperPercentile, final long maxSamples )
	{
		return estimateDisplayRange( setupId, timepointIndex, lowerPercentile, upperPercentile, maxSamples, null );
	}

	/** one frame at screen z = 0 of the viewer transform */
	public Frame renderSlice( final AffineTransform3D viewerTransform, final int width, final int height )
	{
		return renderMip( viewerTransform, width, height, 0, 0, 1 );
	}

	/**
	 * Maximum projection over the planes screen z = zMin, zMin + step, ... up to zMax (screen pixels along the
	 * viewing axis of the viewer transform); zMin = zMax renders a single plane.
	 */
	public Frame renderMip( final AffineTransform3D viewerTransform, final int width, final int height, final double zMin, final double zMax, final double stepScreen )
	{
		target.setSize( width, height );
		target.clear();

		final int n = Math.max( 1, (int) Math.floor( ( zMax - zMin ) / Math.max( 1e-9, stepScreen ) ) + 1 );
		final AffineTransform3D shifted = new AffineTransform3D();
		for ( int k = 0; k < n; ++k )
		{
			final double z = zMin + k * stepScreen;
			shifted.set( 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, -z ); // the plane screen z = z becomes the rendered plane
			shifted.concatenate( viewerTransform );
			state.setViewerTransform( shifted );
			renderer.requestRepaint();
			if ( !renderer.paint( state ) )
				throw new IllegalStateException( "rendering failed (" + width + " x " + height + ")" );
		}

		final BufferedImage copy = new BufferedImage( width, height, BufferedImage.TYPE_INT_ARGB );
		System.arraycopy( target.accumulated.getData(), 0, ( ( DataBufferInt ) copy.getRaster().getDataBuffer() ).getData(), 0, width * height );
		return new Frame( copy, viewerTransform.copy(), width, height, n );
	}

	/** writes the image as an opaque PNG: pixels without data become black */
	public static void writePng( final BufferedImage argb, final File file ) throws IOException
	{
		final BufferedImage rgb = new BufferedImage( argb.getWidth(), argb.getHeight(), BufferedImage.TYPE_INT_RGB );
		final int[] src = ( ( DataBufferInt ) argb.getRaster().getDataBuffer() ).getData();
		final int[] dst = ( ( DataBufferInt ) rgb.getRaster().getDataBuffer() ).getData();
		for ( int i = 0; i < dst.length; ++i )
			dst[ i ] = src[ i ] & 0x00ffffff;
		if ( !ImageIO.write( rgb, "png", file ) )
			throw new IOException( "no PNG writer available" );
	}

	@Override
	public void close()
	{
		renderer.kill();
		if ( wrappedLoader )
			WrapBasicImgLoader.removeWrapperIfPresent( data );
		closeQuietly( data.getSequenceDescription().getImgLoader() );
	}

	/** calls a public no-argument close() if the loader has one (N5/OME-Zarr loaders shut down their fetcher threads there) */
	static void closeQuietly( final Object loader )
	{
		if ( loader == null )
			return;
		try
		{
			final Method m = loader.getClass().getMethod( "close" );
			m.invoke( loader );
		}
		catch ( final NoSuchMethodException e )
		{
			// nothing to close
		}
		catch ( final Exception e )
		{
			System.err.println( "WARNING: closing the image loader failed: " + e );
		}
	}

	/** the render target: the current frame's maximum into an ARGB accumulator, resizable between renders */
	static final class MaxProjectionTarget implements RenderTarget< BufferedImageRenderResult >
	{
		private int width = 0, height = 0;
		ARGBScreenImage accumulated = new ARGBScreenImage( 1, 1 );
		private final BufferedImageRenderResult result = new BufferedImageRenderResult();

		void setSize( final int w, final int h )
		{
			if ( w != width || h != height )
			{
				width = w;
				height = h;
				accumulated = new ARGBScreenImage( w, h );
			}
		}

		void clear()
		{
			Arrays.fill( accumulated.getData(), 0 );
		}

		@Override
		public BufferedImageRenderResult getReusableRenderResult()
		{
			return result;
		}

		@Override
		public BufferedImageRenderResult createRenderResult()
		{
			return new BufferedImageRenderResult();
		}

		@Override
		public void setRenderResult( final BufferedImageRenderResult renderResult )
		{
			final int[] in = ( ( DataBufferInt ) renderResult.getBufferedImage().getRaster().getDataBuffer() ).getData();
			final int[] acc = accumulated.getData();
			final int n = width * height;
			for ( int i = 0; i < n; ++i )
			{
				final int a = acc[ i ], b = in[ i ];
				acc[ i ] = ARGBType.rgba(
						Math.max( ARGBType.red( a ), ARGBType.red( b ) ),
						Math.max( ARGBType.green( a ), ARGBType.green( b ) ),
						Math.max( ARGBType.blue( a ), ARGBType.blue( b ) ),
						Math.max( ARGBType.alpha( a ), ARGBType.alpha( b ) ) );
			}
		}

		@Override
		public int getWidth()
		{
			return width;
		}

		@Override
		public int getHeight()
		{
			return height;
		}
	}

	/** a source restricted to one of its resolution levels */
	static final class SingleLevelSource< T > implements Source< T >
	{
		private final Source< T > source;
		private final int level;

		SingleLevelSource( final Source< T > source, final int requestedLevel )
		{
			this.source = source;
			this.level = Math.min( Math.max( 0, requestedLevel ), source.getNumMipmapLevels() - 1 );
			if ( this.level != requestedLevel )
				System.out.println( "WARNING: " + source.getName() + " has " + source.getNumMipmapLevels() + " resolution levels, using level " + this.level + " instead of " + requestedLevel );
		}

		@Override
		public boolean isPresent( final int t )
		{
			return source.isPresent( t );
		}

		@Override
		public RandomAccessibleInterval< T > getSource( final int t, final int l )
		{
			return source.getSource( t, level );
		}

		@Override
		public RealRandomAccessible< T > getInterpolatedSource( final int t, final int l, final Interpolation method )
		{
			return source.getInterpolatedSource( t, level, method );
		}

		@Override
		public void getSourceTransform( final int t, final int l, final AffineTransform3D transform )
		{
			source.getSourceTransform( t, level, transform );
		}

		@Override
		public T getType()
		{
			return source.getType();
		}

		@Override
		public String getName()
		{
			return source.getName();
		}

		@Override
		public VoxelDimensions getVoxelDimensions()
		{
			return source.getVoxelDimensions();
		}

		@Override
		public int getNumMipmapLevels()
		{
			return 1;
		}

		@Override
		public boolean doBoundingBoxCulling()
		{
			return source.doBoundingBoxCulling();
		}
	}
}
