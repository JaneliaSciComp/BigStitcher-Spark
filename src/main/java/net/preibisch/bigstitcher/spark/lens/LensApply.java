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
package net.preibisch.bigstitcher.spark.lens;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.janelia.saalfeldlab.n5.KeyValueAccess;

import ij.process.FloatProcessor;
import ij.process.ImageProcessor;
import mpicbg.ij.TransformMeshMapping;
import mpicbg.models.CoordinateTransform;
import mpicbg.models.CoordinateTransformMesh;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.basictypeaccess.AccessFlags;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.lens.LensModels.Model;
import util.Lazy;
import util.URITools;

/**
 * Apply a precomputed 2D lens / aberration warp to a view, plane by plane. The
 * warp is the mpicbg TrakEM2 {@code NonLinearCoordinateTransform} (optionally
 * followed by a per-channel {@code AffineModel2D}) read from a JSON lens-model
 * file; see {@link LensModels}.
 * <p>
 * The warp is 2D and is broadcast across z (the same per-channel model is
 * applied to every plane). It is rendered through an mpicbg
 * {@link CoordinateTransformMesh} + {@link TransformMeshMapping} with bilinear
 * interpolation, producing a full-plane {@link FloatType} view. Because a
 * nonlinear warp needs the whole plane (not a sub-block), the corrected view is
 * materialized lazily one full plane at a time and cached across the many
 * XY-blocks that share a plane.
 * <p>
 * The parsed models and per-(channel,size) mappings are cached per JVM, mirroring
 * {@link net.preibisch.bigstitcher.spark.flatfield.FlatfieldApply}'s field cache.
 */
public class LensApply
{
	/** per-JVM cache: lens JSON URI -&gt; parsed models. */
	private static final Map< String, List< Model > > MODELS_CACHE = new ConcurrentHashMap<>();

	/** per-JVM cache: uri#channel#WxH#mesh#affine -&gt; mapping (holder allows a null = "no model"). */
	private static final Map< String, MappingHolder > MAPPING_CACHE = new ConcurrentHashMap<>();

	/** Holder so the concurrent cache can memoize a "no matching model" result (null mapping). */
	private static final class MappingHolder
	{
		final TransformMeshMapping< CoordinateTransformMesh > mapping;

		MappingHolder( final TransformMeshMapping< CoordinateTransformMesh > mapping )
		{
			this.mapping = mapping;
		}
	}

	/**
	 * Build the mesh + mapping for one transform.
	 * <p>
	 * Uses {@link CoordinateTransformMesh} (absolute target coordinates) rather
	 * than {@code mpicbg.trakem2.transform.TransformMesh}, which re-origins its
	 * bounding box and would reintroduce the inter-channel offset lens correction
	 * exists to remove. Safe to share across threads as long as the mesh is never
	 * mutated and callers pass {@code numThreads = 1} to {@code mapInterpolated}.
	 */
	public static TransformMeshMapping< CoordinateTransformMesh > createMapping(
			final CoordinateTransform t,
			final int meshResolution,
			final int width,
			final int height )
	{
		return new TransformMeshMapping< CoordinateTransformMesh >(
				new CoordinateTransformMesh( t, meshResolution, width, height ) );
	}

	/**
	 * Resolve (and cache per JVM) the warp mapping for a view's channel.
	 *
	 * @param lensURI        lens-model JSON file (local or cloud)
	 * @param channelName    the view's channel name (matched as a substring of a model name)
	 * @param w              view X size (must equal the model's fitted width)
	 * @param h              view Y size (must equal the model's fitted height)
	 * @param meshResolution mesh resolution passed to {@link CoordinateTransformMesh}
	 * @param includeAffine  also apply the per-channel {@code AffineModel2D}
	 * @return the mapping, or {@code null} if no model matches this channel (caller skips lens)
	 * @throws IllegalArgumentException if the match is ambiguous or the fitted size differs
	 */
	public static TransformMeshMapping< CoordinateTransformMesh > loadMappingCached(
			final URI lensURI,
			final String channelName,
			final int w,
			final int h,
			final int meshResolution,
			final boolean includeAffine )
	{
		final String mkey = lensURI + "#" + channelName + "#" + w + "x" + h + "#m" + meshResolution + "#a" + includeAffine;
		final MappingHolder holder = MAPPING_CACHE.computeIfAbsent( mkey, k ->
		{
			final List< Model > models = loadModelsCached( lensURI );
			final Model m = LensModels.findForChannel( models, channelName );
			if ( m == null )
				return new MappingHolder( null );
			if ( m.fittedWidth != w || m.fittedHeight != h )
				throw new IllegalArgumentException(
						"Lens model '" + m.name + "' was fitted at " + m.fittedWidth + "x" + m.fittedHeight
								+ " but the view (channel '" + channelName + "') is " + w + "x" + h
								+ "; the non-linear model is only valid at its fitted size." );
			return new MappingHolder( createMapping( m.toTransform( includeAffine ), meshResolution, w, h ) );
		} );
		return holder.mapping;
	}

	/** Load (and cache per JVM) the parsed models for a lens JSON URI. */
	public static List< Model > loadModelsCached( final URI lensURI )
	{
		return MODELS_CACHE.computeIfAbsent( lensURI.toString(), u ->
		{
			try
			{
				return LensModels.load( readText( lensURI ), lensURI.toString() );
			}
			catch ( final IOException e )
			{
				throw new RuntimeException( "Failed to read lens model file '" + lensURI + "': " + e.getMessage(), e );
			}
		} );
	}

	/** Read a (small) text file from a local or cloud URI via the shared n5 key-value access. */
	private static String readText( final URI uri ) throws IOException
	{
		final KeyValueAccess kva = URITools.getKeyValueAccess( uri );
		final StringBuilder sb = new StringBuilder();
		try ( final BufferedReader r = URITools.openFileReadCloudReader( kva, uri ) )
		{
			String line;
			while ( ( line = r.readLine() ) != null )
				sb.append( line ).append( '\n' );
		}
		return sb.toString();
	}

	/**
	 * A lazily-materialized, lens-corrected view of a 3D source image (X,Y,Z). Each
	 * z-plane is warped independently through {@code mapping} with bilinear
	 * interpolation; the result is a zero-min {@code [w,h,depth]} {@link FloatType}
	 * image. Planes are computed on demand and cached (one full plane per cell) so
	 * the many XY-blocks that share a plane reuse a single warp.
	 *
	 * @param img     the full source view (X,Y,Z); may have a non-zero min
	 * @param mapping the warp mapping for this view's channel/size
	 * @param w       X size (= img X size = model fitted width)
	 * @param h       Y size
	 * @param depth   Z size
	 */
	public static < T extends RealType< T > > RandomAccessibleInterval< FloatType > correctedFullView(
			final RandomAccessibleInterval< T > img,
			final TransformMeshMapping< CoordinateTransformMesh > mapping,
			final int w,
			final int h,
			final int depth )
	{
		final long minX = img.min( 0 ), minY = img.min( 1 ), minZ = img.min( 2 );

		return Lazy.process(
				new FinalInterval( w, h, depth ),
				new int[] { w, h, 1 },
				new FloatType(),
				AccessFlags.setOf(),
				cell ->
				{
					final int z = ( int ) cell.min( 2 );

					// read raw plane z into a row-major float[] (fast axis = x)
					final float[] srcPixels = new float[ w * h ];
					final RandomAccess< T > ra = img.randomAccess();
					ra.setPosition( minZ + z, 2 );
					for ( int y = 0; y < h; ++y )
					{
						ra.setPosition( minY + y, 1 );
						final int rowOff = y * w;
						for ( int x = 0; x < w; ++x )
						{
							ra.setPosition( minX + x, 0 );
							srcPixels[ rowOff + x ] = ( float ) ra.get().getRealDouble();
						}
					}

					// warp the plane (crop = 0, so the destination keeps the full canvas)
					final FloatProcessor src = new FloatProcessor( w, h, srcPixels );
					src.setInterpolationMethod( ImageProcessor.BILINEAR );
					final FloatProcessor dst = new FloatProcessor( w, h );
					// numThreads = 1: parallelism is over Spark blocks, not within a plane
					mapping.mapInterpolated( src, dst, 1 );
					final float[] dstPixels = ( float[] ) dst.getPixels();

					// copy the warped plane into the cell (x fastest, matching row-major dstPixels)
					final Cursor< FloatType > c = Views.flatIterable( cell ).cursor();
					int i = 0;
					while ( c.hasNext() )
						c.next().set( dstPixels[ i++ ] );
				} );
	}
}
