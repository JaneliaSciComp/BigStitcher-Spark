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
package net.preibisch.bigstitcher.spark.flatfield;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;

import mpicbg.spim.data.sequence.SetupImgLoader;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.ViewSetup;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Cast;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.process.n5api.N5ApiTools;
import util.URITools;

/**
 * Apply a precomputed BaSiC flatfield / darkfield correction to a single s0
 * grid block of a view. The 2D field is broadcast across z (and t).
 * <p>
 * Per pixel:
 * {@code corrected = clamp( round( (raw - darkfield[x,y]) / flatfield[x,y] ), 0, dtypeMax )}.
 * Where {@code flatfield[x,y] <= eps} the output is 0.
 * <p>
 * The flatfield/darkfield of a {@code (channel, illumination)} group are read
 * from the estimation container once per JVM and cached in {@link #FIELD_CACHE}.
 */
public class FlatfieldApply
{
	/** flatfield values below this are treated as zero (divide-by-zero guard). */
	public static final float FLAT_EPS = 1e-6f;

	/** per-JVM cache: fieldsURI + '#' + groupKey + '#' + w + 'x' + h -&gt; fields. */
	private static final Map< String, Field2D > FIELD_CACHE = new ConcurrentHashMap<>();

	/** Materialized 2D flatfield / darkfield (row-major float[y*w+x], fast axis = x). */
	public static final class Field2D
	{
		public final float[] flat;
		public final float[] dark;
		public final int w;
		public final int h;

		public Field2D( final float[] flat, final float[] dark, final int w, final int h )
		{
			this.flat = flat;
			this.dark = dark;
			this.w = w;
			this.h = h;
		}
	}

	/**
	 * Correct a single s0 grid block of a view and write it to the output container.
	 * Mirrors {@link N5ApiTools#resaveS0Block} but applies the correction.
	 *
	 * @param data          per-task SpimData2 (source images)
	 * @param n5Out         output writer
	 * @param storageType   output storage format
	 * @param outputDataType output data type (may differ from source, e.g. FLOAT32)
	 * @param fieldsURI     estimation container URI
	 * @param fieldsFormat  estimation container storage format
	 * @param gridBlockToDataset s0 dataset naming function
	 * @param gridBlock     the extended grid block (gridBlock[3] encodes the ViewId)
	 */
	public static < T extends RealType< T > & NativeType< T >, O extends RealType< O > & NativeType< O > > void correctS0Block(
			final SpimData2 data,
			final N5Writer n5Out,
			final StorageFormat storageType,
			final DataType outputDataType,
			final URI fieldsURI,
			final StorageFormat fieldsFormat,
			final java.util.function.Function< long[][], String > gridBlockToDataset,
			final long[][] gridBlock )
	{
		final ViewId viewId = N5ApiTools.gridBlockToViewId( gridBlock );
		final String dataset = gridBlockToDataset.apply( gridBlock );

		final ViewDescription vd = data.getSequenceDescription().getViewDescription( viewId );
		final ViewSetup vs = vd.getViewSetup();
		final int ch = ( vs.getChannel() != null ) ? vs.getChannel().getId() : 0;
		final int il = ( vs.getIllumination() != null ) ? vs.getIllumination().getId() : 0;
		final String groupKey = "channel" + ch + "/illumination" + il;

		final SetupImgLoader< ? > imgLoader = data.getSequenceDescription().getImgLoader().getSetupImgLoader( viewId.getViewSetupId() );
		final RandomAccessibleInterval< T > img = Cast.unchecked( imgLoader.getImage( viewId.getTimePointId() ) );

		// view X/Y size (the raw 3D volume is X,Y,Z)
		final int viewW = ( int ) img.dimension( 0 );
		final int viewH = ( int ) img.dimension( 1 );

		final Field2D field = loadFieldCached( fieldsURI, fieldsFormat, groupKey, viewW, viewH );

		final long[] blockOffset, blockSize, gridOffset;

		// 5D OME-ZARR CONTAINER (output). Source image is always 3D here.
		final boolean isZarr = ( storageType == StorageFormat.ZARR || storageType == StorageFormat.ZARR2 );

		if ( isZarr )
		{
			blockOffset = new long[] { gridBlock[ 0 ][ 0 ], gridBlock[ 0 ][ 1 ], gridBlock[ 0 ][ 2 ], 0, 0 };
			blockSize = new long[] { gridBlock[ 1 ][ 0 ], gridBlock[ 1 ][ 1 ], gridBlock[ 1 ][ 2 ], 1, 1 };
			gridOffset = new long[] { gridBlock[ 2 ][ 0 ], gridBlock[ 2 ][ 1 ], gridBlock[ 2 ][ 2 ], 0, 0 };
		}
		else
		{
			blockOffset = gridBlock[ 0 ];
			blockSize = gridBlock[ 1 ];
			gridOffset = gridBlock[ 2 ];
		}

		// crop the raw 3D block: X,Y,Z from gridBlock (2d field is broadcast across z)
		final long[] blockOffset3d = new long[] { gridBlock[ 0 ][ 0 ], gridBlock[ 0 ][ 1 ], gridBlock[ 0 ][ 2 ] };
		final long[] blockSize3d = new long[] { gridBlock[ 1 ][ 0 ], gridBlock[ 1 ][ 1 ], gridBlock[ 1 ][ 2 ] };

		final RandomAccessibleInterval< T > src = Views.offsetInterval( img, blockOffset3d, blockSize3d );

		final boolean isFloat = ( outputDataType == DataType.FLOAT32 || outputDataType == DataType.FLOAT64 );

		// build the corrected block (3D) as an ArrayImg of the output type
		final ArrayImg< O, ? > corrected3d = applyCorrection(
				src, field,
				( int ) blockOffset3d[ 0 ], ( int ) blockOffset3d[ 1 ],
				outputDataType );

		// write
		final RandomAccessibleInterval< O > toWrite;
		if ( isZarr )
			toWrite = Views.addDimension( Views.addDimension( corrected3d, 0, 0 ), 0, 0 );
		else
			toWrite = corrected3d;

		N5Utils.saveBlock( toWrite, n5Out, dataset, gridOffset );

		System.out.println( "ViewId [" + viewId.getTimePointId() + "," + viewId.getViewSetupId() + "] group " + groupKey
				+ ", corrected block: offset=" + Util.printCoordinates( blockOffset )
				+ ", dimension=" + Util.printCoordinates( blockSize ) );
	}

	/**
	 * Pure per-pixel correction of a 3D source block (X,Y,Z). The 2D field is
	 * broadcast across z. {@code blockOffsetX/Y} give the world position of the
	 * block's min corner within the field. Returns a 3D {@link ArrayImg} of
	 * {@code outputDataType} with values
	 * {@code clamp(round((raw-dark)/flat), 0, dtypeMax)} (round/clamp skipped for
	 * floating output); output is 0 where {@code flat <= FLAT_EPS}.
	 */
	@SuppressWarnings("unchecked")
	public static < T extends RealType< T >, O extends RealType< O > & NativeType< O > > ArrayImg< O, ? > applyCorrection(
			final RandomAccessibleInterval< T > src,
			final Field2D field,
			final int blockOffsetX,
			final int blockOffsetY,
			final DataType outputDataType )
	{
		final int bx = ( int ) src.dimension( 0 );
		final int by = ( int ) src.dimension( 1 );
		final int bz = ( int ) src.dimension( 2 );

		final O outType = Cast.unchecked( N5Utils.type( outputDataType ) );
		final boolean isFloat = ( outputDataType == DataType.FLOAT32 || outputDataType == DataType.FLOAT64 );
		final double dtypeMax = outType.getMaxValue();
		final double dtypeMin = outType.getMinValue();

		final ArrayImg< O, ? > out = ( ArrayImg< O, ? > ) createArrayImg( outputDataType, bx, by, bz );

		final Cursor< O > outCur = out.localizingCursor();
		final net.imglib2.RandomAccess< T > srcRa = src.randomAccess();
		final int[] pos = new int[ 3 ];

		while ( outCur.hasNext() )
		{
			final O o = outCur.next();
			outCur.localize( pos );

			srcRa.setPosition( pos[ 0 ] + src.min( 0 ), 0 );
			srcRa.setPosition( pos[ 1 ] + src.min( 1 ), 1 );
			srcRa.setPosition( pos[ 2 ] + src.min( 2 ), 2 );
			final double raw = srcRa.get().getRealDouble();

			final int fx = blockOffsetX + pos[ 0 ];
			final int fy = blockOffsetY + pos[ 1 ];
			final int fIdx = fy * field.w + fx;

			final double flat = field.flat[ fIdx ];
			final double dark = ( field.dark != null ) ? field.dark[ fIdx ] : 0.0;

			double v;
			if ( flat <= FLAT_EPS )
			{
				v = 0.0;
			}
			else
			{
				v = ( raw - dark ) / flat;
				if ( !isFloat )
					v = Math.rint( v );
				if ( v < 0.0 )
					v = 0.0;
				if ( v > dtypeMax )
					v = dtypeMax;
				if ( v < dtypeMin )
					v = dtypeMin;
			}
			o.setReal( v );
		}

		return out;
	}

	private static RandomAccessibleInterval< ? > createArrayImg( final DataType dataType, final int bx, final int by, final int bz )
	{
		switch ( dataType )
		{
			case UINT8:
				return ArrayImgs.unsignedBytes( bx, by, bz );
			case INT8:
				return ArrayImgs.bytes( bx, by, bz );
			case UINT16:
				return ArrayImgs.unsignedShorts( bx, by, bz );
			case INT16:
				return ArrayImgs.shorts( bx, by, bz );
			case UINT32:
				return ArrayImgs.unsignedInts( bx, by, bz );
			case INT32:
				return ArrayImgs.ints( bx, by, bz );
			case UINT64:
				return ArrayImgs.unsignedLongs( bx, by, bz );
			case INT64:
				return ArrayImgs.longs( bx, by, bz );
			case FLOAT32:
				return ArrayImgs.floats( bx, by, bz );
			case FLOAT64:
				return ArrayImgs.doubles( bx, by, bz );
			default:
				throw new RuntimeException( "Unsupported output data type: " + dataType );
		}
	}

	/**
	 * Load (and cache per JVM) the 2D flatfield/darkfield for a group, materialized
	 * to a row-major float[]. If the field X/Y differs from the view X/Y, the field
	 * is bilinearly resized to the view size (with a warning).
	 */
	public static Field2D loadFieldCached(
			final URI fieldsURI,
			final StorageFormat fieldsFormat,
			final String groupKey,
			final int viewW,
			final int viewH )
	{
		final String cacheKey = fieldsURI + "#" + groupKey + "#" + viewW + "x" + viewH;
		return FIELD_CACHE.computeIfAbsent( cacheKey, k ->
		{
			final N5Reader n5 = URITools.instantiateN5Reader( fieldsFormat, fieldsURI );
			try
			{
				final String flatDataset = groupKey + "/flatfield";
				final String darkDataset = groupKey + "/darkfield";

				if ( !n5.datasetExists( flatDataset ) )
					throw new RuntimeException( "No flatfield for group '" + groupKey + "' in '" + fieldsURI + "'" );

				final RandomAccessibleInterval< FloatType > flatRai = N5Utils.open( n5, flatDataset );
				final int fw = ( int ) flatRai.dimension( 0 );
				final int fh = ( int ) flatRai.dimension( 1 );
				float[] flat = toPlane( flatRai, fw, fh );

				float[] dark;
				if ( n5.datasetExists( darkDataset ) )
				{
					final RandomAccessibleInterval< FloatType > darkRai = N5Utils.open( n5, darkDataset );
					dark = toPlane( darkRai, fw, fh );
				}
				else
				{
					dark = null;
				}

				if ( fw != viewW || fh != viewH )
				{
					System.out.println( "WARNING: field size " + fw + "x" + fh + " differs from view size "
							+ viewW + "x" + viewH + " for group '" + groupKey + "'; bilinearly resizing field." );
					// BasicFlatfield.resize takes (in, H, W, newH, newW)
					flat = BasicFlatfield.resize( flat, fh, fw, viewH, viewW );
					if ( dark != null )
						dark = BasicFlatfield.resize( dark, fh, fw, viewH, viewW );
				}

				return new Field2D( flat, dark, viewW, viewH );
			}
			finally
			{
				n5.close();
			}
		} );
	}

	/** Read a 2D RAI into row-major float[H*W] (fast axis = x = W). */
	private static float[] toPlane( final RandomAccessibleInterval< FloatType > frame, final int w, final int h )
	{
		final float[] plane = new float[ w * h ];
		final net.imglib2.RandomAccess< FloatType > ra = frame.randomAccess();
		final long minX = frame.min( 0 );
		final long minY = frame.min( 1 );
		for ( int y = 0; y < h; ++y )
		{
			ra.setPosition( minY + y, 1 );
			final int rowOff = y * w;
			for ( int x = 0; x < w; ++x )
			{
				ra.setPosition( minX + x, 0 );
				plane[ rowOff + x ] = ra.get().get();
			}
		}
		return plane;
	}

	/** Clear the per-JVM field cache (used by tests). */
	public static void clearCache()
	{
		FIELD_CACHE.clear();
	}
}
