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

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;

import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.ViewSetup;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.basictypeaccess.AccessFlags;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Cast;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.correction.ViewCorrection;
import net.preibisch.bigstitcher.spark.flatfield.FlatfieldApply.Field2D;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import util.Lazy;

/**
 * A {@link ViewCorrection} that applies a precomputed BaSiC flatfield / darkfield
 * (and optional temporal baseline-drift) correction to a view. The 2D field is
 * broadcast across z; the per-pixel math is
 * {@link FlatfieldApply#applyCorrection}, applied here plane-by-plane to build a
 * lazy full-view image of the requested output data type (so this correction is
 * the terminal, quantizing stage of a {@code ViewCorrections} pipeline).
 * <p>
 * The (channel,illumination) group key and per-view baseline delta are resolved
 * from {@code data}/{@code viewId} at apply time, so one instance handles every
 * view. Fields are read once per JVM and cached in {@link FlatfieldApply}.
 */
public class FlatfieldCorrection implements ViewCorrection
{
	private static final long serialVersionUID = 1L;

	private final URI fieldsURI;
	private final StorageFormat fieldsFormat;
	/** per-view-setup output data type (SAME source dtype, or FLOAT32). */
	private final Map< Integer, DataType > outDataTypes;
	/** per-view baseline delta keyed by {@link #viewKey}, or {@code null} to disable. */
	private final Map< String, double[] > baselineDeltas;

	public FlatfieldCorrection(
			final URI fieldsURI,
			final StorageFormat fieldsFormat,
			final Map< Integer, DataType > outDataTypes,
			final Map< String, double[] > baselineDeltas )
	{
		this.fieldsURI = fieldsURI;
		this.fieldsFormat = fieldsFormat;
		this.outDataTypes = outDataTypes;
		this.baselineDeltas = baselineDeltas;
	}

	/** Serializable ViewId key: "&lt;timepoint&gt;_&lt;viewsetup&gt;" (matches the driver). */
	public static String viewKey( final ViewId v )
	{
		return v.getTimePointId() + "_" + v.getViewSetupId();
	}

	@Override
	public < T extends RealType< T > > RandomAccessibleInterval< ? extends RealType< ? > > apply(
			final SpimData2 data,
			final ViewId viewId,
			final RandomAccessibleInterval< T > source )
	{
		final ViewSetup vs = data.getSequenceDescription().getViewDescription( viewId ).getViewSetup();
		final int ch = ( vs.getChannel() != null ) ? vs.getChannel().getId() : 0;
		final int il = ( vs.getIllumination() != null ) ? vs.getIllumination().getId() : 0;
		final String groupKey = "channel" + ch + "/illumination" + il;

		final int w = ( int ) source.dimension( 0 );
		final int h = ( int ) source.dimension( 1 );
		final int depth = ( int ) source.dimension( 2 );

		final Field2D field = FlatfieldApply.loadFieldCached( fieldsURI, fieldsFormat, groupKey, w, h );
		final double[] delta = ( baselineDeltas == null ) ? null : baselineDeltas.get( viewKey( viewId ) );
		final DataType outputDataType = outDataTypes.get( viewId.getViewSetupId() );

		return correctedFullView( source, field, delta, outputDataType, w, h, depth );
	}

	/**
	 * Lazily build the flatfield-corrected full view (X,Y,Z) of the output data type,
	 * one full z-plane per cell. Each plane is corrected with
	 * {@link FlatfieldApply#applyCorrection}, so values are byte-identical to the
	 * per-block correction (the 2D field is broadcast across z; {@code delta} is
	 * subtracted per absolute z).
	 */
	private static < T extends RealType< T >, O extends RealType< O > & NativeType< O > > RandomAccessibleInterval< O > correctedFullView(
			final RandomAccessibleInterval< T > source,
			final Field2D field,
			final double[] delta,
			final DataType outputDataType,
			final int w,
			final int h,
			final int depth )
	{
		final long minX = source.min( 0 ), minY = source.min( 1 ), minZ = source.min( 2 );
		final O type = Cast.unchecked( N5Utils.type( outputDataType ) );

		return Lazy.process(
				new FinalInterval( w, h, depth ),
				new int[] { w, h, 1 },
				type,
				AccessFlags.setOf(),
				cell ->
				{
					final int z = ( int ) cell.min( 2 );

					// full z-plane of the source (X,Y at absolute z), broadcast the 2D field
					final RandomAccessibleInterval< T > plane = Views.interval(
							source,
							new long[] { minX, minY, minZ + z },
							new long[] { minX + w - 1, minY + h - 1, minZ + z } );

					final ArrayImg< O, ? > corr = FlatfieldApply.applyCorrection(
							plane, field, 0, 0, outputDataType, delta, z );

					// copy the corrected plane into the cell (both x-fastest, dims [w,h,1])
					final Cursor< O > in = corr.cursor();
					final Cursor< O > out = Views.flatIterable( cell ).cursor();
					while ( out.hasNext() )
						out.next().set( in.next() );
				} );
	}
}
