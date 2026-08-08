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

import java.net.URI;

import mpicbg.ij.TransformMeshMapping;
import mpicbg.models.CoordinateTransformMesh;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.ViewSetup;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.RealType;
import net.preibisch.bigstitcher.spark.correction.ViewCorrection;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;

/**
 * A {@link ViewCorrection} that applies a precomputed per-channel 2D lens /
 * aberration warp to a view (see {@link LensApply} and {@link LensModels}).
 * <p>
 * The model is matched to the view by channel-name substring. If no model
 * matches the view's channel, the view is returned unchanged (and a message is
 * logged) so a downstream correction (e.g. flatfield) still runs. A matched model
 * whose fitted size differs from the view size is a hard error (raised by
 * {@link LensApply#loadMappingCached}).
 */
public class LensCorrection implements ViewCorrection
{
	private static final long serialVersionUID = 1L;

	private final URI lensURI;
	private final int meshResolution;
	private final boolean includeAffine;

	public LensCorrection( final URI lensURI, final int meshResolution, final boolean includeAffine )
	{
		this.lensURI = lensURI;
		this.meshResolution = meshResolution;
		this.includeAffine = includeAffine;
	}

	@Override
	public < T extends RealType< T > > RandomAccessibleInterval< ? extends RealType< ? > > apply(
			final SpimData2 data,
			final ViewId viewId,
			final RandomAccessibleInterval< T > source )
	{
		final ViewSetup vs = data.getSequenceDescription().getViewDescription( viewId ).getViewSetup();
		final String channelName = ( vs.getChannel() != null ) ? vs.getChannel().getName() : null;

		final int viewW = ( int ) source.dimension( 0 );
		final int viewH = ( int ) source.dimension( 1 );

		final TransformMeshMapping< CoordinateTransformMesh > mapping =
				LensApply.loadMappingCached( lensURI, channelName, viewW, viewH, meshResolution, includeAffine );

		if ( mapping == null )
		{
			System.out.println( "No lens model for channel '" + channelName + "' (view ["
					+ viewId.getTimePointId() + "," + viewId.getViewSetupId()
					+ "]); skipping lens correction for this view." );
			return source;
		}

		return LensApply.correctedFullView( source, mapping, viewW, viewH, ( int ) source.dimension( 2 ) );
	}
}
