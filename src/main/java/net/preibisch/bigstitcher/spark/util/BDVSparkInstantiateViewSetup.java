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
package net.preibisch.bigstitcher.spark.util;

import java.util.Iterator;
import java.util.List;

import mpicbg.spim.data.sequence.Angle;
import mpicbg.spim.data.sequence.Channel;
import mpicbg.spim.data.sequence.FinalVoxelDimensions;
import mpicbg.spim.data.sequence.Illumination;
import mpicbg.spim.data.sequence.Tile;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.ViewSetup;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.Dimensions;
import net.preibisch.mvrecon.process.export.ExportTools.InstantiateViewSetup;

public class BDVSparkInstantiateViewSetup implements InstantiateViewSetup
{
	final String angleIds;
	final String illuminationIds;
	final String channelIds;
	final String tileIds;

	public BDVSparkInstantiateViewSetup(
			final String angleIds,
			final String illuminationIds, 
			final String channelIds,
			final String tileIds)
	{
		this.angleIds = angleIds;
		this.illuminationIds = illuminationIds;
		this.channelIds = channelIds;
		this.tileIds = tileIds;
	}

	@Override
	public ViewSetup instantiate(
			final ViewId viewId,
			final boolean tpExists,
			final Dimensions d,
			final List<ViewSetup> existingSetups)
	{
		Channel c0;
		Angle a0;
		Illumination i0;
		Tile t0;

		if ( existingSetups == null || existingSetups.size() == 0 )
		{
			c0 = new Channel( 0 );
			a0 = new Angle( 0 );
			i0 = new Illumination( 0 );
			t0 = new Tile( 0 );
		}
		else
		{
			final Iterator<ViewSetup> i = existingSetups.iterator(); //existingSetups can be empty!
			ViewSetup tmp = i.next();

			c0 = tmp.getChannel();
			a0 = tmp.getAngle();
			i0 = tmp.getIllumination();
			t0 = tmp.getTile();

			while ( i.hasNext() )
			{
				tmp = i.next();
				if ( tmp.getChannel().getId() > c0.getId() )
					c0 = tmp.getChannel();
				if ( tmp.getAngle().getId() > a0.getId() )
					a0 = tmp.getAngle();
				if ( tmp.getIllumination().getId() > i0.getId() )
					i0 = tmp.getIllumination();
				if ( tmp.getTile().getId() > t0.getId() )
					t0 = tmp.getTile();
			}
		}

		if ( angleIds != null )
			a0 = new Angle( a0.getId() + 1 );
		if ( illuminationIds != null )
			i0 = new Illumination( i0.getId() + 1 );
		if ( tileIds != null )
			t0 = new Tile( t0.getId() + 1 );
		if ( tileIds != null || ( angleIds == null && illuminationIds == null && tileIds == null && tpExists ) ) // nothing was defined, then increase channel
			c0 = new Channel( c0.getId() + 1 );

		final VoxelDimensions vd0 = new FinalVoxelDimensions( "px", 1, 1, 1 );

		return new ViewSetup( viewId.getViewSetupId(), "setup " + viewId.getViewSetupId(), d, vd0, t0, c0, a0, i0 );
	}
}
