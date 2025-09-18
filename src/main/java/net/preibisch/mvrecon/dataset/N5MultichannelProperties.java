/*-
 * #%L
 * Software for the reconstruction of multi-view microscopic acquisitions
 * like Selective Plane Illumination Microscopy (SPIM) Data.
 * %%
 * Copyright (C) 2012 - 2025 Multiview Reconstruction developers.
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
package net.preibisch.mvrecon.dataset;

import java.util.Arrays;
import java.util.Map;

import bdv.img.n5.N5Properties;
import mpicbg.spim.data.generic.sequence.AbstractSequenceDescription;
import mpicbg.spim.data.sequence.TimePoint;
import mpicbg.spim.data.sequence.ViewId;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Reader;

public class N5MultichannelProperties implements N5Properties
{
	private final AbstractSequenceDescription< ?, ?, ? > sequenceDescription;

	private final Map< ViewId, String > viewIdToPath;

	public N5MultichannelProperties(
			final AbstractSequenceDescription< ?, ?, ? > sequenceDescription,
			final Map< ViewId, String > viewIdToPath )
	{
		this.sequenceDescription = sequenceDescription;
		this.viewIdToPath = viewIdToPath;
	}

	private String getPath( final int setupId, final int timepointId )
	{
		return viewIdToPath.get( new ViewId( timepointId, setupId ) );
	}

	@Override
	public String getDatasetPath( final int setupId, final int timepointId, final int level )
	{
		return String.format( getPath( setupId, timepointId )+ "/s%d", level );
	}

	@Override
	public DataType getDataType( final N5Reader n5, final int setupId )
	{
		return getDataType( this, n5, setupId );
	}

	@Override
	public double[][] getMipmapResolutions( final N5Reader n5, final int setupId )
	{
		return getMipMapResolutions( this, n5, setupId );
	}

	@Override
	public long[] getDimensions( final N5Reader n5, final int setupId, final int timepointId, final int level )
	{
		final String path = getDatasetPath( setupId, timepointId, level );
		final long[] dimensions = n5.getDatasetAttributes( path ).getDimensions();
		return Arrays.copyOf( dimensions, 3 );
	}

	//
	// static methods
	//

	private static int getFirstAvailableTimepointId( final AbstractSequenceDescription< ?, ?, ? > seq, final int setupId )
	{
		for ( final TimePoint tp : seq.getTimePoints().getTimePointsOrdered() )
		{
			if ( seq.getMissingViews() == null || seq.getMissingViews().getMissingViews() == null || !seq.getMissingViews().getMissingViews().contains( new ViewId( tp.getId(), setupId ) ) )
				return tp.getId();
		}

		throw new RuntimeException( "All timepoints for setupId " + setupId + " are declared missing. Stopping." );
	}

	private static DataType getDataType(final N5MultichannelProperties n5properties, final N5Reader n5, final int setupId )
	{
		final int timePointId = getFirstAvailableTimepointId( n5properties.sequenceDescription, setupId );
		return n5.getDatasetAttributes( n5properties.getDatasetPath( setupId, timePointId, 0 ) ).getDataType();
	}

	private static double[][] getMipMapResolutions(final N5MultichannelProperties n5properties, final N5Reader n5, final int setupId )
	{
		final int timePointId = getFirstAvailableTimepointId( n5properties.sequenceDescription, setupId );

		// read scales and pixelResolution attributes from the base container and build the mipmap resolutions from that

		return new double[][] { { 1, 1, 1 } }; // default
	}
}
