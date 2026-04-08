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

import java.net.URI;
import java.util.Map;

import bdv.img.n5.N5ImageLoader;
import bdv.img.n5.N5Properties;
import mpicbg.spim.data.generic.sequence.AbstractSequenceDescription;
import mpicbg.spim.data.sequence.ViewId;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import util.URITools;

public class N5MultichannelLoader extends N5ImageLoader
{
	private final AbstractSequenceDescription< ?, ?, ? > sequenceDescription;

	private final Map< ViewId, String > viewIdToPath;

	public N5MultichannelLoader(
			final URI n5URI,
			final StorageFormat storageFormat,
			final AbstractSequenceDescription< ?, ?, ? > sequenceDescription,
			final Map< ViewId, String > viewIdToPath )
	{
		super( URITools.instantiateN5Reader( storageFormat, n5URI ), n5URI, sequenceDescription );
		this.sequenceDescription = sequenceDescription;

		this.viewIdToPath = viewIdToPath;
	}

	@Override
	protected N5Properties createN5PropertiesInstance()
	{
		return new N5MultichannelProperties( sequenceDescription, viewIdToPath );
	}

	public Map<ViewId, String> getViewIdToPath() {
		return viewIdToPath;
	}
}
