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
package net.preibisch.bigstitcher.spark;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mpicbg.spim.data.generic.sequence.BasicViewDescription;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewRegistrations;
import mpicbg.spim.data.sequence.Angle;
import mpicbg.spim.data.sequence.Channel;
import mpicbg.spim.data.sequence.FinalVoxelDimensions;
import mpicbg.spim.data.sequence.Illumination;
import mpicbg.spim.data.sequence.MissingViews;
import mpicbg.spim.data.sequence.SequenceDescription;
import mpicbg.spim.data.sequence.Tile;
import mpicbg.spim.data.sequence.TimePoint;
import mpicbg.spim.data.sequence.TimePoints;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.ViewSetup;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.Dimensions;
import net.imglib2.util.Pair;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractInfrastructure;
import net.preibisch.mvrecon.fiji.datasetmanager.DatasetCreationUtils;
import net.preibisch.mvrecon.fiji.datasetmanager.FileListDatasetDefinitionUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.boundingbox.BoundingBoxes;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.filemap2.FileMapEntry;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.FileMapImgLoaderLOCI;
import net.preibisch.mvrecon.fiji.spimdata.intensityadjust.IntensityAdjustments;
import net.preibisch.mvrecon.fiji.spimdata.interestpoints.ViewInterestPoints;
import net.preibisch.mvrecon.fiji.spimdata.pointspreadfunctions.PointSpreadFunctions;
import net.preibisch.mvrecon.fiji.spimdata.stitchingresults.StitchingResults;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

/**
 * Command-line counterpart of BigStitcher's "Define Multi-View Dataset" for the plain case of one image file per
 * view (every channel of a file becomes a view of its own): the image sizes come from Bio-Formats, the voxel sizes
 * from the files' metadata or {@code --voxelSize}, the attributes (angle, tile, illumination) from the command line.
 * The result is a new BigStitcher XML with the Bio-Formats file-list image loader, the view setups and the
 * calibration transforms, ready for {@code apply-transformation}, {@code resave}, {@code detect-interestpoints} and
 * the registration. Not a Spark job.
 */
public class DefineDataset extends AbstractInfrastructure
{
	private static final long serialVersionUID = 4196573015529176613L;

	@Option(names = { "-x", "--xml" }, required = true, description = "path of the BigStitcher XML to create, e.g. -x /home/dataset.xml (refused if it exists, unless --overwrite)")
	private String xmlURIString = null;

	@Option(names = { "-i", "--image" }, required = true, description = "an image file readable by Bio-Formats, one option per view, e.g. -i viewA.tif -i viewB.tif -i viewC.tif; every channel of a file becomes a view with that channel id, only the first series of a file is used")
	private List< String > images = null;

	@Option(names = { "--angle" }, split = ",", description = "angle ids, one per image or a single id for all (default: the image index, so every image is one angle)")
	private List< Integer > angles = null;

	@Option(names = { "--tile" }, split = ",", description = "tile ids, one per image or a single id for all (default: 0)")
	private List< Integer > tiles = null;

	@Option(names = { "--illumination" }, split = ",", description = "illumination ids, one per image or a single id for all (default: 0)")
	private List< Integer > illuminations = null;

	@Option(names = { "--timepoint" }, description = "timepoint id of all images (default: 0)")
	private int timepoint = 0;

	@Option(names = { "--voxelSize" }, description = "voxel size x,y,z of the images, one option per image or a single one for all, e.g. --voxelSize 0.0534,0.0534,0.549 (default: the files' metadata; ImageJ TIFFs often declare wrong values, so better give it)")
	private List< String > voxelSizes = null;

	@Option(names = { "--unit" }, description = "unit of the voxel sizes (default: the files' metadata, or um when --voxelSize is given)")
	private String unit = null;

	@Option(names = { "--overwrite" }, description = "replace an existing XML (default: refuse)")
	private boolean overwrite = false;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		final URI xmlURI = URITools.toURI( xmlURIString );

		if ( URITools.isFile( xmlURI ) && new File( xmlURI ).exists() && !overwrite )
		{
			System.err.println( "ERROR: " + xmlURI + " exists; use --overwrite to replace it." );
			return null;
		}

		final int n = images.size();
		final List< Integer > angleIds, tileIds, illumIds;
		final List< double[] > voxel;
		try
		{
			angleIds = perImage( angles, n, "--angle", null );
			tileIds = perImage( tiles, n, "--tile", 0 );
			illumIds = perImage( illuminations, n, "--illumination", 0 );
			voxel = perImageVoxelSize( voxelSizes, n );
		}
		catch ( final IllegalArgumentException e )
		{
			System.err.println( "ERROR: " + e.getMessage() );
			return null;
		}

		// the views: one per (image, channel), in the order of the images
		final List< ViewSetup > setups = new ArrayList<>();
		final Map< Integer, FileMapEntry > setupToEntry = new HashMap<>();
		final Set< String > attributeCombinations = new HashSet<>();

		for ( int i = 0; i < n; ++i )
		{
			final File file = new File( images.get( i ) );

			if ( !file.exists() )
			{
				System.err.println( "ERROR: image " + file + " does not exist." );
				return null;
			}

			final Map< FileMapEntry, Pair< Dimensions, VoxelDimensions > > dims = new HashMap<>();
			FileListDatasetDefinitionUtil.detectDimensionsInFile( file, dims, null );

			final List< FileMapEntry > series0 = new ArrayList<>();
			int maxSeries = -1;
			for ( final FileMapEntry e : dims.keySet() )
			{
				maxSeries = Math.max( maxSeries, e.series() );
				if ( e.series() == 0 )
					series0.add( e );
			}

			if ( series0.isEmpty() )
			{
				System.err.println( "ERROR: Bio-Formats could not read " + file + " (no series/channel found)." );
				return null;
			}

			if ( maxSeries > 0 )
				System.out.println( "WARNING: " + file + " has " + ( maxSeries + 1 ) + " series, only the first one is used." );

			series0.sort( ( a, b ) -> Integer.compare( a.channel(), b.channel() ) );

			for ( final FileMapEntry entry : series0 )
			{
				final Dimensions size = dims.get( entry ).getA();
				final VoxelDimensions fileVoxel = dims.get( entry ).getB();

				final VoxelDimensions voxelSize;
				if ( voxel != null )
					voxelSize = new FinalVoxelDimensions( unit != null ? unit : "um", voxel.get( i ) );
				else if ( unit != null )
					voxelSize = new FinalVoxelDimensions( unit, fileVoxel.dimension( 0 ), fileVoxel.dimension( 1 ), fileVoxel.dimension( 2 ) );
				else
					voxelSize = fileVoxel;

				final int setupId = setups.size();
				final int angleId = angleIds == null ? i : angleIds.get( i );
				final Angle angle = new Angle( angleId, Integer.toString( angleId ) );
				final Tile tile = new Tile( tileIds.get( i ), Integer.toString( tileIds.get( i ) ) );
				final Illumination illum = new Illumination( illumIds.get( i ), Integer.toString( illumIds.get( i ) ) );
				final Channel channel = new Channel( entry.channel(), Integer.toString( entry.channel() ) );

				final String combination = "angle " + angleId + ", tile " + tile.getId() + ", illumination " + illum.getId() + ", channel " + channel.getId();
				if ( !attributeCombinations.add( combination ) )
				{
					System.err.println( "ERROR: two views would have the same attributes (" + combination + "); give distinct --angle/--tile/--illumination ids." );
					return null;
				}

				setups.add( new ViewSetup( setupId, Integer.toString( setupId ), size, voxelSize, tile, channel, angle, illum ) );
				setupToEntry.put( setupId, entry );

				System.out.println( "ViewSetup " + setupId + ": " + file.getName() + " (series 0, channel " + entry.channel() + "), size " +
						size.dimension( 0 ) + " x " + size.dimension( 1 ) + " x " + size.dimension( 2 ) + ", voxel size " +
						voxelSize.dimension( 0 ) + " x " + voxelSize.dimension( 1 ) + " x " + voxelSize.dimension( 2 ) + " " + voxelSize.unit() + ", " + combination );
			}
		}

		final SequenceDescription sd = new SequenceDescription(
				new TimePoints( Collections.singletonList( new TimePoint( timepoint ) ) ),
				setups, null, new MissingViews( new ArrayList< ViewId >() ) );

		final Map< BasicViewDescription< ? >, FileMapEntry > fileMap = new HashMap<>();
		for ( final Map.Entry< Integer, FileMapEntry > e : setupToEntry.entrySet() )
		{
			final ViewDescription vd = sd.getViewDescription( timepoint, e.getKey() );
			fileMap.put( vd, e.getValue() );
		}
		sd.setImgLoader( new FileMapImgLoaderLOCI( fileMap, sd, false ) );

		double minResolution = Double.MAX_VALUE;
		for ( final ViewSetup vs : setups )
			for ( int d = 0; d < 3; ++d )
				minResolution = Math.min( minResolution, vs.getVoxelSize().dimension( d ) );

		final ViewRegistrations vrs = DatasetCreationUtils.createViewRegistrations( sd.getViewDescriptions(), minResolution );

		System.out.println( "Calibration (voxel size / " + minResolution + " " + ( unit != null ? unit : setups.get( 0 ).getVoxelSize().unit() ) + "):" );
		for ( final ViewRegistration vr : vrs.getViewRegistrationsOrdered() )
			System.out.println( "\tViewSetup " + vr.getViewSetupId() + ": " + vr.getTransformList().get( vr.getTransformList().size() - 1 ).asAffine3D() );

		final SpimData2 data = new SpimData2(
				URITools.getParentURI( xmlURI ), sd, vrs,
				new ViewInterestPoints(), new BoundingBoxes(), new PointSpreadFunctions(), new StitchingResults(), new IntensityAdjustments() );

		if ( dryRun )
		{
			System.out.println( "Dry run, not saving " + xmlURI );
			return null;
		}

		System.out.println( "Saving " + xmlURI + " ..." );
		new XmlIoSpimData2().save( data, xmlURI );
		System.out.println( "Done." );

		return null;
	}

	/**
	 * Expands a per-image option: {@code null} gives {@code n} times {@code defaultValue} (or {@code null} if that is
	 * {@code null}), one value is repeated, otherwise exactly {@code n} values are required.
	 */
	static List< Integer > perImage( final List< Integer > values, final int n, final String option, final Integer defaultValue )
	{
		if ( values == null || values.isEmpty() )
			return defaultValue == null ? null : Collections.nCopies( n, defaultValue );
		if ( values.size() == 1 )
			return Collections.nCopies( n, values.get( 0 ) );
		if ( values.size() != n )
			throw new IllegalArgumentException( option + " has " + values.size() + " values for " + n + " images; give one per image or a single one for all." );
		return values;
	}

	/** {@code null} without {@code --voxelSize}; otherwise one {@code x,y,z} triple per image (a single one is repeated). */
	static List< double[] > perImageVoxelSize( final List< String > values, final int n )
	{
		if ( values == null || values.isEmpty() )
			return null;
		if ( values.size() != 1 && values.size() != n )
			throw new IllegalArgumentException( "--voxelSize has " + values.size() + " values for " + n + " images; give one per image or a single one for all." );
		final List< double[] > out = new ArrayList<>();
		for ( int i = 0; i < n; ++i )
		{
			final String s = values.get( values.size() == 1 ? 0 : i );
			final String[] parts = s.split( "," );
			if ( parts.length != 3 )
				throw new IllegalArgumentException( "--voxelSize '" + s + "' is not x,y,z." );
			final double[] v = new double[ 3 ];
			for ( int d = 0; d < 3; ++d )
			{
				v[ d ] = Double.parseDouble( parts[ d ].trim() );
				if ( !( v[ d ] > 0 ) )
					throw new IllegalArgumentException( "--voxelSize '" + s + "' must be positive." );
			}
			out.add( v );
		}
		return out;
	}

	public static void main( final String... args )
	{
		System.out.println( Arrays.toString( args ) );
		System.exit( new CommandLine( new DefineDataset() ).execute( args ) );
	}
}
