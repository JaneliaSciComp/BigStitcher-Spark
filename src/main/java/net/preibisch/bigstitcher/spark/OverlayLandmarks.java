/*-
 * #%L
 * Spark-based parallel BigStitcher project.
 * %%
 * Copyright (C) 2021 - 2026 Developers.
 * %%
 * Licensed under GPL v2; see LICENSE.
 * #L%
 */
package net.preibisch.bigstitcher.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import org.janelia.saalfeldlab.n5.universe.metadata.axes.Axis;
import org.janelia.saalfeldlab.n5.universe.metadata.ome.ngff.OmeNgffMultiScaleMetadata;
import org.janelia.saalfeldlab.n5.universe.metadata.ome.ngff.coordinateTransformations.CoordinateTransformation;
import org.janelia.saalfeldlab.n5.universe.metadata.ome.ngff.coordinateTransformations.ScaleCoordinateTransformation;

import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import bdv.util.RandomAccessibleIntervalMipmapSource;
import bdv.viewer.Source;
import ij.process.FloatProcessor;
import mpicbg.spim.data.sequence.FinalVoxelDimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealPoint;
import net.imglib2.converter.Converters;
import net.imglib2.converter.RealUnsignedByteConverter;
import net.imglib2.img.basictypeaccess.AccessFlags;
import net.imglib2.RealPointSampleList;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Util;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.util.bdv.CLLCN;
import net.preibisch.bigstitcher.spark.util.bdv.ImageJStackOp;
import net.preibisch.bigstitcher.spark.util.bdv.Lazy;
import net.preibisch.bigstitcher.spark.util.bdv.Normalization;
import net.preibisch.mvrecon.imglib2.st.filter.GaussianFilterFactory;
import net.preibisch.mvrecon.imglib2.st.filter.GaussianFilterFactory.WeightType;
import net.preibisch.mvrecon.imglib2.st.render.Render;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

/**
 * BDV viewer for fused N5/Zarr containers + landmark overlays from the TPS
 * landmark CSV. Not a Spark job; pure local viewer.
 *
 * <p>Sources can be loaded from either an N5 dataset (with {@code s0, s1, ...}
 * children) or an OME-Zarr group (multiscales attribute auto-detected).
 * Each source can optionally be normalized with CLLCN / CLAHE.
 *
 * <p>Default color-coding (one source per landmark type):
 * <ul>
 *   <li><b>correspondence centers-of-mass (corrCOM)</b> → magenta</li>
 *   <li><b>correspondence midpoints</b> → green</li>
 *   <li><b>self-donation surface nails (nail)</b> → yellow</li>
 *   <li><b>cross-view tie nails (partner_nail)</b> → cyan</li>
 * </ul>
 * With {@code --csvSplitByViewId}, each type is split into one source per
 * view_setup_id and colored by a golden-ratio hue derived from the view id.
 */
public class OverlayLandmarks implements Callable< Void >, Serializable
{
	private static final long serialVersionUID = 1L;

	@Option( names = "-i", arity = "1..*", required = true,
			description = "N5/Zarr container path(s)" )
	private String[] inputs;

	@Option( names = "-d", arity = "1..*", required = true,
			description = "Dataset path within each container, parallel to -i. For OME-Zarr: group containing 'multiscales' attribute. For N5: parent group containing 's0, s1, ...'." )
	private String[] datasets;

	@Option( names = "--filter", arity = "1..*",
			description = "Per-dataset normalization (parallel to -i): NONE, CLLCN, CLAHE, CLAHE_WITH_THRESHOLDMASK. Default: NONE." )
	private Normalization[] filters;

	@Option( names = "--csv",
			description = "Landmarks CSV from --tpsLandmarksOut. Required unless --debugPoint is given." )
	private String csv;

	@Option( names = "--sigma", defaultValue = "40",
			description = "Gaussian sigma (render-space pixels) for landmark rendering (default: 40)." )
	private double sigma;

	@Option( names = "-c", arity = "1..*",
			description = "Channel index per -i input (parallel arrays). Required for an OME-Zarr input with >1 channel. "
					+ "Optional (defaults to 0) for single-channel OME-Zarr and 3D N5 inputs. "
					+ "If specified, must have the same count as -i." )
	private Integer[] channelIndices;

	@Option( names = "-t", arity = "1..*",
			description = "Timepoint index per -i input (parallel arrays). Required for an OME-Zarr input with >1 timepoint. "
					+ "Optional (defaults to 0) for single-timepoint OME-Zarr and 3D N5 inputs. "
					+ "If specified, must have the same count as -i." )
	private Integer[] timepointIndices;

	@Option( names = "--minIntensity", defaultValue = "0",
			description = "Min intensity for converting input to UnsignedByteType (default 0)." )
	private double minIntensity;

	@Option( names = "--maxIntensity", defaultValue = "65535",
			description = "Max intensity for converting input to UnsignedByteType (default 65535)." )
	private double maxIntensity;

	@Option( names = "--csvSplitByViewId",
			description = "If set, split every landmark type by view_setup_id (one BDV source per (type, view), per-view colored). Default: merge all view ids within each type (3 sources total: corrCOM, midpoints, nails)." )
	private boolean csvSplitByViewId;

	@Override
	public Void call() throws Exception
	{
		// === Validate parallel arrays ===
		if ( inputs.length != datasets.length )
			throw new IllegalArgumentException( "-i and -d must have the same count (got " + inputs.length + " vs " + datasets.length + ")" );
		if ( channelIndices != null && channelIndices.length != inputs.length )
			throw new IllegalArgumentException( "-c count must match -i count when provided (got "
					+ channelIndices.length + " vs " + inputs.length + ")" );
		if ( timepointIndices != null && timepointIndices.length != inputs.length )
			throw new IllegalArgumentException( "-t count must match -i count when provided (got "
					+ timepointIndices.length + " vs " + inputs.length + ")" );
		final Normalization[] effectiveFilters = new Normalization[ inputs.length ];
		Arrays.fill( effectiveFilters, Normalization.NONE );
		if ( filters != null )
			for ( int i = 0; i < Math.min( filters.length, effectiveFilters.length ); ++i )
				effectiveFilters[ i ] = filters[ i ];

		// === Probe each container; validate per-input -c/-t before opening BDV ===
		final N5Reader[] readers = new N5Reader[ inputs.length ];
		final int[] effC = new int[ inputs.length ];
		final int[] effT = new int[ inputs.length ];
		for ( int i = 0; i < inputs.length; ++i )
		{
			final URI uri = URITools.toURI( inputs[ i ] );
			final StorageFormat storage = guessStorage( inputs[ i ] );
			readers[ i ] = URITools.instantiateN5Reader( storage, uri );

			final InputDims d = probeContainer( readers[ i ], datasets[ i ] );

			if ( d.isOmeZarr && d.numChannels > 1 && channelIndices == null )
				throw new IllegalArgumentException( "input #" + i + " (" + inputs[ i ]
						+ ") is OME-Zarr with " + d.numChannels + " channels — -c is required for every input." );
			if ( d.isOmeZarr && d.numTimepoints > 1 && timepointIndices == null )
				throw new IllegalArgumentException( "input #" + i + " (" + inputs[ i ]
						+ ") is OME-Zarr with " + d.numTimepoints + " timepoints — -t is required for every input." );

			effC[ i ] = ( channelIndices == null ) ? 0 : channelIndices[ i ];
			effT[ i ] = ( timepointIndices == null ) ? 0 : timepointIndices[ i ];

			if ( effC[ i ] < 0 || effC[ i ] >= d.numChannels )
				throw new IllegalArgumentException( "input #" + i + " (" + inputs[ i ]
						+ "): -c=" + effC[ i ] + " out of range [0, " + d.numChannels + ")" );
			if ( effT[ i ] < 0 || effT[ i ] >= d.numTimepoints )
				throw new IllegalArgumentException( "input #" + i + " (" + inputs[ i ]
						+ "): -t=" + effT[ i ] + " out of range [0, " + d.numTimepoints + ")" );
		}

		// === Open image sources ===
		BdvStackSource< ? > bdv = null;
		for ( int i = 0; i < inputs.length; ++i )
		{
			final String name = Paths.get( inputs[ i ] ).getFileName() + " / " + datasets[ i ];
			System.out.println( "[overlay-landmarks] opening " + name + " (filter=" + effectiveFilters[ i ]
					+ ", c=" + effC[ i ] + ", t=" + effT[ i ] + ")" );
			final Source< UnsignedByteType > src = createMipmapSource(
					readers[ i ], datasets[ i ], effectiveFilters[ i ], name,
					effC[ i ], effT[ i ], minIntensity, maxIntensity );
			final BdvOptions opts = (bdv == null) ? BdvOptions.options() : BdvOptions.options().addTo( bdv );
			bdv = BdvFunctions.show( src, opts );
			bdv.setDisplayRangeBounds( 0, 255 );
			bdv.setDisplayRange( 0, 255 );
		}

		// === Load landmarks (CSV or single debug point) ===
		if ( csv != null && !csv.isEmpty() )
		{
			final Map< Integer, List< RealPoint > > corrCOMByView = new HashMap<>();
			final Map< Integer, List< RealPoint > > midpointsByView = new HashMap<>();
			final Map< Integer, List< RealPoint > > nailsByView = new HashMap<>();
			final Map< Integer, List< RealPoint > > partnerNailsByView = new HashMap<>();
			loadCsv( csv, corrCOMByView, midpointsByView, nailsByView, partnerNailsByView );
			System.out.println( "[overlay-landmarks] loaded "
					+ corrCOMByView.values().stream().mapToInt( List::size ).sum() + " corrCOM, "
					+ midpointsByView.values().stream().mapToInt( List::size ).sum() + " midpoints, "
					+ nailsByView.values().stream().mapToInt( List::size ).sum() + " nails, "
					+ partnerNailsByView.values().stream().mapToInt( List::size ).sum() + " partner_nails across "
					+ allViewIds( corrCOMByView, midpointsByView, nailsByView, partnerNailsByView ).size() + " views" );

			// === Transform landmark coordinates into the image's BDV world frame ===
			// Two pieces are needed (read from the first input — landmarks are one CSV per fusion):
			//   1. Bigstitcher-Spark/Boundingbox_min — the ZARR is always indexed from (0,0,0)
			//      while the CSV uses global world coords; subtract bbox_min to get voxel coords.
			//   2. Bigstitcher-Spark/AnisotropyFactor — for --preserveAnisotropy fusions the CSV
			//      and the bbox are in anisotropic z-voxel units, but the image source is shown
			//      with sourceTransform = scale(1, 1, anisoZ) so voxel k ends up at world z = k*anisoZ
			//      in BDV. Multiply landmark z by anisoZ so they land in the same world frame.
			final double[] bboxMin = readBoundingBoxMin( readers[ 0 ] );
			final Double anisoZ = readBssAnisotropyFactor( readers[ 0 ] );
			final double zScale = ( anisoZ != null ) ? anisoZ : 1.0;
			if ( bboxMin != null || zScale != 1.0 )
			{
				final double[] off = ( bboxMin != null ) ? bboxMin : new double[ 3 ];
				System.out.println( "[overlay-landmarks] landmark transform: subtract " + Arrays.toString( off )
						+ ( zScale != 1.0 ? "  then *z " + zScale : "" ) );
				for ( final Map< Integer, List< RealPoint > > byView :
						Arrays.asList( corrCOMByView, midpointsByView, nailsByView, partnerNailsByView ) )
					for ( final List< RealPoint > pts : byView.values() )
						transformAll( pts, off, zScale );
			}

			// === Compute viewer interval from the landmark world-coord extent (not image
			// voxel extent — landmarks are in global/world coordinates and may sit outside
			// the image's voxel bounds, especially when --preserveAnisotropy has stretched
			// the image's world z). The image source is shown with its own transform; the
			// landmark sources stay at identity since their coordinates are already world. ===
			final Interval viewerInterval = enclosingInterval( sigma, corrCOMByView, midpointsByView, nailsByView, partnerNailsByView );

			// === Build landmark sources ===
			// Without --csvSplitByViewId: one source per type. corrCOM=magenta, midpoints=green,
			// nails (self donations) = yellow, partner_nails (cross-view ties) = cyan; display
			// range 0..2 for the dot types, 0..6 for both nail variants.
			// With --csvSplitByViewId: one source per (type, view_id); each source is colored by
			// its view (golden-ratio hue), so cross-view structure is visible.
			final ARGBType magenta = new ARGBType( ARGBType.rgba( 255,   0, 255, 255 ) );
			final ARGBType green   = new ARGBType( ARGBType.rgba(   0, 255,   0, 255 ) );
			final ARGBType yellow  = new ARGBType( ARGBType.rgba( 255, 255,   0, 255 ) );
			final ARGBType cyan    = new ARGBType( ARGBType.rgba(   0, 255, 255, 255 ) );
			if ( csvSplitByViewId )
			{
				bdv = addSplitSources( bdv, corrCOMByView,      viewerInterval, sigma, "corrCOM",       2.0 );
				bdv = addSplitSources( bdv, midpointsByView,    viewerInterval, sigma, "midpoints",     2.0 );
				bdv = addSplitSources( bdv, nailsByView,        viewerInterval, sigma, "nails",         6.0 );
				bdv = addSplitSources( bdv, partnerNailsByView, viewerInterval, sigma, "partner_nails", 6.0 );
			}
			else
			{
				final List< RealPoint > corrCOMAll      = flatten( corrCOMByView );
				final List< RealPoint > midpointsAll    = flatten( midpointsByView );
				final List< RealPoint > nailsAll        = flatten( nailsByView );
				final List< RealPoint > partnerNailsAll = flatten( partnerNailsByView );
				if ( !corrCOMAll.isEmpty() )      bdv = addPointSource( bdv, corrCOMAll,      viewerInterval, sigma, "corrCOM",       magenta, 2.0 );
				if ( !midpointsAll.isEmpty() )    bdv = addPointSource( bdv, midpointsAll,    viewerInterval, sigma, "midpoints",     green,   2.0 );
				if ( !nailsAll.isEmpty() )        bdv = addPointSource( bdv, nailsAll,        viewerInterval, sigma, "nails",         yellow,  6.0 );
				if ( !partnerNailsAll.isEmpty() ) bdv = addPointSource( bdv, partnerNailsAll, viewerInterval, sigma, "partner_nails", cyan,    6.0 );
			}
		}

		System.out.println( "[overlay-landmarks] BDV ready. Close the window to exit." );
		return null;
	}

	// ===========================================================================
	// CSV parsing
	// ===========================================================================

	private static void loadCsv(
			final String path,
			final Map< Integer, List< RealPoint > > corrCOMByView,
			final Map< Integer, List< RealPoint > > midpointsByView,
			final Map< Integer, List< RealPoint > > nailsByView,
			final Map< Integer, List< RealPoint > > partnerNailsByView ) throws IOException
	{
		try ( final BufferedReader r = Files.newBufferedReader( Paths.get( path ), StandardCharsets.UTF_8 ) )
		{
			final String header = r.readLine();
			if ( header == null || !header.startsWith( "view_setup_id" ) )
				throw new IOException( "Unexpected CSV header: " + header );

			// Forward-compatible: accept the legacy 9-column format (no donor) AND the new
			// 10-column format with a trailing donor_view_setup_id. Per-view bucketing uses
			// the RECIPIENT view (cols[0]) — that's the TPS they actually feed.
			//
			// "nail" rows split into two buckets based on the donor column:
			//   donor == recipient → self-donation, bucket as "nails"
			//   donor != recipient → cross-view tie,  bucket as "partner_nails"
			// Legacy CSVs without a donor column treat every nail as self (no info to do otherwise).
			String line;
			while ( ( line = r.readLine() ) != null )
			{
				if ( line.isEmpty() ) continue;
				final String[] cols = line.split( "," );
				if ( cols.length < 9 ) continue;
				final int viewSetupId = Integer.parseInt( cols[ 0 ].trim() );
				final String type = cols[ 2 ].trim();
				final double tx = Double.parseDouble( cols[ 6 ].trim() );
				final double ty = Double.parseDouble( cols[ 7 ].trim() );
				final double tz = Double.parseDouble( cols[ 8 ].trim() );
				final RealPoint p = new RealPoint( tx, ty, tz );
				if ( "corrCOM".equals( type ) )
					corrCOMByView.computeIfAbsent( viewSetupId, k -> new ArrayList<>() ).add( p );
				else if ( "midpoint".equals( type ) )
					midpointsByView.computeIfAbsent( viewSetupId, k -> new ArrayList<>() ).add( p );
				else if ( "nail".equals( type ) )
				{
					final int donorViewId = ( cols.length >= 10 )
							? Integer.parseInt( cols[ 9 ].trim() )
							: viewSetupId;
					final Map< Integer, List< RealPoint > > bucket =
							( donorViewId == viewSetupId ) ? nailsByView : partnerNailsByView;
					bucket.computeIfAbsent( viewSetupId, k -> new ArrayList<>() ).add( p );
				}
			}
		}
	}

	// ===========================================================================
	// Image sources (N5 + OME-Zarr autodetect + optional normalization)
	// ===========================================================================

	private static StorageFormat guessStorage( final String path )
	{
		final String p = path.toLowerCase();
		if ( p.endsWith( ".zarr" ) || p.endsWith( ".zarr/" ) ) return StorageFormat.ZARR2;
		if ( p.endsWith( ".n5" ) || p.endsWith( ".n5/" ) ) return StorageFormat.N5;
		// Default to ZARR2 (broader coverage); user can wrap.
		return StorageFormat.ZARR2;
	}

	private static Source< UnsignedByteType > createMipmapSource(
			final N5Reader n5, final String groupPath, final Normalization normalization, final String name,
			final int channelIdx, final int timepointIdx,
			final double minIntensity, final double maxIntensity )
	{
		// Discover scale paths.
		final List< String > scalePaths = new ArrayList<>();
		final OmeNgffMultiScaleMetadata[] omeMs = tryReadMultiscales( n5, groupPath );
		final boolean isOmeZarr = omeMs != null && omeMs.length > 0 && omeMs[ 0 ].datasets != null;

		if ( isOmeZarr )
		{
			for ( int i = 0; i < omeMs[ 0 ].datasets.length; ++i )
				scalePaths.add( joinPath( groupPath, omeMs[ 0 ].datasets[ i ].path ) );
		}
		else
		{
			// N5 hot-knife style.
			int i = 0;
			while ( true )
			{
				final String dsPath = joinPath( groupPath, "s" + i );
				if ( !n5.exists( dsPath ) ) break;
				scalePaths.add( dsPath );
				++i;
			}
			if ( scalePaths.isEmpty() )
				throw new IllegalArgumentException( "No multiscales attribute and no s0 subdir under '" + groupPath + "' — cannot load." );
		}

		// Load each scale, converting / normalizing to UnsignedByteType.
		@SuppressWarnings( "unchecked" )
		final RandomAccessibleInterval< UnsignedByteType >[] scales = new RandomAccessibleInterval[ scalePaths.size() ];
		long[] s0Dims = null;
		for ( int i = 0; i < scalePaths.size(); ++i )
		{
			RandomAccessibleInterval< ? > raw = N5Utils.openVolatile( n5, scalePaths.get( i ) );
			// 5D → 3D (for OME-Zarr X Y Z C T containers).
			raw = sliceTo3D( raw, channelIdx, timepointIdx );

			// Convert to UnsignedByteType lazily via Converters.
			@SuppressWarnings( { "rawtypes", "unchecked" } )
			final RandomAccessibleInterval< UnsignedByteType > asByte = Converters.convert(
					( RandomAccessibleInterval ) raw,
					new RealUnsignedByteConverter<>( minIntensity, maxIntensity ),
					new UnsignedByteType() );

			final RandomAccessibleInterval< UnsignedByteType > processed = ( normalization == Normalization.NONE )
					? asByte
					: applyNormalization( asByte, normalization );
			scales[ i ] = processed;
			if ( i == 0 ) s0Dims = scales[ i ].dimensionsAsLongArray();
		}

		// Build per-level XYZ scales (world units). For OME-Zarr containers we lift the
		// absolute scales from multiscales.datasets[i].coordinateTransformations — this
		// already encodes anisotropy (e.g. z=anisotropyFactor when --preserveAnisotropy
		// was used) × downsampling. For N5 hot-knife containers we fall back to dim
		// ratios and post-multiply z by the BSS anisotropy attribute if present.
		final double[][] resolutions = new double[ scalePaths.size() ][ 3 ];
		final FinalVoxelDimensions voxelDims;
		if ( isOmeZarr )
		{
			boolean ok = true;
			for ( int i = 0; i < scalePaths.size() && ok; ++i )
			{
				final double[] s = readScaleXYZ( omeMs[ 0 ], i );
				if ( s == null ) { ok = false; break; }
				resolutions[ i ] = s;
			}
			if ( !ok )
				fillResolutionsFromDimRatios( resolutions, scales, s0Dims );
			final double[] s0Scale = resolutions[ 0 ].clone();
			voxelDims = new FinalVoxelDimensions( omeZarrSpatialUnit( omeMs[ 0 ] ), s0Scale );
		}
		else
		{
			fillResolutionsFromDimRatios( resolutions, scales, s0Dims );
			final Double aniso = readBssAnisotropyFactor( n5 );
			if ( aniso != null )
			{
				for ( int i = 0; i < resolutions.length; ++i )
					resolutions[ i ][ 2 ] *= aniso;
				voxelDims = new FinalVoxelDimensions( "micrometer", new double[] { 1.0, 1.0, aniso } );
			}
			else
			{
				voxelDims = new FinalVoxelDimensions( "px", 1, 1, 1 );
			}
		}

		return new RandomAccessibleIntervalMipmapSource<>(
				scales,
				new UnsignedByteType(),
				resolutions,
				voxelDims,
				name );
	}

	private static void fillResolutionsFromDimRatios(
			final double[][] resolutions,
			final RandomAccessibleInterval< UnsignedByteType >[] scales,
			final long[] s0Dims )
	{
		for ( int i = 0; i < scales.length; ++i )
		{
			final long[] dims = scales[ i ].dimensionsAsLongArray();
			for ( int d = 0; d < 3; ++d )
				resolutions[ i ][ d ] = ( double ) s0Dims[ Math.min( d, s0Dims.length - 1 ) ] / Math.max( 1, dims[ Math.min( d, dims.length - 1 ) ] );
		}
	}

	private static double[] readScaleXYZ( final OmeNgffMultiScaleMetadata ms, final int level )
	{
		if ( ms.datasets == null || level >= ms.datasets.length ) return null;
		final CoordinateTransformation< ? >[] cts = ms.datasets[ level ].coordinateTransformations;
		if ( cts == null ) return null;
		for ( final CoordinateTransformation< ? > c : cts )
		{
			if ( c instanceof ScaleCoordinateTransformation )
			{
				final double[] s = ( ( ScaleCoordinateTransformation ) c ).getScale();
				// BSS writes the scale in XYZ(CT) F-order (see OMEZarrAttributes.createOMEZarrMetadata),
				// matching the convention used by AllenOMEZarrProperties.getMipmapResolutions.
				if ( s != null && s.length >= 3 )
					return new double[] { s[ 0 ], s[ 1 ], s[ 2 ] };
			}
		}
		return null;
	}

	private static String omeZarrSpatialUnit( final OmeNgffMultiScaleMetadata ms )
	{
		if ( ms.axes != null )
		{
			for ( final Axis ax : ms.axes )
			{
				if ( "space".equals( ax.getType() ) && ax.getUnit() != null && !ax.getUnit().isEmpty() )
					return ax.getUnit();
			}
		}
		return "micrometer";
	}

	private static Double readBssAnisotropyFactor( final N5Reader n5 )
	{
		try
		{
			final Boolean preserve = n5.getAttribute( "/", "Bigstitcher-Spark/PreserveAnisotropy", Boolean.class );
			if ( preserve == null || !preserve ) return null;
			return n5.getAttribute( "/", "Bigstitcher-Spark/AnisotropyFactor", Double.class );
		}
		catch ( final Exception e )
		{
			return null;
		}
	}

	private static double[] readBoundingBoxMin( final N5Reader n5 )
	{
		try
		{
			final long[] m = n5.getAttribute( "/", "Bigstitcher-Spark/Boundingbox_min", long[].class );
			if ( m == null || m.length < 3 ) return null;
			return new double[] { m[ 0 ], m[ 1 ], m[ 2 ] };
		}
		catch ( final Exception e )
		{
			return null;
		}
	}

	private static void transformAll( final List< RealPoint > pts, final double[] offset, final double zScale )
	{
		for ( final RealPoint p : pts )
		{
			for ( int d = 0; d < 3; ++d )
				p.move( -offset[ d ], d );
			if ( zScale != 1.0 )
				p.setPosition( p.getDoublePosition( 2 ) * zScale, 2 );
		}
	}

	/** Container shape probe, used to validate -c/-t against the actual axis sizes. */
	private static final class InputDims
	{
		final boolean isOmeZarr;
		final int numChannels;
		final int numTimepoints;
		final int numDimensions;

		InputDims( final boolean isOmeZarr, final int numChannels, final int numTimepoints, final int numDimensions )
		{
			this.isOmeZarr = isOmeZarr;
			this.numChannels = numChannels;
			this.numTimepoints = numTimepoints;
			this.numDimensions = numDimensions;
		}
	}

	private static InputDims probeContainer( final N5Reader n5, final String groupPath )
	{
		final String scale0;
		final boolean isOmeZarr;
		final OmeNgffMultiScaleMetadata[] omeMs = tryReadMultiscales( n5, groupPath );
		if ( omeMs != null && omeMs.length > 0 && omeMs[ 0 ].datasets != null && omeMs[ 0 ].datasets.length > 0 )
		{
			isOmeZarr = true;
			scale0 = joinPath( groupPath, omeMs[ 0 ].datasets[ 0 ].path );
		}
		else
		{
			isOmeZarr = false;
			scale0 = joinPath( groupPath, "s0" );
		}

		if ( !n5.datasetExists( scale0 ) )
			throw new IllegalArgumentException( "No s0/multiscales[0] dataset found under '" + groupPath
					+ "' (tried '" + scale0 + "')" );

		final long[] shape = n5.getDatasetAttributes( scale0 ).getDimensions();
		final int n = shape.length;
		// Assumes XYZCT layout: numC = shape[3] (if present), numT = shape[4] (if present).
		final int numC = ( n >= 4 ) ? ( int ) shape[ 3 ] : 1;
		final int numT = ( n >= 5 ) ? ( int ) shape[ 4 ] : 1;
		return new InputDims( isOmeZarr, numC, numT, n );
	}

	private static OmeNgffMultiScaleMetadata[] tryReadMultiscales( final N5Reader n5, final String groupPath )
	{
		try
		{
			OmeNgffMultiScaleMetadata[] ms = n5.getAttribute( groupPath, "multiscales", OmeNgffMultiScaleMetadata[].class );
			if ( ms == null || ms.length == 0 )
				ms = n5.getAttribute( groupPath, "attributes/ome/multiscales", OmeNgffMultiScaleMetadata[].class );
			return ms;
		}
		catch ( final Exception e )
		{
			return null;
		}
	}

	private static String joinPath( final String base, final String rel )
	{
		if ( base == null || base.isEmpty() || base.equals( "/" ) ) return rel;
		if ( base.endsWith( "/" ) ) return base + rel;
		return base + "/" + rel;
	}

	private static RandomAccessibleInterval< ? > sliceTo3D(
			final RandomAccessibleInterval< ? > raw, final int channelIdx, final int timepointIdx )
	{
		RandomAccessibleInterval< ? > r = raw;
		// Drop trailing dims down to 3 via hyperSlice at c/t. Assumes XYZCT layout:
		// 5D → slice last (=t) with timepointIdx, then 4D → slice last (=c) with channelIdx.
		// Bounds were already checked in call(), so no clamping here.
		while ( r.numDimensions() > 3 )
		{
			final int last = r.numDimensions() - 1;
			final int idx = ( r.numDimensions() == 5 ) ? timepointIdx : channelIdx;
			r = Views.hyperSlice( r, last, idx );
		}
		return r;
	}

	private static RandomAccessibleInterval< UnsignedByteType > applyNormalization(
			final RandomAccessibleInterval< UnsignedByteType > input,
			final Normalization normalization )
	{
		final int[] blockSize = { 256, 256, 32 };
		final ImageJStackOp< UnsignedByteType > op;
		final int blockRadius = 500;
		switch ( normalization )
		{
		case CLLCN:
			op = new ImageJStackOp< UnsignedByteType >(
					Views.extendZero( input ),
					( fp ) -> new CLLCN( fp ).run( blockRadius, blockRadius, 3f, 10, 0.5f, true, true, true ),
					blockRadius, 0.0, 255.0, false );
			break;
		case CLAHE:
			op = new ImageJStackOp< UnsignedByteType >(
					Views.extendZero( input ),
					( fp ) -> mpicbg.ij.clahe.Flat.getFastInstance().run(
							new ij.ImagePlus( "", fp ), blockRadius, 256, 5f, null, false ),
					blockRadius, 0.0, 255.0, false );
			break;
		case CLAHE_WITH_THRESHOLDMASK:
			op = new ImageJStackOp< UnsignedByteType >(
					Views.extendZero( input ),
					( fp ) -> {
						final FloatProcessor mask = ( FloatProcessor ) fp.duplicate();
						mask.threshold( 1 ); // exclude near-zero from CLAHE
						mpicbg.ij.clahe.Flat.getFastInstance().run(
								new ij.ImagePlus( "", fp ), blockRadius, 256, 5f,
								( ij.process.ByteProcessor ) mask.convertToByte( false ), false );
					},
					blockRadius, 0.0, 255.0, false );
			break;
		default:
			return input;
		}
		return Lazy.process( input, blockSize, new UnsignedByteType(), AccessFlags.setOf( AccessFlags.VOLATILE ), op );
	}

	// ===========================================================================
	// Landmark sources
	// ===========================================================================

	private static BdvStackSource< ? > addPointSource(
			final BdvStackSource< ? > existing,
			final List< RealPoint > pts,
			final Interval viewerInterval,
			final double sigma,
			final String name,
			final ARGBType color,
			final double displayMax )
	{
		final RealPointSampleList< FloatType > samples = new RealPointSampleList<>( 3 );
		for ( final RealPoint p : pts )
			samples.add( p, new FloatType( 1f ) );

		final GaussianFilterFactory< FloatType, FloatType > filterFactory =
				new GaussianFilterFactory<>( new FloatType( 0f ), sigma, WeightType.NONE );
		final RandomAccessibleInterval< FloatType > rendered = Views.interval(
				Views.raster( Render.render( samples, filterFactory ) ),
				viewerInterval );

		// Landmarks are in world/global coordinates already → identity sourceTransform.
		BdvOptions opts = BdvOptions.options().sourceTransform( new AffineTransform3D() );
		if ( existing != null )
			opts = opts.addTo( existing );
		final BdvStackSource< ? > src = BdvFunctions.show( rendered, name, opts );
		src.setColor( color );
		src.setDisplayRangeBounds( 0, Math.max( 1, displayMax ) );
		src.setDisplayRange( 0, displayMax );
		return src;
	}

	/** Deterministic but visually well-spread color per view id (golden-ratio hue). */
	private static ARGBType colorForViewId( final int vid )
	{
		final float hue = ( ( vid * 0.6180339887f ) % 1.0f + 1.0f ) % 1.0f;
		final int rgb = java.awt.Color.HSBtoRGB( hue, 0.85f, 1.0f );
		return new ARGBType( ARGBType.rgba(
				( rgb >> 16 ) & 0xff,
				( rgb >> 8 ) & 0xff,
				rgb & 0xff,
				255 ) );
	}

	@SafeVarargs
	private static Interval enclosingInterval(
			final double pad,
			final Map< Integer, List< RealPoint > >... byViewMaps )
	{
		final double[] min = new double[ 3 ];
		final double[] max = new double[ 3 ];
		Arrays.fill( min, Double.POSITIVE_INFINITY );
		Arrays.fill( max, Double.NEGATIVE_INFINITY );
		for ( final Map< Integer, List< RealPoint > > m : byViewMaps )
			for ( final List< RealPoint > pts : m.values() )
				updateBounds( min, max, pts );
		final long[] lmin = new long[ 3 ];
		final long[] lmax = new long[ 3 ];
		for ( int d = 0; d < 3; ++d )
		{
			lmin[ d ] = ( long ) Math.floor( min[ d ] - 3 * pad );
			lmax[ d ] = ( long ) Math.ceil( max[ d ] + 3 * pad );
		}
		return new FinalInterval( lmin, lmax );
	}

	private static List< RealPoint > flatten( final Map< Integer, List< RealPoint > > byView )
	{
		final List< RealPoint > all = new ArrayList<>();
		for ( final List< RealPoint > pts : byView.values() )
			all.addAll( pts );
		return all;
	}

	@SafeVarargs
	private static TreeSet< Integer > allViewIds( final Map< Integer, List< RealPoint > >... byViewMaps )
	{
		final TreeSet< Integer > ids = new TreeSet<>();
		for ( final Map< Integer, List< RealPoint > > m : byViewMaps )
			ids.addAll( m.keySet() );
		return ids;
	}

	private static BdvStackSource< ? > addSplitSources(
			BdvStackSource< ? > bdv,
			final Map< Integer, List< RealPoint > > byView,
			final Interval viewerInterval,
			final double sigma,
			final String typeName,
			final double displayMax )
	{
		for ( final Integer vid : new TreeSet<>( byView.keySet() ) )
		{
			final List< RealPoint > pts = byView.get( vid );
			if ( pts == null || pts.isEmpty() ) continue;
			bdv = addPointSource( bdv, pts, viewerInterval, sigma,
					typeName + " view " + vid, colorForViewId( vid ), displayMax );
		}
		return bdv;
	}

	private static void updateBounds( final double[] min, final double[] max, final List< RealPoint > pts )
	{
		for ( final RealPoint p : pts )
			for ( int d = 0; d < 3; ++d )
			{
				final double v = p.getDoublePosition( d );
				if ( v < min[ d ] ) min[ d ] = v;
				if ( v > max[ d ] ) max[ d ] = v;
			}
	}

	@SuppressWarnings( { "unchecked", "rawtypes" } )
	private static < T > Source< T > toSource( final BdvStackSource< ? > s )
	{
		return ( Source ) s.getSources().get( 0 ).getSpimSource();
	}

	public static void main( final String... args )
	{
		new CommandLine( new OverlayLandmarks() ).execute( args );
	}
}
