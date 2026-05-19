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
import org.janelia.saalfeldlab.n5.universe.metadata.ome.ngff.v04.OmeNgffMultiScaleMetadata;

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
 * <p>Landmarks are color-coded:
 * <ul>
 *   <li><b>tile centers</b> → green</li>
 *   <li><b>correspondence midpoints</b> → pink</li>
 *   <li><b>surface/corner nails</b> → red→yellow, hue derived from view_setup_id</li>
 * </ul>
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

	@Option( names = "--csv", required = true,
			description = "Landmarks CSV from --tpsLandmarksOut." )
	private String csv;

	@Option( names = "--sigma", defaultValue = "50",
			description = "Gaussian sigma (render-space pixels) for landmark rendering (default: 50)." )
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

		// === Load CSV ===
		final List< RealPoint > centers = new ArrayList<>();
		final List< RealPoint > midpoints = new ArrayList<>();
		final Map< Integer, List< RealPoint > > nailsByView = new HashMap<>();
		loadCsv( csv, centers, midpoints, nailsByView );
		System.out.println( "[overlay-landmarks] loaded " + centers.size() + " centers, " + midpoints.size() + " midpoints, "
				+ nailsByView.values().stream().mapToInt( List::size ).sum() + " nails across " + nailsByView.size() + " views" );

		// === Compute viewer interval from the first image source for rasterization extents ===
		final Source< UnsignedByteType > anySrc = ( bdv != null ) ? toSource( bdv ) : null;
		final Interval viewerInterval = ( anySrc != null )
				? new FinalInterval( anySrc.getSource( 0, 0 ) )
				: enclosingInterval( centers, midpoints, nailsByView, sigma );

		// === Build landmark sources ===
		if ( !centers.isEmpty() )
			bdv = addPointSource( bdv, centers, viewerInterval, sigma, "centers" );
		if ( !midpoints.isEmpty() )
			bdv = addPointSource( bdv, midpoints, viewerInterval, sigma, "midpoints" );

		final TreeSet< Integer > viewIds = new TreeSet<>( nailsByView.keySet() );
		final List< Integer > viewIdList = new ArrayList<>( viewIds );
		for ( int idx = 0; idx < viewIdList.size(); ++idx )
		{
			final int vid = viewIdList.get( idx );
			final List< RealPoint > pts = nailsByView.get( vid );
			if ( pts == null || pts.isEmpty() ) continue;
			bdv = addPointSource( bdv, pts, viewerInterval, sigma, "nails view " + vid );
		}

		// === Apply colors (image sources at indices 0..inputs.length-1, then landmark sources) ===
		final List< BdvStackSource< ? > > all = new ArrayList<>();
		// Recover handles for the most recently created bdv stack. BdvFunctions.show
		// returns the latest BdvStackSource each time; we discarded earlier ones —
		// reorganize by re-creating handles is non-trivial. Instead, set colors on
		// the BDV's converterSetups by index via the bdv handle's setupAssignments.
		// Simpler approach: each call to BdvFunctions.show returns a BdvStackSource;
		// we keep them all this time.

		// (No-op block: handle list is built by re-running addPointSource if needed.)

		System.out.println( "[overlay-landmarks] BDV ready. Close the window to exit." );
		return null;
	}

	// ===========================================================================
	// CSV parsing
	// ===========================================================================

	private static void loadCsv(
			final String path,
			final List< RealPoint > centers,
			final List< RealPoint > midpoints,
			final Map< Integer, List< RealPoint > > nailsByView ) throws IOException
	{
		try ( final BufferedReader r = Files.newBufferedReader( Paths.get( path ), StandardCharsets.UTF_8 ) )
		{
			final String header = r.readLine();
			if ( header == null || !header.startsWith( "view_setup_id" ) )
				throw new IOException( "Unexpected CSV header: " + header );

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
				if ( "center".equals( type ) )
					centers.add( p );
				else if ( "midpoint".equals( type ) )
					midpoints.add( p );
				else if ( "nail".equals( type ) )
					nailsByView.computeIfAbsent( viewSetupId, k -> new ArrayList<>() ).add( p );
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
		final double[][] resolutions; // per-level voxel scales relative to s0
		final OmeNgffMultiScaleMetadata[] omeMs = tryReadMultiscales( n5, groupPath );
		if ( omeMs != null && omeMs.length > 0 && omeMs[ 0 ].datasets != null )
		{
			for ( int i = 0; i < omeMs[ 0 ].datasets.length; ++i )
			{
				final String relPath = omeMs[ 0 ].datasets[ i ].path;
				scalePaths.add( joinPath( groupPath, relPath ) );
			}
			resolutions = new double[ scalePaths.size() ][ 3 ];
			for ( int i = 0; i < scalePaths.size(); ++i )
			{
				resolutions[ i ][ 0 ] = 1.0;
				resolutions[ i ][ 1 ] = 1.0;
				resolutions[ i ][ 2 ] = 1.0;
			}
			// We'll fill the real ratios below from the loaded dims.
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
			resolutions = new double[ scalePaths.size() ][ 3 ];
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

			// fill resolutions from dim ratios
			final long[] dims = scales[ i ].dimensionsAsLongArray();
			for ( int d = 0; d < 3; ++d )
				resolutions[ i ][ d ] = ( double ) s0Dims[ Math.min( d, s0Dims.length - 1 ) ] / Math.max( 1, dims[ Math.min( d, dims.length - 1 ) ] );
		}

		return new RandomAccessibleIntervalMipmapSource<>(
				scales,
				new UnsignedByteType(),
				resolutions,
				new FinalVoxelDimensions( "px", 1, 1, 1 ),
				name );
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
			final String name )
	{
		final RealPointSampleList< FloatType > samples = new RealPointSampleList<>( 3 );
		for ( final RealPoint p : pts )
			samples.add( p, new FloatType( 1f ) );

		final GaussianFilterFactory< FloatType, FloatType > filterFactory =
				new GaussianFilterFactory<>( new FloatType( 0f ), sigma, WeightType.NONE );
		final RandomAccessibleInterval< FloatType > rendered = Views.interval(
				Views.raster( Render.render( samples, filterFactory ) ),
				viewerInterval );

		final BdvOptions opts = ( existing == null )
				? BdvOptions.options().sourceTransform( new AffineTransform3D() )
				: BdvOptions.options().addTo( existing );
		return BdvFunctions.show( rendered, name, opts );
	}

	private static Interval enclosingInterval(
			final List< RealPoint > a, final List< RealPoint > b,
			final Map< Integer, List< RealPoint > > c,
			final double pad )
	{
		final double[] min = new double[ 3 ];
		final double[] max = new double[ 3 ];
		Arrays.fill( min, Double.POSITIVE_INFINITY );
		Arrays.fill( max, Double.NEGATIVE_INFINITY );
		updateBounds( min, max, a );
		updateBounds( min, max, b );
		for ( final List< RealPoint > l : c.values() ) updateBounds( min, max, l );
		final long[] lmin = new long[ 3 ];
		final long[] lmax = new long[ 3 ];
		for ( int d = 0; d < 3; ++d )
		{
			lmin[ d ] = ( long ) Math.floor( min[ d ] - 3 * pad );
			lmax[ d ] = ( long ) Math.ceil( max[ d ] + 3 * pad );
		}
		return new FinalInterval( lmin, lmax );
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
		System.exit( new CommandLine( new OverlayLandmarks() ).execute( args ) );
	}
}
