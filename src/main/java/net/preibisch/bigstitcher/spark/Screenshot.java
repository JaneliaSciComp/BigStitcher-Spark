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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.sequence.TimePoint;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import mpicbg.spim.data.sequence.ViewSetup;
import net.imglib2.Dimensions;
import net.imglib2.FinalRealInterval;
import net.imglib2.RealInterval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.render.Camera;
import net.preibisch.bigstitcher.spark.render.Camera.Plane;
import net.preibisch.bigstitcher.spark.render.HeadlessBdvRenderer;
import net.preibisch.bigstitcher.spark.render.HeadlessBdvRenderer.Frame;
import net.preibisch.bigstitcher.spark.render.ScreenshotManifest;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.ViewSetupUtils;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.overlap.SimpleBoundingBoxOverlap;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.ColorStream;

/**
 * Headless BigDataViewer screenshots of a BigStitcher project: the selected views under their CURRENT registrations,
 * composited with per-view colours and display ranges, rendered into PNG files without a window (works with
 * {@code DISPLAY} unset). One image set per view pair (A magenta, B green: aligned structure turns white, a
 * misalignment shows as coloured doubles), an overview of all views, and single views in grey, each in the xy, xz
 * and/or yz plane of the global frame as a slice through the box centre or a maximum projection. Read-only: the XML
 * is never saved. Not a Spark job.
 */
public class Screenshot extends AbstractSelectableViews
{
	private static final long serialVersionUID = 7152035160935241733L;

	@Option(names = { "-o", "--outputDir" }, required = true, description = "directory for the PNG files (created if missing)")
	private String outputDir = null;

	// what to render
	@Option(names = { "--pairs" }, description = "one image set per view pair: 'overlapping' (every pair of selected views whose registered bounding boxes intersect, the OVERLAPPING_ONLY rule of the registration) or explicit setup-id pairs 'A,B;C,D' within the selected timepoint; A magenta, B green (--colors), camera centred on the intersection of the two registered boxes; files pair_tp<T>_s<A>_s<B>_<plane>.png")
	private String pairs = null;

	@Option(names = { "--overview" }, description = "all selected views together, one colour per view (--palette), camera centred on their union box; files overview_<plane>.png")
	private boolean overview = false;

	@Option(names = { "--single" }, description = "every selected view alone, in grey; files view_tp<T>_s<S>_<plane>.png")
	private boolean single = false;

	@Option(names = { "--transform" }, description = "JSON file with viewer transforms as BigStitcher's BDV fly-through saves them (an array of 12-value row-major affines, global -> screen pixels): renders the selected views (--palette colours) once per transform, applied as-is to --width x --height (the transforms depend on the canvas size they were saved for); --planes does not apply; files transform<i>.png")
	private String transform = null;

	// camera
	@Option(names = { "--planes" }, description = "comma-separated planes of the global frame: xy (looking along z, x right, y down, BigDataViewer's default), xz (x right, z down), yz (y right, z down) (default: xy)")
	private String planes = "xy";

	@Option(names = { "--render" }, description = "slice (one plane through the box centre) or mip (maximum intensity projection over --depth) (default: mip)")
	private String render = "mip";

	@Option(names = { "--depth" }, description = "depth of the projection along the viewing axis in global pixels, centred on the box (default: the full box for --pairs and --single, a quarter of it for --overview and --transform)")
	private Double depth = null;

	@Option(names = { "--step" }, description = "step of the projection along the viewing axis in global pixels (default: max(one voxel of the shown views along that axis, one screen pixel, depth / 256), i.e. at most 257 frames)")
	private Double step = null;

	@Option(names = { "--width" }, description = "image width in pixels (default: 1024)")
	private int width = 1024;

	@Option(names = { "--height" }, description = "image height in pixels (default: from the aspect of the box, 16..4096; for --transform: the width)")
	private Integer height = null;

	@Option(names = { "--margin" }, description = "empty margin around the box as a fraction of its extent on every side (default: 0.1)")
	private double margin = 0.1;

	@Option(names = { "--maxScale" }, description = "largest magnification in screen pixels per global pixel; a small box (e.g. the overlap strip of two tiles) is drawn at this scale into a smaller image instead of being blown up to --width (default: 4)")
	private double maxScale = 4;

	@Option(names = { "--json" }, arity = "0..1", fallbackValue = "", description = "write a manifest of every image (file, kind, views, plane, render, depth, box, viewer transform, size, scale, colours, ranges); without a path: <outputDir>/screenshots.json")
	private String json = null;

	// display
	@Option(names = { "--min" }, description = "display range minimum of every view (with --max)")
	private Double min = null;

	@Option(names = { "--max" }, description = "display range maximum of every view (with --min)")
	private Double max = null;

	@Option(names = { "--range" }, description = "display range of one setup as SETUP=MIN:MAX, repeatable; overrides --min/--max and --auto for that setup")
	private String[] range = null;

	@Option(names = { "--auto" }, description = "display ranges from the 0.1 / 99.9 percentiles of each view, sampled at a coarse resolution level; for a pair image the percentiles are taken inside the pair's overlap box (the default when neither --min/--max nor --range is given)")
	private boolean auto = false;

	@Option(names = { "--colors" }, description = "the two colours of --pairs (A,B): red, green, blue, cyan, magenta, yellow, orange, white, grey or hex rrggbb (default: magenta,green)")
	private String colors = "magenta,green";

	@Option(names = { "--palette" }, description = "colours of the views in --overview and --transform: 'colorstream' (BigStitcher's per-view colours, starting at --paletteStart), 'fixed' (magenta, green, cyan, yellow, orange, blue, cycling from --paletteStart), or a comma-separated list with exactly one colour per selected view (default: colorstream)")
	private String palette = "colorstream";

	@Option(names = { "--paletteStart" }, description = "first index into the colorstream or fixed palette (default: 0)")
	private int paletteStart = 0;

	@Option(names = { "--level" }, description = "pin the resolution level of every view (0 = full resolution; default: chosen by the renderer from the screen scale)")
	private Integer level = null;

	static final String[] FIXED_PALETTE = { "magenta", "green", "cyan", "yellow", "orange", "blue" };

	/** one image to render */
	record Job( String kind, String file, List< Integer > setups, Plane plane, RealInterval box, AffineTransform3D t, int w, int h,
			double zMin, double zMax, double stepScreen, Double depthGlobal, Double stepGlobal, Map< Integer, Integer > colors ) {}

	@Override
	public Void call() throws Exception
	{
		headless();
		this.setRegion();

		// ---- validate everything before touching the dataset
		if ( pairs == null && !overview && !single && transform == null )
			return error( "nothing to render; give at least one of --pairs, --overview, --single, --transform." );

		final List< Plane > planeList = new ArrayList<>();
		final boolean mip;
		final int[] pairColors;
		final Map< Integer, double[] > rangeMap;
		final List< Integer > paletteList; // null for colorstream/fixed
		final List< int[] > explicitPairs; // null for 'overlapping' or no --pairs
		try
		{
			for ( final String s : planes.split( "," ) )
				if ( !planeList.contains( Plane.parse( s ) ) )
					planeList.add( Plane.parse( s ) );
			if ( !render.equalsIgnoreCase( "slice" ) && !render.equalsIgnoreCase( "mip" ) )
				throw new IllegalArgumentException( "--render must be slice or mip, not '" + render + "'" );
			mip = render.equalsIgnoreCase( "mip" );
			if ( width < Camera.MIN_HEIGHT )
				throw new IllegalArgumentException( "--width must be at least " + Camera.MIN_HEIGHT );
			if ( height != null && ( height < Camera.MIN_HEIGHT || height > Camera.MAX_HEIGHT ) )
				throw new IllegalArgumentException( "--height must be in " + Camera.MIN_HEIGHT + ".." + Camera.MAX_HEIGHT );
			if ( margin < 0 )
				throw new IllegalArgumentException( "--margin must be >= 0" );
			if ( !( maxScale > 0 ) )
				throw new IllegalArgumentException( "--maxScale must be > 0" );
			if ( depth != null && !( depth > 0 ) || step != null && !( step > 0 ) )
				throw new IllegalArgumentException( "--depth and --step must be > 0" );
			if ( level != null && level < 0 )
				throw new IllegalArgumentException( "--level must be >= 0" );
			if ( ( min == null ) != ( max == null ) )
				throw new IllegalArgumentException( "--min and --max go together" );
			if ( min != null && !( max > min ) )
				throw new IllegalArgumentException( "--max must be larger than --min" );
			if ( auto && min != null )
				throw new IllegalArgumentException( "--auto and --min/--max exclude each other" );
			rangeMap = parseRanges( range );
			final String[] c = colors.split( "," );
			if ( c.length != 2 )
				throw new IllegalArgumentException( "--colors needs two colours, e.g. magenta,green" );
			pairColors = new int[] { parseColor( c[ 0 ] ), parseColor( c[ 1 ] ) };
			if ( palette.equalsIgnoreCase( "colorstream" ) || palette.equalsIgnoreCase( "fixed" ) )
				paletteList = null;
			else
			{
				paletteList = new ArrayList<>();
				for ( final String s : palette.split( "," ) )
					paletteList.add( parseColor( s ) );
			}
			if ( paletteStart < 0 )
				throw new IllegalArgumentException( "--paletteStart must be >= 0" );
			explicitPairs = parsePairs( pairs );
			if ( transform != null && !new File( transform ).isFile() )
				throw new IllegalArgumentException( "--transform file " + transform + " does not exist" );
		}
		catch ( final IllegalArgumentException e )
		{
			return error( e.getMessage() );
		}

		// ---- the dataset and the views
		final SpimData2 data = this.loadSpimData2();
		if ( data == null )
			return null;

		final ArrayList< ViewId > selected;
		try
		{
			selected = this.loadViewIds( data );
		}
		catch ( final IllegalArgumentException e )
		{
			return error( e.getMessage() );
		}

		final List< TimePoint > timepoints = data.getSequenceDescription().getTimePoints().getTimePointsOrdered();
		final int tpId;
		if ( timepointIds == null && vi == null )
		{
			tpId = timepoints.get( 0 ).getId();
			selected.removeIf( v -> v.getTimePointId() != tpId );
		}
		else
		{
			final TreeSet< Integer > tps = new TreeSet<>();
			for ( final ViewId v : selected )
				tps.add( v.getTimePointId() );
			if ( tps.size() != 1 )
				return error( "screenshot renders one timepoint per call, the selection spans timepoints " + tps + "; select one with --timepointId or -vi." );
			tpId = tps.first();
		}
		int tpIndex = -1;
		for ( int i = 0; i < timepoints.size(); ++i )
			if ( timepoints.get( i ).getId() == tpId )
				tpIndex = i;
		if ( tpIndex < 0 || selected.isEmpty() )
			return error( "no views at timepoint " + tpId );

		final List< Integer > setups = new ArrayList<>();
		for ( final ViewId v : selected )
			setups.add( v.getViewSetupId() );
		setups.sort( null );

		if ( paletteList != null && ( overview || transform != null ) && paletteList.size() != setups.size() )
			return error( "--palette lists " + paletteList.size() + " colours for " + setups.size() + " selected views; give exactly one per view, or 'colorstream' / 'fixed'." );

		final Map< Integer, Integer > viewColors = new LinkedHashMap<>();
		for ( int i = 0; i < setups.size(); ++i )
			viewColors.put( setups.get( i ), paletteList != null ? paletteList.get( i )
					: palette.equalsIgnoreCase( "fixed" ) ? parseColor( FIXED_PALETTE[ ( paletteStart + i ) % FIXED_PALETTE.length ] )
					: ColorStream.get( paletteStart + i ) );

		// ---- registered bounding boxes
		final Map< Integer, RealInterval > boxes = new LinkedHashMap<>();
		for ( final int s : setups )
			boxes.put( s, registeredBox( data, new ViewId( tpId, s ) ) );
		RealInterval union = null;
		for ( final RealInterval b : boxes.values() )
			union = union == null ? b : Intervals.union( union, b );

		// ---- the jobs
		final List< Job > jobs = new ArrayList<>();
		final String tp = "tp" + tpId;

		if ( pairs != null )
		{
			final List< int[] > pairList = new ArrayList<>();
			if ( explicitPairs == null )
			{
				final SimpleBoundingBoxOverlap< ViewId > overlap = new SimpleBoundingBoxOverlap<>( data );
				for ( int i = 0; i < setups.size(); ++i )
					for ( int j = i + 1; j < setups.size(); ++j )
						if ( overlap.overlaps( new ViewId( tpId, setups.get( i ) ), new ViewId( tpId, setups.get( j ) ) ) )
							pairList.add( new int[] { setups.get( i ), setups.get( j ) } );
				System.out.println( pairList.size() + " overlapping pair(s) among the " + setups.size() + " selected views." );
			}
			else
			{
				for ( final int[] p : explicitPairs )
					for ( final int s : p )
						if ( !setups.contains( s ) )
							return error( "--pairs: setup " + s + " is not among the selected views " + setups + " of timepoint " + tpId );
				pairList.addAll( explicitPairs );
			}

			for ( final int[] p : pairList )
			{
				final RealInterval box = Intervals.intersect( boxes.get( p[ 0 ] ), boxes.get( p[ 1 ] ) );
				if ( Intervals.isEmpty( box ) )
				{
					System.out.println( "skipping pair " + p[ 0 ] + "," + p[ 1 ] + ": the registered boxes do not intersect." );
					continue;
				}
				final Map< Integer, Integer > c = new LinkedHashMap<>();
				c.put( p[ 0 ], pairColors[ 0 ] );
				c.put( p[ 1 ], pairColors[ 1 ] );
				for ( final Plane plane : planeList )
					jobs.add( job( data, tpId, "pair", "pair_" + tp + "_s" + p[ 0 ] + "_s" + p[ 1 ] + "_" + plane.lower() + ".png", Arrays.asList( p[ 0 ], p[ 1 ] ), plane, box, mip, false, c ) );
			}
		}

		if ( overview )
			for ( final Plane plane : planeList )
				jobs.add( job( data, tpId, "overview", "overview_" + plane.lower() + ".png", setups, plane, union, mip, true, viewColors ) );

		if ( single )
			for ( final int s : setups )
				for ( final Plane plane : planeList )
					jobs.add( job( data, tpId, "single", "view_" + tp + "_s" + s + "_" + plane.lower() + ".png", Arrays.asList( s ), plane, boxes.get( s ), mip, false,
							Map.of( s, ARGBType.rgba( 255, 255, 255, 255 ) ) ) );

		if ( transform != null )
		{
			final List< AffineTransform3D > ts = ScreenshotManifest.loadTransforms( new File( transform ) );
			final int h = height != null ? height : width;
			for ( int i = 0; i < ts.size(); ++i )
			{
				final AffineTransform3D t = ts.get( i );
				final double s = Camera.scale( t );
				final double[] zr = Camera.screenZRange( union, t );
				final double depthGlobal = depth != null ? depth : ( zr[ 1 ] - zr[ 0 ] ) / s / 4;
				final double stepGlobal = Camera.mipStepGlobal( depthGlobal, s, step, voxelAlongScreenZ( data, tpId, setups, t ) );
				final double half = mip ? depthGlobal * s / 2 : 0;
				jobs.add( new Job( "transform", "transform" + i + ".png", setups, null, null, t, width, h, -half, half, stepGlobal * s,
						mip ? depthGlobal : null, mip ? stepGlobal : null, viewColors ) );
			}
		}

		if ( jobs.isEmpty() )
			return error( "nothing to render." );

		System.out.println( jobs.size() + " image(s) to render into " + outputDir + ":" );
		for ( final Job j : jobs )
			System.out.println( "\t" + j.file() + ": " + j.kind() + " of setups " + j.setups() + ( j.plane() != null ? ", plane " + j.plane().lower() : "" ) +
					", " + j.w() + " x " + j.h() + ", scale " + String.format( "%.4f", Camera.scale( j.t() ) ) +
					( j.depthGlobal() != null ? String.format( ", mip over %.1f px in steps of %.2f px", j.depthGlobal(), j.stepGlobal() ) : ", slice" ) );

		if ( dryRun )
		{
			System.out.println( "Dry run, nothing rendered." );
			return null;
		}

		// ---- render
		final File dir = new File( outputDir );
		if ( !dir.isDirectory() && !dir.mkdirs() )
			return error( "could not create " + dir );

		final ScreenshotManifest manifest = new ScreenshotManifest();
		manifest.xml = xmlURI.toString();
		manifest.timepointId = tpId;
		manifest.render = mip ? "mip" : "slice";
		manifest.margin = margin;

		try ( final HeadlessBdvRenderer renderer = new HeadlessBdvRenderer( data, Runtime.getRuntime().availableProcessors(), level ) )
		{
			renderer.setTimepointIndex( tpIndex );

			// display ranges: --range > --min/--max > percentiles (of the whole view) > the type's range
			final Map< Integer, double[] > ranges = new LinkedHashMap<>();
			final boolean autoRanges = min == null;
			for ( final int s : setups )
			{
				double[] r = rangeMap.get( s );
				if ( r == null && min != null )
					r = new double[] { min, max };
				if ( r == null )
					r = renderer.estimateDisplayRange( s, tpIndex, 0.001, 0.999, 1_000_000 );
				if ( r == null )
				{
					final Object type = renderer.source( s ).getType();
					r = type instanceof RealType ? new double[] { ( ( RealType< ? > ) type ).getMinValue(), ( ( RealType< ? > ) type ).getMaxValue() } : new double[] { 0, 65535 };
				}
				ranges.put( s, r );
				System.out.println( "display range of setup " + s + ": " + r[ 0 ] + " .. " + r[ 1 ] );
			}

			for ( final Job j : jobs )
			{
				final long t0 = System.currentTimeMillis();
				renderer.setActiveSetups( j.setups() );
				for ( final Map.Entry< Integer, Integer > e : j.colors().entrySet() )
					renderer.setColor( e.getKey(), e.getValue() );

				// a pair is judged on its overlap: automatic ranges come from the percentiles inside the pair's box, so
				// both views show with comparable brightness there whatever the rest of the tile contains
				final Map< Integer, double[] > jobRanges = new LinkedHashMap<>();
				for ( final int s : j.setups() )
				{
					double[] r = ranges.get( s );
					if ( j.kind().equals( "pair" ) && autoRanges && !rangeMap.containsKey( s ) )
					{
						final double[] inBox = renderer.estimateDisplayRange( s, tpIndex, 0.001, 0.999, 1_000_000, j.box() );
						if ( inBox != null )
							r = inBox;
					}
					jobRanges.put( s, r );
					renderer.setDisplayRange( s, r[ 0 ], r[ 1 ] );
				}
				final Frame frame = renderer.renderMip( j.t(), j.w(), j.h(), j.zMin(), j.zMax(), j.stepScreen() );
				final File file = new File( dir, j.file() );
				HeadlessBdvRenderer.writePng( frame.image(), file );
				final long ms = System.currentTimeMillis() - t0;
				final StringBuilder rs = new StringBuilder();
				for ( final Map.Entry< Integer, double[] > e : jobRanges.entrySet() )
					rs.append( rs.length() == 0 ? "" : ", " ).append( e.getKey() ).append( ": " ).append( e.getValue()[ 0 ] ).append( ".." ).append( e.getValue()[ 1 ] );
				System.out.println( "wrote " + file + " (" + j.w() + " x " + j.h() + ", " + frame.frames() + " frame(s), " + ms + " ms, ranges " + rs + ")" );

				final ScreenshotManifest.Entry entry = new ScreenshotManifest.Entry();
				entry.file = j.file();
				entry.kind = j.kind();
				entry.timepointId = tpId;
				entry.setups = j.setups().stream().mapToInt( Integer::intValue ).toArray();
				entry.plane = j.plane() == null ? null : j.plane().lower();
				entry.render = j.depthGlobal() == null ? "slice" : "mip";
				entry.depth = j.depthGlobal();
				entry.step = j.stepGlobal();
				entry.frames = frame.frames();
				if ( j.box() != null )
				{
					entry.boxMin = j.box().minAsDoubleArray();
					entry.boxMax = j.box().maxAsDoubleArray();
				}
				entry.viewerTransform = j.t().getRowPackedCopy();
				entry.width = j.w();
				entry.height = j.h();
				entry.scale = Camera.scale( j.t() );
				entry.renderMillis = ms;
				for ( final int s : j.setups() )
				{
					entry.colors.put( Integer.toString( s ), hex( j.colors().get( s ) ) );
					entry.ranges.put( Integer.toString( s ), jobRanges.get( s ) );
				}
				manifest.images.add( entry );
			}
		}

		if ( json != null )
		{
			final File file = json.isEmpty() ? new File( dir, "screenshots.json" ) : new File( json );
			manifest.save( file );
			System.out.println( "wrote " + file );
		}

		return null;
	}

	/** the camera of one plane image: box fitted into the image, depth centred on the box */
	private Job job( final SpimData2 data, final int tpId, final String kind, final String file, final List< Integer > setups, final Plane plane, final RealInterval box, final boolean mip, final boolean quarterDepth, final Map< Integer, Integer > colors )
	{
		final Camera.Fit fit = Camera.fit( box, plane, width, height, margin, maxScale );
		final double s = fit.scale();
		final double[] zr = Camera.screenZRange( box, fit.t() );
		final double depthGlobal = depth != null ? depth : ( zr[ 1 ] - zr[ 0 ] ) / s / ( quarterDepth ? 4 : 1 );
		final double stepGlobal = Camera.mipStepGlobal( depthGlobal, s, step, voxelAlongScreenZ( data, tpId, setups, fit.t() ) );
		final double half = mip ? depthGlobal * s / 2 : 0;
		return new Job( kind, file, setups, plane, box, fit.t(), fit.width(), fit.height(), -half, half, stepGlobal * s, mip ? depthGlobal : null, mip ? stepGlobal : null, colors );
	}

	/** the smallest voxel extent along the viewing axis over the shown views, in global pixels */
	static double voxelAlongScreenZ( final SpimData2 data, final int tpId, final List< Integer > setups, final AffineTransform3D viewer )
	{
		double min = Double.POSITIVE_INFINITY;
		for ( final int s : setups )
		{
			final ViewRegistration vr = data.getViewRegistrations().getViewRegistration( new ViewId( tpId, s ) );
			vr.updateModel();
			min = Math.min( min, Camera.voxelExtentAlongScreenZ( viewer, vr.getModel() ) );
		}
		return Double.isFinite( min ) ? min : 1.0;
	}

	/** the registered bounding box of a view in the global frame (the size is loaded if the XML has none) */
	static RealInterval registeredBox( final SpimData2 data, final ViewId v )
	{
		final ViewSetup vs = data.getSequenceDescription().getViewSetups().get( v.getViewSetupId() );
		final ViewRegistration vr = data.getViewRegistrations().getViewRegistration( v );
		final RealInterval b = SimpleBoundingBoxOverlap.getBoundingBoxReal( vs, vr );
		if ( b != null )
			return b;
		final ViewDescription vd = data.getSequenceDescription().getViewDescription( v );
		final Dimensions size = ViewSetupUtils.getSizeOrLoad( vs, vd.getTimePoint(), data.getSequenceDescription().getImgLoader() );
		final double[] mn = new double[ 3 ], mx = new double[ 3 ];
		for ( int d = 0; d < 3; ++d )
			mx[ d ] = size.dimension( d ) - 1;
		vr.updateModel();
		return vr.getModel().estimateBounds( new FinalRealInterval( mn, mx ) );
	}

	static Map< Integer, double[] > parseRanges( final String[] range )
	{
		final Map< Integer, double[] > m = new LinkedHashMap<>();
		if ( range == null )
			return m;
		for ( final String r : range )
		{
			final String[] p = r.split( "=" );
			final String[] mm = p.length == 2 ? p[ 1 ].split( ":" ) : new String[ 0 ];
			if ( mm.length != 2 )
				throw new IllegalArgumentException( "--range '" + r + "' is not SETUP=MIN:MAX" );
			try
			{
				final double lo = Double.parseDouble( mm[ 0 ].trim() ), hi = Double.parseDouble( mm[ 1 ].trim() );
				if ( !( hi > lo ) )
					throw new IllegalArgumentException( "--range '" + r + "': MAX must be larger than MIN" );
				m.put( Integer.parseInt( p[ 0 ].trim() ), new double[] { lo, hi } );
			}
			catch ( final NumberFormatException e )
			{
				throw new IllegalArgumentException( "--range '" + r + "' is not SETUP=MIN:MAX" );
			}
		}
		return m;
	}

	/** null for 'overlapping' or no --pairs; otherwise the explicit pairs of setup ids */
	static List< int[] > parsePairs( final String pairs )
	{
		if ( pairs == null || pairs.trim().equalsIgnoreCase( "overlapping" ) )
			return null;
		final List< int[] > list = new ArrayList<>();
		final LinkedHashSet< String > seen = new LinkedHashSet<>();
		for ( final String p : pairs.split( ";" ) )
		{
			final String[] ab = p.split( "," );
			if ( ab.length != 2 )
				throw new IllegalArgumentException( "--pairs must be 'overlapping' or 'A,B;C,D' with setup ids, not '" + pairs + "'" );
			try
			{
				final int a = Integer.parseInt( ab[ 0 ].trim() ), b = Integer.parseInt( ab[ 1 ].trim() );
				if ( a == b )
					throw new IllegalArgumentException( "--pairs: '" + p + "' pairs a view with itself" );
				if ( seen.add( a + "," + b ) )
					list.add( new int[] { a, b } );
			}
			catch ( final NumberFormatException e )
			{
				throw new IllegalArgumentException( "--pairs must be 'overlapping' or 'A,B;C,D' with setup ids, not '" + pairs + "'" );
			}
		}
		return list;
	}

	/** a colour name or hex rrggbb as ARGB with alpha 255 */
	static int parseColor( final String s )
	{
		final String c = s.trim().toLowerCase();
		switch ( c )
		{
		case "red": return ARGBType.rgba( 255, 0, 0, 255 );
		case "green": return ARGBType.rgba( 0, 255, 0, 255 );
		case "blue": return ARGBType.rgba( 0, 0, 255, 255 );
		case "cyan": return ARGBType.rgba( 0, 255, 255, 255 );
		case "magenta": return ARGBType.rgba( 255, 0, 255, 255 );
		case "yellow": return ARGBType.rgba( 255, 255, 0, 255 );
		case "orange": return ARGBType.rgba( 255, 160, 0, 255 );
		case "white": return ARGBType.rgba( 255, 255, 255, 255 );
		case "grey": case "gray": return ARGBType.rgba( 160, 160, 160, 255 );
		default:
			final String hex = c.startsWith( "#" ) ? c.substring( 1 ) : c;
			if ( !hex.matches( "[0-9a-f]{6}" ) )
				throw new IllegalArgumentException( "unknown colour '" + s + "'; use a name (red, green, blue, cyan, magenta, yellow, orange, white, grey) or hex rrggbb" );
			final int rgb = Integer.parseInt( hex, 16 );
			return ARGBType.rgba( ( rgb >> 16 ) & 0xff, ( rgb >> 8 ) & 0xff, rgb & 0xff, 255 );
		}
	}

	static String hex( final int argb )
	{
		return String.format( "#%06x", argb & 0xffffff );
	}

	private static Void error( final String message )
	{
		System.err.println( "ERROR: " + message );
		return null;
	}

	/** no window, no DISPLAY: must be set before any AWT class is loaded */
	static void headless()
	{
		if ( System.getProperty( "java.awt.headless" ) == null )
			System.setProperty( "java.awt.headless", "true" );
	}

	public static void main( final String... args )
	{
		headless();
		System.out.println( Arrays.toString( args ) );
		System.exit( new CommandLine( new Screenshot() ).execute( args ) );
	}
}
