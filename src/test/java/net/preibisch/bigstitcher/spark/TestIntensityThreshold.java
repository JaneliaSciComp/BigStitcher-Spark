package net.preibisch.bigstitcher.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.preibisch.bigstitcher.spark.util.IntensityThresholds;
import net.preibisch.mvrecon.SimulateUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.process.fusion.intensity.Coefficients;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrection;
import picocli.CommandLine;
import util.URITools;

/**
 * --minThreshold LI/OTSU: one threshold per view, persisted next to the matches and
 * carried into the coefficients so fusion can mask on it.
 */
public class TestIntensityThreshold
{
	@TempDir
	Path tempDir;

	private File xmlFile;
	private SpimData2 spimData;

	@BeforeEach
	public void setUp() throws Exception
	{
		System.setProperty( "spark.master", "local[*]" );

		spimData = SimulateUtil.setUp();

		final File simulated = tempDir.resolve( "simulated.xml" ).toFile();
		new XmlIoSpimData2().save( spimData, simulated.toURI() );

		// intensity matching needs a MultiResolutionSetupImgLoader (mvr's TileInfo requires
		// one), which the simulated in-memory loader is not - so resave to N5 first
		xmlFile = tempDir.resolve( "dataset.xml" ).toFile();

		assertEquals( 0, new CommandLine( new SparkResaveN5() ).execute(
				"-x", simulated.getAbsolutePath(),
				"-xo", xmlFile.getAbsolutePath(),
				"-s", "N5",
				"--blockSize", "32,32,32",
				// a single level trips OME-ZARR metadata creation on small data
				"-ds", "1,1,1; 2,2,2",
				"--localSparkBindAddress" ), "resave to N5 should succeed" );

		spimData = new XmlIoSpimData2().load( xmlFile.toURI() );
	}

	/** note the trailing slash: the matches IO resolves file names against this URI */
	private String matchesURI( final String name )
	{
		return tempDir.resolve( name ).toUri().toString() + "/";
	}

	private int match( final String matches, final String minThreshold )
	{
		return new CommandLine( new SparkIntensityMatching() ).execute(
				"-x", xmlFile.getAbsolutePath(),
				"-o", matches,
				"--minThreshold", minThreshold,
				"--method", "HISTOGRAM",
				"--numCoefficients", "1,1,1",
				"--renderScale", "1.0",
				"--localSparkBindAddress" );
	}

	private int solve( final String matches, final File coefficients )
	{
		return new CommandLine( new IntensitySolver() ).execute(
				"-x", xmlFile.getAbsolutePath(),
				"--matchesPath", matches,
				"-o", coefficients.getAbsolutePath(),
				"-s", "N5",
				"--numCoefficients", "1,1,1",
				"--localSparkBindAddress" );
	}

	private double storedThreshold( final File coefficients, final ViewId viewId )
	{
		try ( final N5Reader reader = URITools.instantiateN5Reader( StorageFormat.N5, coefficients.toURI() ) )
		{
			return IntensityCorrection.readCoefficients( reader, "", "intensity", viewId ).threshold();
		}
	}

	@Test
	public void otsuThresholdIsPerViewAndReachesTheCoefficients() throws Exception
	{
		final String matches = matchesURI( "matches_otsu" );
		assertEquals( 0, match( matches, "OTSU" ), "match-intensities --minThreshold OTSU should succeed" );

		final Path thresholdsFile = tempDir.resolve( "matches_otsu" ).resolve( "thresholds.txt" );
		assertTrue( Files.exists( thresholdsFile ), "thresholds.txt should be written next to the matches" );

		final List< String > lines = Files.readAllLines( thresholdsFile );
		System.out.println( "thresholds.txt:\n" + String.join( "\n", lines ) );
		assertTrue( lines.size() >= 2, "at least the two views of one pair need a threshold" );

		final Map< ViewId, Double > thresholds = IntensityThresholds.read( java.net.URI.create( matches ) );
		assertEquals( lines.size(), thresholds.size(), "write/read round trip should preserve every view" );
		thresholds.forEach( ( v, t ) -> assertTrue( t > 0, "threshold for " + v.getViewSetupId() + " should be positive, was " + t ) );

		final File coefficients = tempDir.resolve( "coefficients_otsu.n5" ).toFile();
		assertEquals( 0, solve( matches, coefficients ), "solve-intensities should succeed" );

		thresholds.forEach( ( v, t ) -> assertEquals( t, storedThreshold( coefficients, v ), 1e-9,
				"the coefficients of view " + v.getViewSetupId() + " should carry its threshold" ) );
	}

	/**
	 * Artificial coefficients (dst = 2 * src) with a threshold, so the masking is visible
	 * regardless of what the solve would have found on this dataset.
	 */
	private void writeDoublingCoefficients( final File coefficients, final double threshold )
	{
		final Map< ViewId, Coefficients > map = new HashMap<>();

		for ( final ViewId v : spimData.getSequenceDescription().getViewDescriptions().keySet() )
			map.put( v, new Coefficients( new double[][] { { 2.0 }, { 0.0 } }, 1, 1, 1 ).withThreshold( threshold ) );

		try ( final N5Writer writer = URITools.instantiateN5Writer( StorageFormat.N5, coefficients.toURI() ) )
		{
			IntensityCorrection.writeCoefficients( writer, "", "intensity", map );
		}
	}

	private File fuse( final String name, final File coefficients, final boolean mask )
	{
		final File out = tempDir.resolve( name ).toFile();

		assertEquals( 0, new CommandLine( new CreateFusionContainer() ).execute(
				"-x", xmlFile.getAbsolutePath(),
				"-o", out.getAbsolutePath(),
				"-s", "ZARR2",
				"-d", "UINT16",
				"--preserveAnisotropy",
				"--blockSize", "32,32,32",
				"-ds", "1,1,1",
				"-ds", "2,2,2" ), "CreateFusionContainer should succeed for " + name );

		final List< String > args = new java.util.ArrayList<>( List.of(
				"-o", out.getAbsolutePath(), "-s", "ZARR2", "--localSparkBindAddress" ) );

		if ( coefficients != null )
		{
			args.addAll( List.of( "--intensityN5Path", coefficients.getAbsolutePath(), "--intensityN5Storage", "N5" ) );

			if ( mask )
				args.add( "--intensityMaskBelowThreshold" );
		}

		assertEquals( 0, new CommandLine( new SparkFusion() ).execute( args.toArray( new String[ 0 ] ) ),
				"SparkFusion should succeed for " + name );

		return out;
	}

	private static int valueAt( final File fused, final long[] pos )
	{
		try ( final N5Reader reader = URITools.instantiateN5Reader( StorageFormat.ZARR2, fused.toURI() ) )
		{
			final RandomAccessibleInterval< UnsignedShortType > img = N5Utils.open( reader, "/0" );
			return img.getAt( pos[ 0 ], pos[ 1 ], pos[ 2 ], 0, 0 ).get();
		}
	}

	/** a dim and a bright voxel of the uncorrected fusion, so the assertions don't hardcode coordinates */
	private static long[][] findDimAndBrightVoxel( final File fused )
	{
		try ( final N5Reader reader = URITools.instantiateN5Reader( StorageFormat.ZARR2, fused.toURI() ) )
		{
			final RandomAccessibleInterval< UnsignedShortType > img = N5Utils.open( reader, "/0" );
			final long[] dims = img.dimensionsAsLongArray();

			long[] dim = null, bright = null;

			for ( long z = 0; z < dims[ 2 ] && ( dim == null || bright == null ); ++z )
				for ( long y = 0; y < dims[ 1 ] && ( dim == null || bright == null ); ++y )
					for ( long x = 0; x < dims[ 0 ] && ( dim == null || bright == null ); ++x )
					{
						final int v = img.getAt( x, y, z, 0, 0 ).get();

						if ( dim == null && v > 0 && v < 20 )
							dim = new long[] { x, y, z };
						else if ( bright == null && v > 100 )
							bright = new long[] { x, y, z };
					}

			assertNotNull( dim, "test needs a dim non-zero voxel in the fused image" );
			assertNotNull( bright, "test needs a bright voxel in the fused image" );

			System.out.println( "dim voxel " + java.util.Arrays.toString( dim ) + ", bright voxel " + java.util.Arrays.toString( bright ) );
			return new long[][] { dim, bright };
		}
	}

	@Test
	public void fusionOnlyCorrectsAboveTheThreshold() throws Exception
	{
		final File uncorrected = fuse( "fused_plain.zarr", null, false );
		final long[][] probes = findDimAndBrightVoxel( uncorrected );
		final long[] dim = probes[ 0 ], bright = probes[ 1 ];

		final int dimPlain = valueAt( uncorrected, dim );
		final int brightPlain = valueAt( uncorrected, bright );

		// threshold between the two probes, so the dim one is background and the bright one is not
		final File coefficients = tempDir.resolve( "coefficients_doubling.n5" ).toFile();
		writeDoublingCoefficients( coefficients, ( dimPlain + brightPlain ) / 2.0 );

		final int dimMasked = valueAt( fuse( "fused_masked.zarr", coefficients, true ), dim );
		final int dimAll = valueAt( fuse( "fused_all.zarr", coefficients, false ), dim );
		final int brightMasked = valueAt( fuse( "fused_masked.zarr", coefficients, true ), bright );

		System.out.println( "dim: plain " + dimPlain + ", masked " + dimMasked + ", unmasked " + dimAll );
		System.out.println( "bright: plain " + brightPlain + ", masked " + brightMasked );

		assertEquals( dimPlain, dimMasked, "below the threshold, --intensityMaskBelowThreshold must leave the voxel alone" );
		assertTrue( dimAll > dimPlain, "without the flag the same voxel must be corrected (was " + dimAll + " vs " + dimPlain + ")" );
		assertTrue( brightMasked > brightPlain, "above the threshold the correction must still apply" );
	}

	@Test
	public void fixedThresholdStoresNothing() throws Exception
	{
		final String matches = matchesURI( "matches_fixed" );
		assertEquals( 0, match( matches, "1" ), "match-intensities with a fixed threshold should succeed" );

		assertTrue( !Files.exists( tempDir.resolve( "matches_fixed" ).resolve( "thresholds.txt" ) ),
				"a fixed threshold should not write thresholds.txt" );
		assertNull( IntensityThresholds.read( java.net.URI.create( matches ) ),
				"reading absent thresholds should give null, not throw" );

		final File coefficients = tempDir.resolve( "coefficients_fixed.n5" ).toFile();
		assertEquals( 0, solve( matches, coefficients ), "solve-intensities should succeed" );

		for ( final ViewId v : spimData.getSequenceDescription().getViewDescriptions().keySet() )
			assertTrue( Double.isNaN( storedThreshold( coefficients, v ) ),
					"without thresholds.txt the coefficients must not carry a threshold" );
	}
}
