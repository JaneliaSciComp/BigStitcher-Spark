package net.preibisch.bigstitcher.spark;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;
import net.preibisch.mvrecon.SimulateUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import picocli.CommandLine;

/**
 * {@link SparkResaveN5} on the simulated beads (local Spark): the resaved views equal the originals, a second run into the
 * same container is refused and leaves it intact, {@code --overwriteExistingDatasets} rewrites the views, with
 * {@code --no-preloadComputeBlocks} as well.
 */
public class TestSparkResaveLocal
{
	@TempDir
	Path tempDir;

	@Test
	public void resavesRefusesExistingAndOverwrites() throws Exception
	{
		System.setProperty( "spark.master", "local[*]" );

		final File xmlFile = tempDir.resolve( "dataset.xml" ).toFile();
		final SpimData2 original = SimulateUtil.setUp();
		new XmlIoSpimData2().save( original, xmlFile.toURI() );

		final File xmlOut = tempDir.resolve( "dataset-zarr.xml" ).toFile();
		final Path container = tempDir.resolve( "dataset.ome.zarr" );
		final List< String > base = List.of( "-x", xmlFile.getAbsolutePath(), "-xo", xmlOut.getAbsolutePath(), "-o", container.toString(),
				"--blockSize", "64,64,32", "--blockScale", "2,2,2", "--localSparkBindAddress" );

		final List< ViewId > views = new ArrayList<>();
		for ( final ViewDescription vd : original.getSequenceDescription().getViewDescriptions().values() )
			if ( vd.isPresent() )
				views.add( new ViewId( vd.getTimePointId(), vd.getViewSetupId() ) );
		assertTrue( views.size() >= 2 );

		// first run (preloading by default): the resaved views equal the originals
		final StringBuilder out = new StringBuilder();
		assertEquals( 0, run( base, out ) );
		assertTrue( out.toString().contains( "Preloading compute blocks into memory" ), out.toString() );
		assertTrue( Files.isDirectory( container ) );
		assertSameImages( original, new XmlIoSpimData2().load( xmlOut.toURI() ), views );

		// a second run into the same container is refused before anything is written, the container stays intact
		final long modifiedBefore = newestModification( container );
		Thread.sleep( 20 );
		assertNotEquals( 0, run( base ) );
		assertEquals( modifiedBefore, newestModification( container ) );
		assertSameImages( original, new XmlIoSpimData2().load( xmlOut.toURI() ), views );

		// a dry run reports, deletes nothing
		final List< String > dry = new ArrayList<>( base );
		dry.add( "--overwriteExistingDatasets" );
		dry.add( "--dryRun" );
		assertEquals( 0, run( dry ) );
		assertEquals( modifiedBefore, newestModification( container ) );

		// with the flag the views are deleted and written again, streaming this time
		final List< String > overwrite = new ArrayList<>( base );
		overwrite.add( "--overwriteExistingDatasets" );
		overwrite.add( "--no-preloadComputeBlocks" );
		out.setLength( 0 );
		assertEquals( 0, run( overwrite, out ) );
		assertTrue( out.toString().contains( "Streaming compute blocks from the image loader" ), out.toString() );
		assertTrue( newestModification( container ) > modifiedBefore );
		assertSameImages( original, new XmlIoSpimData2().load( xmlOut.toURI() ), views );
	}

	static int run( final List< String > args )
	{
		return new CommandLine( new SparkResaveN5() ).execute( args.toArray( new String[ 0 ] ) );
	}

	/** as {@link #run(List)}, with what the command prints to System.out appended to {@code out} (and still printed) */
	static int run( final List< String > args, final StringBuilder out )
	{
		final PrintStream original = System.out;
		final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		System.setOut( new PrintStream( buffer, true ) );
		try
		{
			return run( args );
		}
		finally
		{
			System.setOut( original );
			out.append( buffer.toString() );
			original.print( buffer.toString() );
		}
	}

	/** the newest modification time below a directory (files and directories) */
	static long newestModification( final Path dir ) throws Exception
	{
		try ( final var paths = Files.walk( dir ) )
		{
			return paths.mapToLong( p -> p.toFile().lastModified() ).max().orElse( 0 );
		}
	}

	static void assertSameImages( final SpimData2 expected, final SpimData2 actual, final List< ViewId > views )
	{
		for ( final ViewId view : views )
			assertArrayEquals( summary( expected, view ), summary( actual, view ), 1e-3, "view " + view.getViewSetupId() );
	}

	/** dimensions, the sum, the sum of squares and every 9973rd pixel of a view's image (the whole image would be large) */
	@SuppressWarnings( { "unchecked", "rawtypes" } )
	static double[] summary( final SpimData2 data, final ViewId view )
	{
		final RandomAccessibleInterval< RealType > img = (RandomAccessibleInterval< RealType >) data.getSequenceDescription().getImgLoader().getSetupImgLoader( view.getViewSetupId() ).getImage( view.getTimePointId() );
		final List< Double > values = new ArrayList<>();
		for ( int d = 0; d < img.numDimensions(); ++d )
			values.add( (double) img.dimension( d ) );
		double sum = 0, sumSq = 0;
		long i = 0;
		for ( final RealType t : Views.flatIterable( img ) )
		{
			final double v = t.getRealDouble();
			sum += v;
			sumSq += v * v;
			if ( i++ % 9973 == 0 )
				values.add( v );
		}
		values.add( sum );
		values.add( sumSq );
		return values.stream().mapToDouble( Double::doubleValue ).toArray();
	}
}
