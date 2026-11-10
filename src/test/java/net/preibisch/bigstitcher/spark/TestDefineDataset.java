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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import ij.ImagePlus;
import ij.ImageStack;
import ij.io.FileSaver;
import ij.measure.Calibration;
import ij.process.ShortProcessor;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.sequence.Angle;
import mpicbg.spim.data.sequence.Channel;
import mpicbg.spim.data.sequence.Illumination;
import mpicbg.spim.data.sequence.Tile;
import mpicbg.spim.data.sequence.ViewSetup;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.type.numeric.RealType;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import picocli.CommandLine;

/**
 * {@code define-dataset}: ImageJ TIFF stacks written with known pixel values and calibration become a BigStitcher
 * XML whose view setups, voxel sizes, attributes and calibration transforms match, and whose Bio-Formats image
 * loader reads the pixels back.
 */
public class TestDefineDataset
{
	@TempDir
	Path tempDir;

	/** pixel value of the synthetic stacks: distinct per file, channel, slice and position */
	static int value( final int fileOffset, final int c, final int x, final int y, final int z, final int w )
	{
		return fileOffset + c * 5000 + z * 100 + y * w + x;
	}

	/** writes a uint16 ImageJ TIFF (a hyperstack for channels > 1) with the given calibration */
	static File writeStack( final Path dir, final String name, final int w, final int h, final int d, final int channels,
			final double pw, final double ph, final double pd, final int fileOffset )
	{
		final ImageStack stack = new ImageStack( w, h );
		for ( int z = 0; z < d; ++z )
			for ( int c = 0; c < channels; ++c )
			{
				final ShortProcessor sp = new ShortProcessor( w, h );
				for ( int y = 0; y < h; ++y )
					for ( int x = 0; x < w; ++x )
						sp.set( x, y, value( fileOffset, c, x, y, z, w ) );
				stack.addSlice( sp );
			}
		final ImagePlus imp = new ImagePlus( name, stack );
		imp.setDimensions( channels, d, 1 );
		if ( channels > 1 )
			imp.setOpenAsHyperStack( true );
		final Calibration cal = imp.getCalibration();
		cal.pixelWidth = pw;
		cal.pixelHeight = ph;
		cal.pixelDepth = pd;
		cal.setUnit( "um" );
		final File file = dir.resolve( name + ".tif" ).toFile();
		assertTrue( new FileSaver( imp ).saveAsTiff( file.getAbsolutePath() ), "writing " + file );
		return file;
	}

	int run( final String... args )
	{
		return new CommandLine( new DefineDataset() ).execute( args );
	}

	SpimData2 load( final File xml ) throws Exception
	{
		return new XmlIoSpimData2().load( xml.toURI() );
	}

	static ViewSetup setup( final SpimData2 d, final int id )
	{
		return d.getSequenceDescription().getViewSetups().get( id );
	}

	static AffineGet calibration( final SpimData2 d, final int setupId )
	{
		final ViewRegistration vr = d.getViewRegistrations().getViewRegistration( 0, setupId );
		return vr.getTransformList().get( vr.getTransformList().size() - 1 ).asAffine3D();
	}

	static double pixel( final SpimData2 d, final int setupId, final int x, final int y, final int z )
	{
		final RandomAccessibleInterval< ? extends RealType< ? > > img =
				( RandomAccessibleInterval< ? extends RealType< ? > > ) d.getSequenceDescription().getImgLoader().getSetupImgLoader( setupId ).getImage( 0 );
		final RandomAccess< ? extends RealType< ? > > ra = img.randomAccess();
		ra.setPosition( new long[] { x, y, z } );
		return ra.get().getRealDouble();
	}

	@Test
	public void threeViewsWithGivenVoxelSizes() throws Exception
	{
		final File a = writeStack( tempDir, "a", 32, 16, 8, 1, 0.5, 1.0, 2.0, 100 );
		final File b = writeStack( tempDir, "b", 24, 12, 6, 1, 0.5, 1.0, 2.0, 20000 );
		final File c = writeStack( tempDir, "c", 20, 20, 10, 1, 0.5, 1.0, 2.0, 40000 );
		final File xml = tempDir.resolve( "dataset.xml" ).toFile();

		assertEquals( 0, run( "-x", xml.getAbsolutePath(), "-i", a.getAbsolutePath(), "-i", b.getAbsolutePath(), "-i", c.getAbsolutePath(),
				"--voxelSize", "0.5,1.0,2.0", "--voxelSize", "1,1,1", "--voxelSize", "0.25,0.25,0.5", "--unit", "um" ) );
		assertTrue( xml.exists() );

		final String text = Files.readString( xml.toPath() );
		assertTrue( text.contains( "format=\"spimreconstruction.filemap2\"" ), "Bio-Formats file-list loader" );
		assertTrue( text.contains( ">a.tif<" ) && text.contains( ">c.tif<" ), "relative file paths" );

		final SpimData2 d = load( xml );
		assertEquals( 3, d.getSequenceDescription().getViewSetupsOrdered().size() );
		assertEquals( 1, d.getSequenceDescription().getTimePoints().size() );

		final long[][] sizes = { { 32, 16, 8 }, { 24, 12, 6 }, { 20, 20, 10 } };
		final double[][] voxels = { { 0.5, 1.0, 2.0 }, { 1, 1, 1 }, { 0.25, 0.25, 0.5 } };
		for ( int s = 0; s < 3; ++s )
		{
			final ViewSetup vs = setup( d, s );
			assertEquals( s, vs.getAttribute( Angle.class ).getId(), "angle = image index" );
			assertEquals( 0, vs.getAttribute( Tile.class ).getId() );
			assertEquals( 0, vs.getAttribute( Illumination.class ).getId() );
			assertEquals( 0, vs.getAttribute( Channel.class ).getId() );
			for ( int k = 0; k < 3; ++k )
			{
				assertEquals( sizes[ s ][ k ], vs.getSize().dimension( k ), "size of setup " + s );
				assertEquals( voxels[ s ][ k ], vs.getVoxelSize().dimension( k ), 1e-9, "voxel size of setup " + s );
			}
			assertEquals( "um", vs.getVoxelSize().unit() );
		}

		// calibration = voxel size / smallest voxel size (0.25)
		final AffineGet c0 = calibration( d, 0 );
		assertEquals( 2.0, c0.get( 0, 0 ), 1e-9 );
		assertEquals( 4.0, c0.get( 1, 1 ), 1e-9 );
		assertEquals( 8.0, c0.get( 2, 2 ), 1e-9 );
		assertEquals( 2.0, calibration( d, 2 ).get( 2, 2 ), 1e-9 );
		assertEquals( 1, d.getViewRegistrations().getViewRegistration( 0, 1 ).getTransformList().size(), "only the calibration" );

		// the loader reads the written pixels
		assertEquals( value( 20000, 0, 3, 2, 4, 24 ), pixel( d, 1, 3, 2, 4 ), 0 );
		assertEquals( value( 40000, 0, 19, 7, 9, 20 ), pixel( d, 2, 19, 7, 9 ), 0 );
		assertEquals( value( 100, 0, 0, 0, 0, 32 ), pixel( d, 0, 0, 0, 0 ), 0 );
	}

	@Test
	public void voxelSizesFromMetadataAndMultiChannel() throws Exception
	{
		final File two = writeStack( tempDir, "two", 16, 12, 5, 2, 0.5, 1.0, 2.0, 100 );
		final File one = writeStack( tempDir, "one", 10, 10, 4, 1, 0.25, 0.25, 0.75, 30000 );
		final File xml = tempDir.resolve( "dataset.xml" ).toFile();

		assertEquals( 0, run( "-x", xml.getAbsolutePath(), "-i", two.getAbsolutePath(), "-i", one.getAbsolutePath(), "--tile", "3,4", "--angle", "7" ) );

		final SpimData2 d = load( xml );
		assertEquals( 3, d.getSequenceDescription().getViewSetupsOrdered().size(), "two channels + one" );

		// setups 0 and 1: the two channels of the first file, setup 2: the second file
		assertEquals( 0, setup( d, 0 ).getAttribute( Channel.class ).getId() );
		assertEquals( 1, setup( d, 1 ).getAttribute( Channel.class ).getId() );
		assertEquals( 0, setup( d, 2 ).getAttribute( Channel.class ).getId() );
		assertEquals( 3, setup( d, 0 ).getAttribute( Tile.class ).getId() );
		assertEquals( 3, setup( d, 1 ).getAttribute( Tile.class ).getId() );
		assertEquals( 4, setup( d, 2 ).getAttribute( Tile.class ).getId() );
		for ( int s = 0; s < 3; ++s )
			assertEquals( 7, setup( d, s ).getAttribute( Angle.class ).getId(), "one angle for all" );

		// voxel sizes from the ImageJ calibration
		assertEquals( 0.5, setup( d, 0 ).getVoxelSize().dimension( 0 ), 1e-9 );
		assertEquals( 1.0, setup( d, 0 ).getVoxelSize().dimension( 1 ), 1e-9 );
		assertEquals( 2.0, setup( d, 0 ).getVoxelSize().dimension( 2 ), 1e-9 );
		assertEquals( 0.75, setup( d, 2 ).getVoxelSize().dimension( 2 ), 1e-9 );
		assertEquals( 2, setup( d, 2 ).getVoxelSize().dimension( 2 ) / setup( d, 2 ).getVoxelSize().dimension( 0 ) / 1.5, 1e-9 );

		// calibration of the finer stack: 0.25 / 0.25 = 1 in x, 0.75 / 0.25 = 3 in z
		assertEquals( 1.0, calibration( d, 2 ).get( 0, 0 ), 1e-9 );
		assertEquals( 3.0, calibration( d, 2 ).get( 2, 2 ), 1e-9 );

		// channel 1 of the hyperstack is read as its own view
		assertEquals( value( 100, 1, 5, 6, 3, 16 ), pixel( d, 1, 5, 6, 3 ), 0 );
		assertEquals( value( 100, 0, 5, 6, 3, 16 ), pixel( d, 0, 5, 6, 3 ), 0 );
		assertEquals( value( 30000, 0, 9, 9, 3, 10 ), pixel( d, 2, 9, 9, 3 ), 0 );
	}

	@Test
	public void refusalsWriteNothing() throws Exception
	{
		final File a = writeStack( tempDir, "a", 8, 8, 2, 1, 1, 1, 1, 0 );
		final File b = writeStack( tempDir, "b", 8, 8, 2, 1, 1, 1, 1, 0 );
		final File xml = tempDir.resolve( "dataset.xml" ).toFile();

		// the same attributes for two views
		assertEquals( 0, run( "-x", xml.getAbsolutePath(), "-i", a.getAbsolutePath(), "-i", b.getAbsolutePath(), "--angle", "0" ) );
		assertFalse( xml.exists(), "duplicate attributes refused" );

		// wrong number of per-image values
		assertEquals( 0, run( "-x", xml.getAbsolutePath(), "-i", a.getAbsolutePath(), "-i", b.getAbsolutePath(), "--tile", "0,1,2" ) );
		assertFalse( xml.exists(), "three tile ids for two images refused" );

		// dry run
		assertEquals( 0, run( "-x", xml.getAbsolutePath(), "-i", a.getAbsolutePath(), "-i", b.getAbsolutePath(), "--dryRun" ) );
		assertFalse( xml.exists(), "dry run writes nothing" );

		// a missing image
		assertEquals( 0, run( "-x", xml.getAbsolutePath(), "-i", a.getAbsolutePath(), "-i", tempDir.resolve( "missing.tif" ).toString() ) );
		assertFalse( xml.exists(), "missing image refused" );

		// written, then protected against overwriting, unless asked
		assertEquals( 0, run( "-x", xml.getAbsolutePath(), "-i", a.getAbsolutePath(), "-i", b.getAbsolutePath() ) );
		assertTrue( xml.exists() );
		final String first = Files.readString( xml.toPath() );
		assertEquals( 0, run( "-x", xml.getAbsolutePath(), "-i", a.getAbsolutePath(), "--voxelSize", "2,2,2" ) );
		assertEquals( first, Files.readString( xml.toPath() ), "existing XML untouched without --overwrite" );
		assertEquals( 0, run( "-x", xml.getAbsolutePath(), "-i", a.getAbsolutePath(), "--voxelSize", "2,2,2", "--overwrite" ) );
		final SpimData2 d = load( xml );
		assertEquals( 1, d.getSequenceDescription().getViewSetupsOrdered().size(), "replaced with --overwrite" );
		assertEquals( 2.0, setup( d, 0 ).getVoxelSize().dimension( 2 ), 1e-9 );
	}

	@Test
	public void perImageHelpers()
	{
		assertEquals( List.of( 0, 0, 0 ), DefineDataset.perImage( null, 3, "--tile", 0 ) );
		assertEquals( null, DefineDataset.perImage( null, 3, "--angle", null ) );
		assertEquals( List.of( 5, 5 ), DefineDataset.perImage( List.of( 5 ), 2, "--angle", null ) );
		assertEquals( List.of( 1, 2 ), DefineDataset.perImage( List.of( 1, 2 ), 2, "--angle", null ) );
		final List< double[] > v = DefineDataset.perImageVoxelSize( List.of( "0.5, 1,2" ), 2 );
		assertEquals( 2, v.size() );
		assertEquals( 1.0, v.get( 1 )[ 1 ], 0 );
		final List< Integer > bad = new ArrayList<>( List.of( 1, 2, 3 ) );
		boolean thrown = false;
		try { DefineDataset.perImage( bad, 2, "--tile", 0 ); } catch ( final IllegalArgumentException e ) { thrown = true; }
		assertTrue( thrown );
		thrown = false;
		try { DefineDataset.perImageVoxelSize( List.of( "1,2" ), 1 ); } catch ( final IllegalArgumentException e ) { thrown = true; }
		assertTrue( thrown );
	}
}
