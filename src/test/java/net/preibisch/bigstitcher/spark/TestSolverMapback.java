package net.preibisch.bigstitcher.spark;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.util.ValuePair;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.AllToAll;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.PairwiseSetup;
import net.preibisch.mvrecon.SimulateUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.tests.TestInterestPointDetection;
import picocli.CommandLine;

/**
 * Rigid solve + rigid mapback onto view (0,0) must leave view (0,0)'s registration exactly where it was
 * (the mapback is fitted to cancel that view's solved model), while the other views do move.
 */
public class TestSolverMapback
{
	@TempDir
	Path tempDir;

	@Test
	public void testRigidMapbackPinsReferenceView() throws Exception
	{
		System.setProperty( "spark.master", "local[*]" );

		final File xmlFile = tempDir.resolve( "dataset.xml" ).toFile();
		final SpimData2 simulated = SimulateUtil.setUp();
		new XmlIoSpimData2().save( simulated, xmlFile.toURI() ); // sets basePath for interest points
		TestInterestPointDetection.testDoG( simulated, "beads" ); // in-process DoG (Spark detection finds 0 peaks on this data right now)
		new XmlIoSpimData2().save( simulated, xmlFile.toURI() );
		final String xml = xmlFile.getAbsolutePath();

		run( new SparkGeometricDescriptorMatching(), "-x", xml, "-l", "beads", "-m", "FAST_ROTATION", "--localSparkBindAddress" );

		final SpimData2 before = new XmlIoSpimData2().load( xmlFile.toURI() );
		final ViewId ref = new ViewId( 0, 0 ), other = new ViewId( 0, 1 );
		final AffineTransform3D refBefore = before.getViewRegistrations().getViewRegistration( ref ).getModel().copy();
		final AffineTransform3D otherBefore = before.getViewRegistrations().getViewRegistration( other ).getModel().copy();

		run( new Solver(), "-x", xml, "-s", "IP", "-l", "beads", "-tm", "RIGID", "-rm", "NONE",
				"--disableFixedViews", "--enableMapbackViews", "--mapbackViews", "0,0", "--mapbackModel", "RIGID" );

		final SpimData2 after = new XmlIoSpimData2().load( xmlFile.toURI() );
		final AffineTransform3D refAfter = after.getViewRegistrations().getViewRegistration( ref ).getModel();
		final AffineTransform3D otherAfter = after.getViewRegistrations().getViewRegistration( other ).getModel();

		assertEquals( before.getViewRegistrations().getViewRegistration( ref ).getTransformList().size() + 1,
				after.getViewRegistrations().getViewRegistration( ref ).getTransformList().size(), "solver should add one transform" );
		assertArrayEquals( refBefore.getRowPackedCopy(), refAfter.getRowPackedCopy(), 1e-3, "mapback view must not move" );
		assertFalse( close( otherBefore.getRowPackedCopy(), otherAfter.getRowPackedCopy(), 1e-3 ), "other views must have been registered" );
	}

	@Test
	public void testPerSubsetAssignment()
	{
		// two disconnected islands: {0,1} and {2,3}
		final List< ViewId > views = new ArrayList<>();
		for ( int i = 0; i < 4; ++i )
			views.add( new ViewId( 0, i ) );

		final PairwiseSetup< ViewId > setup = new AllToAll<>( views, new HashSet<>() );
		setup.setPairs( List.of( new ValuePair<>( views.get( 0 ), views.get( 1 ) ), new ValuePair<>( views.get( 2 ), views.get( 3 ) ) ) );
		setup.detectSubsets();
		assertEquals( 2, setup.getSubsets().size() );

		assertEquals( Set.of( views.get( 0 ), views.get( 2 ) ), Solver.assembleFixed( setup.getSubsets() ) );

		final HashMap< ViewId, ViewId > auto = Solver.assembleMapBack( setup.getSubsets(), null );
		assertEquals( views.get( 0 ), auto.get( views.get( 1 ) ) );
		assertEquals( views.get( 2 ), auto.get( views.get( 3 ) ) );

		// user picks (0,3) for the second island; first island falls back to its first view
		final HashMap< ViewId, ViewId > user = Solver.assembleMapBack( setup.getSubsets(), new ArrayList<>( List.of( views.get( 3 ) ) ) );
		assertEquals( views.get( 0 ), user.get( views.get( 1 ) ) );
		assertEquals( views.get( 3 ), user.get( views.get( 2 ) ) );
		assertEquals( views.get( 3 ), user.get( views.get( 3 ) ) );
	}

	private static void run( final Object cmd, final String... args )
	{
		final int exit = new CommandLine( cmd ).execute( args );
		if ( exit != 0 )
			throw new RuntimeException( cmd.getClass().getSimpleName() + " failed with exit code " + exit );
	}

	private static boolean close( final double[] a, final double[] b, final double eps )
	{
		for ( int i = 0; i < a.length; ++i )
			if ( Math.abs( a[ i ] - b[ i ] ) > eps )
				return false;
		return true;
	}
}
