/*-
 * #%L
 * Spark-based parallel BigStitcher project.
 * %%
 * Copyright (C) 2021 - 2024 Developers.
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
package net.preibisch.bigstitcher.spark.cloud;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import bdv.ViewerImgLoader;
import ij.ImageJ;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.generic.sequence.BasicImgLoader;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.explorer.ViewSetupExplorer;
import util.URITools;

public class TestN5Loading
{
	/*
	public static <T extends NativeType<T> & RealType<T>> void testBDV() throws IOException, URISyntaxException
	{
		final N5Reader n5 = new N5Factory().openReader("s3://janelia-bigstitcher-spark/Stitching/dataset.n5");

		String[] l = n5.list( "/" );

		for ( final String s : l )
			System.out.println(s);

		final String n5Dataset = "/setup0/timepoint0/s0/";
		final RandomAccessibleInterval<T> img = N5Utils.openVolatile(n5, n5Dataset);

		BdvFunctions.show(
				VolatileViews.wrapAsVolatile( img ),
				n5Dataset,
				BdvOptions.options()).setDisplayRange(0, 255);
	}

	public static <S extends NativeType<S> & IntegerType<S>, T extends NativeType<T> & RealType<T>> void testInterestPoints() throws IOException
	{
		final N5Reader n5 = new N5Factory().openReader("s3://janelia-bigstitcher-spark/Stitching/interestpoints.n5");

		String[] l = n5.list( "/" );

		for ( final String s : l )
			System.out.println( s );

		final RandomAccessibleInterval< S > id = N5Utils.open(n5, l[2] + "/beads/interestpoints/id" );
		final RandomAccessibleInterval< T > loc = N5Utils.open(n5, l[2] + "/beads/interestpoints/loc" );

		System.out.println( "id: " + Util.printInterval( id ));
		System.out.println( "loc: " + Util.printInterval( loc ));

		final long numPoints = id.dimension( 1 );

		final RandomAccess< S > idR = id.randomAccess();
		final RandomAccess< T > locR = loc.randomAccess();

		idR.setPosition( new long[] { 0, 0 } );
		locR.setPosition( new long[] { 0, 0 } );

		for ( long i = 0; i < numPoints; ++i )
		{
			System.out.print( "id=" + idR.setPositionAndGet( 0, i ) );

			System.out.println( ", [" + locR.setPositionAndGet( 0, i ) + ", " + locR.setPositionAndGet( 1, i ) + ", " + locR.setPositionAndGet( 2, i ) + "]");

		}
	}

	public static void testLoadInterestPoints() throws SpimDataException, IOException
	{
		final SpimData2 data = Spark.getSparkJobSpimData2( "s3://janelia-bigstitcher-spark/Stitching/dataset.xml" );

		System.out.println( "num viewsetups: " + data.getSequenceDescription().getViewSetupsOrdered().size() );

		final Map<ViewId, ViewInterestPointLists> ips = data.getViewInterestPoints().getViewInterestPoints();
		final ViewInterestPointLists ipl = ips.values().iterator().next();
		final InterestPoints ip = ipl.getHashMap().values().iterator().next();
		
		System.out.println("base dir: " + ip.getBaseDir() );
		System.out.println("base dir modified: " + InterestPointsN5.assembleURI( ip.getBaseDir(), InterestPointsN5.baseN5 ) );

		List<InterestPoint> ipList = ip.getInterestPointsCopy();

		System.out.println( "Loaded " + ipList.size() + " interest points.");

		System.out.println( "Saving s3://janelia-bigstitcher-spark/Stitching/dataset-save.xml ...");

		Spark.saveSpimData2( data, "s3://janelia-bigstitcher-spark/Stitching/dataset-save.xml" );

		System.out.println( "Done.");
	}
	*/
	public static void testBigStitcherGUI( final URI xml ) throws SpimDataException
	{
		new ImageJ();

		final SpimData2 data = Spark.getSparkJobSpimData2( xml );

		final BasicImgLoader imgLoader = data.getSequenceDescription().getImgLoader();
		if (imgLoader instanceof ViewerImgLoader)
			((ViewerImgLoader) imgLoader).setNumFetcherThreads(-1);

		
		final ViewSetupExplorer< SpimData2 > explorer = new ViewSetupExplorer<>( data, xml, new XmlIoSpimData2() );

		explorer.getFrame().toFront();
	}

	public static void main( String[] args ) throws IOException, URISyntaxException, SpimDataException
	{
		//CloudUtil.parseCloudLink( "s3://janelia-bigstitcher-spark/Stitching/dataset.xml" );

		//testLoadInterestPoints();
		//testBigStitcherGUI( "s3://janelia-bigstitcher-spark/Stitching/dataset.xml" );
		testBigStitcherGUI( URITools.toURI( "/Users/preibischs/Documents/Janelia/Projects/BigStitcher/Allen/bigstitcher_emr_708369_2024-04-23_06-52-14_2.xml" ) );
		//s3://aind-open-data/exaSPIM_708369_2024-04-08_15-20-36_flatfield-correction_2024-04-16_20-33-12/SPIM.ome.zarr
		//testBDV();
		//testInterestPoints();
	}
}
