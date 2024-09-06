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
package net.preibisch.bigstitcher.spark;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;

import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.realtransform.AffineTransform3D;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.legacy.io.TextFileAccess;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class TransformPoints extends AbstractBasic
{
	@Option(names = { "-vi" }, required = true, description = "specifically list the view id (time point id, view setup id) from which the transformation should be loaded, e.g. -vi '0,5' (timepoint 0, viewsetup 5)")
	protected String vi = null;

	@Option(names = { "--csvIn" }, description = "path to a comma separated file (x,y,z) from which points will be loaded (e.g. /home/jimmy/mypoints.csv)")
	private String csvIn = null;

	@Option(names = { "-p" }, description = "coordinates in 3d (e.g. -p 3.4,423.1,-12134.14142 -p 12.1,1.2,12.1)")
	private String p[] = null;

	//@Option(names = { "--zeroMin" }, description = "add code to set the minimal coordinate to 0,0,0")
	//private boolean p[] = null;
 
	@Option(names = { "--csvOut" }, description = "path to a comma separated file (x,y,z) to which points will be saved. If ommited, it will go to stoud (printed out);(e.g. /home/jimmy/mypoints_transformed.csv)")
	private String csvOut = null;
	
	private static final long serialVersionUID = -7892604354139919145L;

	@Override
	public Void call() throws Exception
	{
		//System.out.println( "-325.12  ,  3434.23".matches( "[\\p{Space}*[-]*\\d+\\.?\\d*\\p{Space}*\\,?]+"));
		//System.exit( 0 );

		final ArrayList< double[] > points = new ArrayList<>();

		if ( csvIn != null )
		{
			final File f = new File( csvIn );

			if ( !f.exists() )
			{
				System.out.println( "File '" + f + "' doesn't exist.");
				return null;
			}

			System.out.println( "Parsing '" + f.getAbsolutePath() + "'");

			try
			{
				final BufferedReader in = TextFileAccess.openFileRead( f.getAbsoluteFile() );
				in.lines().forEach( s -> 
				{
					if ( s.trim().length() > 0 && s.trim().matches( "[\\p{Space}*[-]*\\d+\\.?\\d*\\p{Space}*\\,?]+") )
						points.add( Import.csvStringToDoubleArray( s.trim() ) );
					else
						System.out.println( "Ignoring line: " + s );
				} );
				in.close();
			}
			catch ( Exception e )
			{
				System.out.println( "Error parsing CSV file '" + f + "': " + e);
				return null;
			}
		}

		if ( points != null )
			for ( final String point : p )
				if ( point.trim().length() > 0 ) points.add( Import.csvStringToDoubleArray( point ) ); 

		if ( points.size() == 0 )
		{
			System.err.println( "No points defined (use either --csv and/or -p), stopping.");
			return null;
		}

		// load transfromation
		final SpimData2 data = this.loadSpimData2();

		if ( data == null )
			return null;

		final ViewId view = Import.getViewId( vi );

		System.out.println( "Using transformations of viewId: " + Group.pvid( view ) );

		final ViewRegistration vr = data.getViewRegistrations().getViewRegistration( view );
		vr.updateModel();
		final AffineTransform3D model = vr.getModel();

		// apply transformations
		System.out.println( "Applying 3d affine: " + model );

		for ( final double[] point : points )
			model.apply( point, point );

		// save file or print out
		PrintWriter out = null;
		if ( csvOut != null )
		{
			final File f = new File( csvOut );
			System.out.println( "Writing to '" + f.getAbsolutePath() + "'");
			out = TextFileAccess.openFileWrite( f );
		}

		for ( final double[] point : points )
		{
			String text = point[0] + "," + point[1] + "," + point[2];

			if ( out == null )
				System.out.println( text );
			else
				out.println( text );
		}

		if ( out != null )
			out.close();

		return null;
	}

//	private double[] parse(// final )
	public static void main(final String... args)
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new TransformPoints()).execute(args));
	}

}
