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

import java.util.ArrayList;
import java.util.Arrays;

import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewTransform;
import mpicbg.spim.data.registration.ViewTransformAffine;
import mpicbg.spim.data.sequence.ViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Dimensions;
import net.imglib2.realtransform.AffineTransform3D;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.ViewSetupUtils;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * Command-line counterpart of BigStitcher's "Apply Transformations": adds one view transform to the registrations
 * of the selected views, after the existing ones (like the dialog) or, with {@code --applyFirst}, before them. The
 * transform is a rotation about an axis, a translation, a scaling, a mirror, a full affine matrix, or the
 * translation that puts the centre of every selected view onto the centre of a reference view; exactly one of them
 * per call. A shear is deliberately not offered yet: BigDataViewer's interpolation of a sheared view breaks down and
 * the images are not shown properly (future work). Not a Spark job.
 */
public class ApplyTransformation extends AbstractSelectableViews
{
	private static final long serialVersionUID = -6417213378120538273L;

	@Option(names = { "--rotate" }, description = "rotation about one axis as AXIS,DEGREES, e.g. --rotate y,-57 (right-handed, the matrix of the dialog's 'Rotation around axis')")
	private String rotate = null;

	@Option(names = { "--translate" }, description = "translation dx,dy,dz in the registered space (calibrated pixels), e.g. --translate -1635,-184,-524")
	private String translate = null;

	@Option(names = { "--scale" }, description = "scaling sx,sy,sz about the origin, e.g. --scale 1,1,2")
	private String scale = null;

	@Option(names = { "--mirror" }, description = "mirror one axis (x, y or z) about the origin, e.g. --mirror z (reverses the slice order of a view)")
	private String mirror = null;

	@Option(names = { "--affine" }, description = "a full affine as 12 comma-separated values, row-major as in the XML: m00,m01,m02,m03,m10,m11,m12,m13,m20,m21,m22,m23")
	private String affine = null;

	@Option(names = { "--centerOnView" }, description = "translation that moves the centre of every selected view's image (through its current transforms) onto the centre of the reference view TIMEPOINT,SETUP, e.g. --centerOnView 0,2")
	private String centerOnView = null;

	@Option(names = { "--name" }, description = "name of the new view transform in the XML (default: a description of the transform, like the dialog's)")
	private String name = null;

	@Option(names = { "--applyFirst" }, description = "apply the transform BEFORE the existing ones of each view, i.e. to the image's pixel coordinates, instead of after them (default: after, like the dialog)")
	private boolean applyFirst = false;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		int given = 0;
		for ( final String s : new String[] { rotate, translate, scale, mirror, affine, centerOnView } )
			if ( s != null )
				++given;

		if ( given != 1 )
		{
			System.err.println( "ERROR: give exactly one of --rotate, --translate, --scale, --mirror, --affine, --centerOnView (" + given + " given)." );
			return null;
		}

		if ( centerOnView != null && applyFirst )
		{
			System.err.println( "ERROR: --centerOnView is a translation in the registered space and cannot be combined with --applyFirst." );
			return null;
		}

		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIdsGlobal = this.loadViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			return null;

		// the transform common to all selected views (null for --centerOnView, which is computed per view)
		final AffineTransform3D model;
		final String defaultName;
		try
		{
			if ( rotate != null )
			{
				final String[] p = split( rotate, 2, "--rotate" );
				final int axis = axisIndex( p[ 0 ], "--rotate" );
				final double degrees = Double.parseDouble( p[ 1 ].trim() );
				model = rotation( axis, degrees );
				defaultName = "Manually defined transformation (Rotation around " + "xyz".charAt( axis ) + "-axis by " + degrees + " degrees)";
			}
			else if ( translate != null )
			{
				final double[] t = doubles( translate, 3, "--translate" );
				model = new AffineTransform3D();
				model.translate( t );
				defaultName = "Manually defined transformation (Translation " + Arrays.toString( t ) + ")";
			}
			else if ( scale != null )
			{
				final double[] s = doubles( scale, 3, "--scale" );
				model = new AffineTransform3D();
				model.set( s[ 0 ], 0, 0, 0, 0, s[ 1 ], 0, 0, 0, 0, s[ 2 ], 0 );
				defaultName = "Manually defined transformation (Scaling " + Arrays.toString( s ) + ")";
			}
			else if ( mirror != null )
			{
				final int axis = axisIndex( mirror.trim(), "--mirror" );
				final double[] s = new double[] { 1, 1, 1 };
				s[ axis ] = -1;
				model = new AffineTransform3D();
				model.set( s[ 0 ], 0, 0, 0, 0, s[ 1 ], 0, 0, 0, 0, s[ 2 ], 0 );
				defaultName = "Mirror " + "xyz".charAt( axis );
			}
			else if ( affine != null )
			{
				final double[] m = doubles( affine, 12, "--affine" );
				model = new AffineTransform3D();
				model.set( m );
				defaultName = "Manually defined transformation (Affine " + Arrays.toString( m ) + ")";
			}
			else
			{
				model = null;
				defaultName = null;
			}
		}
		catch ( final IllegalArgumentException e )
		{
			System.err.println( "ERROR: " + e.getMessage() );
			return null;
		}

		// the reference of --centerOnView
		double[] referenceCenter = null;
		ViewId referenceView = null;
		if ( centerOnView != null )
		{
			try
			{
				final String[] p = split( centerOnView, 2, "--centerOnView" );
				referenceView = new ViewId( Integer.parseInt( p[ 0 ].trim() ), Integer.parseInt( p[ 1 ].trim() ) );
			}
			catch ( final IllegalArgumentException e )
			{
				System.err.println( "ERROR: " + e.getMessage() );
				return null;
			}
			final ViewDescription vd = dataGlobal.getSequenceDescription().getViewDescription( referenceView );
			if ( vd == null || !vd.isPresent() )
			{
				System.err.println( "ERROR: the reference view " + Group.pvid( referenceView ) + " does not exist or is missing." );
				return null;
			}
			referenceCenter = transformedCenter( dataGlobal, referenceView );
			System.out.println( "Reference " + Group.pvid( referenceView ) + ": centre in the registered space " + Arrays.toString( referenceCenter ) );
		}

		for ( final ViewId viewId : viewIdsGlobal )
		{
			final ViewRegistration vr = dataGlobal.getViewRegistrations().getViewRegistration( viewId );

			final AffineTransform3D m;
			final String n;
			if ( centerOnView != null )
			{
				final double[] c = transformedCenter( dataGlobal, viewId );
				final double[] t = new double[] { referenceCenter[ 0 ] - c[ 0 ], referenceCenter[ 1 ] - c[ 1 ], referenceCenter[ 2 ] - c[ 2 ] };
				m = new AffineTransform3D();
				m.translate( t );
				n = name != null ? name : "Manually defined transformation (Translation " + Arrays.toString( t ) + " onto the centre of " + Group.pvid( referenceView ) + ")";
			}
			else
			{
				m = model;
				n = name != null ? name : defaultName;
			}

			final ViewTransform vt = new ViewTransformAffine( n, m );

			if ( applyFirst )
				vr.concatenateTransform( vt );
			else
				vr.preconcatenateTransform( vt );

			System.out.println( Group.pvid( viewId ) + ": " + ( applyFirst ? "applied first: " : "applied last: " ) + n + " " + m + "\n\tfull model now " + vr.getModel() );
		}

		if ( !dryRun )
		{
			System.out.println( "Saving XML ..." );
			new XmlIoSpimData2().save( dataGlobal, xmlURI );
		}

		return null;
	}

	/**
	 * The rotation matrix of BigStitcher's "Rotation around axis" (right-handed, javax.vecmath convention):
	 * about y, {@code x' = cos x + sin z, z' = -sin x + cos z}.
	 */
	public static AffineTransform3D rotation( final int axis, final double degrees )
	{
		final double c = Math.cos( Math.toRadians( degrees ) );
		final double s = Math.sin( Math.toRadians( degrees ) );
		final AffineTransform3D m = new AffineTransform3D();
		if ( axis == 0 )
			m.set( 1, 0, 0, 0, 0, c, -s, 0, 0, s, c, 0 );
		else if ( axis == 1 )
			m.set( c, 0, s, 0, 0, 1, 0, 0, -s, 0, c, 0 );
		else
			m.set( c, -s, 0, 0, s, c, 0, 0, 0, 0, 1, 0 );
		return m;
	}

	/** the centre of the view's image, {@code (size - 1) / 2} per axis, through the view's current full transform */
	public static double[] transformedCenter( final SpimData2 data, final ViewId viewId )
	{
		final ViewDescription vd = data.getSequenceDescription().getViewDescription( viewId );
		final Dimensions size = ViewSetupUtils.getSizeOrLoad( vd.getViewSetup(), vd.getTimePoint(), data.getSequenceDescription().getImgLoader() );
		final double[] c = new double[ 3 ];
		for ( int d = 0; d < 3; ++d )
			c[ d ] = ( size.dimension( d ) - 1 ) / 2.0;
		final ViewRegistration vr = data.getViewRegistrations().getViewRegistration( viewId );
		vr.updateModel();
		final double[] out = new double[ 3 ];
		vr.getModel().apply( c, out );
		return out;
	}

	static String[] split( final String s, final int n, final String option )
	{
		final String[] p = s.split( "," );
		if ( p.length != n )
			throw new IllegalArgumentException( option + " '" + s + "' needs " + n + " comma-separated values." );
		return p;
	}

	static double[] doubles( final String s, final int n, final String option )
	{
		final double[] v;
		try
		{
			v = Import.csvStringToDoubleArray( s );
		}
		catch ( final NumberFormatException e )
		{
			throw new IllegalArgumentException( option + " '" + s + "' is not a list of numbers." );
		}
		if ( v.length != n )
			throw new IllegalArgumentException( option + " '" + s + "' needs " + n + " comma-separated values." );
		return v;
	}

	static int axisIndex( final String axis, final String option )
	{
		final String a = axis.trim().toLowerCase();
		if ( a.equals( "x" ) ) return 0;
		if ( a.equals( "y" ) ) return 1;
		if ( a.equals( "z" ) ) return 2;
		throw new IllegalArgumentException( option + ": axis must be x, y or z, not '" + axis + "'." );
	}

	public static void main( final String... args )
	{
		System.out.println( Arrays.toString( args ) );
		System.exit( new CommandLine( new ApplyTransformation() ).execute( args ) );
	}
}
