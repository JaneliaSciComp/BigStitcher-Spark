package net.preibisch.bigstitcher.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewTransform;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.constellation.grouping.Group;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class ClearRegistrations extends AbstractSelectableViews
{
	private static final long serialVersionUID = -526317635920954010L;

	@Option(names = { "--keep" }, description = "keep only the first N transfomations (in human order of transform application, e.g. first calibration, then tile locations, then affine transformation)")
	private Integer keep = null;

	@Option(names = { "--remove" }, description = "remove the last N transfomations (in human order of transform application, e.g. first calibration, then tile locations, then affine transformation)")
	private Integer remove = null;

	@Override
	public Void call() throws Exception
	{
		if ( keep == null && remove == null || keep != null && remove != null || keep != null && keep < 0 || remove != null && remove < 0 )
		{
			System.out.println( "Please specify --keep OR --remove as >=0." );
			return null;
		}

		final SpimData2 dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			return null;

		final ArrayList< ViewId > viewIdsGlobal = this.loadViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			return null;

		final Map<ViewId, ViewRegistration> regs = dataGlobal.getViewRegistrations().getViewRegistrations();

		System.out.println( "The following transformations will be removed:");

		for ( final Entry<ViewId, ViewRegistration> reg : regs.entrySet() )
		{
			System.out.println( Group.pvid( reg.getKey() ) + ":" );

			final ViewRegistration r = reg.getValue();

			if ( remove != null )
			{
				for ( int i = 0; i < remove; ++i )
				{
					if ( r.getTransformList().size() > 0 )
					{
						final ViewTransform t = r.getTransformList().get( 0 );
						System.out.println( "\t" + t.getName() + ", " + t.asAffine3D() );
						r.getTransformList().remove( 0 );
					}
				}
			}
			else if ( keep != null )
			{
				while ( r.getTransformList().size() > keep )
				{
					final ViewTransform t = r.getTransformList().get( 0 );
					System.out.println( "\t" + t.getName() + ", " + t.asAffine3D() );
					r.getTransformList().remove( 0 );
				}
			}
		}

		if ( !dryRun )
		{
			System.out.println( "Saving XML ..." );
			new XmlIoSpimData2( null ).save( dataGlobal, xmlPath );
		}

		return null;
	}

	public static void main(final String... args)
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new ClearRegistrations()).execute(args));
	}

}
