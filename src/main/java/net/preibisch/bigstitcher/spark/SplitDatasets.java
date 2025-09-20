package net.preibisch.bigstitcher.spark;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import ij.ImageJ;
import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.generic.sequence.BasicViewDescription;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.util.Pair;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.legacy.io.IOFunctions;
import net.preibisch.mvrecon.fiji.plugin.Split_Views;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import net.preibisch.mvrecon.fiji.spimdata.explorer.SelectedViewDescriptionListener;
import net.preibisch.mvrecon.process.splitting.SplittingTools;
import net.preibisch.stitcher.gui.StitchingExplorer;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

public class SplitDatasets extends AbstractBasic
{
	private static final long serialVersionUID = -1983886010602093433L;

	@Option(names = { "-xo", "--xmlout" }, description = "path to the output BigStitcher xml, e.g. /home/project-n5.xml or s3://myBucket/dataset.xml (default: overwrite input)")
	private String xmlOutURIString = null;

	@Option(names = { "-tis", "--targetImageSize" }, required = true, description = "target image size after splitting e.g.: 512,512,256")
	private String targetImageSizeString = null;

	@Option(names = { "-to", "--targetOverlap" }, required = true, description = "target overlap after splitting e.g.: 32,32,32")
	private String targetOverlapString = null;

	@Option(names = { "--disableOptimization" }, description = "do not optimize image size and overlap")
	private boolean disableOptimization = false;

	@Option(names = { "-fip", "--fakeInterestPoints" }, description = "add fake interest points to overlapping regions of split images/views")
	private boolean fakeInterestPoints = false;

	@Option(names = { "--fipDensity" }, description = "density of fake interest points; number of points per 100x100x100 px volume (default: 100.0)")
	private double fipDensity = 100.0;

	@Option(names = { "--fipMinNumPoints" }, description = "minimal number of fake interest points per overlap (default: 20)")
	private int fipMinNumPoints = 20;

	@Option(names = { "--fipMaxNumPoints" }, description = "maximal number of fake interest points per overlap (default: 500)")
	private int fipMaxNumPoints = 500;

	@Option(names = { "--fipError" }, description = "artificial error for fake corresponding interest points (default: 0.5)")
	private double fipError = 0.5;

	@Option(names = { "--fipExclusionRadius" }, description = "exclusion radius for fake interest points; to not put them close to existing points (default: 20)")
	private double fipExclusionRadius = 20.0;

	@Option(names = { "--assignIlluminations" }, description = "assign old tile id's as illumination id's, this can be great for visualization")
	private boolean assignIlluminations = false;

	@Option(names = { "--displayResult" }, description = "display the result, do not save (you can still click save in the GUI that will pop up")
	private boolean displayResult = false;


	private URI xmlOutURI = null;

	protected SpimData2 dataGlobal;
	protected ArrayList< ViewId > viewI1dsGlobal;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		// if we want to display the result, we need to set the fetcher threads to anything but 0
		this.dataGlobal = displayResult ? this.loadSpimData2( Runtime.getRuntime().availableProcessors() ) : this.loadSpimData2();

		if ( dataGlobal == null )
			throw new IllegalArgumentException( "Couldn't load SpimData XML project." );

		if ( xmlOutURIString == null )
			xmlOutURI = xmlURI;
		else
			xmlOutURI = URITools.toURI( xmlOutURIString );

		System.out.println( "xmlout: " + xmlOutURI );

		final int[] targetImageSize = Import.csvStringToIntArray(targetImageSizeString);
		final int[] targetOverlap = Import.csvStringToIntArray(targetOverlapString);

		final Pair< HashMap< String, Integer >, long[] > imgSizes = Split_Views.collectImageSizes( this.dataGlobal );

		System.out.println( "Current image sizes of dataset:");

		for ( final String size : imgSizes.getA().keySet() )
			IOFunctions.println( imgSizes.getA().get( size ) + "x: " + size );

		final long[] minStepSize = Split_Views.findMinStepSize( this.dataGlobal );

		System.out.println( "Target image sizes and overlaps need be adjusted to be divisible by " + Arrays.toString( minStepSize ) );

		final long sx = Split_Views.closestLargerLongDivisableBy( targetImageSize[ 0 ], minStepSize[ 0 ] );
		final long sy = Split_Views.closestLargerLongDivisableBy( targetImageSize[ 1 ], minStepSize[ 1 ] );
		final long sz = Split_Views.closestLargerLongDivisableBy( targetImageSize[ 2 ], minStepSize[ 2 ] );

		final long ox = Split_Views.closestLargerLongDivisableBy( targetOverlap[ 0 ], minStepSize[ 0 ] );
		final long oy = Split_Views.closestLargerLongDivisableBy( targetOverlap[ 1 ], minStepSize[ 1 ] );
		final long oz = Split_Views.closestLargerLongDivisableBy( targetOverlap[ 2 ], minStepSize[ 2 ] );

		System.out.println( "Adjusted target image size: [" + sx + ", " + sy + ", " + sz + "]" );
		System.out.println( "Adjusted target overlap: [" + ox + ", " + oy + ", " + oz + "]" );

		if ( ox > sx || oy > sy || oz > sz )
		{
			System.out.println( "overlap cannot be bigger than size." );
			return null;
		}

		final SpimData2 newData = SplittingTools.splitImages(
				this.dataGlobal, new long[]{ ox, oy, oz }, new long[]{ sx, sy, sz }, minStepSize, assignIlluminations,
				!disableOptimization, fakeInterestPoints, fipDensity, fipMinNumPoints, fipMaxNumPoints, fipError, fipExclusionRadius );

		if ( displayResult )
		{
			new ImageJ();

			final StitchingExplorer< SpimData2 > explorer = new StitchingExplorer< >( newData, xmlOutURI, new XmlIoSpimData2() );
			explorer.getFrame().toFront();

			explorer.addListener( new SelectedViewDescriptionListener<SpimData2>() {
				
				@Override
				public void updateContent(SpimData2 data) {}
				
				@Override
				public void selectedViewDescriptions(List<List<BasicViewDescription<?>>> viewDescriptions) {}
				
				@Override
				public void save() {}
				
				@Override
				public void quit()
				{
					System.out.println( "quitting GUI.");
					System.exit( 0 );
				}
			});

			// program will be quit by the listener above
			try
			{
				Thread.sleep( Long.MAX_VALUE );
			}
			catch ( final InterruptedException e )
			{
				System.err.println( "MultiThreading.threadWait(): Thread woken up: " + e );
			}
		}
		else if ( !dryRun )
		{
			new XmlIoSpimData2().save( newData, xmlOutURI );
		}

		return null;
	}

	public static void main(final String... args) throws SpimDataException
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new SplitDatasets()).execute(args));
	}
}
