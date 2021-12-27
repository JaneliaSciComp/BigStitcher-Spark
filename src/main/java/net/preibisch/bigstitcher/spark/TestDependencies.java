package net.preibisch.bigstitcher.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;

import ij.ImageJ;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.multithreading.SimpleMultiThreading;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class TestDependencies implements Callable<Void>, Serializable
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2618746921863437076L;
	@Option(names = { "-x", "--xml" }, required = true, description = "path to the BigStitcher xml, e.g. /home/project.xml")
	private String xmlPath = null;

	@Override
	public Void call() throws Exception {

		final XmlIoSpimData2 io = new XmlIoSpimData2( "" );
		final SpimData2 data = io.load( xmlPath );

		// select views to process
		final ArrayList< ViewId > viewIds =
				Import.getViewIds( data );

		RandomAccessibleInterval img = data.getSequenceDescription().getImgLoader().getSetupImgLoader(
				viewIds.iterator().next().getViewSetupId() ).getImage( viewIds.iterator().next().getTimePointId() );

		new ImageJ();
		ImageJFunctions.show( img );
		SimpleMultiThreading.threadHaltUnClean();
		
		return null;
	}

	public static final void main(final String... args) {

		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new TestDependencies()).execute(args));
	}

}
