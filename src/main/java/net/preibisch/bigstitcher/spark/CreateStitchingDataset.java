package net.preibisch.bigstitcher.spark;

import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.Callable;

import mpicbg.spim.data.SpimDataException;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractBasic;
import net.preibisch.mvrecon.dataset.SpimDatasetBuilder;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.fiji.spimdata.XmlIoSpimData2;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import util.URITools;

public class CreateStitchingDataset extends AbstractBasic implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = -5155338208494730656L;

	@Option(names = {"--input-path"}, required = true, description = "Path to the input images, e.g. /data/images/")
	private String inputPath = "/Users/goinac/Work/HHMI/stitching/datasets/tiny_4_bigstitcher/t1/";

	@Option(names = {"--input-pattern"}, description = "Glob pattern for input images, e.g. /data/images/*.tif")
	private String inputPattern = "*";

	@Override
	public Void call() throws Exception {
		this.setRegion();

		SpimData2 spimData = createDataset();

		URI xmlURI = URITools.toURI(xmlURIString);

		System.out.println("Save spimData with original tiles to " + xmlURI);
		new XmlIoSpimData2().save(spimData, xmlURI);

		return null;
	}

	private SpimData2 createDataset() {
		SpimDatasetBuilder spimDatasetBuilder = new SpimDatasetBuilder(inputPattern);
		return spimDatasetBuilder.createDataset(inputPath);
	}

	public static void main(final String... args) throws SpimDataException {
		System.out.println(Arrays.toString(args));

		System.exit(new CommandLine(new CreateStitchingDataset()).execute(args));
	}
}
