package net.preibisch.bigstitcher.spark.abstractcmdline;

import java.io.Serializable;
import java.util.concurrent.Callable;

import mpicbg.spim.data.SpimDataException;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import picocli.CommandLine.Option;

public abstract class AbstractBasic implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = -4916959775650710928L;

	@Option(names = { "-x", "--xml" }, required = true, description = "Path to the existing BigStitcher project xml, e.g. -x /home/project.xml")
	protected String xmlPath = null;

	public SpimData2 loadSpimData2() throws SpimDataException
	{
		final SpimData2 dataGlobal = Spark.getSparkJobSpimData2("", xmlPath);

		return dataGlobal;
	}
}
