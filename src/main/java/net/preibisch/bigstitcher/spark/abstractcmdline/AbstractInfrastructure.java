package net.preibisch.bigstitcher.spark.abstractcmdline;

import java.io.Serializable;
import java.util.concurrent.Callable;

import net.preibisch.bigstitcher.spark.util.Spark;
import picocli.CommandLine.Option;
import util.URITools;

/**
 * Base class of all BigStitcher-Spark command-line tools. It holds the infrastructure options every command
 * shares (dry run, local Spark bind address, AWS S3 region, maximum number of Spark partitions); subclasses
 * add their own picocli {@code @Option} fields and implement {@code call()}.
 */
public abstract class AbstractInfrastructure implements Callable<Void>, Serializable
{

	private static final long serialVersionUID = 5199967181629299878L;

	@Option(names = { "--dryRun" }, description = "perform a 'dry run', i.e. do not save any results (default: false)")
	protected boolean dryRun = false;

	@Option(names = "--localSparkBindAddress", description = "specify Spark bind address as localhost")
	protected boolean localSparkBindAddress = false;

	@Option(names = { "--s3Region" }, description = "Manually set AWS s3 region, e.g. us-west-2")
	protected String s3Region = null;

	@Option(names = { "--maxPartitions" }, description = "maximum number of Spark partitions to use (default: 10000)")
	protected int maxPartitions = Spark.maxPartitions;

	/**
	 * Applies the infrastructure options to their static homes: sets {@code URITools.s3Region} if
	 * {@code --s3Region} was given and copies {@code --maxPartitions} into {@code Spark.maxPartitions}.
	 * Has to run before the dataset is loaded or a Spark job is started for the settings to take effect.
	 */
	public void setRegion()
	{
		if ( s3Region != null )
			URITools.s3Region = s3Region;

		Spark.maxPartitions = maxPartitions;
	}
}
