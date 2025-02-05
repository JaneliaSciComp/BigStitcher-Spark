package net.preibisch.bigstitcher.spark.abstractcmdline;

import java.io.Serializable;
import java.util.concurrent.Callable;

import picocli.CommandLine.Option;

public abstract class AbstractInfrastructure implements Callable<Void>, Serializable
{

	private static final long serialVersionUID = 5199967181629299878L;

	@Option(names = { "--dryRun" }, description = "perform a 'dry run', i.e. do not save any results (default: false)")
	protected boolean dryRun = false;

	@Option(names = "--localSparkBindAddress", description = "specify Spark bind address as localhost")
	protected boolean localSparkBindAddress = false;

}
