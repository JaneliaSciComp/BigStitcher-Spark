package net.preibisch.bigstitcher.spark.abstractcmdline;

import java.io.Serializable;
import java.util.concurrent.Callable;

import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.mvrecon.process.interestpointregistration.TransformationTools;
import net.preibisch.mvrecon.process.interestpointregistration.global.pointmatchcreating.strong.InterestPointMatchCreator;
import net.preibisch.mvrecon.process.interestpointregistration.pairwise.PairwiseResult;
import picocli.CommandLine.Option;
import util.URITools;

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

	@Option(names = { "--maxPerPairLog" }, description = "max number of per-pair 'Connecting … <-> …' log lines printed by global registration before per-pair output is suppressed (the per-(labelA,labelB) summary is always emitted). Default: 1000")
	protected int maxPerPairLog = InterestPointMatchCreator.maxPerPairLog;

	@Option(names = { "--maxPerPairCorrLog" }, description = "max number of per-pair '[…] >>> […]: Loaded N corresponding…' / 'Not enough…' log lines printed during pairwise correspondence loading before per-pair output is suppressed (the per-(labelA,labelB) summary is always emitted). Default: 1000")
	protected int maxPerPairCorrLog = PairwiseResult.maxPerPairCorrLog;

	@Option(names = { "--maxPerViewTransformLog" }, description = "max number of per-view 'Transformation Models:' lines printed after each global optimization round before output is suppressed (the identity-vs-non-identity summary is always emitted). Default: 1000")
	protected int maxPerViewTransformLog = TransformationTools.maxPerViewTransformLog;

	public void setRegion()
	{
		if ( s3Region != null )
			URITools.s3Region = s3Region;

		Spark.maxPartitions = maxPartitions;

		// propagate log-cap knobs into their mvr static homes
		InterestPointMatchCreator.maxPerPairLog = maxPerPairLog;
		PairwiseResult.maxPerPairCorrLog = maxPerPairCorrLog;
		TransformationTools.maxPerViewTransformLog = maxPerViewTransformLog;
	}
}
