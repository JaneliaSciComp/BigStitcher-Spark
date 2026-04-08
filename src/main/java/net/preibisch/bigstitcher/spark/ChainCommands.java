package net.preibisch.bigstitcher.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.Callable;

import mpicbg.spim.data.SpimDataException;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractInfrastructure;
import picocli.CommandLine;

public class ChainCommands extends AbstractInfrastructure implements Callable<Void>, Serializable
{
	private static final long serialVersionUID = 1584686229152127469L;

	static class CommandWithArguments {
		final List<String> cmdArgs;

		CommandWithArguments(List<String> cmdArgs) {
			this.cmdArgs = cmdArgs;
		}
	}

	/**
	 * Custom converter to collect all arguments after --command until a separator
	 * (either ';' or '+') or the end of the input.
	 */
	static class CommandArgsConverter implements CommandLine.IParameterConsumer {
		@Override
		public void consumeParameters(Stack<String> args, CommandLine.Model.ArgSpec argSpec, CommandLine.Model.CommandSpec commandSpec) {
			List<CommandWithArguments> currentCommands = argSpec.getValue();
			List<String> commandArgs = new ArrayList<>();
			while (!args.isEmpty()) {
				String arg = args.pop();

				if (";".equals(arg) || "+".equals(arg)) {
					break;
				}
				if (arg.equals("-h") || arg.equals("--help")) {
					// add back the help flag at the bottom of the stack
					// but before check if there was anything left and if there wasn't stop after this
					boolean done = args.isEmpty();
					args.add(0, arg);
					if (done) break;
				} else
					commandArgs.add(arg);
			}
			currentCommands.add(new CommandWithArguments(commandArgs));
		}
	}

	@CommandLine.Option(names = { "-h", "--help" }, description = "display this help message", usageHelp = true)
	boolean helpFlag;

	@CommandLine.Option(names = { "--command" }, parameterConsumer = CommandArgsConverter.class,
						description = "Command to execute with its arguments. Multiple commands can be chained using ';' or '+'.\n"
									+ "Example: --command create-dataset --input-path /data/images/ --input-pattern '*.tif' ; "
									+ "--command detect-interestpoints --detector SIFT --descriptor SIFT ; "
									+ "--command match-interestpoints --matcher FLANN ; stitching --stitchingModel Affine")
	List<CommandWithArguments> commands = new ArrayList<>();

	@Override
	public Void call() throws Exception {
		for (CommandWithArguments commandArgs : commands) {
			if (commandArgs.cmdArgs.isEmpty())
				continue;

			String cmdName = commandArgs.cmdArgs.get(0);
			List<String> cmdArgs = new ArrayList<>(commandArgs.cmdArgs.subList(1, commandArgs.cmdArgs.size()));
			addCommonOptions(cmdArgs);

			AbstractInfrastructure cmdInstance = getCmdInstance(cmdName);
			CommandLine currentCmdLine = new CommandLine(cmdInstance);
			System.out.println("Execute command: " + cmdName + " with args: " + cmdArgs);
			int exitCode = currentCmdLine.execute(cmdArgs.toArray(new String[0]));
			if (exitCode != 0) {
				System.err.println("Command " + cmdName + " failed with exit code " + exitCode);
				System.exit(exitCode);
			}
		}
		return null;
	}

	private AbstractInfrastructure getCmdInstance(String name) {
		switch (name) {
			case "clear-interestpoints": return new ClearInterestPoints();
			case "clear-registrations": return new ClearRegistrations();
			case "create-container": return new CreateFusionContainer();
			case "detect-interestpoints": return new SparkInterestPointDetection();
			case "match-interestpoints": return new SparkGeometricDescriptorMatching();
			case "nonrigid-fusion": return new SparkNonRigidFusion();
			case "create-dataset": return new CreateDataset();
			case "stitching": return new SparkPairwiseStitching();
			case "resave": return new SparkResaveN5();
			case "downsample": return new SparkDownsample();
			case "affine-fusion": return new SparkAffineFusion();
			case "solver": return new Solver();
			default: throw new IllegalArgumentException("Unknown command: " + name);
		}
	}

	private void addCommonOptions(List<String> cmdArgs) {
		if (this.dryRun) {
			cmdArgs.add("--dryRun");
		}
		if (this.localSparkBindAddress) {
			cmdArgs.add("--localSparkBindAddress");
		}
		if (this.s3Region != null && !this.s3Region.isEmpty()) {
			cmdArgs.add("--s3Region");
			cmdArgs.add(this.s3Region);
		}
	}

	public static void main(final String... args) throws SpimDataException {
		System.out.println(Arrays.toString(args));

		ChainCommands chainedCommands = new ChainCommands();
		CommandLine commandLine = new CommandLine(chainedCommands)
				.setUnmatchedOptionsArePositionalParams(true)
				;


		System.exit(commandLine.execute(args));
	}

}
