package net.preibisch.bigstitcher.spark;

import java.util.Arrays;

import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import picocli.CommandLine;

public class RegistrationFastDescriptorSpark extends AbstractSelectableViews
{

	private static final long serialVersionUID = 6435121614117716228L;

	@Override
	public Void call() throws Exception
	{

		return null;
	}

	public static void main(final String... args)
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new RegistrationFastDescriptorSpark()).execute(args));
	}

}
