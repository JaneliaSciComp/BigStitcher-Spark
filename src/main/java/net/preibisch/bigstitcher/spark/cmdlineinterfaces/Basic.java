package net.preibisch.bigstitcher.spark.cmdlineinterfaces;

import picocli.CommandLine.Option;

public interface Basic
{
	@Option(names = { "-x", "--xml" }, required = true, description = "Path to the existing BigStitcher project xml, e.g. -x /home/project.xml")
	String xmlPath = null;
}
