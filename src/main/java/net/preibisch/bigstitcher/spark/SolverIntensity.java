package net.preibisch.bigstitcher.spark;

import java.util.ArrayList;
import java.util.Arrays;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import picocli.CommandLine;

public class SolverIntensity  extends AbstractSelectableViews {

	protected SpimData2 dataGlobal;
	protected ArrayList< ViewId > viewIdsGlobal;

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		this.dataGlobal = this.loadSpimData2();

		if ( dataGlobal == null )
			throw new IllegalArgumentException( "Couldn't load SpimData XMl project." );

		this.viewIdsGlobal = this.loadViewIds( dataGlobal );

		if ( viewIdsGlobal == null || viewIdsGlobal.size() == 0 )
			throw new IllegalArgumentException( "No ViewIds found." );

		// no spark, load text files, run global opt, save coefficients (multi-threaded)

		// TODO Auto-generated method stub
		return null;
	}

	public static void main(final String... args) throws SpimDataException
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new SolverIntensity()).execute(args));
	}
}
