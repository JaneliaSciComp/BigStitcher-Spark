package net.preibisch.bigstitcher.spark.util;

import java.util.List;

public class Downsampling
{
	public static boolean testDownsamplingParameters( final boolean multiRes, final List<String> downsampling, final String dataset )
	{
		// no not create multi-res pyramid
		if ( !multiRes && downsampling == null )
			return true;

		if ( multiRes && downsampling != null )
		{
			System.out.println( "If you want to create a multi-resolution pyramid, you must select either automatic (--multiRes) - OR - manual mode (e.g. --downsampling 2,2,1; 2,2,1; 2,2,2)");
			return false;
		}

		// non-bdv multi-res dataset
		if ( dataset != null )
		{
			if ( !dataset.endsWith("/s0") )
			{
				System.out.println( "In order to create a multi-resolution pyramid for a non-BDV dataset, the dataset must end with '/s0', right not it is '" + dataset + "'.");
				return false;
			}
		}

		return true;
	}

}
