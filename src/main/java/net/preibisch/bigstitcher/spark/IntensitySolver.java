package net.preibisch.bigstitcher.spark;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.universe.StorageFormat;
import org.slf4j.LoggerFactory;

import mpicbg.spim.data.SpimDataException;
import mpicbg.spim.data.sequence.ViewId;
import net.preibisch.bigstitcher.spark.abstractcmdline.AbstractSelectableViews;
import net.preibisch.bigstitcher.spark.util.Import;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.bigstitcher.spark.util.ViewUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.process.fusion.intensity.Coefficients;
import net.preibisch.mvrecon.process.fusion.intensity.IntensityCorrection;
import net.preibisch.mvrecon.process.fusion.intensity.ViewPairCoefficientMatches;
import net.preibisch.mvrecon.process.fusion.intensity.ViewPairCoefficientMatchesIO;
import picocli.CommandLine;
import util.URITools;

public class IntensitySolver extends AbstractSelectableViews {

	@CommandLine.Option(names = { "--numCoefficients" }, description = "number of coefficients per dimension (default: 8,8,8)")
	private String numCoefficientsString = "8,8,8";

	@CommandLine.Option(names = { "--matchesPath" }, required = true, description = "path (URI) for loading pairwise intensity matches, e.g., file:/home/fused.n5/intensity/ or e.g. s3://myBucket/data.zarr/intensity/")
	private String matchesPathURIString = null;

	@CommandLine.Option(names = { "--maxIterations" }, description = "max number of iterations for solve (default: 1000)")
	private Integer maxIterations = 1000;

	@CommandLine.Option(names = { "-o", "--intensityN5Path" }, required = true, description = "N5/ZARR/HDF5 base path for saving coefficients (e.g. s3://myBucket/coefficients.n5)")
	private String outputPathURIString = null;

	@CommandLine.Option(names = {"-s", "--intensityN5Storage"}, description = "output storage type, can be used to override guessed format (default: guess from n5Path file/directory-ending)")
	private StorageFormat storageType = null;

	@CommandLine.Option(names = { "--intensityN5Group" }, description = "group under which coefficient datasets are stored (default: \"\")")
	private String outputGroup = "";

	@CommandLine.Option(names = { "--intensityN5Dataset" }, description = "dataset name for each coefficient dataset (default: \"intensity\"). The coefficients for view(s,t) are stored in dataset \"{-n5Group}/setup{s}/timepoint{t}/{n5Dataset}\"")
	private String outputDataset = "intensity";

	@Override
	public Void call() throws Exception
	{
		this.setRegion();

		final URI outPathURI = URITools.toURI( outputPathURIString );
		System.out.println( "Writing coefficients to: " + outPathURI );

		if ( storageType == null )
		{
			if ( outputPathURIString.toLowerCase().endsWith( ".zarr" ) )
				storageType = StorageFormat.ZARR;
			else if ( outputPathURIString.toLowerCase().endsWith( ".n5" ) )
				storageType = StorageFormat.N5;
			else if ( outputPathURIString.toLowerCase().endsWith( ".h5" ) || outPathURI.toString().toLowerCase().endsWith( ".hdf5" ) )
				storageType = StorageFormat.HDF5;
			else
			{
				System.out.println( "Unable to guess format from URI '" + outPathURI + "', please specify using '-s'");
				return null;
			}

			System.out.println( "Guessed format " + storageType + " will be used to open URI '" + outPathURI + "', you can override it using '-s'");
		}
		else
		{
			System.out.println( "Format " + storageType + " will be used to open " + outPathURI );
		}

		final N5Writer n5Writer = N5Util.createN5Writer( outPathURI, storageType );


		final SpimData2 spimData = this.loadSpimData2();
		if ( spimData == null )
			throw new IllegalArgumentException( "Couldn't load SpimData XMl project." );

		final List< ViewId > views = loadViewIds( spimData );
		if ( views == null || views.isEmpty() )
			throw new IllegalArgumentException( "No ViewIds found." );

		final int[] coefficientsSize = Import.csvStringToIntArray( numCoefficientsString );
		final URI matchesURI = URITools.toURI( matchesPathURIString );

		final ViewPairCoefficientMatchesIO matchesIO = new ViewPairCoefficientMatchesIO( matchesURI );
		final int[] matchesCoefficientsSize = matchesIO.readCoefficientsSize();
		if ( !Arrays.equals( matchesCoefficientsSize, coefficientsSize ) )
		{
			System.err.println( "numCoefficients stored with matches is different from specified numCoefficients argument!" );
		}

		final List< ViewPairCoefficientMatches > pairwiseMatches = new ArrayList<>();
		for ( int i = 0; i < views.size(); ++i )
		{
			ViewUtil.progressPercentage(i, views.size());

			for ( int j = i + 1; j < views.size(); ++j )
			{
				final ViewPairCoefficientMatches matches = matchesIO.read( views.get( i ), views.get( j ) );
				if ( matches != null )
					pairwiseMatches.add( matches );
			}
		}

		System.out.println( "\nConnected pairs: " + pairwiseMatches.size() );
		System.out.println( "Running solve... " );

		final Map< ViewId, Coefficients > coefficients = IntensityCorrection.solve( coefficientsSize, pairwiseMatches, maxIterations );

		IntensityCorrection.writeCoefficients( n5Writer, outputGroup, outputDataset, coefficients );

		System.out.println( "Done.");

		return null;
	}

	public static void main(final String... args) throws SpimDataException
	{
		System.out.println(Arrays.toString(args));
		System.exit(new CommandLine(new IntensitySolver()).execute(args));
	}
}
