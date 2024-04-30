package net.preibisch.bigstitcher.spark.fusion;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.VoidFunction;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewRegistrations;
import mpicbg.spim.data.registration.ViewTransformAffine;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.Cursor;
import net.imglib2.Dimensions;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Util;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.preibisch.bigstitcher.spark.blk.N5Helper;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.bigstitcher.spark.util.ViewUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.process.export.ExportN5API.StorageType;

public class WriteSuperBlockMasks implements VoidFunction< long[][] >
{

	private static final long serialVersionUID = 1346166310488822467L;

	private final String xmlPath;

	private final boolean preserveAnisotropy;

	private final double anisotropyFactor;

	private final long[] minBB;

	private final String n5Path;

	private final String n5Dataset;

	private final StorageType storageType;

	private final int[][] serializedViewIds;

	private final boolean uint8;

	private final boolean uint16;

	private final double[] maskOffset;

	private final int[] blockSize;

	public WriteSuperBlockMasks(
			final String xmlPath,
			final boolean preserveAnisotropy,
			final double anisotropyFactor,
			final long[] minBB,
			final String n5Path,
			final String n5Dataset,
			final StorageType storageType,
			final int[][] serializedViewIds,
			final boolean uint8,
			final boolean uint16,
			final double[] maskOffset,
			final int[] blockSize )
	{
		this.xmlPath = xmlPath;
		this.preserveAnisotropy = preserveAnisotropy;
		this.anisotropyFactor = anisotropyFactor;
		this.minBB = minBB;
		this.n5Path = n5Path;
		this.n5Dataset = n5Dataset;
		this.storageType = storageType;
		this.serializedViewIds = serializedViewIds;
		this.uint8 = uint8;
		this.uint16 = uint16;
		this.maskOffset = maskOffset;
		this.blockSize = blockSize;
	}

	@Override
	public void call( final long[][] gridBlock ) throws Exception
	{
		final int n = blockSize.length;

		// The min coordinates of the block that this job renders (in pixels)
		final long[] superBlockOffset = new long[ n ];
		Arrays.setAll( superBlockOffset, d -> gridBlock[ 0 ][ d ] + minBB[ d ] );

		// The size of the block that this job renders (in pixels)
		final long[] superBlockSize = gridBlock[ 1 ];

		System.out.println( "Fusing block: offset=" + Util.printCoordinates( gridBlock[0] ) + ", dimension=" + Util.printCoordinates( gridBlock[1] ) );

		// The min grid coordinate of the block that this job renders, in units of the output grid.
		// Note, that the block that is rendered may cover multiple output grid cells.
		final long[] outputGridOffset = gridBlock[ 2 ];

		// --------------------------------------------------------
		// initialization work that is happening in every job,
		// independent of gridBlock parameters
		// --------------------------------------------------------

		// custom serialization
		final SpimData2 dataLocal = Spark.getSparkJobSpimData2("", xmlPath);
		final List< ViewId > viewIds = Spark.deserializeViewIds( serializedViewIds );

		// If requested, preserve the anisotropy of the data (such that
		// output data has the same anisotropy as input data) by prepending
		// an affine to each ViewRegistration
		if ( preserveAnisotropy )
		{
			final AffineTransform3D aniso = new AffineTransform3D();
			aniso.set(
					1.0, 0.0, 0.0, 0.0,
					0.0, 1.0, 0.0, 0.0,
					0.0, 0.0, 1.0 / anisotropyFactor, 0.0 );
			final ViewTransformAffine preserveAnisotropy = new ViewTransformAffine( "preserve anisotropy", aniso );

			final ViewRegistrations registrations = dataLocal.getViewRegistrations();
			for ( final ViewId viewId : viewIds )
			{
				final ViewRegistration vr = registrations.getViewRegistration( viewId );
				vr.preconcatenateTransform( preserveAnisotropy );
				vr.updateModel();
			}
		}

		final long[] fusedBlockMin = new long[ n ];
		final long[] fusedBlockMax = new long[ n ];
		final Interval fusedBlock = FinalInterval.wrap( fusedBlockMin, fusedBlockMax );

		// pre-filter views that overlap the superBlock
		Arrays.setAll( fusedBlockMin, d -> superBlockOffset[ d ] );
		Arrays.setAll( fusedBlockMax, d -> superBlockOffset[ d ] + superBlockSize[ d ] - 1 );

		final List< ViewId > overlappingViews = WriteSuperBlock.findOverlappingViews( dataLocal, viewIds, fusedBlock );

		final N5Writer executorVolumeWriter = N5Util.createWriter( n5Path, storageType );

		final Img<UnsignedByteType> img = ArrayImgs.unsignedBytes( fusedBlock.dimensionsAsLongArray() );
		final RandomAccessibleInterval<UnsignedByteType> block = Views.translate( img, fusedBlockMin );

		for ( final ViewId viewId : overlappingViews )
		{
			final Cursor<UnsignedByteType> c = Views.iterable( block ).localizingCursor();
			final double[] l = new double[ 3 ];

			final Interval dim = new FinalInterval( ViewUtil.getDimensions( dataLocal, viewId ) );
			final ViewRegistration vr = ViewUtil.getViewRegistration( dataLocal, viewId );
			final AffineTransform3D model = vr.getModel();

			final double[] min = new double[ 3 ];
			final double[] max = new double[ 3 ];

			Arrays.setAll( min, d -> dim.min( d ) - maskOffset[ d ] );
			Arrays.setAll( max, d -> dim.max( d ) + maskOffset[ d ] );

A:			while ( c.hasNext() )
			{
				final UnsignedByteType t = c.next();

				if ( t.get() > 0 )
					continue;

				c.localize(l);
				model.applyInverse(l, l);

				for ( int d = 0; d < 3; ++d )
					if ( l[ d ] < min[ d ] || l[ d ] > max[ d ] )
						continue A;

				t.set( 255 );
			}
		}

		if ( uint8 )
		{
			N5Utils.saveBlock(img, executorVolumeWriter, n5Dataset, gridBlock[2]);
		}
		else if ( uint16 )
		{
			final RandomAccessibleInterval< UnsignedShortType > sourceUINT16 =
					Converters.convertRAI(
							img,
							(i, o) -> o.setInteger( i.get() > 0 ? 65535 : 0 ),
							new UnsignedShortType());

			N5Utils.saveBlock(sourceUINT16, executorVolumeWriter, n5Dataset, gridBlock[2]);
		}
		else
		{
			final RandomAccessibleInterval< FloatType > sourceFloat =
					Converters.convertRAI(
							img,
							(i, o) -> o.set( i.get() > 0 ? 1.0f : 0.0f ),
							new FloatType());

			N5Utils.saveBlock(sourceFloat, executorVolumeWriter, n5Dataset, gridBlock[2]);
		}
	}
}
