package net.preibisch.bigstitcher.spark.fusion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.api.java.function.VoidFunction;
import org.janelia.saalfeldlab.n5.N5Writer;

import mpicbg.spim.data.SpimData;
import mpicbg.spim.data.registration.ViewRegistration;
import mpicbg.spim.data.registration.ViewRegistrations;
import mpicbg.spim.data.registration.ViewTransformAffine;
import mpicbg.spim.data.sequence.ViewId;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.preibisch.bigstitcher.spark.blk.Fusion;
import net.preibisch.bigstitcher.spark.blk.N5Helper;
import net.preibisch.bigstitcher.spark.util.N5Util;
import net.preibisch.bigstitcher.spark.util.Spark;
import net.preibisch.bigstitcher.spark.util.ViewUtil;
import net.preibisch.mvrecon.fiji.spimdata.SpimData2;
import net.preibisch.mvrecon.process.export.ExportN5API.StorageType;

public class WriteSuperBlock implements VoidFunction< long[][] >
{
	private static final long serialVersionUID = 4185504467908084954L;

	private final String xmlPath;

	private final boolean preserveAnisotropy;

	private final double anisotropyFactor;

	private final long[] minBB;

	private final String n5Path;

	private final String n5Dataset;

	private final String bdvString;

	private final StorageType storageType;

	private final int[][] serializedViewIds;

	private final boolean uint8;

	private final boolean uint16;

	private final double minIntensity;

	private final double range;

	private final int[] blockSize;

	public WriteSuperBlock(
			final String xmlPath,
			final boolean preserveAnisotropy,
			final double anisotropyFactor,
			final long[] minBB,
			final String n5Path,
			final String n5Dataset,
			final String bdvString,
			final StorageType storageType,
			final int[][] serializedViewIds,
			final boolean uint8,
			final boolean uint16,
			final double minIntensity,
			final double range,
			final int[] blockSize )
	{
		this.xmlPath = xmlPath;
		this.preserveAnisotropy = preserveAnisotropy;
		this.anisotropyFactor = anisotropyFactor;
		this.minBB = minBB;
		this.n5Path = n5Path;
		this.n5Dataset = n5Dataset;
		this.bdvString = bdvString;
		this.storageType = storageType;
		this.serializedViewIds = serializedViewIds;
		this.uint8 = uint8;
		this.uint16 = uint16;
		this.minIntensity = minIntensity;
		this.range = range;
		this.blockSize = blockSize;
	}

	/**
	 * Find all views among the given {@code viewIds} that overlap the given {@code interval}.
	 * The image interval of each view is transformed into world coordinates
	 * and checked for overlap with {@code interval}, with a conservative
	 * extension of 2 pixels in each direction.
	 *
	 * @param spimData contains bounds and registrations for all views
	 * @param viewIds which views to check
	 * @param interval interval in world coordinates
	 * @return views that overlap {@code interval}
	 */
	private static List<ViewId> findOverlappingViews(
			final SpimData spimData,
			final List<ViewId> viewIds,
			final Interval interval )
	{
		final List< ViewId > overlapping = new ArrayList<>();

		// expand to be conservative ...
		final Interval expandedInterval = Intervals.expand( interval, 2 );

		for ( final ViewId viewId : viewIds )
		{
			final Interval bounds = ViewUtil.getTransformedBoundingBox( spimData, viewId );
			if ( ViewUtil.overlaps( expandedInterval, bounds ) )
				overlapping.add( viewId );
		}

		return overlapping;
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
		final SpimData2 dataLocal = Spark.getSparkJobSpimData2(xmlPath);
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


		final long[] gridPos = new long[ n ];
		final long[] fusedBlockMin = new long[ n ];
		final long[] fusedBlockMax = new long[ n ];
		final Interval fusedBlock = FinalInterval.wrap( fusedBlockMin, fusedBlockMax );

		// pre-filter views that overlap the superBlock
		Arrays.setAll( fusedBlockMin, d -> superBlockOffset[ d ] );
		Arrays.setAll( fusedBlockMax, d -> superBlockOffset[ d ] + superBlockSize[ d ] - 1 );
		final List< ViewId > overlappingViews = findOverlappingViews( dataLocal, viewIds, fusedBlock );

		final N5Writer executorVolumeWriter = N5Util.createWriter( n5Path, storageType );
		final ExecutorService prefetchExecutor = Executors.newCachedThreadPool(); //Executors.newFixedThreadPool( N_PREFETCH_THREADS );

		final CellGrid blockGrid = new CellGrid( superBlockSize, blockSize );
		final int numCells = ( int ) Intervals.numElements( blockGrid.getGridDimensions() );
		for ( int gridIndex = 0; gridIndex < numCells; ++gridIndex )
		{
			blockGrid.getCellGridPositionFlat( gridIndex, gridPos );
			blockGrid.getCellInterval( gridPos, fusedBlockMin, fusedBlockMax );

			for ( int d = 0; d < n; ++d )
			{
				gridPos[ d ] += outputGridOffset[ d ];
				fusedBlockMin[ d ] += superBlockOffset[ d ];
				fusedBlockMax[ d ] += superBlockOffset[ d ];
			}
			// gridPos is now the grid coordinate in the N5 output

			// determine which Cells and Views we need to compute the fused block
			final OverlappingBlocks overlappingBlocks = OverlappingBlocks.find( dataLocal, overlappingViews, fusedBlock );

			if ( overlappingBlocks.overlappingViews().isEmpty() )
				continue;

			try ( AutoCloseable prefetched = overlappingBlocks.prefetch( prefetchExecutor ) )
			{
				NativeType type;
				if ( uint8 )
					type = new UnsignedByteType();
				else if ( uint16 )
					type = new UnsignedShortType();
				else
					type = new FloatType();

				final RandomAccessibleInterval< NativeType > source = Fusion.fuseVirtual(
						dataLocal,
						overlappingBlocks.overlappingViews(),
						fusedBlock,
						type,
						minIntensity,
						range );

				N5Helper.saveBlock( source, executorVolumeWriter, n5Dataset, gridPos );
			}
		}
		prefetchExecutor.shutdown();

		// not HDF5
		if ( N5Util.hdf5DriverVolumeWriter != executorVolumeWriter )
			executorVolumeWriter.close();
	}
}
