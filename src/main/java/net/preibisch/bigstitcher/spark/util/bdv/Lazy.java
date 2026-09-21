package net.preibisch.bigstitcher.spark.util.bdv;

import java.util.Set;
import java.util.function.Consumer;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.ReadOnlyCachedCellImgFactory;
import net.imglib2.cache.img.ReadOnlyCachedCellImgOptions;
import net.imglib2.img.basictypeaccess.AccessFlags;
import net.imglib2.type.NativeType;
import net.imglib2.util.Intervals;

/**
 * Minimal port of hot-knife's {@code org.janelia.saalfeldlab.hotknife.util.Lazy}:
 * just the {@code process(Consumer)}-based factory used by
 * {@link OverlayLandmarks#createMipmapSource}. The full hot-knife
 * implementation also exposes ImageJ-Ops-based overloads which require
 * dependencies BSS doesn't carry; those are omitted here.
 */
public class Lazy
{
	private Lazy() {}

	/**
	 * Build a {@link CachedCellImg} of the given dimensions/type/block-size that
	 * is populated on demand by {@code op}. The operator receives, per
	 * requested cell, a {@code RandomAccessibleInterval} writable view at the
	 * cell's interval and must fill it.
	 */
	public static < O extends NativeType< O > > CachedCellImg< O, ? > process(
			final RandomAccessibleInterval< O > targetInterval,
			final int[] blockSize,
			final O type,
			final Set< AccessFlags > accessFlags,
			final Consumer< RandomAccessibleInterval< O > > op )
	{
		final long[] dims = Intervals.dimensionsAsLongArray( targetInterval );
		final ReadOnlyCachedCellImgOptions options = ReadOnlyCachedCellImgOptions.options()
				.cellDimensions( blockSize )
				.accessFlags( accessFlags );
		final CellLoader< O > loader = cell -> op.accept( cell );
		return new ReadOnlyCachedCellImgFactory( options ).create( dims, type, loader );
	}
}
