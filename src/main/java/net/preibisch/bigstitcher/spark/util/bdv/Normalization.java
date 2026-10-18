package net.preibisch.bigstitcher.spark.util.bdv;

/**
 * Per-source normalization mode for the overlay-landmarks viewer, mirroring
 * hot-knife's {@code VNCMovie.Normalization}.
 */
public enum Normalization
{
	/** Show the raw intensities, no filtering. */
	NONE,
	/** Contrast-limited local contrast normalization (the {@code CLLCN} filter in this package). */
	CLLCN,
	/** Contrast-limited adaptive histogram equalization. */
	CLAHE,
	/** CLAHE restricted to pixels inside a threshold mask, so that background does not drive the equalization. */
	CLAHE_WITH_THRESHOLDMASK
}
