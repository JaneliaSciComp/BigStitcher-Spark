package net.preibisch.bigstitcher.spark.util.bdv;

/**
 * Per-source normalization mode for the overlay-landmarks viewer, mirroring
 * hot-knife's {@code VNCMovie.Normalization}.
 */
public enum Normalization
{
	NONE,
	CLLCN,
	CLAHE,
	CLAHE_WITH_THRESHOLDMASK
}
