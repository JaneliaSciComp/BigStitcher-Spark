package net.preibisch.bigstitcher.spark;

/**
 * Compression codecs selectable via {@code --compression}; mapped to the corresponding N5
 * {@code Compression} implementations, honouring {@code --compressionLevel} where supported.
 */
public enum Compressions {
	/** LZ4 compression (no level). */
	Lz4,
	/** Gzip compression (default level 1). */
	Gzip,
	/** Zstandard compression (default level 3); the default codec. */
	Zstandard,
	/** Blosc compression (no level). */
	Blosc,
	/** Bzip2 compression (no level). */
	Bzip2,
	/** XZ compression (default level 6). */
	Xz,
	/** No compression. */
	Raw
}
