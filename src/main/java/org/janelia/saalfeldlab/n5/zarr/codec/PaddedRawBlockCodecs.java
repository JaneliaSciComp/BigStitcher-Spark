package org.janelia.saalfeldlab.n5.zarr.codec;

import java.nio.ByteOrder;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeDataCodec;
import org.janelia.saalfeldlab.n5.codec.IdentityCodec;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecs;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.zarr.DType;
import org.janelia.saalfeldlab.n5.zarr.ZarrKeyValueWriter;


/**
 * Patched version of n5-zarr's PaddedRawBlockCodecs that fixes
 * BufferUnderflowException when reading edge chunks that were stored
 * without fill-value padding (e.g., by external Zarr v3 writers).
 *
 * The decode() method now pads under-sized data to the full block size
 * using the fill value before passing it to the wrapped codec.
 */
public class PaddedRawBlockCodecs {

	public static <T> BlockCodec<T> create(
			final DataType dataType,
			final ByteOrder byteOrder,
			final int[] blockSize,
			final DataCodec codec,
			final byte[] fillBytes ) {

		final BlockCodec<T> baseCodec = RawBlockCodecs.create(dataType, byteOrder, blockSize, new IdentityCodec());
		return new PaddedRawBlockCodec<T>( baseCodec, blockSize, new DType(dataType), codec, fillBytes);
	}

	private static class PaddedRawBlockCodec<T> implements BlockCodec<T> {

		private final BlockCodec<T> wrappedBlockCodec;
		private final int[] blockSize;
		private final DataCodec codec;
		private final DType dtype;
		private final byte[] fillBytes;
		private final int expectedBytes;

		PaddedRawBlockCodec(
				final BlockCodec<T> wrappedBlockCodec,
				final int[] blockSize,
				final DType dtype,
				final DataCodec codec,
				final byte[] fillBytes) {

			this.wrappedBlockCodec = wrappedBlockCodec;
			this.blockSize = blockSize;
			this.dtype = dtype;
			this.codec = codec;

			final int nBytes = dtype.getNBytes();
			this.fillBytes = new byte[nBytes];
			System.arraycopy(fillBytes, 0, this.fillBytes, 0, nBytes);

			this.expectedBytes = DataBlock.getNumElements(blockSize) * nBytes;
		}

		@Override
		public ReadData encode(DataBlock<T> dataBlock) throws N5IOException {

			final ReadData rawBlockData = wrappedBlockCodec.encode(dataBlock);
			final ReadData blockData;

			if (Arrays.equals(blockSize, dataBlock.getSize())) {
				blockData = rawBlockData;
			} else {
				blockData = ReadData.from(
						ZarrKeyValueWriter.padCrop(
								rawBlockData.allBytes(),
								dataBlock.getSize(),
								blockSize,
								dtype.getNBytes(),
								dtype.getNBits(),
								fillBytes));
			}
			return codec.encode(blockData);
		}

		@Override
		public DataBlock<T> decode(ReadData readData, long[] gridPosition) throws N5IOException {

			ReadData decodedData = codec.decode(readData);

			// If the decoded data is smaller than the full block size
			// (edge chunks written without padding by external tools),
			// pad it with fill value bytes to prevent BufferUnderflowException.
			final byte[] bytes = decodedData.allBytes();
			if (bytes.length < expectedBytes) {
				final byte[] padded = new byte[expectedBytes];
				System.arraycopy(bytes, 0, padded, 0, bytes.length);
				final int fillLen = fillBytes.length;
				for (int i = bytes.length; i < expectedBytes; i += fillLen) {
					System.arraycopy(fillBytes, 0, padded, i, Math.min(fillLen, expectedBytes - i));
				}
				decodedData = ReadData.from(padded);
			}

			return wrappedBlockCodec.decode(decodedData, gridPosition);
		}

		@Override
		public long encodedSize(final int[] blockSize) throws UnsupportedOperationException {
			if (codec instanceof DeterministicSizeDataCodec) {
				return ((DeterministicSizeDataCodec) codec).encodedSize(wrappedBlockCodec.encodedSize(blockSize));
			} else {
				throw new UnsupportedOperationException();
			}
		}
	}

}
