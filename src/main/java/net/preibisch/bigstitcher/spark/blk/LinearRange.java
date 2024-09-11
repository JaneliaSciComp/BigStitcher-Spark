package net.preibisch.bigstitcher.spark.blk;

import net.imglib2.algorithm.blocks.AbstractDimensionlessBlockProcessor;
import net.imglib2.algorithm.blocks.BlockProcessor;
import net.imglib2.algorithm.blocks.DefaultUnaryBlockOperator;
import net.imglib2.algorithm.blocks.UnaryBlockOperator;
import net.imglib2.type.PrimitiveType;
import net.imglib2.type.numeric.real.FloatType;

public class LinearRange
{
	public static UnaryBlockOperator< FloatType, FloatType > linearRange( final double min, final double max )
	{
		final double offset = -min / ( max - min );
		final double scale = 1.0 / ( max - min );
		return scaleAndOffset( scale, offset );
	}

	public static UnaryBlockOperator< FloatType, FloatType > scaleAndOffset( final double scale, final double offset )
	{
		return scaleAndOffset( ( float ) scale, ( float ) offset );
	}

	public static UnaryBlockOperator< FloatType, FloatType > scaleAndOffset( final float scale, final float offset )
	{
		return new DefaultUnaryBlockOperator<>( type, type, 0, 0, new LinearRangeBlockProcessor( scale, offset ) );
	}

	private static final FloatType type = new FloatType();

	private static class LinearRangeBlockProcessor extends AbstractDimensionlessBlockProcessor< float[], float[] >
	{
		private final float scale;

		private final float offset;

		LinearRangeBlockProcessor( final float scale, final float offset )
		{
			super( PrimitiveType.FLOAT );
			this.scale = scale;
			this.offset = offset;
		}

		protected LinearRangeBlockProcessor( LinearRangeBlockProcessor proc )
		{
			super( proc );
			this.scale = proc.scale;
			this.offset = proc.offset;
		}

		@Override
		public BlockProcessor< float[], float[] > independentCopy()
		{
			return new LinearRangeBlockProcessor( this );
		}

		@Override
		public void compute( final float[] src, final float[] dest )
		{
			final int len = sourceLength();
			for ( int i = 0; i < len; i++ )
				dest[ i ] = src[ i ] * scale + offset;
		}
	}
}
