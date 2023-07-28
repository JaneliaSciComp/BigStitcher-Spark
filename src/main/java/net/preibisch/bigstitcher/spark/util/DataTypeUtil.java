package net.preibisch.bigstitcher.spark.util;

import org.janelia.saalfeldlab.n5.DataType;

import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.ShortType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;

public class DataTypeUtil {

	@SuppressWarnings("unchecked")
	public static < T extends RealType< T > & NativeType< T > > T toType( final DataType dataType )
	{
		if ( dataType == DataType.UINT8 )
			return (T)(Object)new UnsignedByteType();
		else if ( dataType == DataType.UINT16 )
			return (T)(Object)new UnsignedShortType();
		else if ( dataType == DataType.UINT32 )
			return (T)(Object)new UnsignedIntType();
		else if ( dataType == DataType.UINT64 )
			return (T)(Object)new UnsignedLongType();
		else if ( dataType == DataType.INT8 )
			return (T)(Object)new ByteType();
		else if ( dataType == DataType.INT16 )
			return (T)(Object)new ShortType();
		else if ( dataType == DataType.INT32 )
			return (T)(Object)new IntType();
		else if ( dataType == DataType.INT64 )
			return (T)(Object)new LongType();
		else if ( dataType == DataType.FLOAT32 )
			return (T)(Object)new FloatType();
		else if ( dataType == DataType.FLOAT64 )
			return (T)(Object)new DoubleType();
		else
			throw new RuntimeException( "DataType '" + dataType.toString() + "' is unknown." );
	}
}
