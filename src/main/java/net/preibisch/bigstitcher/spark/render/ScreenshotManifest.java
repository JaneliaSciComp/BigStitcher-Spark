/*-
 * #%L
 * Spark-based parallel BigStitcher project.
 * %%
 * Copyright (C) 2021 - 2026 Developers.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */
package net.preibisch.bigstitcher.spark.render;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import net.imglib2.realtransform.AffineTransform3D;

/**
 * What {@code screenshot} rendered: one entry per image with the camera and the display settings, written as
 * JSON with {@code --json}; and the reading/writing of viewer-transform files in the format BigStitcher's BDV
 * fly-through saves (a JSON array of 12-value row-major affines, global to screen pixels).
 */
public class ScreenshotManifest
{
	public String xml;
	public int timepointId;
	public String render;
	public double margin;
	public List< Entry > images = new ArrayList<>();

	public static class Entry
	{
		/** file name relative to the output directory */
		public String file;
		/** pair, overview, single or transform */
		public String kind;
		public int timepointId;
		/** the view setups shown, for a pair A then B */
		public int[] setups;
		/** xy, xz, yz; null for transform */
		public String plane;
		/** slice or mip */
		public String render;
		/** depth and step of a mip in global pixels; null for a slice */
		public Double depth;
		public Double step;
		public int frames;
		/** the box the camera was centred on, in global pixels; null for transform */
		public double[] boxMin;
		public double[] boxMax;
		/** the viewer transform, 12 values row-major, global -> image pixels (same layout as a transform file) */
		public double[] viewerTransform;
		public int width;
		public int height;
		/** image pixels per global pixel */
		public double scale;
		public long renderMillis;
		/** setup -> #rrggbb */
		public Map< String, String > colors = new LinkedHashMap<>();
		/** setup -> [min, max] */
		public Map< String, double[] > ranges = new LinkedHashMap<>();
	}

	public static Gson gson()
	{
		return new GsonBuilder().setPrettyPrinting().registerTypeAdapter( AffineTransform3D.class, new AffineTransform3DAdapter() ).create();
	}

	public void save( final File file ) throws IOException
	{
		try ( final Writer w = new FileWriter( file ) )
		{
			gson().toJson( this, w );
		}
	}

	public static ScreenshotManifest load( final File file ) throws IOException
	{
		try ( final Reader r = new FileReader( file ) )
		{
			return gson().fromJson( r, ScreenshotManifest.class );
		}
	}

	/** reads a transform file (JSON array of 12-value affines) */
	public static List< AffineTransform3D > loadTransforms( final File file ) throws IOException
	{
		try ( final Reader r = new FileReader( file ) )
		{
			final AffineTransform3D[] t = gson().fromJson( r, AffineTransform3D[].class );
			if ( t == null )
				throw new IOException( file + " holds no transforms" );
			return new ArrayList<>( Arrays.asList( t ) );
		}
	}

	public static void saveTransforms( final List< AffineTransform3D > transforms, final File file ) throws IOException
	{
		try ( final Writer w = new FileWriter( file ) )
		{
			gson().toJson( transforms.toArray( new AffineTransform3D[ 0 ] ), w );
		}
	}

	/** an AffineTransform3D as the JSON array of its 12 row-major values (as BDVFlyThrough writes it) */
	public static class AffineTransform3DAdapter implements JsonSerializer< AffineTransform3D >, JsonDeserializer< AffineTransform3D >
	{
		@Override
		public JsonElement serialize( final AffineTransform3D src, final Type typeOfSrc, final JsonSerializationContext context )
		{
			return context.serialize( src.getRowPackedCopy() );
		}

		@Override
		public AffineTransform3D deserialize( final JsonElement json, final Type typeOfT, final JsonDeserializationContext context ) throws JsonParseException
		{
			final double[] values = context.deserialize( json, double[].class );
			if ( values == null || values.length != 12 )
				throw new JsonParseException( "an affine transform needs 12 values, got " + ( values == null ? "null" : values.length ) );
			final AffineTransform3D t = new AffineTransform3D();
			t.set( values );
			return t;
		}
	}
}
