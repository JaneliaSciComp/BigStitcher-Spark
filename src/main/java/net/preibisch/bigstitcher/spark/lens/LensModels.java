/*-
 * #%L
 * Spark-based parallel BigStitcher project.
 * %%
 * Copyright (C) 2021 - 2024 Developers.
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
package net.preibisch.bigstitcher.spark.lens;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import mpicbg.models.CoordinateTransform;
import mpicbg.models.CoordinateTransformList;
import mpicbg.trakem2.transform.AffineModel2D;
import mpicbg.trakem2.transform.NonLinearCoordinateTransform;
import mpicbg.trakem2.transform.TranslationModel2D;

/**
 * Reads the lens-model JSON produced by the aberration-correction project, i.e.
 * an array of per-channel entries:
 *
 * <pre>
 * [ { "name": "Cam1-T1, G4_SG1, 498nm em",
 *     "transform": [ { "className": "...NonLinearCoordinateTransform", "dataString": "5 21 ..." },
 *                    { "className": "...AffineModel2D",                "dataString": "0.999 ..." } ] } ]
 * </pre>
 *
 * This is the apply-only port from
 * {@code org.janelia.saalfeldlab.confocallens.LensModels}: it parses the JSON,
 * instantiates the mpicbg TrakEM2 transforms, and matches a model to a view by
 * channel name (substring). The model-estimation code is intentionally omitted.
 */
public class LensModels
{
	/** Serialisation shape of one transform inside an entry. */
	public static class TransformSpec
	{
		public String className;
		public String dataString;

		public TransformSpec() {}

		public TransformSpec( final String className, final String dataString )
		{
			this.className = className;
			this.dataString = dataString;
		}
	}

	/** Serialisation shape of one channel's model. */
	public static class ModelSpec
	{
		public String name;
		public List< TransformSpec > transform;
	}

	/** One channel's model, with its transforms already instantiated. */
	public static class Model
	{
		public final String name;
		public final NonLinearCoordinateTransform nonLinear;
		public final AffineModel2D affine;
		/** Image size the non-linear model was fitted at. */
		public final int fittedWidth;
		public final int fittedHeight;

		Model( final String name, final NonLinearCoordinateTransform nonLinear,
				final AffineModel2D affine, final int fittedWidth, final int fittedHeight )
		{
			this.name = name;
			this.nonLinear = nonLinear;
			this.affine = affine;
			this.fittedWidth = fittedWidth;
			this.fittedHeight = fittedHeight;
		}

		/**
		 * The transform to apply to a plane.
		 *
		 * @param includeAffine whether to append the per-channel affine, which
		 *            carries the inter-channel co-registration residual
		 */
		public CoordinateTransform toTransform( final boolean includeAffine )
		{
			if ( !includeAffine || affine == null )
				return nonLinear;
			final CoordinateTransformList< CoordinateTransform > list =
					new CoordinateTransformList< CoordinateTransform >();
			list.add( nonLinear );
			list.add( affine );
			return list;
		}
	}

	private LensModels() {}

	/** Parse the lens-model JSON text into instantiated {@link Model}s. */
	public static List< Model > load( final String jsonText, final String source ) throws IOException
	{
		final Gson gson = new Gson();
		final List< ModelSpec > specs = gson.fromJson(
				jsonText, new TypeToken< List< ModelSpec > >() {}.getType() );
		if ( specs == null || specs.isEmpty() )
			throw new IOException( "No lens models found in " + source );

		final List< Model > models = new ArrayList< Model >( specs.size() );
		for ( int i = 0; i < specs.size(); ++i )
			models.add( toModel( specs.get( i ), i, source ) );
		return models;
	}

	private static Model toModel( final ModelSpec spec, final int index, final String source )
			throws IOException
	{
		if ( spec.transform == null || spec.transform.isEmpty() )
			throw new IOException( String.format(
					"Lens model %d (%s) in %s has no transforms", index, spec.name, source ) );

		NonLinearCoordinateTransform nonLinear = null;
		AffineModel2D affine = null;
		for ( final TransformSpec t : spec.transform )
		{
			if ( t.className == null || t.dataString == null )
				continue;
			// Matched by class-name suffix rather than Class.forName, so both
			// "mpicbg.trakem2.transform.NonLinearCoordinateTransform" and the
			// TrakEM2-internal "lenscorrection.NonLinearTransform" are accepted.
			final String simple = t.className.substring( t.className.lastIndexOf( '.' ) + 1 );
			if ( simple.equals( "NonLinearTransform" ) || simple.equals( "NonLinearCoordinateTransform" ) )
			{
				nonLinear = new NonLinearCoordinateTransform();
				nonLinear.init( t.dataString );
			}
			else if ( simple.equals( "AffineModel2D" ) )
			{
				affine = new AffineModel2D();
				affine.init( t.dataString );
			}
			else if ( simple.equals( "TranslationModel2D" ) )
			{
				final TranslationModel2D translation = new TranslationModel2D();
				translation.init( t.dataString );
				affine = new AffineModel2D();
				affine.set( translation.createAffine() );
			}
		}

		if ( nonLinear == null )
		{
			final List< String > seen = new ArrayList< String >();
			for ( final TransformSpec t : spec.transform )
				seen.add( String.valueOf( t.className ) );
			throw new IOException( String.format(
					"Lens model %d (%s) in %s has no non-linear transform; found %s",
					index, spec.name, source, seen ) );
		}

		final int[] fitted = parseFittedSize( spec.transform, index, spec.name, source );
		return new Model( spec.name == null ? "model " + index : spec.name,
				nonLinear, affine, fitted[ 0 ], fitted[ 1 ] );
	}

	/**
	 * A {@code NonLinearCoordinateTransform} data string ends with the width and
	 * height it was fitted at. The class keeps those fields protected and exposes
	 * no getters, so they are read back out of the data string.
	 */
	private static int[] parseFittedSize(
			final List< TransformSpec > transforms,
			final int index,
			final String name,
			final String source ) throws IOException
	{
		for ( final TransformSpec t : transforms )
		{
			if ( t.className == null || t.dataString == null )
				continue;
			final String simple = t.className.substring( t.className.lastIndexOf( '.' ) + 1 );
			if ( !simple.equals( "NonLinearTransform" ) && !simple.equals( "NonLinearCoordinateTransform" ) )
				continue;
			final String[] tokens = t.dataString.trim().split( "\\s+" );
			if ( tokens.length < 2 )
				break;
			try
			{
				return new int[] {
						( int ) Double.parseDouble( tokens[ tokens.length - 2 ] ),
						( int ) Double.parseDouble( tokens[ tokens.length - 1 ] ) };
			}
			catch ( final NumberFormatException e )
			{
				break;
			}
		}
		throw new IOException( String.format(
				"Could not read the fitted image size from lens model %d (%s) in %s",
				index, name, source ) );
	}

	/**
	 * Find the unique model whose {@link Model#name} contains {@code channelName}
	 * as a substring (e.g. channel "Cam1-T1" matches
	 * "lightsheet, Cam1-T1, G4_SG1, 498nm em").
	 *
	 * @return the matching model, or {@code null} if none matches (including when
	 *         {@code channelName} is null/empty)
	 * @throws IllegalArgumentException if more than one model matches (ambiguous)
	 */
	public static Model findForChannel( final List< Model > models, final String channelName )
	{
		if ( channelName == null || channelName.isEmpty() )
			return null;

		Model match = null;
		for ( final Model m : models )
		{
			if ( m.name != null && m.name.contains( channelName ) )
			{
				if ( match != null )
					throw new IllegalArgumentException(
							"Channel name '" + channelName + "' matches more than one lens model ('"
									+ match.name + "' and '" + m.name + "'); names must be unambiguous." );
				match = m;
			}
		}
		return match;
	}
}
