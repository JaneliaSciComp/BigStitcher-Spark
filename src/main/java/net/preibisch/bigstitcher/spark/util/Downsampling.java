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
package net.preibisch.bigstitcher.spark.util;

import java.util.List;

/**
 * Validation of the multi-resolution pyramid command-line options shared by the fusion and export tools.
 */
public class Downsampling
{
	/**
	 * Checks that the pyramid options are consistent: either automatic ({@code --multiRes}) or manual
	 * ({@code --downsampling}) mode may be selected, not both. Prints an explanation to {@code System.out}
	 * if the combination is invalid.
	 *
	 * @param multiRes whether automatic multi-resolution pyramid creation was requested
	 * @param downsampling the manually specified downsampling steps (e.g. {@code 2,2,1; 2,2,1; 2,2,2}),
	 *        or {@code null} if none were given
	 * @return {@code false} if both modes were selected, {@code true} otherwise
	 */
	public static boolean testDownsamplingParameters( final boolean multiRes, final List<String> downsampling )
	{
		// no not create multi-res pyramid
		if ( !multiRes && downsampling == null )
			return true;

		if ( multiRes && downsampling != null )
		{
			System.out.println( "If you want to create a multi-resolution pyramid, you must select either automatic (--multiRes) - OR - manual mode (e.g. --downsampling 2,2,1; 2,2,1; 2,2,2)");
			return false;
		}

		return true;
	}

}
