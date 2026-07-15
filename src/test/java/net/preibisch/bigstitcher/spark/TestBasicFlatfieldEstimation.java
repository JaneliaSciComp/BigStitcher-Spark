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
package net.preibisch.bigstitcher.spark;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class TestBasicFlatfieldEstimation
{
	@Test
	public void testShardSizeUsesLongArithmetic()
	{
		assertArrayEquals(
				new int[] { 2048, 2048 },
				BasicFlatfieldEstimation.shardSize( new int[] { 128, 128 }, new int[] { 16, 16 } ) );

		assertThrows(
				IllegalArgumentException.class,
				() -> BasicFlatfieldEstimation.shardSize( new int[] { Integer.MAX_VALUE, 1 }, new int[] { 2, 1 } ) );
	}

	@Test
	public void testRejectOversizedN5BlockAllocation()
	{
		BasicFlatfieldEstimation.validateN5BlockElementCount( "chunk", new int[] { 32768, 32768 } );

		assertThrows(
				IllegalArgumentException.class,
				() -> BasicFlatfieldEstimation.validateN5BlockElementCount( "chunk", new int[] { 65536, 65536 } ) );
	}

	@Test
	public void testRejectMalformedBlockOptions()
	{
		assertThrows(
				IllegalArgumentException.class,
				() -> BasicFlatfieldEstimation.validateBlockSizeOption( "--blockSize", new int[] { 128, 0 }, 2 ) );
		assertThrows(
				IllegalArgumentException.class,
				() -> BasicFlatfieldEstimation.validateBlockSizeOption( "--blockSize", new int[] { 128, 128, 1 }, 2 ) );
	}
}
