/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package net.preibisch.bigstitcher.spark.util.bdv;

import ij.process.FloatProcessor;
import mpicbg.ij.integral.BlockStatistics;

/**
 * Contrast Limited Local Contrast Normalization
 *
 * @author Stephan Saalfeld
 */
public class CLLCN extends BlockStatistics {

	/**
	 * Creates the normalizer for a {@link FloatProcessor}; the integral images of pixel values and squared
	 * values that all {@code run*} methods use for local block statistics are built here.
	 *
	 * @param fp the image to normalize in place
	 */
	public CLLCN(final FloatProcessor fp) {

		super(fp);
	}


//	g(a) = 1.0 / (a**(1.0 / (a - 1.0)))
//	f(x, a, b) = x < b ? x : (x - b + g(a))**a + b - g(a)**a


	/**
	 * Subtracts the local block mean from every pixel and re-centers the result at the midpoint of the
	 * image's display range ({@code fp.getMin()..fp.getMax()}), i.e. removes low-frequency intensity
	 * variation without changing local contrast. Operates in place on {@code fp}.
	 *
	 * @param blockRadiusX half width of the local block (the block spans {@code 2*blockRadiusX+1} pixels,
	 *        clipped at the image border)
	 * @param blockRadiusY half height of the local block
	 */
	public void runCenter(
			final int blockRadiusX,
			final int blockRadiusY) {

		final int width = fp.getWidth();
		final int height = fp.getHeight();

		final double fpMin = fp.getMin();
		final double fpLength = fp.getMax() - fpMin;
		final double fpMean = fpLength / 2.0 + fpMin;

		final int w = width - 1;
		final int h = height - 1;
		for (int y = 0; y < height; ++y) {
			final int row = y * width;
			final int yMin = Math.max(-1, y - blockRadiusY - 1);
			final int yMax = Math.min(h, y + blockRadiusY);
			final int bh = yMax - yMin;
			for (int x = 0; x < width; ++x) {
				final int xMin = Math.max(-1, x - blockRadiusX - 1);
				final int xMax = Math.min(w, x + blockRadiusX);
				final double bs = (xMax - xMin) * bh;
				final double scale = 1.0 / bs;
				final double sum = sums.getDoubleSum(xMin, yMin, xMax, yMax);
				final int i = row + x;

				final double mean = sum * scale;
				final float v = fp.getf(i);

				fp.setf(i, (float)((v - mean) + fpMean));
			}
		}
	}

	/**
	 * Stretches contrast by the local block standard deviation: each pixel's deviation from the
	 * display-range midpoint is scaled by {@code fpLength / (2 * meanFactor * std)}, so a deviation of
	 * {@code meanFactor} local standard deviations maps onto half the display range. The local mean is
	 * not removed. There is no guard against zero standard deviation. Operates in place on {@code fp}.
	 *
	 * @param blockRadiusX half width of the local block
	 * @param blockRadiusY half height of the local block
	 * @param meanFactor how many local standard deviations span half the display range
	 */
	public void runStretch(
			final int blockRadiusX,
			final int blockRadiusY,
			final float meanFactor) {

		final int width = fp.getWidth();
		final int height = fp.getHeight();

		final double fpMin = fp.getMin();
		final double fpLength = fp.getMax() - fpMin;
		final double fpMean = fpLength / 2.0 + fpMin;

		final int w = width - 1;
		final int h = height - 1;
		for (int y = 0; y < height; ++y) {
			final int row = y * width;
			final int yMin = Math.max(-1, y - blockRadiusY - 1);
			final int yMax = Math.min(h, y + blockRadiusY);
			final int bh = yMax - yMin;
			for (int x = 0; x < width; ++x) {
				final int xMin = Math.max(-1, x - blockRadiusX - 1);
				final int xMax = Math.min(w, x + blockRadiusX);
				final double bs = (xMax - xMin) * bh;
				final double scale1 = 1.0 / (bs - 1);
				final double scale2 = 1.0 / (bs * bs - bs);
				final double sum = sums.getDoubleSum(xMin, yMin, xMax, yMax);
				final double var = scale1 * sumsOfSquares.getDoubleSum(xMin, yMin, xMax, yMax) - scale2 * sum * sum;
				final int i = row + x;

				final double std = var < 0 ? 0 : Math.sqrt(var);
				final float v = fp.getf(i);
				final double d = meanFactor * std;

				fp.setf(i, (float)((v - fpMean) / 2 / d * fpLength + fpMean));
			}
		}
	}

	private static double limit(
			final double x,
			final double limit,
			final double gamma,
			final double gradientOnePointMinusLimit,
			final double limitMinusGradientOnePointPowGamma) {

//		g(gamma) = 1.0 / (gamma**(1.0 / (gamma - 1.0)))
//		f(x, gamma, threshold) = x < threshold ? x : (x - threshold + g(gamma))**gamma + threshold - g(gamma)**gamma

		return x < limit ? x : Math.pow(x + gradientOnePointMinusLimit, gamma) + limitMinusGradientOnePointPowGamma;
	}

	/**
	 * Contrast-limited variant of {@link #runStretch(int, int, float)}: the local stretch factor
	 * {@code fpLength / (meanFactor * std)} is passed through a soft limiter that is the identity below
	 * {@code limit} and continues as a power function with exponent {@code gamma} (continuous, with unit
	 * slope at {@code limit}) above it, so that nearly flat blocks are not amplified without bound.
	 * Pixels in blocks with zero standard deviation are set to the display-range midpoint. Operates in
	 * place on {@code fp}.
	 *
	 * @param blockRadiusX half width of the local block
	 * @param blockRadiusY half height of the local block
	 * @param meanFactor how many local standard deviations span half the display range
	 * @param limit stretch factor above which the limiting power function takes over
	 * @param gamma exponent of the limiting power function; must not be {@code 1} (the limiter is
	 *        undefined there), see {@link #run(int, int, float, float, float, boolean, boolean, boolean)}
	 */
	public void runStretch(
			final int blockRadiusX,
			final int blockRadiusY,
			final float meanFactor,
			final float limit,
			final float gamma) {

		final double gradientOnePoint = 1.0 / (Math.pow(gamma, 1.0 / (gamma - 1.0)));
		final double gradientOnePointMinusLimit = gradientOnePoint - limit;
		final double limitMinusGradientOnePointPowGamma = limit - Math.pow(gradientOnePoint, gamma);

		final int width = fp.getWidth();
		final int height = fp.getHeight();

		final double fpMin = fp.getMin();
		final double fpLength = fp.getMax() - fpMin;
		final double fpMean = fpLength / 2.0 + fpMin;

		final int w = width - 1;
		final int h = height - 1;
		for (int y = 0; y < height; ++y) {
			final int row = y * width;
			final int yMin = Math.max(-1, y - blockRadiusY - 1);
			final int yMax = Math.min(h, y + blockRadiusY);
			final int bh = yMax - yMin;
			for (int x = 0; x < width; ++x) {
				final int xMin = Math.max(-1, x - blockRadiusX - 1);
				final int xMax = Math.min(w, x + blockRadiusX);
				final double bs = (xMax - xMin) * bh;
				final double scale1 = 1.0 / (bs - 1);
				final double scale2 = 1.0 / (bs * bs - bs);
				final double sum = sums.getDoubleSum(xMin, yMin, xMax, yMax);
				final double var = scale1 * sumsOfSquares.getDoubleSum(xMin, yMin, xMax, yMax) - scale2 * sum * sum;
				final int i = row + x;

				final double std = var < 0 ? 0 : Math.sqrt(var);
				final float v = fp.getf(i);
				final double d = meanFactor * std;
				final double s = d == 0 ? 0 : 0.5 * limit(
						1 / d * fpLength,
						limit,
						gamma,
						gradientOnePointMinusLimit,
						limitMinusGradientOnePointPowGamma);

				fp.setf(i, (float)((v - fpMean) * s * fpLength + fpMean));
			}
		}
	}


	/**
	 * Centers and stretches in one pass: maps the local range
	 * {@code [mean - meanFactor*std, mean + meanFactor*std]} of each pixel's block linearly onto the
	 * display range {@code [fp.getMin(), fp.getMax()]}. There is no guard against zero standard
	 * deviation. Operates in place on {@code fp}.
	 *
	 * @param blockRadiusX half width of the local block
	 * @param blockRadiusY half height of the local block
	 * @param meanFactor how many local standard deviations map onto half the display range
	 */
	protected void runCenterStretch(
			final int blockRadiusX,
			final int blockRadiusY,
			final float meanFactor) {

		final int width = fp.getWidth();
		final int height = fp.getHeight();

		final double fpMin = fp.getMin();
		final double fpLength = fp.getMax() - fpMin;

		final int w = width - 1;
		final int h = height - 1;
		for (int y = 0; y < height; ++y) {
			final int row = y * width;
			final int yMin = Math.max(-1, y - blockRadiusY - 1);
			final int yMax = Math.min(h, y + blockRadiusY);
			final int bh = yMax - yMin;
			for (int x = 0; x < width; ++x) {
				final int xMin = Math.max(-1, x - blockRadiusX - 1);
				final int xMax = Math.min(w, x + blockRadiusX);
				final double bs = (xMax - xMin) * bh;
				final double scale = 1.0 / bs;
				final double scale1 = 1.0 / (bs - 1);
				final double scale2 = 1.0 / (bs * bs - bs);
				final double sum = sums.getDoubleSum(xMin, yMin, xMax, yMax);
				final double var = scale1 * sumsOfSquares.getDoubleSum(xMin, yMin, xMax, yMax) - scale2 * sum * sum;
				final int i = row + x;

				final double mean = sum * scale;
				final double std = var < 0 ? 0 : Math.sqrt(var);
				final float v = fp.getf(i);
				final double d = meanFactor * std;
				final double min = mean - d;

				fp.setf(i, (float)((v - min) / 2 / d * fpLength + fpMin));
			}
		}
	}


	/**
	 * Contrast-limited variant of {@link #runCenterStretch(int, int, float)}, using the same soft
	 * limiter as {@link #runStretch(int, int, float, float, float)}. Pixels in blocks with zero standard
	 * deviation end up as {@code NaN} (no guard). Operates in place on {@code fp}.
	 *
	 * @param blockRadiusX half width of the local block
	 * @param blockRadiusY half height of the local block
	 * @param meanFactor how many local standard deviations map onto half the display range
	 * @param limit stretch factor above which the limiting power function takes over
	 * @param gamma exponent of the limiting power function; must not be {@code 1}
	 * @param keepMinMax if {@code true}, pixels that are exactly at the display-range minimum or maximum
	 *        (e.g. background or saturated values) are left unchanged
	 */
	protected void runCenterStretch(
			final int blockRadiusX,
			final int blockRadiusY,
			final float meanFactor,
			final float limit,
			final float gamma,
			final boolean keepMinMax) {

		final double gradientOnePoint = 1.0 / (Math.pow(gamma, 1.0 / (gamma - 1.0)));
		final double gradientOnePointMinusLimit = gradientOnePoint - limit;
		final double limitMinusGradientOnePointPowGamma = limit - Math.pow(gradientOnePoint, gamma);

		final int width = fp.getWidth();
		final int height = fp.getHeight();

		final double fpMin = fp.getMin();
		final double fpMax = fp.getMax();
		final double fpLength = fpMax - fpMin;

		final int w = width - 1;
		final int h = height - 1;
		for (int y = 0; y < height; ++y) {
			final int row = y * width;
			final int yMin = Math.max(-1, y - blockRadiusY - 1);
			final int yMax = Math.min(h, y + blockRadiusY);
			final int bh = yMax - yMin;
			for (int x = 0; x < width; ++x) {
				final int xMin = Math.max(-1, x - blockRadiusX - 1);
				final int xMax = Math.min(w, x + blockRadiusX);
				final double bs = (xMax - xMin) * bh;
				final double scale = 1.0 / bs;
				final double scale1 = 1.0 / (bs - 1);
				final double scale2 = 1.0 / (bs * bs - bs);
				final double sum = sums.getDoubleSum(xMin, yMin, xMax, yMax);
				final double var = scale1 * sumsOfSquares.getDoubleSum(xMin, yMin, xMax, yMax) - scale2 * sum * sum;
				final int i = row + x;

				final double mean = sum * scale;
				final double std = var < 0 ? 0 : Math.sqrt(var);
				final float v = fp.getf(i);
				if (keepMinMax && (v == fpMin || v == fpMax))
					continue;
				final double d = meanFactor * std;
				final double s = d == 0 ? 0 : 0.5 * limit(
						1 / d * fpLength,
						limit,
						gamma,
						gradientOnePointMinusLimit,
						limitMinusGradientOnePointPowGamma);
				final double min = mean - fpLength / s * 0.5;

//				if (d != 0 )
//					System.out.println(0.5 / d * fpLength + " " + s);

				fp.setf(i, (float)((v - min) * s + fpMin));
			}
		}
	}


	/**
	 * Entry point that dispatches to the centering and/or stretching variants. With {@code gamma == 1}
	 * the unlimited variants are used and {@code limit} is ignored; otherwise the contrast-limited ones.
	 * {@code center} alone removes the local mean, {@code stretch} alone normalizes by the local standard
	 * deviation, both together map the local mean +/- {@code meanFactor} standard deviations onto the
	 * display range. Nothing happens if neither flag is set. Operates in place on {@code fp}.
	 *
	 * @param blockRadiusX half width of the local block
	 * @param blockRadiusY half height of the local block
	 * @param meanFactor how many local standard deviations span half the display range
	 * @param limit stretch factor above which the limiting power function takes over (unused if
	 *        {@code gamma == 1})
	 * @param gamma exponent of the limiting power function; {@code 1} selects the unlimited variants
	 * @param center whether to subtract the local mean
	 * @param stretch whether to normalize by the local standard deviation
	 * @param keepMinMax whether to leave pixels at the display-range min/max untouched (only honoured by
	 *        the contrast-limited center+stretch variant)
	 */
	public void run(
			final int blockRadiusX,
			final int blockRadiusY,
			final float meanFactor,
			final float limit,
			final float gamma,
			final boolean center,
			final boolean stretch,
			final boolean keepMinMax) {

		if (gamma == 1) {
			if (center) {
				if (stretch)
					runCenterStretch(blockRadiusX, blockRadiusY, meanFactor);
				else
					runCenter(blockRadiusX, blockRadiusY);
			} else {
				if (stretch)
					runStretch(blockRadiusX, blockRadiusY, meanFactor);
			}
		} else {
			if (center) {
				if (stretch)
					runCenterStretch(blockRadiusX, blockRadiusY, meanFactor, limit, gamma, keepMinMax);
				else
					runCenter(blockRadiusX, blockRadiusY);
			} else {
				if (stretch)
					runStretch(blockRadiusX, blockRadiusY, meanFactor, limit, gamma);
			}
		}
	}
}
