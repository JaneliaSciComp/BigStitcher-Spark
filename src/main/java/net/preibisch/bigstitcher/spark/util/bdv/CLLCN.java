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

	public CLLCN(final FloatProcessor fp) {

		super(fp);
	}


//	g(a) = 1.0 / (a**(1.0 / (a - 1.0)))
//	f(x, a, b) = x < b ? x : (x - b + g(a))**a + b - g(a)**a


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
