package net.preibisch.mvrecon.dataset;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import loci.common.services.ServiceFactory;
import loci.formats.FormatTools;
import loci.formats.IFormatReader;
import loci.formats.meta.IMetadata;
import loci.formats.meta.MetadataRetrieve;
import loci.formats.services.OMEXMLService;
import net.preibisch.mvrecon.fiji.spimdata.imgloaders.util.BioformatsReaderUtils;
import ome.units.UNITS;
import ome.units.quantity.Length;

/**
 * Prints the metadata of a Nikon ND2 file as Bio-Formats (ND2Reader) reports it: series (= XY stage positions),
 * sizes, voxel size, channels, the stage position of every series (stored as OME plane positions, ND2Reader sets
 * no stage labels), the Z positions of the planes, selected global keys and a summary of the tile grid
 * (step between neighbouring positions versus the field of view, i.e. the overlap).
 *
 * Not a test; run it from the IDE or with the test classpath:
 * <pre>
 * ND2MetadataDebug file.nd2 [--series N] [--dumpGlobal out.txt] [--readPlane]
 * </pre>
 * --series N prints the full description of the first N series (default 3, the others get one line each),
 * --dumpGlobal writes all global metadata (sorted) to a file (an ND2 has one entry per plane, ~125k for 379 positions),
 * --readPlane reads the first plane of series 0 and prints the time and the intensity range.
 */
public class ND2MetadataDebug {

    /** global keys that matter for the orientation and calibration of the tiles */
    private static final List<String> GLOBAL_KEYS = Arrays.asList(
            "Metadata", "Rotate", "dAngle", "FlipH", "FlipV", "Cam0Flip",
            "dCalibration", "dZoom", "dObjectiveMag", "dObjectiveNA", "dRelayLensZoom", "dCameraCalibrationZoom",
            "dBinningX", "dBinningY", "Camera Name", "CameraUniqueName");

    public static class SeriesInfo {
        public final int series;
        public final int sizeX, sizeY, sizeZ, sizeC, sizeT;
        public final double[] position; // plane 0, um; null if missing
        public final double minZ, maxZ, zStep; // over the planes of channel 0, timepoint 0, um

        SeriesInfo(int series, int sizeX, int sizeY, int sizeZ, int sizeC, int sizeT,
                   double[] position, double minZ, double maxZ, double zStep) {
            this.series = series;
            this.sizeX = sizeX;
            this.sizeY = sizeY;
            this.sizeZ = sizeZ;
            this.sizeC = sizeC;
            this.sizeT = sizeT;
            this.position = position;
            this.minZ = minZ;
            this.maxZ = maxZ;
            this.zStep = zStep;
        }

        String dims() {
            return String.format("%d x %d x %d, C=%d, T=%d", sizeX, sizeY, sizeZ, sizeC, sizeT);
        }
    }

    private static Double um(Length l) {
        if (l == null || l.value(UNITS.MICROMETER) == null)
            return null;
        return l.value(UNITS.MICROMETER).doubleValue();
    }

    private static String fmt(Double d) {
        return d == null ? "null" : String.format("%.3f", d);
    }

    public static void main(String[] args) throws Exception {
        String filePath = args.length > 0 && !args[0].startsWith("--")
                ? args[0]
                : "/nrs/liu/Yumin/wholebrain8.11.25/b5/Time00006_Channel640,561,488,405_Seq0006.nd2";

        int nDetailedSeries = 3;
        File dumpGlobalFile = null;
        boolean readPlane = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--series":
                    nDetailedSeries = Integer.parseInt(args[++i]);
                    break;
                case "--dumpGlobal":
                    dumpGlobalFile = new File(args[++i]);
                    break;
                case "--readPlane":
                    readPlane = true;
                    break;
                default:
                    if (args[i].startsWith("--"))
                        throw new IllegalArgumentException("Unknown option " + args[i]);
            }
        }

        File nd2File = new File(filePath);
        if (!nd2File.exists()) {
            System.err.println("File not found: " + filePath);
            return;
        }

        System.out.println("=== ND2 Metadata Debug ===");
        System.out.println("ND2 File: " + nd2File.getAbsolutePath());
        System.out.printf("File size: %.2f GB%n", nd2File.length() / 1e9);
        System.out.println();

        IFormatReader formatReader = BioformatsReaderUtils.createImageReaderWithSetupHooks();
        IMetadata meta = new ServiceFactory().getInstance(OMEXMLService.class).createOMEXMLMetadata();
        formatReader.setMetadataStore(meta);

        long startTime = System.currentTimeMillis();
        formatReader.setId(nd2File.toString());
        System.out.println("setId() took: " + (System.currentTimeMillis() - startTime) + " ms");
        System.out.println("Reader class: " + readerClassName(formatReader));

        MetadataRetrieve retrieve = (MetadataRetrieve) formatReader.getMetadataStore();
        Hashtable<String, Object> globalMeta = formatReader.getGlobalMetadata();
        System.out.println("Global metadata entries: " + globalMeta.size());
        System.out.println();

        System.out.println("=== Selected global metadata ===");
        for (String key : GLOBAL_KEYS)
            System.out.printf("%s = %s%n", key, globalMeta.get(key));
        System.out.println();

        int seriesCount = formatReader.getSeriesCount();
        System.out.println("=== Series (XY positions): " + seriesCount + " ===");
        List<SeriesInfo> infos = new ArrayList<>();
        Map<String, Integer> dimGroups = new LinkedHashMap<>();
        for (int s = 0; s < seriesCount; s++) {
            formatReader.setSeries(s);
            SeriesInfo info = seriesInfo(formatReader, retrieve, s);
            infos.add(info);
            dimGroups.merge(info.dims(), 1, Integer::sum);

            if (s < nDetailedSeries) {
                printSeriesDetails(formatReader, retrieve, info);
            } else {
                System.out.printf("Series %d: position=%s z=[%.3f, %.3f] zStep=%.3f%n",
                        s, Arrays.toString(info.position), info.minZ, info.maxZ, info.zStep);
            }
        }
        formatReader.setSeries(0);
        System.out.println();

        System.out.println("=== Series grouped by size ===");
        dimGroups.forEach((dims, n) -> System.out.printf("%d series of %s%n", n, dims));
        System.out.println();

        printGridSummary(infos, um(retrieve.getPixelsPhysicalSizeX(0)), um(retrieve.getPixelsPhysicalSizeY(0)));

        if (readPlane) {
            System.out.println("=== Read first plane of series 0 ===");
            readFirstPlane(formatReader);
            System.out.println();
        }

        if (dumpGlobalFile != null) {
            try (PrintWriter w = new PrintWriter(dumpGlobalFile, StandardCharsets.UTF_8)) {
                new TreeMap<>(globalMeta).forEach((k, v) -> w.println(k + " = " + v));
            }
            System.out.println("Wrote " + globalMeta.size() + " global metadata entries to " + dumpGlobalFile.getAbsolutePath());
        }

        formatReader.close();
    }

    private static String readerClassName(IFormatReader formatReader) {
        if (formatReader instanceof loci.formats.ImageReader)
            return ((loci.formats.ImageReader) formatReader).getReader().getClass().getName();
        return formatReader.getClass().getName();
    }

    private static SeriesInfo seriesInfo(IFormatReader formatReader, MetadataRetrieve retrieve, int series) {
        double[] position = null;
        if (retrieve.getPlaneCount(series) > 0) {
            Double x = um(retrieve.getPlanePositionX(series, 0));
            Double y = um(retrieve.getPlanePositionY(series, 0));
            Double z = um(retrieve.getPlanePositionZ(series, 0));
            if (x != null || y != null || z != null)
                position = new double[]{x == null ? 0 : x, y == null ? 0 : y, z == null ? 0 : z};
        }

        // Z positions of the planes of channel 0, timepoint 0, in the order of the z index
        double minZ = Double.NaN, maxZ = Double.NaN, zStep = Double.NaN;
        int sizeZ = formatReader.getSizeZ();
        double[] zs = new double[sizeZ];
        boolean allZ = true;
        for (int z = 0; z < sizeZ; z++) {
            int plane = formatReader.getIndex(z, 0, 0);
            Double pz = plane < retrieve.getPlaneCount(series) ? um(retrieve.getPlanePositionZ(series, plane)) : null;
            if (pz == null) {
                allZ = false;
                break;
            }
            zs[z] = pz;
        }
        if (allZ && sizeZ > 0) {
            minZ = Arrays.stream(zs).min().getAsDouble();
            maxZ = Arrays.stream(zs).max().getAsDouble();
            zStep = sizeZ > 1 ? (zs[sizeZ - 1] - zs[0]) / (sizeZ - 1) : 0;
        }

        return new SeriesInfo(series,
                formatReader.getSizeX(), formatReader.getSizeY(), sizeZ,
                formatReader.getSizeC(), formatReader.getSizeT(),
                position, minZ, maxZ, zStep);
    }

    private static void printSeriesDetails(IFormatReader formatReader, MetadataRetrieve retrieve, SeriesInfo info) {
        int s = info.series;
        System.out.printf("Series %d: size=%s order=%s pixelType=%s bitsPerPixel=%d rgb=%s orderCertain=%s planes=%d%n",
                s, info.dims(),
                formatReader.getDimensionOrder(),
                FormatTools.getPixelTypeString(formatReader.getPixelType()),
                formatReader.getBitsPerPixel(),
                formatReader.isRGB(),
                formatReader.isOrderCertain(),
                retrieve.getPlaneCount(s));
        System.out.printf("  voxel size (um): %s x %s x %s%n",
                fmt(um(retrieve.getPixelsPhysicalSizeX(s))),
                fmt(um(retrieve.getPixelsPhysicalSizeY(s))),
                fmt(um(retrieve.getPixelsPhysicalSizeZ(s))));
        System.out.printf("  plane 0 position (um): %s%n", Arrays.toString(info.position));
        System.out.printf("  z positions (um): [%.3f, %.3f], step %.3f%n", info.minZ, info.maxZ, info.zStep);
        Length stageX = null;
        try {
            stageX = retrieve.getStageLabelX(s);
        } catch (Exception e) {
            // no stage label
        }
        System.out.println("  stage label X: " + (stageX == null ? "none" : fmt(um(stageX))));
        int channels = retrieve.getChannelCount(s);
        for (int c = 0; c < channels; c++) {
            System.out.printf("  channel %d: name=%s excitation=%s emission=%s%n",
                    c,
                    retrieve.getChannelName(s, c),
                    retrieve.getChannelExcitationWavelength(s, c),
                    retrieve.getChannelEmissionWavelength(s, c));
        }
    }

    /**
     * Neighbours along x: the closest position with |dy| < half a field of view and dx > 0 (likewise along y).
     * The median step is compared with the field of view of the first series.
     */
    private static void printGridSummary(List<SeriesInfo> infos, Double resX, Double resY) {
        System.out.println("=== Tile grid ===");
        List<SeriesInfo> withPos = new ArrayList<>();
        for (SeriesInfo i : infos)
            if (i.position != null)
                withPos.add(i);
        if (withPos.isEmpty() || resX == null || resY == null) {
            System.out.println("No positions or no voxel size, nothing to summarize");
            System.out.println();
            return;
        }

        double fovX = withPos.get(0).sizeX * resX;
        double fovY = withPos.get(0).sizeY * resY;
        System.out.printf("Field of view (um): %.3f x %.3f%n", fovX, fovY);

        double[] min = {Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE};
        double[] max = {-Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE};
        for (SeriesInfo i : withPos)
            for (int d = 0; d < 3; d++) {
                min[d] = Math.min(min[d], i.position[d]);
                max[d] = Math.max(max[d], i.position[d]);
            }
        System.out.printf("Position range (um): x [%.3f, %.3f] y [%.3f, %.3f] z [%.3f, %.3f]%n",
                min[0], max[0], min[1], max[1], min[2], max[2]);
        System.out.printf("Extent incl. one field of view (um): %.3f x %.3f%n",
                max[0] - min[0] + fovX, max[1] - min[1] + fovY);

        double[] stepsX = neighbourSteps(withPos, 0, fovY / 2);
        double[] stepsY = neighbourSteps(withPos, 1, fovX / 2);
        printSteps("x", stepsX, fovX);
        printSteps("y", stepsY, fovY);
        System.out.println();
    }

    private static double[] neighbourSteps(List<SeriesInfo> infos, int dim, double maxOrthogonal) {
        int other = 1 - dim;
        List<Double> steps = new ArrayList<>();
        for (SeriesInfo a : infos) {
            double best = Double.MAX_VALUE;
            for (SeriesInfo b : infos) {
                double d = b.position[dim] - a.position[dim];
                if (d > 0 && Math.abs(b.position[other] - a.position[other]) < maxOrthogonal)
                    best = Math.min(best, d);
            }
            if (best < Double.MAX_VALUE)
                steps.add(best);
        }
        return steps.stream().mapToDouble(Double::doubleValue).sorted().toArray();
    }

    private static void printSteps(String axis, double[] steps, double fov) {
        if (steps.length == 0) {
            System.out.printf("Steps along %s: no neighbours%n", axis);
            return;
        }
        double median = steps[steps.length / 2];
        System.out.printf("Steps along %s (um): %d neighbours, min %.3f median %.3f max %.3f -> overlap at median %.3f um (%.2f%% of the field of view)%n",
                axis, steps.length, steps[0], median, steps[steps.length - 1], fov - median, 100 * (fov - median) / fov);
    }

    private static void readFirstPlane(IFormatReader formatReader) throws Exception {
        formatReader.setSeries(0);
        long t0 = System.currentTimeMillis();
        byte[] bytes = formatReader.openBytes(0);
        long t = System.currentTimeMillis() - t0;
        int bpp = FormatTools.getBytesPerPixel(formatReader.getPixelType());
        boolean little = formatReader.isLittleEndian();
        long minV = Long.MAX_VALUE, maxV = Long.MIN_VALUE;
        double sum = 0;
        int n = bytes.length / bpp;
        for (int i = 0; i < n; i++) {
            long v;
            if (bpp == 1) {
                v = bytes[i] & 0xff;
            } else if (bpp == 2) {
                int lo = bytes[2 * i] & 0xff, hi = bytes[2 * i + 1] & 0xff;
                v = little ? (hi << 8 | lo) : (lo << 8 | hi);
            } else {
                System.out.println("Read " + bytes.length + " bytes in " + t + " ms (" + bpp + " bytes per pixel, no statistics)");
                return;
            }
            minV = Math.min(minV, v);
            maxV = Math.max(maxV, v);
            sum += v;
        }
        System.out.printf("Read %d bytes in %d ms: min %d max %d mean %.1f%n", bytes.length, t, minV, maxV, sum / n);
    }
}
