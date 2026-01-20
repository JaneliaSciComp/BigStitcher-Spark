package net.preibisch.mvrecon.dataset;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import loci.formats.in.DefaultMetadataOptions;
import loci.formats.in.MetadataLevel;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import loci.common.services.ServiceFactory;
import loci.formats.ChannelSeparator;
import loci.formats.IFormatReader;
import loci.formats.meta.MetadataRetrieve;
import loci.formats.ome.OMEXMLMetadataImpl;
import loci.formats.services.OMEXMLService;
import ome.units.UNITS;
import ome.units.quantity.Length;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;

public class CZIMetadataDebug {

    /**
     * Simple class to hold tile position from MVL file
     */
    public static class MvlTilePosition {
        public final int index;
        public final double x;
        public final double y;
        public final double z;

        public MvlTilePosition(int index, double x, double y, double z) {
            this.index = index;
            this.x = x;
            this.y = y;
            this.z = z;
        }

        @Override
        public String toString() {
            return String.format("MvlTile[%d: (%.2f, %.2f, %.2f)]", index, x, y, z);
        }
    }

    /**
     * Parses an MVL file and extracts tile positions from Entry elements.
     * Entry elements have attributes like PositionX, PositionY, PositionZ.
     */
    public static List<MvlTilePosition> parseMvlFile(File mvlFile) throws Exception {
        List<MvlTilePosition> positions = new ArrayList<>();

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(false);
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(mvlFile);

        // Look for Entry elements (Entry1, Entry2, etc.) which contain position data
        NodeList allElements = doc.getElementsByTagName("*");
        int entryIndex = 0;
        for (int i = 0; i < allElements.getLength(); i++) {
            Element el = (Element) allElements.item(i);
            String tagName = el.getTagName();

            // Match Entry1, Entry2, Entry3, etc.
            if (tagName.startsWith("Entry") && el.hasAttribute("PositionX")) {
                double posX = Double.parseDouble(el.getAttribute("PositionX"));
                double posY = el.hasAttribute("PositionY") ? Double.parseDouble(el.getAttribute("PositionY")) : 0;
                double posZ = el.hasAttribute("PositionZ") ? Double.parseDouble(el.getAttribute("PositionZ")) : 0;

                positions.add(new MvlTilePosition(entryIndex, posX, posY, posZ));
                entryIndex++;
            }
        }

        System.out.println("MVL: Found " + positions.size() + " Entry elements with positions");

        return positions;
    }

    /**
     * Extracts the index number from a metadata key like "Something #3" -> 3
     */
    private static int extractImageNumberFromKey(String key) {
        int hashIdx = key.lastIndexOf('#');
        if (hashIdx >= 0 && hashIdx < key.length() - 1) {
            try {
                return Integer.parseInt(key.substring(hashIdx + 1).trim());
            } catch (NumberFormatException e) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Formats a position value, converting from meters to micrometers if needed.
     */
    private static String formatPositionValue(Object val) {
        if (val == null) return "null";
        try {
            double d = Double.parseDouble(val.toString());
            // If value is very small (looks like meters), convert to micrometers
            if (Math.abs(d) < 1 && Math.abs(d) > 0) {
                return String.format("%.6f m = %.2f µm", d, d * 1e6);
            } else {
                return String.format("%.2f µm", d);
            }
        } catch (NumberFormatException e) {
            return val.toString();
        }
    }

    public static void main(String[] args) throws Exception {
        String filePath = args.length > 0
            ? args[0]
//            : "/Users/goinac/Work/HHMI/stitching/datasets/medium/t1/LHA3_R3_medium.czi";
            : "/nrs/scicompsoft/goinac/multifish/testlaura/results/stitching/r0/Gel1_04x_GFP_Tpbg_633_Ribo550_nDapi.czi";

        String mvlFileName = args.length > 1
            ? args[1]
            : null; // "Gel1_04x_4x6.mvl";

        File cziFile = new File(filePath);
        if (!cziFile.exists()) {
            System.err.println("File not found: " + filePath);
            return;
        }

        // Try to find MVL file
        File mvlFile = null;
        if (mvlFileName != null) {
            mvlFile = new File(cziFile.getParentFile(), mvlFileName);
        } else {
            // Try same name with .mvl extension
            String baseName = cziFile.getName().replaceAll("\\.czi$", "");
            mvlFile = new File(cziFile.getParentFile(), baseName + ".mvl");
        }

        System.out.println("=== CZI/MVL Metadata Debug ===");
        System.out.println("CZI File: " + cziFile.getAbsolutePath());
        System.out.println("MVL File: " + mvlFile.getAbsolutePath());
        System.out.println("MVL exists: " + mvlFile.exists());
        System.out.println();

        // Parse MVL file if it exists
        List<MvlTilePosition> mvlPositions = null;
        if (mvlFile.exists()) {
            System.out.println("=== Parsing MVL file ===");
            mvlPositions = parseMvlFile(mvlFile);
            System.out.println();
            System.out.println("=== Tile positions from MVL ===");
            for (MvlTilePosition pos : mvlPositions) {
                System.out.println(pos);
            }
            System.out.println();
        }

        // Use IFormatReader with ChannelSeparator (auto-detects format)
        IFormatReader formatReader = new ChannelSeparator();

        formatReader.setMetadataOptions(new DefaultMetadataOptions(MetadataLevel.ALL));

        // Set up OME-XML metadata store
        ServiceFactory factory = new ServiceFactory();
        OMEXMLService service = factory.getInstance(OMEXMLService.class);
        OMEXMLMetadataImpl meta = (OMEXMLMetadataImpl) service.createOMEXMLMetadata();
        formatReader.setMetadataStore(meta);

        System.out.println("Set IFormatReader ID: " + cziFile);
        long startTime = System.currentTimeMillis();
        formatReader.setId(cziFile.toString());
        System.out.println("setId() took: " + (System.currentTimeMillis() - startTime) + " ms");

        MetadataRetrieve retrieve = (MetadataRetrieve) formatReader.getMetadataStore();

        int seriesCount = formatReader.getSeriesCount();

        // Dump original metadata looking for MVL-like entry data
        // MVL Entry elements have: PositionX, PositionY, PositionZ, PositionR, StackRange, StackSlices, etc.
        System.out.println("=== CZI Global Metadata (MVL-like Entry data) ===");
        Hashtable<String, Object> globalMeta = formatReader.getGlobalMetadata();

        // Find all view/tile indices by looking for Position keys
        // Look for patterns like: "Information|Image|V|View|Position|X #N"
        List<Integer> viewIndices = new ArrayList<>();
        for (String key : globalMeta.keySet()) {
            if (key.contains("Position|X") || key.contains("PositionX")) {
                int idx = extractImageNumberFromKey(key);
                if (idx >= 0 && !viewIndices.contains(idx)) {
                    viewIndices.add(idx);
                }
            }
        }
        java.util.Collections.sort(viewIndices);

        System.out.println("Found " + viewIndices.size() + " views/entries with position data in CZI metadata");
        System.out.println();

        // For each view, print position attributes
        for (int idx : viewIndices) {
            System.out.println("  View/Entry " + idx + ":");

            // Look for various position key patterns
            String[] positionPatterns = {
                "Information|Image|V|View|Position|X",
                "Information|Image|V|View|Position|Y",
                "Information|Image|V|View|Position|Z",
                "Position|X", "Position|Y", "Position|Z",
                "PositionX", "PositionY", "PositionZ"
            };

            for (String pattern : positionPatterns) {
                String searchKey = pattern + " #" + idx;
                // Try exact match first
                Object val = globalMeta.get(searchKey);
                if (val != null) {
                    String displayVal = formatPositionValue(val);
                    System.out.println("    " + pattern + " = " + displayVal);
                } else {
                    // Try partial match
                    for (String key : globalMeta.keySet()) {
                        if (key.contains(pattern) && key.endsWith("#" + idx)) {
                            val = globalMeta.get(key);
                            String displayVal = formatPositionValue(val);
                            System.out.println("    " + pattern + " = " + displayVal + "  (key: " + key + ")");
                            break;
                        }
                    }
                }
            }
        }
        System.out.println();

        // Also try getStageLabelX/Y/Z
        System.out.println("=== StageLabel positions ===");
        for (int s = 0; s < Math.min(5, seriesCount); s++) {
            try {
                Length stageLabelX = retrieve.getStageLabelX(s);
                Length stageLabelY = retrieve.getStageLabelY(s);
                Length stageLabelZ = retrieve.getStageLabelZ(s);
                double slX = stageLabelX != null && stageLabelX.value(UNITS.MICROMETER) != null ? stageLabelX.value(UNITS.MICROMETER).doubleValue() : 0;
                double slY = stageLabelY != null && stageLabelY.value(UNITS.MICROMETER) != null ? stageLabelY.value(UNITS.MICROMETER).doubleValue() : 0;
                double slZ = stageLabelZ != null && stageLabelZ.value(UNITS.MICROMETER) != null ? stageLabelZ.value(UNITS.MICROMETER).doubleValue() : 0;
                System.out.println("  Series " + s + " StageLabel: (" + slX + ", " + slY + ", " + slZ + ")");
            } catch (Exception e) {
                System.out.println("  Series " + s + " StageLabel: not available (" + e.getMessage() + ")");
            }
        }
        System.out.println();

        System.out.println("=== CZI File Metadata Debug ===");
        System.out.println("File: " + filePath);
        System.out.println("Series count (from IFormatReader): " + seriesCount);
        System.out.println();

        // Display ViewSetups as created by SpimDatasetBuilder.LOCIViewSetupBuilder.createViewSetups()
        System.out.println("=== ViewSetups (as created by SpimDatasetBuilder) ===");
        System.out.println();
        System.out.printf("%-8s | %-8s | %-20s | %-8s | %-30s | %-30s | %-25s%n",
                "ViewIdx", "TileID", "Tile Location (um)", "Channel", "Dimensions (X,Y,Z)", "Voxel Size (um)", "Channel Name");
        System.out.println("--------------------------------------------------------------------------------------------------------------------------------------");

        int viewIndex = 0;
        for (int series = 0; series < seriesCount; series++) {
            formatReader.setSeries(series);

            // Get dimensions from formatReader (after setSeries)
            int sizeX = formatReader.getSizeX();
            int sizeY = formatReader.getSizeY();
            int sizeZ = formatReader.getSizeZ();

            // Get position from global metadata or MetadataRetrieve
            double oX = getOffsetX(globalMeta, series);
            double oY = getOffsetY(globalMeta, series);
            double oZ = getOffsetZ(globalMeta, series);

            // Get voxel/pixel size from MetadataRetrieve
            Length physSizeX = retrieve.getPixelsPhysicalSizeX(series);
            Length physSizeY = retrieve.getPixelsPhysicalSizeY(series);
            Length physSizeZ = retrieve.getPixelsPhysicalSizeZ(series);

            double voxelX = physSizeX != null ? physSizeX.value(UNITS.MICROMETER).doubleValue() : 1;
            double voxelY = physSizeY != null ? physSizeY.value(UNITS.MICROMETER).doubleValue() : 1;
            double voxelZ = physSizeZ != null ? physSizeZ.value(UNITS.MICROMETER).doubleValue() : 1;

            // Get channel count from metadata (this is what SpimDatasetBuilder uses)
            int metadataChannels = retrieve.getChannelCount(series);

            // Tile ID = series (for single file case)
            int tileId = series;

            String tileLocation = String.format("(%.2f, %.2f, %.2f)", oX, oY, oZ);
            String dims = String.format("(%d, %d, %d)", sizeX, sizeY, sizeZ);
            String voxel = String.format("(%.4f, %.4f, %.4f)", voxelX, voxelY, voxelZ);

            // Iterate over channels (as SpimDatasetBuilder does)
            for (int chIndex = 0; chIndex < metadataChannels; chIndex++) {
                String chName = retrieve.getChannelName(series, chIndex);
                if (chName == null) chName = "(unnamed)";

                System.out.printf("%-8d | %-8d | %-20s | %-8d | %-30s | %-30s | %-25s%n",
                        viewIndex, tileId, tileLocation, chIndex, dims, voxel, chName);

                viewIndex++;
            }
        }

        System.out.println();
        System.out.println("Total ViewSetups created: " + viewIndex);
        System.out.println();

        // Summary
        System.out.println("=== Summary ===");
        System.out.println("Series (tiles): " + seriesCount);
        formatReader.setSeries(0);
        int channelsPerTile = retrieve.getChannelCount(0);
        System.out.println("Channels per tile: " + channelsPerTile);
        System.out.println("Expected ViewSetups: " + (seriesCount * channelsPerTile));
        System.out.println("Actual ViewSetups: " + viewIndex);

        formatReader.close();

        // If we have MVL positions, show comparison
        if (mvlPositions != null && !mvlPositions.isEmpty()) {
            System.out.println();
            System.out.println("=== COMPARISON: MVL vs CZI positions ===");
            System.out.println("(This shows if MVL file has the correct positions that CZI is missing)");
            System.out.println();
            System.out.printf("%-6s | %-40s | %-40s%n", "Image", "CZI Position (um)", "MVL Position (um)");
            System.out.println("-----------------------------------------------------------------------------------------");

            // Re-open to get CZI positions for comparison (using seriesCount)
            IFormatReader formatReader2 = new ChannelSeparator();
            ServiceFactory factory2 = new ServiceFactory();
            OMEXMLService service2 = factory2.getInstance(OMEXMLService.class);
            formatReader2.setMetadataStore(service2.createOMEXMLMetadata());
            formatReader2.setId(cziFile.toString());
            MetadataRetrieve retrieve2 = (MetadataRetrieve) formatReader2.getMetadataStore();

            int seriesCount2 = formatReader2.getSeriesCount();
            int compareCount = Math.min(seriesCount2, mvlPositions.size());

            for (int series = 0; series < compareCount; series++) {
                formatReader2.setSeries(series);

                Length cziX = retrieve2.getPlanePositionX(series, 0);
                Length cziY = retrieve2.getPlanePositionY(series, 0);
                Length cziZ = retrieve2.getPlanePositionZ(series, 0);

                double cx = cziX != null && cziX.value(UNITS.MICROMETER) != null ? cziX.value(UNITS.MICROMETER).doubleValue() : 0;
                double cy = cziY != null && cziY.value(UNITS.MICROMETER) != null ? cziY.value(UNITS.MICROMETER).doubleValue() : 0;
                double cz = cziZ != null && cziZ.value(UNITS.MICROMETER) != null ? cziZ.value(UNITS.MICROMETER).doubleValue() : 0;

                MvlTilePosition mvl = mvlPositions.get(series);

                String cziPos = String.format("(%.2f, %.2f, %.2f)", cx, cy, cz);
                String mvlPos = String.format("(%.2f, %.2f, %.2f)", mvl.x, mvl.y, mvl.z);

                // Mark if CZI position is (0,0,0) but MVL has real values
                String marker = (cx == 0 && cy == 0 && (mvl.x != 0 || mvl.y != 0)) ? " <-- MVL has data!" : "";

                System.out.printf("%-6d | %-40s | %-40s%s%n", series, cziPos, mvlPos, marker);
            }

            formatReader2.close();

            System.out.println();
            System.out.println("CONCLUSION: If MVL has positions but CZI shows (0,0,0), use MVL file for tile positions!");
        }

        System.out.println();
        System.out.println("=== Done ===");
    }

    /**
     * Gets X offset for a series, trying multiple sources:
     * 1. PlanePosition with plane index 0
     * 2. Global metadata: Information|Image|V|View|Position|X or Information|Image|S|Scene|Position|X
     */
    private static double getOffsetX(Map<String, Object> globalMetadata, int series) {
        // Try global metadata keys (series is 1-based in global metadata)
        Double val = findPositionInGlobalMeta(globalMetadata, series + 1, Arrays.asList("Position|X", "PositionX"));
        if (val != null) {
            String dir = (String) globalMetadata.get("Experiment|Axis|X|Direction");
            return "IncreasingLeft".equals(dir) ? -val : val;
        }

        System.out.println("Series " + series + ": No x offset found");
        return 0;
    }

    /**
     * Gets Y offset for a series, trying multiple sources.
     */
    private static double getOffsetY(Map<String, Object> globalMetadata, int series) {
        // Try global metadata keys (series is 1-based in global metadata)
        Double val = findPositionInGlobalMeta(globalMetadata, series + 1, Arrays.asList("Position|Y", "PositionY"));
        if (val != null) {
            String dir = (String) globalMetadata.get("Experiment|Axis|Y|Direction");
            return "IncreasingLeft".equals(dir) ? -val : val;
        }

        System.out.println("Series " + series + ": No y offset found");
        return 0;
    }

    /**
     * Gets Z offset for a series, trying multiple sources.
     */
    private static double getOffsetZ(Map<String, Object> globalMetadata, int series) {
        // Try global metadata keys (series is 1-based in global metadata)
        Double val = findPositionInGlobalMeta(globalMetadata, series + 1, Arrays.asList("Position|Z", "PositionZ"));
        if (val != null) {
            String dir = (String) globalMetadata.get("Experiment|Axis|Z|Direction");
            return "IncreasingLeft".equals(dir) ? -val : val;
        }

        System.out.println("Series " + series + ": No z offset found");
        return 0;
    }

    /**
     * Searches global metadata for a position value matching the image number.
     * Handles different number formats like "#1", "#01", "#001".
     */
    private static Double findPositionInGlobalMeta(Map<String, Object> globalMeta, int imageNumber, List<String> patterns) {
        // Preferred key patterns in order of priority
        String[] prefixes = {
            "Information|Image|V|View|",
            "Information|Image|S|Scene|",
            ""
        };

        for (String prefix : prefixes) {
            for (String pattern : patterns) {
                String searchPattern = prefix + pattern;
                for (String key : globalMeta.keySet()) {
                    if (key.contains(searchPattern) && key.contains("#")) {
                        int imageNumberFromKey = extractImageNumberFromKey(key);
                        if (imageNumberFromKey == imageNumber) {
                            Object val = globalMeta.get(key);
                            Double dval = null;
                            if (val instanceof Double) {
                                dval = (Double) val;
                            } else if (val instanceof String) {
                                dval = Double.parseDouble((String) val);
                            } else if (val != null) {
                                dval = Double.parseDouble(val.toString());
                            }
                            if (dval != null) {
                                // Convert from meters to micrometers if needed
                                if (Math.abs(dval) < 1 && Math.abs(dval) > 0)
                                    dval *= 1e6;
                                System.out.println("Image " + imageNumber + ": Found " + pattern + " offset: " + dval + " µm using " + key);
                                return dval;
                            }
                        }
                    }
                }
            }
        }

        return null;
    }

}
