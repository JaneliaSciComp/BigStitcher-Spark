# BigStitcher-Spark - Development Documentation

## Overview

BigStitcher-Spark is a distributed computing framework built on Apache Spark for processing multi-view microscopy data. It enables parallel fusion of large-scale SPIM (Selective Plane Illumination Microscopy) datasets that are too large to process on a single machine.

**See README.md for comprehensive usage documentation, installation instructions, and examples.**

This document contains **technical implementation details** for developers working on the codebase.

## Project Structure

```
BigStitcher-Spark/
├── install                    # Build script that generates executable commands
├── pom.xml                   # Maven dependencies
├── README.md                  # User documentation and usage examples
├── src/main/java/net/preibisch/bigstitcher/spark/
│   ├── CreateFusionContainer.java       # Container setup
│   ├── SparkFusion.java                 # Distributed fusion (affine-fusion command)
│   ├── SparkResaveN5.java              # Format conversion (resave command)
│   ├── SparkDownsample.java            # Downsampling (downsample command)
│   ├── SparkInterestPointDetection.java  # Interest point detection
│   ├── SparkGeometricDescriptorMatching.java  # Interest point matching
│   ├── SparkPairwiseStitching.java      # Stitching
│   ├── Solver.java                      # Global optimization
│   ├── util/
│   │   ├── N5Util.java                 # N5Writer utilities
│   │   └── Spark.java                  # Spark configuration
│   └── fusion/
│       ├── FusionTools.java            # Fusion utilities
│       └── GenerateComputeBlockMasks.java
└── target/                    # Build output
    └── BigStitcher-Spark-0.1.0-SNAPSHOT.jar
```

### Command Generation

The `install` script:
1. Runs `mvn clean install` to build the JAR
2. Generates dependency classpath with `mvn dependency:build-classpath`
3. Creates executable scripts (resave, fusion, create-fusion-container, etc.)
4. Each script sets up Java with Spark parameters and the full classpath

## Core Dependencies

### Versions (as of 2025-01-08)

**N5 Ecosystem (alpha-6 series for Zarr v3 support)**:
- n5: 4.0.0-alpha-6
- n5-imglib2: 7.1.0-alpha-6
- n5-zarr: 2.0.0-alpha-4
- n5-universe: 2.4.0-alpha-6
- n5-zstandard: 2.0.0-alpha-4
- n5-blosc: 2.0.0-alpha-4

**Core Libraries**:
- multiview-reconstruction: 9.0.0-SNAPSHOT (must be installed locally)
- Apache Spark: 3.3.2
- Scala: 2.12.15
- BigStitcher: 2.6.0

### Dependency Resolution

**Critical**: multiview-reconstruction 9.0.0-SNAPSHOT is not in Maven Central. Before building:

```bash
cd /path/to/multiview-reconstruction
mvn clean install -DskipTests
```

This installs to `~/.m2/repository` where BigStitcher-Spark can find it.

## Key Architecture Concepts

### 1. Container-Based Workflow

BigStitcher-Spark uses a two-phase approach:

**Phase 1: CreateFusionContainer**
- Sets up empty container with metadata
- NOT distributed (plain Java)
- Stores all configuration as N5/Zarr attributes

**Phase 2: SparkFusion**
- Reads metadata from container
- Distributes work across Spark workers
- Each worker fuses its assigned blocks

### 2. Storage Formats

**StorageFormat Enum** (N5ApiTools.java):
```java
public enum StorageFormat {
    N5,      // Standard N5 format (no sharding)
    ZARR,    // Zarr v3 with sharding support
    ZARR2,   // Zarr v2 legacy (no sharding)
    HDF5     // HDF5 (no sharding, local only)
}
```

**CRITICAL**: All format checks must distinguish ZARR from ZARR2:
```java
// CORRECT - works for both Zarr versions:
if (storageType == StorageFormat.ZARR || storageType == StorageFormat.ZARR2)

// WRONG - excludes ZARR2:
if (storageType == StorageFormat.ZARR)  // Missing ZARR2!
```

### 3. Zarr v3 Sharding

**What is Sharding?**

Zarr v3 groups multiple small chunks into larger "shards" to reduce file count and metadata overhead in cloud storage.

**Example**:
- Without sharding: 1 million 32³ chunks = 1 million files
- With sharding: 8x8x2 chunks per shard = ~2000 files

**Key Design Pattern**:
```java
// When sharding is ENABLED:
computeBlockSize = shardSize;  // MUST equal shard size!

// When sharding is DISABLED:
computeBlockSize = blockSize * blocksPerJob;
```

**Why**: Zarr v3 shards must be written as complete units. The N5 API doesn't buffer, so write granularity must match shard size.

### 4. Grid.create() and Block Coordinates

**Grid.java** (multiview-reconstruction/src/main/java/util/Grid.java):

```java
public static List<long[][]> create(
    final long[] dimensions,      // Image dimensions
    final int[] gridBlockSize,    // computeBlockSize (write granularity)
    final int[] outBlockSize)     // blockSize (inner chunk size)
```

**Returns**: List of `long[3][n]` where each element contains:
- `gridBlock[0]` = pixel offset in world coordinates
- `gridBlock[1]` = block size in pixels (cropped at edges)
- `gridBlock[2]` = **gridOffset in BLOCK coordinates** (NOT shard coordinates)

**Grid position calculation** (Grid.java:80):
```java
gridPosition[d] = offset[d] / outBlockSize[d];  // Always in inner block coordinates
```

**Important**: `gridOffset` is always in inner block/chunk coordinates. The N5 sharding codec internally maps block coordinates to shards. This is correct and doesn't need to change.

## CreateFusionContainer Implementation

**Location**: `src/main/java/net/preibisch/bigstitcher/spark/CreateFusionContainer.java`

### CLI Options Related to Sharding

```java
@Option(names = {"-s", "--storage"}, description = "Dataset storage type")
private StorageFormat storageType = StorageFormat.ZARR;  // Default to Zarr v3

@Option(names = {"--blockSize"}, description = "Inner chunk size (default: 128,128,128)")
private String blockSizeString = "128,128,128";

@Option(names = {"--useSharding"}, description = "Enable Zarr v3 sharding (default: auto-detect)")
private Boolean useSharding = null;  // null = auto-detect based on format

@Option(names = {"--shardSizeFactor"}, description = "Shard size factor (default: 8,8,2)")
private String shardSizeFactorString = "8,8,2";
```

### Sharding Logic Flow

```java
// 1. Auto-detect sharding based on storage format
if (useSharding == null)
    useSharding = (storageType == StorageFormat.ZARR);

// 2. Validate: sharding only for ZARR v3
if (useSharding && storageType != StorageFormat.ZARR) {
    System.out.println("WARNING: Sharding only supported for ZARR v3. Disabling.");
    useSharding = false;
}

// 3. Calculate shard size
final int[] shardSize;
if (useSharding) {
    shardSize = new int[] {
        blockSize[0] * shardSizeFactor[0],
        blockSize[1] * shardSizeFactor[1],
        blockSize[2] * shardSizeFactor[2]
    };
} else {
    shardSize = null;
}

// 4. Store metadata for SparkFusion to read
driverVolumeWriter.setAttribute("/", "Bigstitcher-Spark/UseSharding", useSharding);
if (useSharding) {
    driverVolumeWriter.setAttribute("/", "Bigstitcher-Spark/ShardSize", shardSize);
    driverVolumeWriter.setAttribute("/", "Bigstitcher-Spark/ShardSizeFactor", shardSizeFactor);
}
```

### N5ApiTools Integration

**Location**: multiview-reconstruction/src/main/java/net/preibisch/mvrecon/process/n5api/N5ApiTools.java

**Key method**:
```java
public static MultiResolutionLevelInfo[] setupMultiResolutionPyramid(
    final N5Writer driverVolumeWriter,
    final Function<Integer, String> levelToDataset,
    final DataType dataType,
    final long[] dimensions,
    final Compression compression,
    final int[] blockSize,
    final int[][] downsamplings,
    final boolean useSharding,     // Sharding enabled?
    final int[] shardSize)          // Shard size (null if disabled)
```

**Usage in CreateFusionContainer**:

**OME-ZARR (5D format)**:
```java
// Expand to 5D: [x, y, z, channels, timepoints]
final int[] blockSize5d = new int[] { blockSize[0], blockSize[1], blockSize[2], 1, 1 };
final int[] shardSize5d = useSharding ?
    new int[] { shardSize[0], shardSize[1], shardSize[2], 1, 1 } : null;

mrInfos[0] = N5ApiTools.setupMultiResolutionPyramid(
    driverVolumeWriter, levelToName, dt, dim5d,
    compression, blockSize5d, downsamplings5d,
    useSharding, shardSize5d);
```

**BDV N5 (3D datasets)**:
```java
myMrInfo[c + t*c] = N5ApiTools.setupBdvDatasetsN5(
    driverVolumeWriter, viewDescription, dataType,
    boundingBox, compression, blockSize, downsamplings);

// Note: setupBdvDatasetsN5() internally calls setupMultiResolutionPyramid()
// with hardcoded useSharding=false, shardSize=null
// BDV format does not currently support sharding
```

## SparkFusion Implementation

**Location**: `src/main/java/net/preibisch/bigstitcher/spark/SparkFusion.java`

### Sharding Metadata Loading

```java
// Load sharding configuration from container
final boolean useSharding = driverVolumeWriter.getAttribute("/", "Bigstitcher-Spark/UseSharding", boolean.class);
final int[] shardSize;
final int[] shardSizeFactor;

if (useSharding) {
    shardSize = driverVolumeWriter.getAttribute("/", "Bigstitcher-Spark/ShardSize", int[].class);
    shardSizeFactor = driverVolumeWriter.getAttribute("/", "Bigstitcher-Spark/ShardSizeFactor", int[].class);

    // Validate: blockScale cannot be specified when sharding is enabled
    if (blockScaleString != null) {
        throw new IllegalArgumentException("Cannot specify --blockScale when sharding is enabled.");
    }
} else {
    shardSize = null;
    shardSizeFactor = null;

    // Set default blockScale if not specified
    if (blockScaleString == null)
        blockScaleString = "2,2,1";
}
```

### CLI Option Design

```java
@Option(names = "--blockScale", description = "Compute block multiplier (default: 2,2,1; not compatible with sharding)")
private String blockScaleString = null;  // null = not specified by user
```

**Key insight**: Default is `null` (not `"2,2,1"`) so we can detect whether the user explicitly set it.

### computeBlockSize Calculation

**CRITICAL SECTION** (SparkFusion.java:535-548):

```java
// CRITICAL: When sharding enabled, computeBlockSize MUST equal shardSize
final int[] computeBlockSize;
if (useSharding) {
    computeBlockSize = shardSize;
    System.out.println("Sharding enabled: computeBlockSize = shardSize = " +
                       Util.printCoordinates(computeBlockSize));
} else {
    // using bigger blocksizes than being stored for efficiency
    computeBlockSize = new int[3];
    Arrays.setAll(computeBlockSize, d -> blockSize[d] * blocksPerJob[d]);
    System.out.println("No sharding: computeBlockSize = blockSize * blocksPerJob = " +
                       Util.printCoordinates(computeBlockSize));
}

final List<long[][]> grid = Grid.create(dimensions, computeBlockSize, blockSize);
```

### Multi-Resolution Pyramid

**IMPORTANT**: Shard size is **constant across all pyramid levels** (doesn't scale with downsampling):

```java
// s0 (full resolution) - computeBlockSize set above
Grid.create(dimensions, computeBlockSize, blockSize);

// s1, s2, ... (downsampled levels) - use SAME computeBlockSize
for (int level = 1; level < mrInfo.length; ++level) {
    final List<long[][]> allBlocks = Grid.create(
        mrInfo[level].dimensions,
        computeBlockSize,  // Same value as s0!
        blockSize);
}
```

### Fusion Execution Flow

```java
// 1. Create grid of blocks to process
final List<long[][]> grid = Grid.create(dimensions, computeBlockSize, blockSize);

// 2. Distribute to Spark workers
final JavaRDD<long[][]> rdd = sc.parallelize(grid, Math.min(Spark.maxPartitions, grid.size()));

// 3. Each worker processes one gridBlock:
rdd.map(gridBlock -> {
    // Load SpimData2 from XML
    final SpimData2 dataLocal = Spark.getSparkJobSpimData2(xmlURI);

    // Open source views with transformations
    final RandomAccessibleInterval<T> fused = /* fusion logic */;

    // Write block
    final long[] gridOffset = gridBlock[2];  // Block coordinates!
    N5Utils.saveBlock(fused, writer, dataset, gridOffset);

    return gridBlock;
});
```

## SparkResaveN5 Implementation

**Location**: `src/main/java/net/preibisch/bigstitcher/spark/SparkResaveN5.java`

### Dual-Purpose blockScale Parameter

```java
@Option(names = "--blockScale", description =
    "Serves dual purpose:\n" +
    "1. Compute block multiplier (no sharding)\n" +
    "2. Shard size factor (with sharding)\n" +
    "(default: 16,16,1)")
private String blockScaleString = "16,16,1";

@Option(names = {"--useSharding"}, description =
    "Enable Zarr v3 sharding (default: enabled for ZARR v3, disabled for N5/ZARR v2)")
private Boolean useSharding = null;  // null = auto-detect
```

### Sharding Configuration

```java
// Auto-detect sharding
if (useSharding == null)
    useSharding = (storageFormat == StorageFormat.ZARR);

// Validate
if (useSharding && storageFormat != StorageFormat.ZARR) {
    System.out.println("WARNING: Sharding only supported for ZARR v3. Disabling.");
    useSharding = false;
}

// Calculate shard size (blockScale serves as shardSizeFactor)
final int[] shardSize;
if (useSharding) {
    shardSize = new int[] {
        blockSize[0] * blockScale[0],
        blockSize[1] * blockScale[1],
        blockSize[2] * blockScale[2]
    };
}
```

## Common Issues and Solutions

### Issue 1: ZARR vs ZARR2 Format Checks

**Problem**: Code only checked for `ZARR`, excluding `ZARR2` from operations that should apply to both.

**Fixed Locations** (2025-01-08):
- CreateFusionContainer.java: Lines 338, 384, 457, 498, 522
- SparkFusion.java: Lines 485, 683, 776
- N5Util.java: Line 66

**Pattern**:
```java
// CORRECT - includes both:
if (storageType == StorageFormat.ZARR || storageType == StorageFormat.ZARR2)

// CORRECT - sharding only for v3:
if (useSharding && storageType != StorageFormat.ZARR) { /* error */ }
```

### Issue 2: blockScale vs Sharding Conflict

**Problem**: User tries to specify `--blockScale` when sharding is enabled.

**Solution**: SparkFusion throws clear error:
```
ERROR: Sharding is enabled in this container (shard size: [256,256,64]).
       The --blockScale parameter cannot be used with sharded containers.
       Shard size automatically determines the compute block size for shard-aware writing.
```

### Issue 3: multiview-reconstruction Not Found

**Problem**: Maven can't find multiview-reconstruction 9.0.0-SNAPSHOT.

**Solution**: Install it locally first:
```bash
cd /path/to/multiview-reconstruction
mvn clean install -DskipTests
```

### Issue 4: Grid.create() Parameter Confusion

**Problem**: Mixing up which parameter is computeBlockSize vs blockSize.

**Solution**: Remember the order:
```java
Grid.create(
    dimensions,        // Image dimensions
    computeBlockSize,  // Write granularity (can be shard size)
    blockSize)         // Inner chunk size
```

## Testing and Validation

### Building and Testing

```bash
# Build project
mvn clean compile

# Install (generates executable commands)
./install -t 8 -m 64

# Build fatjar for cluster/cloud
mvn clean package -P fatjar
```

### Testing Sharding

**Create sharded container**:
```bash
./create-fusion-container \
    -x dataset.xml \
    -o output.zarr \
    -s ZARR \
    --blockSize 32,32,32 \
    --shardSizeFactor 8,8,2 \
    --useSharding true
```

**Run fusion**:
```bash
./fusion -o output.zarr -c 0 -t 0
```

**Verify metadata**:
```python
import zarr
store = zarr.open('output.zarr')
print(store.attrs['Bigstitcher-Spark/UseSharding'])  # Should be True
print(store.attrs['Bigstitcher-Spark/ShardSize'])     # Should be [256,256,64]
```

## Metadata Attributes

**Container root** ("/"):
- `Bigstitcher-Spark/StorageType` - N5/ZARR/ZARR2/HDF5
- `Bigstitcher-Spark/DataType` - UINT8/UINT16/FLOAT32
- `Bigstitcher-Spark/UseSharding` - boolean
- `Bigstitcher-Spark/ShardSize` - int[] (3D)
- `Bigstitcher-Spark/ShardSizeFactor` - int[] (3D)
- `Bigstitcher-Spark/MultiResolutionInfos` - MultiResolutionLevelInfo[][]
- `Bigstitcher-Spark/BlockSize` - int[] (3D)
- `Bigstitcher-Spark/Compression` - Compression object
- `Bigstitcher-Spark/MinIntensity` - double
- `Bigstitcher-Spark/MaxIntensity` - double
- `Bigstitcher-Spark/NumChannels` - int
- `Bigstitcher-Spark/NumTimepoints` - int
- `Bigstitcher-Spark/FusionFormat` - String (BDV/N5, BDV/OME-ZARR, etc.)

## Recent Changes

### Zarr v3 Sharding Support (2025-01-08)

**Branch**: `zarrv3`

**Changes**:
1. Updated N5 dependencies to alpha-6 series (pom.xml)
2. Added --useSharding and --shardSizeFactor options (CreateFusionContainer.java)
3. Loads sharding metadata, uses shardSize as computeBlockSize (SparkFusion.java)
4. Dual-purpose blockScale parameter (SparkResaveN5.java)
5. Fixed 9 ZARR vs ZARR2 format checks (4 files)
6. Validation prevents --blockScale override when sharding enabled

**Files Modified**:
- BigStitcher-Spark/pom.xml
- BigStitcher-Spark/src/main/java/net/preibisch/bigstitcher/spark/:
  - CreateFusionContainer.java
  - SparkFusion.java
  - SparkResaveN5.java
  - util/N5Util.java

**NOT Modified**: multiview-reconstruction (setupBdvDatasetsN5 remains without sharding parameters)

## Important Development Notes

- **Never commit without explicit user consent**
- Current development branch: `zarrv3`
- Main branch: `master`
- Build system: Maven
- Java version: 8 (Java ≥21 currently not compatible with Spark 3.3.2)
- Spark version: 3.3.2

## Build Configuration

### Local Execution

Generated scripts use:
```bash
java -Xmx${MEM}g -Dspark.master=local[${THREADS}] \
    -cp $JAR:$(dependencies) \
    net.preibisch.bigstitcher.spark.ClassName "$@"
```

### Cluster/Cloud Execution

```bash
mvn clean package -P fatjar
# Creates: target/BigStitcher-Spark-0.1.0-SNAPSHOT.jar

# Example submission:
spark-submit \
    --class net.preibisch.bigstitcher.spark.SparkFusion \
    --master yarn \
    BigStitcher-Spark-0.1.0-SNAPSHOT.jar \
    -x dataset.xml -o output.zarr -c 0 -t 0
```

## References

- **Repository**: https://github.com/PreibischLab/BigStitcher-Spark
- **multiview-reconstruction**: https://github.com/PreibischLab/multiview-reconstruction
- **N5 Zarr v3 Blog**: https://imglib.github.io/imglib2-blog/posts/2025-12-22-n5-shard-dev/
- **Zarr Specification**: https://zarr-specs.readthedocs.io/
- **Apache Spark**: https://spark.apache.org/docs/3.3.2/

## Additional Resources

For comprehensive usage documentation, installation instructions, example datasets, and command-line parameter details, see **README.md**.
