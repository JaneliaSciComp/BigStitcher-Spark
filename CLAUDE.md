# BigStitcher-Spark — Project Notes

## Overview

BigStitcher-Spark is the Apache Spark distributed-processing front end for SPIM / light-sheet multi-view datasets. It wraps the algorithms from **multiview-reconstruction (mvr)** and exposes them as CLI tools that scale across local cores, clusters, or cloud.

See `README.md` for usage. This file contains technical / co-dev notes.

## Co-development with mvr

This project pins to `multiview-reconstruction 9.0.0-SNAPSHOT` and imports ~30+ mvr packages directly. SNAPSHOT is **not** in Maven Central — every mvr change must be installed locally before BigStitcher-Spark sees it:

```bash
cd /path/to/multiview-reconstruction
mvn clean install -DskipTests
```

### Key mvr packages used here
- `process.fusion.blk.*` — `BlkAffineFusion`, `BlkThinPlateSplineFusion` (fusion entry points)
- `process.fusion.intensity.*` — `IntensityCorrection`, `Coefficients`, `ViewPairCoefficientMatchesIO`
- `process.fusion.transformed.*` — `TransformVirtual`, `nonrigid.NonRigidTools`
- `process.export.{ExportN5Api, RetryTracker}`
- `process.n5api.N5ApiTools` (+ `MultiResolutionLevelInfo`, `setupBdvDatasetsN5/HDF5/OMEZARR_ResaveRaw`, `setupMultiResolutionPyramid`, `assembleJobs`, `resaveS0Block`, `writeDownsampledBlock`, `writeDownsampledBlock5dOMEZARR`)
- `process.splitting.*` — `SplitOctTree`, `SplitView`, `SplitResult`, `ConsensusSetCriterion`, `CrossViewCorrespondenceCriterion`, `OctTreeSplitCriterion`, `SplitDistributeEvenly`, `SplittingTools`
- `process.interestpointregistration.{global.*, pairwise.constellation.*, TransformationTools}`
- `process.interestpointdetection.methods.dog.{DoGImgLib2, DoGParameters}`
- `fiji.spimdata.{SpimData2, XmlIoSpimData2, imgloaders.AllenOMEZarrLoader, imgloaders.splitting.*, interestpoints.*}`
- `util.URITools` — used everywhere for URI parsing and `instantiateN5Writer`

### Co-dev risks
- mvr API changes ripple here on the **same day**. Recent splitting-API churn in mvr (`SplitOctTree`, `SplitView`, single-dim adaptive splitting, scoring statistics) hits `SplitDatasets.java` and `createSplitView()` directly.
- Tests are integration-style and depend on shared mvr test utilities (`SimulateUtil`, `TestInterestPointDetection`, `TestRegistration`, `TestBoundingBox`) — good for catching breakage automatically.

## Project Structure

```
src/main/java/net/preibisch/bigstitcher/spark/
├── abstractcmdline/                      # Shared CLI base classes
│   ├── AbstractBasic.java                # SpimData2 loading, friendly XML errors
│   ├── AbstractInfrastructure.java       # Spark/threading/maxPartitions, --max*Log knobs
│   ├── AbstractRegistration.java         # Registration-specific args
│   └── AbstractSelectableViews.java      # View-selection args
├── cloud/                                # Cloud test/loader
├── detection/
│   └── LazyBackgroundNormalize.java
├── fusion/
│   ├── GenerateComputeBlockMasks.java
│   ├── OverlappingBlocks.java
│   └── OverlappingViews.java
├── render/                               # headless BigDataViewer rendering (screenshot command)
│   ├── Camera.java                       # planes, viewer transform, screen-z range, MIP step rule (pure maths)
│   ├── HeadlessBdvRenderer.java          # sources/converters, BasicViewerState, MultiResolutionRenderer, MIP target, PNG
│   └── ScreenshotManifest.java           # --json manifest and BDVFlyThrough-style transform files (Gson)
├── util/                                 # Spark/N5/IO helpers
│   ├── BDVSparkInstantiateViewSetup.java
│   ├── DataTypeUtil.java
│   ├── Downsampling.java
│   ├── Import.java
│   ├── N5Util.java
│   ├── RetryTrackerSpark.java
│   ├── SetupIDMapper.java
│   ├── Spark.java
│   ├── SpimData2Util.java
│   └── ViewUtil.java
├── ApplyTransformation.java              # 'Apply Transformations' as a command (rotate/translate/scale/mirror/affine/centerOnView)
├── ClearInterestPoints.java
├── ClearRegistrations.java
├── CreateFusionContainer.java            # Phase 1: container metadata setup
├── DefineDataset.java                    # 'Define Multi-View Dataset' for one image file per view (virtual Bio-Formats loader, filemap2)
├── Filter_Views.java                     # New: filter SpimData2 XML by attributes
├── IntensitySolver.java
├── Solver.java                           # Global optimization
├── SparkDownsample.java
├── SparkFusion.java                      # Phase 2: distributed fusion
├── SparkGeometricDescriptorMatching.java
├── SparkInterestPointDetection.java
├── SparkIntensityMatching.java
├── SparkNonRigidFusion.java
├── SparkPairwiseStitching.java
├── Screenshot.java                       # headless BDV screenshots of pairs / overview / single views (driver-only)
├── SparkResaveN5.java
├── SplitDatasets.java                    # Adaptive image splitting
└── TransformPoints.java
```

The `install` script runs `mvn install`, then generates per-command shell wrappers (`fusion`, `resave`, `affine-fusion`, `clear-interestpoints`, …) with the right classpath.

## Core Dependencies

Parent: `pom-scijava` `44.0.0` (matches mvr; provides Java 21 defaults and the managed `imglib2` / `spim_data` base versions, so those are **no longer pinned here** — inherited from the parent to stay in lock-step with mvr).

Currently pinned (top of `pom.xml`):
- `multiview-reconstruction.version` = `9.0.1` (released; the zarrv3 line's release — BigStitcher 3.0.0 mirrors its dependency overrides)
- `imglib2-cache.version` = `1.0.0-beta-20`
- `imglib2-algorithm.version` = `0.18.3` (provides `algorithm.blocks.dfield.PositionFieldFunction`, required by mvr's `SplitImgLoaderThinPlateSplineFusion`)
- `imglib2-realtransform.version` = `4.0.5`
- `bigdataviewer-core.version` = `10.6.11` (plain upstream — the `-bsspark` Java-8 fork was dropped when the project moved to Java 21)
- `bigdataviewer-n5.version` = `1.0.3` (plain upstream — fork dropped)
- `n5.version` = `4.0.1`
- `n5-hdf5.version` = `3.0.0`
- `n5-imglib2.version` = `8.0.0`
- `n5-universe.version` = `3.0.2`
- `n5-zarr.version` = `2.0.1`
- `n5-zstandard.version` = `2.0.0`
- `n5-blosc.version` = `2.0.0`
- `n5-aws-s3` = `5.0.1` (hardcoded in the dependency, not a property)
- Apache **Spark 4.1.2 (Scala 2.13.17)** — requires **Java 17 or 21** (Spark 4 dropped JDK 8/11). Note: mvr 9.0.1 / BigStitcher 3.0.0 artifacts are Java 21 bytecode, so the cluster must run Java 21. Build/run with `JAVA_HOME` on Java 21.
- `jackson-databind` = `2.21.2` (matches Spark 4.1.2; Scala is sensitive to the fasterxml version)
- `netty-all` = `4.2.7.Final` (matches Spark 4.1.2's netty; note Spark 4.1 moved to the netty 4.2.x line)
- BigStitcher 3.0.0 (released)

These imglib2/n5 versions must stay in lock-step with mvr's `pom.xml`. A divergence produces silent `NoSuchMethodError`/`NoClassDefFoundError` at runtime (the parallel-save path in mvr's `XmlIoSpimData2` is particularly prone to swallowing the cause).

### Java 21 / Spark 4 migration notes
- The project moved off Java 8 specifically to drop the `-bsspark` bdv forks (plain upstream `bigdataviewer-core:10.6.11` is Java 11 bytecode, which a Java 8 target can't read). Java 21 ⇒ Spark 4.0 ⇒ Scala 2.13 — there is no Scala-2.12 / Java-21 Spark build.
- **Operational:** the fatjar now runs only on Spark 4.1 (Scala 2.13) clusters/cloud on Java 21; it will not run on a Spark 3.3.2 cluster.
- The generated local-mode wrappers (`install`) include Spark 4's `--add-opens` module flags; without them Java 17/21 throws `InaccessibleObjectException` at startup.

## Two-Phase Container Workflow

1. **CreateFusionContainer** (driver-only, plain Java) — creates the empty output container with all metadata stored as N5/Zarr attributes (sharding flags, shard size, block size, multires info, data type, intensity range, fusion format).
2. **SparkFusion** (distributed) — reads the metadata, builds the grid of compute blocks, and parallelizes via Spark RDD; each worker fuses its blocks.

This split keeps cluster/cloud submissions stateless: the container itself carries the configuration.

## Storage Formats

```java
enum StorageFormat { N5, ZARR, ZARR2, HDF5 }
```

`ZARR` = Zarr v3 (sharding-capable). `ZARR2` = Zarr v2 (no sharding). **Every format check must include both ZARR and ZARR2** wherever the operation is format-agnostic; only sharding-specific code paths exclude ZARR2:

```java
// Format-agnostic
if (storageType == StorageFormat.ZARR || storageType == StorageFormat.ZARR2) ...

// Sharding-only (v3)
if (useSharding && storageType != StorageFormat.ZARR) /* error */
```

## Zarr v3 Sharding

Sharding groups multiple inner chunks (e.g., 32³) into larger shards (e.g., 256³) stored as one file — drastically fewer files for cloud.

### The one rule that matters
**When sharding is enabled, write granularity must equal shard size.** N5 doesn't buffer, so we have to hand it complete shard-sized chunks ourselves.

```java
// Sharding ON
computeBlockSize = shardSize;

// Sharding OFF
computeBlockSize = blockSize * blocksPerJob;   // multiple of inner chunk
```

### `Grid.create(dimensions, computeBlockSize, blockSize)`
`computeBlockSize` is write granularity (can equal shard size). `blockSize` is the inner chunk size (metadata). Returns `List<long[3][n]>`:
- `gridBlock[0]` = pixel offset (world coords)
- `gridBlock[1]` = block size in pixels (cropped at edges)
- `gridBlock[2]` = `gridOffset` in **inner-block** coordinates (the N5 sharding codec maps blocks → shards internally; this is correct, don't change)

### Multi-resolution: shard size is constant across levels
s0, s1, s2, … all use the **same** `computeBlockSize`. Don't scale shardSize with downsampling factors. Lesson learned: passing `blockSize` instead of `shardSize` for s1+ causes data to land at wrong positions inside shards (symptom: top-left quadrant for z<64, bottom-right for z≥64).

### Auto-detect default
`useSharding == null` → on for `ZARR`, off otherwise. Validation prevents enabling sharding for non-ZARR.

### `--blockScale` vs sharding (SparkFusion / SparkResaveN5)
- **SparkFusion**: `--blockScale` is the compute-block multiplier (default `2,2,1`). With sharding ON, `blockScale` is **rejected** — shard size determines compute size. Default is `null` so we can detect explicit user input.
- **SparkResaveN5**: `--blockScale` is dual-purpose:
  - sharding OFF → compute-block multiplier
  - sharding ON → shard-size factor (default `16,16,1`)

### Resave memory per Spark task scales with the shard volume (2026-11-09)

Measured on the Shroff Optic lobe stacks (three Bio-Formats TIFFs of 5724 x 2498 x 201, 4706 x 2341 x 201 and
2048 x 2048 x 121 uint16, `define-dataset` project, Zarr v3): with the default `--blockScale 16,16,1` (shards of
2048 x 2048 x 64 = 512 MB uncompressed) and `local[4]` the s0 pass needed more than 8 GB of heap and the first
downsampling level more than 16 GB (`AbstractDownsample.getSourceBuffer` allocates the 2 x 2 x 1 source region of a
whole shard, i.e. four shard volumes, per task; OOM in `MemoryStore.putIterator` because the RDD is cached). With
`--blockScale 8,8,1` (1024 x 1024 x 64 shards, 128 MB) the same resave ran with 8 GB and 4 tasks: s0 116 s, the five
pyramid levels 20 s, 3.2 GB of Zarr (1.3 / 1.4 / 0.55 GB per view). Rule of thumb: about five shard volumes of heap per
concurrent task for the downsampling levels, plus Bio-Formats plane buffers for s0.

## Container Metadata (root attributes under `Bigstitcher-Spark/`)

`StorageType`, `DataType`, `UseSharding`, `ShardSize`, `ShardSizeFactor`, `MultiResolutionInfos`, `BlockSize`, `Compression`, `MinIntensity`, `MaxIntensity`, `NumChannels`, `NumTimepoints`, `FusionFormat`.

## CLI Class Hierarchy

`AbstractBasic` (SpimData2 loading, friendly XML errors)
  → `AbstractInfrastructure` (Spark / threads / `--maxPartitions`, shared `--max*Log` knobs, transformation summary)
    → `AbstractRegistration` (registration-specific knobs)
  → `AbstractSelectableViews` (view-selection knobs)

When adding a new CLI flag, decide which level it belongs at — flags shared across multiple commands belong in `AbstractInfrastructure`, registration-only flags in `AbstractRegistration`.

## Recent Themes (November 2026): dataset definition and transformations from the command line

- `screenshot` (`Screenshot` + `render/`, driver-only, 2026-11-08; Stephan's hand-off for the judge-alignment skill): renders
  what BDV shows for the selected views under their current registrations into PNGs with no window (`java.awt.headless` set
  in `main()`/`call()`). Recipe: `WrapBasicImgLoader.wrapImgLoaderIfNecessary` (non-BDV loaders such as the simulated one),
  `BigDataViewer.initSetups` (one `SourceAndConverter` + `RealARGBColorConverterSetup` per setup, i-th = `getViewSetupsOrdered().get(i)`),
  a `BasicViewerState` (`setNumTimepoints` before `setCurrentTimepoint(index)`, FUSED, NLINEAR), `MultiResolutionRenderer(target,
  () -> {}, {1}, 0, threads, null, false, CacheControl.Dummy)` with `requestRepaint(); paint(state)` per frame (non-volatile =
  complete frame); a MIP steps the viewer transform along screen z and keeps the per-channel max (as `RecordMaxProjectionDialog`).
  Colours need alpha 255; PNGs are written opaque (`TYPE_INT_RGB`, no-data = black). Camera: right-handed plane rotations (xy, xz:
  screen z = -y, yz: screen z = +x), box fitted with a margin and a magnification cap (`--maxScale` 4: a thin overlap strip is
  drawn into a small image, not blown up to 1024 x 4096 with 257 frames of 4 Mpx, which made ds1 take 5 min instead of 26 s),
  MIP step = max(one voxel of the shown views along the viewing axis (`Camera.voxelExtentAlongScreenZ`, ds1's z voxel is 3.2
  global px), 1 screen px, depth/256). `--auto` ranges of a PAIR are percentiles inside the pair's overlap box (a tile with
  one bright blob otherwise renders its ordinary structure dim next to the partner). Pairs use
  `SimpleBoundingBoxOverlap` (the registration's OVERLAPPING_ONLY rule) and the intersection of `getBoundingBoxReal` boxes;
  overview colours `ColorStream.get(start+i)` / a fixed cycle / an explicit list. Gson is now a direct dependency. No shear-free
  overlays yet (`--label`, `--boxes` are a second iteration; `Frame` keeps the viewer transform for them). Test gotchas: the
  simulated views are ONE bead cloud rotated 0/45/90 degrees about x (`SimulateMultiViewDataset.axisRotation`; register the
  inverse to align them) and UnsignedShort with peaks about 1000; the pair test measures the red/green cross-correlation peak and
  reads the scale from the manifest of the SAME run, since a shifted view changes the intersection box and the scale. A
  full-resolution MIP of the 127 px fixture is 127 frames (about 1.5 s per source), so the tests render slices or MIPs with
  a coarse `--step` (8 for the pair test, 32 for the manifest test); the whole class takes under 4 s.
- `define-dataset` (`DefineDataset`, driver-only, 2026-11-08): one `-i` image per view (every channel of a file is a view; series 0
  only), sizes through mvr's `FileListDatasetDefinitionUtil.detectDimensionsInFile` (Bio-Formats), voxel sizes from `--voxelSize`
  (per image or one for all) or the file metadata, attributes `--angle`/`--tile`/`--illumination` (per image or one for all; default
  angle = image index), the virtual `FileMapImgLoaderLOCI2` loader (`spimreconstruction.filemap2`, planes on demand; the legacy `FileMapImgLoaderLOCI` loads whole stacks and ran out of heap in `resave`, 2026-11-09), calibration via
  `DatasetCreationUtils.createViewRegistrations` (voxel / min voxel), base path = the XML's directory so the file paths come out
  relative. Refuses an existing XML without `--overwrite` and duplicate attribute combinations. Test `TestDefineDataset` writes
  ImageJ TIFFs (incl. a 2-channel hyperstack) with `ij.io.FileSaver` and reads the pixels back through the loader.
- `apply-transformation` (`ApplyTransformation`, driver-only, 2026-11-08): the GUI's "Apply Transformations" for the selected
  views; exactly one of `--rotate AXIS,DEG` (explicit matrices in the vecmath convention of `Apply_Transformation`, about y
  `x' = cos x + sin z`, pinned by `TestApplyTransformation` to the numbers BigStitcher wrote for +33 degrees), `--translate`,
  `--scale`, `--mirror`, `--affine` (12 values) or `--centerOnView TP,SETUP` (translation of the transformed
  image centre `(size-1)/2` onto the reference's); `preconcatenateTransform` (applied last) by default, `concatenateTransform`
  with `--applyFirst`. No `--shear` for now (Stephan, 2026-11-08: BDV's interpolation of a sheared view breaks down and the
  images are not shown properly; future work). Motivation: the Shroff diSPIM import (see `~/workspace/shroff-shearing/CLAUDE.md`): deskewed stacks get
  calibration, mirror z (View_B), the arm rotation and a centred start translation before detection and registration.
- `clear-registrations` (2026-11-08) removes by `--index` (order of application, 0 = first applied, the chain is printed with
  indices) and by `--name PATTERN` (`*`/`?` globs, `ClearRegistrations.globToPattern`) besides `--keep`/`--remove`; exactly one
  mode, nothing is saved when nothing matched. Test `TestClearRegistrations` (transforms planted with `apply-transformation`).

## Recent Themes (April 2026)

### `SplitDatasets` overhaul
Now ~939 LOC; runs Phase 1 splitting on Spark and uses mvr's new `SplitView` interface. Key commits:
- Spark-distributed via new `SplitInterval` API
- Unified to `SplitView` interface with static factory `createSplitView(...)`
- Phase 2/3 perf, per-dim CLI flags
- `--tolerance` / `--toleranceValue` for `ConsensusSetCriterion`
- `--xmlout` defaults to `<input>.split.xml` (no overwrite)

Tightly coupled to mvr's `process.splitting.*` — recent mvr-side work (single-dim adaptive splitting, removed merging in `SplitOctTree`, scoring statistics) needs to be tracked here.

### `Filter_Views` (new)
New CLI tool to filter a SpimData2 XML by attributes. Defaults to overwriting the input XML.

### `ClearInterestPoints` (expanded)
Added modes: `FIX_INTERESTPOINTS`, `ADD_LABEL`, `REMOVE_LABEL`. Cloud-aware. Now extends
`AbstractSelectableViews`: every mode except `FIX_INTERESTPOINTS` honours the view-selection flags, and
correspondences in *unselected* views that point into the selection are pruned too (dataset stays consistent
in one pass). A selection covering all present views (incl. "no flags") takes the legacy dataset-wide paths,
so `CLEAR_EVERYTHING` still wipes the whole `interestpoints.n5`. The set+label correspondence filter
(`removeCorrespondencesToViewsWithLabel`) lives here because BSS pins a released mvr; upstream it to
`CorrespondenceTools` when mvr moves past 9.0.13. `ClearRegistrations` now actually applies its selection
(its loop used to iterate every registration).

### Shared CLI plumbing
`--max*Log` knobs and the post-registration transformation summary moved into `AbstractInfrastructure` so all commands share them. Registration-specific args migrated to `AbstractRegistration`.

### Earlier (March 2026)
- TPS fusion entry point (`BlkThinPlateSplineFusion`) wired in; prefetch now applies to TPS when `prefetch==true`
- `--maxPartitions` flag in `AbstractInfrastructure`
- `overlapExpansion` parameter in `findOverlappingViews()` (TODOs left)
- ViewID-fusion bug fix
- Spark local-mode IPv6 hostname resolution fix

## Lessons / Gotchas

### Always use `URITools.instantiateN5Writer`
Direct `new N5ZarrWriter(path)` lacks the proper `GsonBuilder` (incl. `CoordinateTransformationAdapter`), Zarr-compatibility flags, and URI handling.

```java
N5Writer w = URITools.instantiateN5Writer(StorageFormat.ZARR, outPathURI);
```

### `Grid.create` parameter order is (dimensions, computeBlockSize, blockSize)
Easy to flip. Remember: middle = write granularity, last = inner chunk size.

### OME-ZARR access is 5D, zero-min
OME-ZARR containers store data as `[x, y, z, c, t]`. Tests must use 5D access:

```java
img.getAt(64, 32, 64, 0, 0).get();   // OK
img.getAt(64, 32, 64).get();          // AssertionError
```

OME-ZARR containers are zero-min — unlike mvr's translated intervals which can have negative mins.

### SparkFusion does NOT take `-x` or `-d`
XML path and data type are stored in container metadata by `CreateFusionContainer`. Only pass output path and storage format:

```bash
./fusion -o out.zarr -s ZARR2 --localSparkBindAddress
```

### `-s` cannot be auto-detected for `.zarr`
Must explicitly specify `ZARR` (v3) or `ZARR2` (v2). Tests should default to `ZARR2` to avoid sharding setup unless that's the focus.

### Simulated beads aren't at the origin
`SimulateUtil.setUp()` distributes 200 beads through the volume. Test pixel-value assertions at coordinates known to contain data, not at `(0,0,0,0,0)`. To find good coords, scan for non-zero pixels first.

### Friendly XML-load errors
`AbstractBasic` catches `SpimDataException` and prints a one-line message instead of a stack trace. Don't undo this when refactoring.

### Bio-Formats file-list datasets need mvr's Memoizer fix (2026-11-09)

The pom excludes `com.esotericsoftware:kryo` 5 (Bio-Formats' dependency) because it clashes with Spark's `kryo-shaded`
4 (same package; Spark's chill/KryoSerializer need the 4.x API, and maven-shade cannot keep both). Bio-Formats'
`Memoizer` serializes reader state with Kryo 5 (`kryo.util.DefaultInstantiatorStrategy`, a class that Kryo 4 does not
have and cannot be shimmed because the Memoizer casts Kryo's own strategy object to it), and mvr's file-list loaders
(`LegacyFileMapImgLoaderLOCI`, `FileMapImgLoaderLOCI2`) wrap every reader in a Memoizer with a temp memo directory.
Result: any pixel access to a `spimreconstruction.filelist` dataset whose `setId` takes longer than 100 ms (every real
stack; the tiny test files never trigger it) died with `NoClassDefFoundError: com/esotericsoftware/kryo/util/
DefaultInstantiatorStrategy` in `Memoizer.saveMemo`: `resave`, `detect-interestpoints`, `screenshot` etc. on a
`define-dataset` project. `define-dataset` itself only reads headers and worked. Fix in mvr (branch scalespace,
`BioformatsReaderUtils.memoizerDirectory`): the Memoizer gets its directory only when Kryo 5 is on the classpath; with a
null directory and the 3-argument constructor it never saves or loads a memo and never touches Kryo (readers are set up
from scratch each time, the loader prints a one-line notice). The released jars built against mvr 9.1.1 (0.1.3 to 0.1.5)
still have the crash until mvr ships the fix and `main` moves to that release.

## Testing

```bash
mvn test -Denforcer.skip=true                              # all
mvn test -Dtest=TestSparkFusion -Denforcer.skip=true       # one class
mvn test -Dtest=TestSparkFusion#testFusionWithMultiResPyramid -Denforcer.skip=true
```

Test classes (`TestSparkInterestPointDetection`, `TestSparkFusion`, `TestClearInterestPoints`) reuse mvr's `SimulateUtil` / `TestInterestPointDetection` / `TestRegistration` / `TestBoundingBox` — so mvr changes flow through here automatically.

Known issue: mvr-side `TestN5Zarr` multi-resolution sharding tests hit NPE at `PaddedRawBlockCodec.encode()`. Production GUI export and Spark fusion both work; suspected to be test-setup parameter init.

## Build & Run

```bash
mvn clean compile                # compile only
./install -t 8 -m 64             # build + generate command shims
./install-release -t 8 -m 64     # no build: download the latest release's local jar + generate shims (java -jar based)
mvn -DskipTests clean package -Pfatjar        # target/BigStitcher-Spark-<v>-cluster.jar (Spark not bundled; spark-submit)
mvn -DskipTests package -Pfatjar-local        # target/BigStitcher-Spark-<v>-local.jar   (Spark bundled; java -jar)
```

Generated local scripts wrap `java -Xmx${MEM}g -Dspark.master=local[${THREADS}] -cp $JAR:$(deps) net.preibisch.bigstitcher.spark.<Class>`.

Cluster/cloud: submit the cluster jar via `spark-submit --class net.preibisch.bigstitcher.spark.<Class> ...` (or without
`--class`: the manifest's `Main-Class` is the `BigStitcherSpark` dispatcher, `spark-submit ... jar <command> ...`).

### Fat jars & releases
- One shade execution in `<build>`, gated by properties: `fatjar.skip` (default true), `fatjar.classifier`,
  `spark.scope` (`provided` by default; the `fatjar-local` profile sets `compile`). The thin main artifact is
  never replaced, so `release:perform` deploys are unaffected. Two `mvn` runs are needed for two jars.
- Manifest (`ManifestResourceTransformer`): `Main-Class` = `BigStitcherSpark` (picocli dispatcher, 17 subcommands
  = shim names), `Add-Opens` = the module flags from `install` (honoured only by `java -jar`), `Implementation-Version`.
  `BigStitcherSpark.main` defaults `spark.master=local[*]` and sets the two `-D` props the shims pass.
- Bundling Spark needs `Log4j2PluginCacheFileTransformer` (three jars ship `Log4j2Plugins.dat`) and excluding
  `ch.qos.logback:*` (Bio-Formats pulls logback next to Spark's log4j binding). `log4j.version` pinned to Spark's 2.24.3.
- Releases: scijava `release-version.sh` (clone at `~/workspace/scijava-scripts`) creates the tag
  `BigStitcher-Spark-<v>`; needs `--skip-version-check` (we stay on pom-scijava 44 with mvr) and
  `--skip-license-update` (pom says bsd, headers are GPL) and a tree without untracked files. On the tag,
  `build.yml` deploys to maven.scijava.org and `release.yml` publishes the GitHub Release with
  `BigStitcher-Spark-<v>-{cluster,local}.jar` plus constant-name copies (`releases/latest/download/...`).
  `workflow_dispatch` on `release.yml` is a dry run (artifact only).

## Branches

- **`zarrv3`** — current dev, matches mvr's `zarrv3`. Pinned to mvr 9.0.0-SNAPSHOT.
- `main` — stable.
- Many feature branches still alive (`TPS`, `intensity`, `blocksupplier`, `OpenDAL`, `keller`, `omezarr`, …) — most look merged-or-stale.
- Remotes: `origin` should point at **JaneliaSciComp/BigStitcher-Spark** (the canonical repo; `PreibischLab/BigStitcher-Spark` is a GitHub redirect), `allen` = AllenNeuralDynamics fork.

## House Rules

- **Never commit without explicit user consent.**
- Branch state: read `git status` / `git log` — don't rely on stale notes here.
- Build: Maven. Java 21 (17 also works; Spark 4 dropped JDK 8/11).
- Spark version: 4.1.2 (Scala 2.13.17).

## References

All PreibischLab repositories moved to the JaneliaSciComp organization; `PreibischLab/...` URLs are GitHub redirects.

- Repo: https://github.com/JaneliaSciComp/BigStitcher-Spark
- Releases (local + cluster jars, `install-release`): https://github.com/JaneliaSciComp/BigStitcher-Spark/releases
- Maven artifacts: https://maven.scijava.org/content/repositories/releases/net/preibisch/BigStitcher-Spark/
- mvr: https://github.com/JaneliaSciComp/multiview-reconstruction
- BigStitcher (GUI): https://github.com/JaneliaSciComp/BigStitcher
- scijava-scripts (`release-version.sh`, `ci-build.sh`): https://github.com/scijava/scijava-scripts
- N5 Zarr v3 blog: https://imglib.github.io/imglib2-blog/posts/2025-12-22-n5-shard-dev/
- Zarr spec: https://zarr-specs.readthedocs.io/
- Apache Spark 4.1.2: https://spark.apache.org/docs/4.1.2/
