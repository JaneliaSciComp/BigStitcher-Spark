# BigStitcher-Spark — Project Notes

## Overview

BigStitcher-Spark is the Apache Spark distributed-processing front end for SPIM / light-sheet multi-view datasets. It wraps the algorithms from **multiview-reconstruction (mvr)** and exposes them as CLI tools that scale across local cores, clusters, or cloud.

See `README.md` for usage. This file contains technical / co-dev notes.

## Co-development with mvr

This project pins to `multiview-reconstruction 10.0.0-SNAPSHOT` (branch `scalespace-pr108`) and imports ~30+ mvr packages directly. SNAPSHOT is **not** in Maven Central — every mvr change must be installed locally before BigStitcher-Spark sees it:

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
- `process.interestpointdetection.{NoiseFloor, InterestPointTools, OverlappingRegions, methods.dog.DoGImgLib2, methods.scalespace.{DoGScaleSpace, ScaleSpaceParameters}}` — detection per block, the overlapping regions and their combine (`--overlappingOnly`), the noise-floor threshold, the cross-block duplicate merge of the scale space
- `fiji.plugin.Filter_Detections` + `fiji.spimdata.interestpoints.{InterestPointFilter, VirtualFilteredInterestPoints}` — filtered labels (`filter-interestpoints`, `clear-interestpoints`, `split-images`)
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
├── FilterInterestPoints.java             # filtered labels of interest points (driver-only)
├── SparkInterestPointEmbedding.java      # embeddings of interest points (pcmatch's ONNX model in Java), one task per view
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
- `multiview-reconstruction.version` = `10.0.0-SNAPSHOT` (branch `scalespace-pr108`: the Zarr interest point store, one `InterestPoint` class with response/sigma/intensity, filtered labels, the scale space; install it locally before building with `mvn install -DskipTests`, which also builds the `tests` jar this project's tests need)
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

### Resave: preloaded compute blocks, existing datasets (2026-11-05)
- Each s0 task hands its compute block to `N5ApiTools.resaveS0Block( ..., preload )` (MVR 2026-11-04): the block is copied into memory
  once, because the virtual Bio-Formats file-list loader caches one plane and `N5Utils.saveBlock` traverses the planes once per N5
  block (streaming re-read every plane hundreds of times). Memory: one compute block per concurrent task (512 MB at 16 bit with the
  defaults 128,128,64 x 16,16,1); the driver prints the exact need. `--no-preloadComputeBlocks` streams.
- Datasets of the views that already exist in the container (a second run, an interrupted one) stop the run with a message naming
  them before anything is written; `--overwriteExistingDatasets` deletes only those views' groups (`N5ApiTools.existsBDVView` /
  `removeBDVView`), a dry run says what it would delete. Test `TestSparkResaveLocal` (local Spark on the simulated beads).
- **picocli negatable options (4.7.7, measured 2026-11-05):** `--no-X` sets the field to its *default*, `--X` to the opposite. A
  `boolean` that defaults to true must therefore be declared as `names = "--no-X"`, `negatable = true` (then `--no-X` = false,
  `--X` = true, as `--no-preloadComputeBlocks`); declared as `--X` with default true, `--no-X` does nothing and `--X` turns it
  off. A `Boolean` field left null (not given) is fine either way: `--X` = true, `--no-X` = false (`--finestLevel`).

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

## Recent Themes (October 2026): interest points of multiview-reconstruction 10

- `detect-interestpoints` stores response, sigma and intensity with every detection (`--storeIntensities` is gone), has `--method SCALE_SPACE` (+ `--steps`, `--octaves`, `--no-finestLevel`), `--anisotropy` and `--noiseFactor` (threshold = factor x the pooled noise floor, one Spark job per view, resolved on the driver before the detection jobs). The temporary per-block results are written with `InterestPointsN5.saveInterestPointsStatic( writer, path, points )` into `interestpoints.n5/spark_tmp_*` and read with `loadInterestPointsStatic`, so the attributes travel with the points; the combined result of a view goes to a staging file of the dataset's interest point store (`InterestPointsZarr.stagePoints`, one file per view task), which the driver's XML save commits after `InterestPointTools.addInterestPointEntries` registered the entries (nothing is loaded on the driver). The scale space merges duplicates across blocks per view with `DoGScaleSpace.mergeDuplicates( points, scale, sigmaMin, combineDistance )`. `--maxSpots` is `InterestPointFilter.limit( .., INTENSITY, HIGHEST, n )`. The legacy DoG path is unchanged otherwise (no block expansion). `--overlappingOnly` (2026-10-27) uses mvr's `OverlappingRegions` like the GUI: `overlapping( data, view, candidates )` for the partners (all other selected views, or `otherVolumes` = another tile or angle of the same timepoint with `--onlyCompareOverlapTiles`, which before compared timepoint and channel), `intersection( data, view, partner, image, mipmap )` for the region of a partner in the downsampled pixels of the view (the metadata job), `combine( lists, OverlappingRegions.combineDistance )` for the per-view merge of the regions' points (0.5 px full resolution), `dropContained` on the driver for regions inside another of the same view (2026-10-28, a diagonal neighbour's corner lies in the side strips; `otherVolumes` also gives one partner per neighbour volume, the same channel preferred, so a neighbour tile is one strip and not one per channel); BSS's own `OverlappingViews.findAllOverlappingViewsFor` and KD-tree loop are gone. `TestSparkInterestPointDetection.testOverlappingOnly` runs it on the simulated dataset with shifted registrations.
- Interest point storage (2026-10-23, branch `scalespace-pr108` of both repos): multiview-reconstruction keeps all interest points of a dataset in `interestpoints.zarr`, one self-contained group per (label, timepoint) that is replaced whole on change (see MVR `claude.md`, "Interest Point Storage"). Rules for Spark code: executors NEVER commit; a task saves through `InterestPointsZarr.saveStaged( lists )` or `stagePoints(...)` (one staging file per task, durable, readable by other tasks at once), the driver's `XmlIoSpimData2.save` (or `saveInterestPointsInParallel`) folds and commits. `split-images` phases 2/3 and `detect-interestpoints` do that; `match-interestpoints` stages and commits on the driver; `clear-interestpoints` deletes list entries (`deleteInterestPoints` = the whole entry) and lets the XML save commit, `CLEAR_ALL_CORRESPONDENCES` runs one batch. Old `interestpoints.n5` datasets are read as they are (fallback per label) and converted once with `java -cp <fat jar> net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsN5ToZarr <dataset.xml>`. The `spark_tmp_*` scratch of the detection stays N5. Never let two JVMs commit the same (label, timepoint) at once (the second one fails loudly and nothing is lost); jobs on different labels or timepoints never collide.
- 2026-10-27 (with mvr e886d74d): `detect-interestpoints` removes the scratch `interestpoints.n5` when it holds nothing else after the run (a Zarr-only dataset keeps no stray legacy container); a label that exists in the selected views has its references dropped on the driver before the tasks run (`InterestPointTools.removeReferences`, what the GUI's re-detection does; before and not after the tasks, since a staging file of the new points shadows the old entry for every read, so the old correspondences could not be found any more); the Spark parameters strings end with the GUI's tail `localization=NONE|QUADRATIC groupTiles=false groupIllums=false thresholdType=fixed|noisefloor [noiseFactor=]` (`DifferenceOfGUI.localizationKey`), which `StoredDetectionParameters.parse` reads for the GUI's "Re-detect interest points with attributes". `TestSparkInterestPointDetection.testRedetectionReplacesLabel` covers the first two.
- `solver` (2026-10-27; Lila Schweinfurth's branch `ip-store-update`, commits 3e3f31c, 4d985e3, c4e2968, ported): `Solver.setupPointMatchesFromInterestPoints` builds the view pairs from the correspondence partners of every (view, label) (`InterestPoints.getCorrespondingViews()`, answered from the store's index without loading points) instead of every pair of the selected views, reads the correspondences of a pair for that partner only (`getCorrespondingInterestPointsCopy( vB, labelB )`, a slice of the store's per-partner grouping) and loads the points of a (view, label) once for all its pairs (`pointMaps`); the pre-load no longer reads whole correspondence lists; pairs without correspondences no longer produce an empty `PairwiseResult`. The pair enumeration goes from n^2 to the connected pairs.
- `match-interestpoints -m EMBEDDINGS` (2026-10-29; mvr `InterestPointArrays` and `pairwise.methods.embeddings`): matches on the per-point array `embedding` (`descriptor` until 2026-10-29) that pcmatch's `embed` writes into `interestpoints.zarr` (`<label>/tp<T>/arrays/<name>/setup<S>/{id, values}`; Java only reads it; a `loc` table, written until 2026-10-29, is ignored). `--searchRadius` is optional (no limit without it, as for PRECISE_TRANSLATION; the registration's uncertainty in global px is the right value), `--descriptorRatio` (0.85) is the ratio test in similarity space, `--minSimilarity` (0.3) the least cosine similarity of a candidate, `--arrayName` picks the array only when the labels have several (`InterestPointArrays.names`; otherwise the one they have); `-r`/`-s`, grouped views and `--candidatesPath` are refused. The matcher is mvr's `EmbeddingsPairwise` (`createMatcherInstance` passes `data.getViewInterestPoints()`: the vectors are joined to the stored points by id through the labels' handles and the array must have exactly one row per point of the (base) label, a filtered label reads its base, a re-detected label fails with "run pcmatch embed again", arrays of two models are refused); `LoadCandidatesPairwise` stays the import path for external candidates. Verified against pcmatch's reference (`~/Documents/pcmatch_data/ds1_java_matching`: 1,825 candidates at R 20 and 3,769 at R 300, id for id; the embryo pair 60,341 at R 500). Test `TestEmbeddings` (planted vectors on the simulated beads; without `--searchRadius` the command refuses and changes nothing).
- `embed-interestpoints` (`SparkInterestPointEmbedding`, 2026-10-30; mvr `process.interestpointembedding`): computes the per-point array `embedding` that `match-interestpoints -m EMBEDDINGS` matches on, with pcmatch's ONNX export (`--model x.onnx` + `x.onnx.json` sidecar: fingerprint, `DescriptorConfig`, tensor contract) evaluated in Java by mvr's `OnnxEvaluator` (no ONNX Runtime; it refuses operators it does not implement by name). One task per view: `Spark.getSparkJobSpimData2`, `EmbeddingModel.load`, `InterestPointEmbedding.embedView` (all points of the base label, `PatchExtractor` samples the cubes from the image pyramid exactly as pcmatch: per-point sigma quantised to 2^(1/12) for scale-space labels, read floor at the detection level, trilinear, NaN outside) and `InterestPointEmbedding.write` into `setup<S>` of the array; the driver prepares the array once before the tasks (`prepareArray`: an array of another model or width is replaced whole) and records the action in the XML afterwards. The arrays are outside the store's generations: no staging, no commit, tasks on different views never collide. `--model` is a path or a Hugging Face Hub model `hf://<namespace>/<name>[@<revision>][/<file>]` (mvr `ModelHub`, 2026-10-30: the driver pins it to its commit and records that URI, every executor downloads the `.onnx` and sidecar once into `--modelCache`, default `~/.bigstitcher/models` or the property `bigstitcher.models`, verified by sha256; the first model is `JaneliaSciComp/bigstitcher-embedding-translation-stack64`, moved from Stephan's account on 2026-10-31; the hub redirects the old name). `--arrayName` (default `embedding`), `--blockSize` (128), `--dryRun`. Verified 2026-10-30 against pcmatch's fixtures (ds1 `beads`, embryo `ss-iso_r0.008` with per-point sigma): identical NaN masks, levels and spacings, vectors within 3e-5 of pcmatch's and 2.5e-4 of its stored GPU rows (TF32), the matcher finds the same 158 candidates / 116 inliers on Java's rows as on pcmatch's. Test `TestInterestPointEmbedding` (tiny ONNX model written by mvr's `TestOnnxModels`; an `hf://` run against mvr's `TestModelHub.FakeHub`).
- `filter-interestpoints` (`FilterInterestPoints`, driver-only) creates filtered labels through `Filter_Detections.create` exactly as the GUI's "Filter/Manage Interest Points"; the option grammar accepts the dialog's `a`/`a-b` and the canonical `a..b` of the action history, so `ActionToSparkCli` renders the GUI's `filter-interestpoints` records as these commands.
- `clear-interestpoints REMOVE_LABEL` removes the filtered labels of the label as well (`InterestPointTools.dependentLabels`), `FIX_INTERESTPOINTS` drops filtered labels whose base is gone; `split-images` re-instantiates split filtered labels as filtered (`SplitDatasets.splitLabelLike`), a plain object over a filtered marker would not load.
- Tests use the constants of mvr's `TestInterestPointDetection` (sigma 1.1, threshold 0.01, intensity 0..1137 of the simulated beads); the previous Spark test constants found zero points without noticing.

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
so `CLEAR_EVERYTHING` still wipes the whole interest point store (`interestpoints.zarr`, plus a legacy `interestpoints.n5`). The set+label correspondence filter
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

### mvr master and mvr scalespace have diverged (2026-11-09)

- mvr 9.1.2 (master) replaced `IntensityAdjustments`' `HashMap<ViewId, AffineModel1D>` by per-view `Coefficients` read lazily
  from the project's container (`getCoefficients()`, `put`, constructor from a `Map`); mvr scalespace (10.0.0-SNAPSHOT) still has
  the `AffineModel1D` API. `Filter_Views` therefore differs between `main` (mvr 9.1.3) and `scalespace`; keep that in mind when
  merging `main` into `scalespace` (take scalespace's version until mvr scalespace gets the intensity update).
- `mvn compile` without `clean` after changing a dependency version in the pom reports success on stale classes (nothing in
  `src` changed, so javac is skipped); always `mvn clean compile` after a version bump before trusting it.
- mvr 9.1.3 lacked scalespace's `Arrays.copyOf(..., 3)` for the OME-Zarr mipmap transform (`N5ApiTools`, commit 7ba11e45b): the
  5-D downsampling array trips BDV's `assert resolution.length == 3` under surefire's `-ea` (`TestSparkResaveLocal`), harmless in
  production because the first three entries are x, y, z. Cherry-picked onto mvr master for 9.1.4.

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

- **`scalespace-pr108`** — current dev, matches mvr's `scalespace-pr108` (the Zarr interest point store on top of `scalespace`: scale space, attributes, filtered labels). Pinned to mvr 10.0.0-SNAPSHOT. `zarrv3` was the dev branch before (mvr 9.0.0-SNAPSHOT).
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
