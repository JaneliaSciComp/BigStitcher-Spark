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
├── ClearInterestPoints.java
├── ClearRegistrations.java
├── CreateFusionContainer.java            # Phase 1: container metadata setup
├── FilterInterestPoints.java             # filtered labels of interest points (driver-only)
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

## Container Metadata (root attributes under `Bigstitcher-Spark/`)

`StorageType`, `DataType`, `UseSharding`, `ShardSize`, `ShardSizeFactor`, `MultiResolutionInfos`, `BlockSize`, `Compression`, `MinIntensity`, `MaxIntensity`, `NumChannels`, `NumTimepoints`, `FusionFormat`.

## CLI Class Hierarchy

`AbstractBasic` (SpimData2 loading, friendly XML errors)
  → `AbstractInfrastructure` (Spark / threads / `--maxPartitions`, shared `--max*Log` knobs, transformation summary)
    → `AbstractRegistration` (registration-specific knobs)
  → `AbstractSelectableViews` (view-selection knobs)

When adding a new CLI flag, decide which level it belongs at — flags shared across multiple commands belong in `AbstractInfrastructure`, registration-only flags in `AbstractRegistration`.

## Recent Themes (October 2026): interest points of multiview-reconstruction 10

- `detect-interestpoints` stores response, sigma and intensity with every detection (`--storeIntensities` is gone), has `--method SCALE_SPACE` (+ `--steps`, `--octaves`, `--no-finestLevel`), `--anisotropy` and `--noiseFactor` (threshold = factor x the pooled noise floor, one Spark job per view, resolved on the driver before the detection jobs). The temporary per-block results are written with `InterestPointsN5.saveInterestPointsStatic( writer, path, points )` into `interestpoints.n5/spark_tmp_*` and read with `loadInterestPointsStatic`, so the attributes travel with the points; the combined result of a view goes to a staging file of the dataset's interest point store (`InterestPointsZarr.stagePoints`, one file per view task), which the driver's XML save commits after `InterestPointTools.addInterestPointEntries` registered the entries (nothing is loaded on the driver). The scale space merges duplicates across blocks per view with `DoGScaleSpace.mergeDuplicates( points, scale, sigmaMin, combineDistance )`. `--maxSpots` is `InterestPointFilter.limit( .., INTENSITY, HIGHEST, n )`. The legacy DoG path is unchanged otherwise (no block expansion). `--overlappingOnly` (2026-10-27) uses mvr's `OverlappingRegions` like the GUI: `overlapping( data, view, candidates )` for the partners (all other selected views, or `otherVolumes` = another tile or angle of the same timepoint with `--onlyCompareOverlapTiles`, which before compared timepoint and channel), `intersection( data, view, partner, image, mipmap )` for the region of a partner in the downsampled pixels of the view (the metadata job), `combine( lists, OverlappingRegions.combineDistance )` for the per-view merge of the regions' points (0.5 px full resolution), `dropContained` on the driver for regions inside another of the same view (2026-10-28, a diagonal neighbour's corner lies in the side strips; `otherVolumes` also gives one partner per neighbour volume, the same channel preferred, so a neighbour tile is one strip and not one per channel); BSS's own `OverlappingViews.findAllOverlappingViewsFor` and KD-tree loop are gone. `TestSparkInterestPointDetection.testOverlappingOnly` runs it on the simulated dataset with shifted registrations.
- Interest point storage (2026-10-23, branch `scalespace-pr108` of both repos): multiview-reconstruction keeps all interest points of a dataset in `interestpoints.zarr`, one self-contained group per (label, timepoint) that is replaced whole on change (see MVR `claude.md`, "Interest Point Storage"). Rules for Spark code: executors NEVER commit; a task saves through `InterestPointsZarr.saveStaged( lists )` or `stagePoints(...)` (one staging file per task, durable, readable by other tasks at once), the driver's `XmlIoSpimData2.save` (or `saveInterestPointsInParallel`) folds and commits. `split-images` phases 2/3 and `detect-interestpoints` do that; `match-interestpoints` stages and commits on the driver; `clear-interestpoints` deletes list entries (`deleteInterestPoints` = the whole entry) and lets the XML save commit, `CLEAR_ALL_CORRESPONDENCES` runs one batch. Old `interestpoints.n5` datasets are read as they are (fallback per label) and converted once with `java -cp <fat jar> net.preibisch.mvrecon.fiji.spimdata.interestpoints.InterestPointsN5ToZarr <dataset.xml>`. The `spark_tmp_*` scratch of the detection stays N5. Never let two JVMs commit the same (label, timepoint) at once (the second one fails loudly and nothing is lost); jobs on different labels or timepoints never collide.
- 2026-10-27 (with mvr e886d74d): `detect-interestpoints` removes the scratch `interestpoints.n5` when it holds nothing else after the run (a Zarr-only dataset keeps no stray legacy container); a label that exists in the selected views has its references dropped on the driver before the tasks run (`InterestPointTools.removeReferences`, what the GUI's re-detection does; before and not after the tasks, since a staging file of the new points shadows the old entry for every read, so the old correspondences could not be found any more); the Spark parameters strings end with the GUI's tail `localization=NONE|QUADRATIC groupTiles=false groupIllums=false thresholdType=fixed|noisefloor [noiseFactor=]` (`DifferenceOfGUI.localizationKey`), which `StoredDetectionParameters.parse` reads for the GUI's "Re-detect interest points with attributes". `TestSparkInterestPointDetection.testRedetectionReplacesLabel` covers the first two.
- `solver` (2026-10-27; Lila Schweinfurth's branch `ip-store-update`, commits 3e3f31c, 4d985e3, c4e2968, ported): `Solver.setupPointMatchesFromInterestPoints` builds the view pairs from the correspondence partners of every (view, label) (`InterestPoints.getCorrespondingViews()`, answered from the store's index without loading points) instead of every pair of the selected views, reads the correspondences of a pair for that partner only (`getCorrespondingInterestPointsCopy( vB, labelB )`, a slice of the store's per-partner grouping) and loads the points of a (view, label) once for all its pairs (`pointMaps`); the pre-load no longer reads whole correspondence lists; pairs without correspondences no longer produce an empty `PairwiseResult`. The pair enumeration goes from n^2 to the connected pairs.
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
