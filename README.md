[![Build Status](https://github.com/PreibischLab/BigStitcher-Spark/actions/workflows/build.yml/badge.svg)](https://github.com/PreibischLab/BigStitcher-Spark/actions/workflows/build.yml)

# BigStitcher-Spark

*Supported by the [HHMI Janelia Open Science Software Initative](https://www.janelia.org/open-science/overview/open-science-software-initiative-ossi)*

[![install4j](https://www.ej-technologies.com/images/product_banners/install4j_small.png)](https://www.ej-technologies.com/products/install4j/overview.html)

This package allows you to run compute-intense parts of BigStitcher distributed on your workstation, a cluster or the cloud using Apache Spark. The following modules are currently available in BigStitcher-Spark listed as `JavaClassName`/**`cmd-line-tool-name`** (you can find documentation below, but a good start is also to just check out the cmd-line arguments, they mostly follow the BigStitcher GUI; each module takes an existing XML):

* `SparkResaveN5`/**`resave`** (resave an XML dataset you defined in BigStitcher - use virtual loading only - into N5 for processing)
* `SparkInterestPointDetection`/**`detect-interestpoints`** (detect interest points for alignment)
* `SparkGeometricDescriptorMatching`/**`match-interestpoints`** (perform pair-wise interest point matching)
* `SparkPairwiseStitching`/**`stitching`** (run pairwise stitching between overlapping tiles)
* `Solver`/**`solver`** (perform the global solve, works with interest points and stitching)
* `CreateFusionContainer`/**`create-fusion-container`** (set up the container into which the affine fusion will write it's data, now supports OME-ZARR)
* `SparkAffineFusion`/**`affine-fusion`** (fuse the aligned dataset using interpolated affine/rigid/translation models)
* `SparkNonRigidFusion`/**`nonrigid-fusion`** (fuse the aligned dataset using non-rigid models)

Additonally there are some utility methods:
* `SparkDownsample`/**`downsample`** (perform downsampling of existing volumes)
* `ClearInterestPoints`/**`clear-interestpoints`** (clears interest points)
* `ClearRegistrations`/**`clear-registrations`** (clears registrations)

***Note: BigStitcher-Spark is designed to work hand-in-hand with BigStitcher.** You can always verify the results of each step BigStitcher-Spark step interactively using BigStitcher by simply opening the XML. You can of course also run certain steps in BigStitcher, and others in BigStitcher-Spark. Not all functionality is 100% identical between BigStitcher and BigStitcher-Spark; important differences in terms of capabilities is described in the respective module documentation below (typically BigStitcher-Spark supports a specific feature that was hard to implement in BigStitcher and vice-versa).*

## Content

* [**Install and Run**](#install)
  * [Local](#installlocal)
  * [Cluster](#installcluster)
  * [Cloud](#installcloud)
* [**Example Datasets**](#examples)
* [**Usage**](#usage)
  * [Resave Dataset](#resave)
  * [Pairwise Stitching](#stitching)
  * [Detect Interest Points](#ip-detect)
  * [Match Interest Points](#ip-match)
  * [Solver](#solver)
  * [Affine Fusion](#affine-fusion)
    * [Create Fusion Container](#create-fusion-container)
    * [Run Affine Fusion](#run-affine-fusion)
  * [Non-Rigid Fusion](#nonrigid-fusion)

<img align="left" src="https://github.com/JaneliaSciComp/BigStitcher-Spark/blob/main/src/main/resources/bs-spark.png" alt="Overview of the BigStitcher-Spark pipeline">

## Install and Run<a name="install">

### To run it on your local computer<a name="installlocal">

* Prerequisites:  **Java** (_[Zulu JDK 8 + FX](https://www.azul.com/downloads/?version=java-8-lts&package=jdk-fx#zulu) is tested, and Java >=21 currently does not work with the Spark version used_) and **[Apache Maven](https://maven.apache.org)** must be installed. Try `java -version` and `mvn -v` to confirm their functionality and versions. You have to set the `JAVA_HOME` environment variable for Maven to find the right Java.
* Clone the repo and `cd` into `BigStitcher-Spark`
* Run the included bash script `./install -t <num-cores> -m <mem-in-GB> ` specifying the number of cores and available memory in GB for running locally. This should build the project and create the executable `resave`, `detect-interestpoints`, `register-interestpoints`, `stitching`, `solver`, `affine-fusion`, `nonrigid-fusion`, `downsample`, `clear-interestpoints` and `clear-registrations` in the working directory.

If you run the code directly from your IDE, you will need to add JVM paramters for the local Spark execution (e.g. 8 cores, 50GB RAM):
```
-Dspark.master=local[8] -Xmx50G
```

### To run it on a compute cluster<a name="installcluster">

`mvn clean package -P fatjar` builds `target/BigStitcher-Spark-0.0.1-SNAPSHOT.jar` for distribution.

***Important:*** if you use HDF5 as input data in a distributed scenario, you need to set a common path for extracting the HDF5 binaries (see solved issue [here](https://github.com/PreibischLab/BigStitcher-Spark/issues/8)), e.g.
```
--conf spark.executor.extraJavaOptions=-Dnative.libpath.jhdf5=/groups/spruston/home/moharb/libjhdf5.so
```

Please ask your sysadmin for help how to run it on your **cluster**, below are hopefully helpful tutorials for different kinds of clusters. They can be helpful to transfer the knowledge to your home institution.

#### Instructions/stories how people set up Spark/BigStitcher-Spark on their respective clusters:
* HHMI Janelia (LSF cluster): [Tutorial on YouTube](https://youtu.be/D3Y1Rv_69xI?si=mp_57Jby0T2ETP0p&t=5520) by [@trautmane](https://github.com/trautmane)
* MDC Berlin (SGE cluster): [Google doc explaining the steps](https://docs.google.com/document/d/119jxXk-w3GWZ3IvpMXlAclE8ZeNXniiuO3YScMyImUQ/edit?usp=sharing) by [@bellonet](https://github.com/bellonet)
* ***We are currently developing a generic [NextFlow](https://www.nextflow.io)-based pipeline for submitting BigStitcher-Spark jobs to a cluster***

### To run it on the cloud<a name="installcloud">

`mvn clean package -P fatjar` builds `target/BigStitcher-Spark-0.0.1-SNAPSHOT.jar` for distribution.

BigStitcher-Spark is now fully "cloud-native". For running the fatjar on the **cloud** check out services such as [Amazon EMR](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark.html) and [Google Serverless Batches](https://cloud.google.com/dataproc-serverless/docs/quickstarts/spark-batch). Note that some modules support prefetching `--prefetch`, which is important for cloud execution due to its delays as it pre-loads all image blocks in parallel before processing. We will soon add detailled information on how to run the examples on both cloud platforms (it works - if you need help now, please contact @StephanPreibisch).

## Example Datasets<a name="examples">

We provide two example datasets (one for *interest-point based registration*, one that works well with *Stitching*), which are available for download several times with increasing level of reconstruction so you can test different modules of **BigStitcher-Spark** directly. The datasets are again linked throughout the documentation for the individual modules. If you would like to test the entire pipeline we suggest to start with RAW datasets and run the entire pipeline. Here is an overview of the two datasets at different stages:

* Dataset for **Stitching**:
  *  [As TIFF](https://drive.google.com/file/d/15xSQCBHcpEvJWd6YD5iKJzuc0IRpWB8a/view?usp=sharing) (unaligned, no BigStitcher project defined)
  *  [As TIFF/XML](https://drive.google.com/file/d/1-nqzBbtff8u93LGbCTPRwikWJH6o6B46/view?usp=sharing) (unaligned)
  *  [As N5/XML](https://drive.google.com/file/d/1Q2SCJW_tCVKFzrdMrgVrFDyiF6nUN5-B/view?usp=sharing) (unaligned)
  *  [As N5/XML](https://drive.google.com/file/d/1we4Iif17bdS4PiWsgRTi3TLNte8scG4u/view?usp=sharing) containing pairwise stitching results (unaligned)
  *  [As N5/XML](https://drive.google.com/file/d/1ajjk4piENbRrhPWlR6HqoUfD7U7d9zlZ/view?usp=sharing) (aligned)

* Dataset for **Interest Points**:
  *  [As TIFF](https://drive.google.com/file/d/1VFT2APVPItBCyzrQ7dPWBNILyNh6yDKB/view?usp=sharing) (unaligned, no BigStitcher project defined)
  *  [As TIFF/XML](https://drive.google.com/file/d/1Qs3juqQgYlDc2KglbcFTFKzdAQxgS9zc/view?usp=sharing) (unaligned)
  *  [As N5/XML](https://drive.google.com/file/d/16V8RBYP3TNrDVToT9BoRxqclGE15TwKM/view?usp=sharing) (unaligned)
  *  [As N5/XML](https://drive.google.com/file/d/14hQAljavSNcpUOWwwUOm0Ev2HqcaqWtI/view?usp=sharing) containing interest points (unaligned)
  *  [As N5/XML](https://drive.google.com/file/d/1Ew9NZaOjz7unkQYCOM5f9D6sdKtFz8Fc/view?usp=sharing) containing matched interest points (unaligned)
  *  [As N5/XML](https://drive.google.com/file/d/13b0UzWuvpT_qL7JFFuGY9WWm-VEiVNj7/view?usp=sharing) (aligned)

For this tutorial I extracted the Stitching dataset into `~/SparkTest/Stitching` and the dataset for experimenting with interest points into `~/SparkTest/IP`.

# Usage<a name="usage">

## Resave Dataset<a name="resave">

When working with BigStitcher the first step is to [define a dataset](https://imagej.net/plugins/bigstitcher/define-new-dataset), where the goal is to provide sufficient meta-data in order to allow BigStitcher (and BigDataViewer) to load your images. This step is typically done in the BigStitcher GUI, but some people have written scripts to automatically generate XML's for their datasets (e.g. [here](https://github.com/nvladimus/npy2bdv/tree/v1.0.0)). If you want to start testing an entire BigStitcher(-Spark) pipeline from scratch, please use [this dataset for stitching](https://drive.google.com/file/d/15xSQCBHcpEvJWd6YD5iKJzuc0IRpWB8a/view?usp=sharing) or [this one using interest points](https://drive.google.com/file/d/1VFT2APVPItBCyzrQ7dPWBNILyNh6yDKB/view?usp=sharing).

After the dataset is defined one usually re-saved the input data (TIFF, CZI, ...) into a multi-resolution format that makes it possible to interactively display and work with the data the image as various resolution levels, and is essential for distributed processing. Right now, we use the N5 format for temporary storage of the input data. This resaving process can take substantial amounts of time if your input is large and can be distributed using Spark. Importantly, you need to define your dataset using the **Automatic Loader (Bioformats based)** and select to **Load data directly** and **load data virtually**.

For testing the re-saving (and multi-resolution pyramid creation) with Spark use your defined dataset(s), or download [this dataset for stitching](https://drive.google.com/file/d/1-nqzBbtff8u93LGbCTPRwikWJH6o6B46/view?usp=sharing) or [this dataset for interest points](https://drive.google.com/file/d/1Qs3juqQgYlDc2KglbcFTFKzdAQxgS9zc/view?usp=sharing)

The command for resaving the stitching dataset could look like this and will overwrite the input XML, a backup XML will be automatically created:

<code>./resave -x ~/SparkTest/Stitching/dataset.xml -xo ~/SparkTest/Stitching/dataset.xml</code>

It is analogous for the interest point dataset:

<code>./resave -x ~/SparkTest/IP/dataset.xml -xo ~/SparkTest/IP/dataset.xml</code>

Please run `resave` without parameters to get help for all command line arguments. Using `--blockSize` you can change the blocksize of the N5, and `--blockScale` defines how many blocks at once will be processed by a Spark job. With `-ds` you can define your own downsampling steps if the automatic ones are not well suited. 

***Note:*** `--dryRun` allows the user to test the functionality without writing any data. The Spark implementation parallelizes over user-defined blocks across all input images at once, so also few, very big images will be processed efficiently.

## Pairwise Stitching<a name="stitching">

To perform classical stitching (translation only), first pair-wise stitching between all overlapping tiles needs to be computed. So far we only support standard grouping where all channels and illuminations of a specific Tile will be grouped together as one image and stitching is performed individually per Timepoint and Angle. To run the stitching with default paramters you can run the following command on this [example dataset](https://drive.google.com/file/d/1Q2SCJW_tCVKFzrdMrgVrFDyiF6nUN5-B/view?usp=sharing):

<code>./stitching -x ~/SparkTest/Stitching/dataset.xml</code>

The results will be written in to the XML, in order to compute transformation models and apply them to the images you need to run the [solver](#solver) that computes a global optimization next.

Please run `stitching` without parameters to get help for all command line arguments. `-ds` sets the resolution at which cross correlation is performed; `2,2,1` is the default and usually superior to `1,1,1` due to suppressed noise, even higher resolution levels typically work well too since by default the peaks are located with subpixel accuracy. `--disableSubpixelResolution` disables subpixel accurate shifts. `-p` sets the number of phase correlation peaks that are checked with cross-correlation (incrementing this number can help with stitching issues). `--minR` and `--maxR` are filters that specify the accepted range for cross correlation for any pair of overlapping tiles, reducing `--minR` may be useful to accept more pair and excluding a `--maxR` of `1` may be useful too if you get wrong links with `r=1.0`. `--maxShiftX/Y/Z` and `--maxShiftTotal` set the maximal allowed shift between any pair of images relative to their current location; limiting it if the current position is close to the correct one might be useful. If your dataset contains multiple channels or illumination directions per Tile, you can select how they will be combined for the pair-wise stitching process using `--channelCombine` and `--illumCombine`, which can be either `AVERAGE` or `PICK_BRIGHTEST`. 

You can choose which Tiles `--tileId`, Channels `--channelId`, Iluminations `--illuminationId`, Angles `--angleId` and Timepoints `--timepointId` will be processed, a typical choice could be `--timepointId 18 --tileId 0,1,2,3,6,7,8` to only process the timepoint 18 and select Tiles. If you would like to choose Views more fine-grained, you can specify their ViewIds directly, e.g. `-vi '0,0' -vi '0,1' -vi '1,1'` to process ViewId 0 & 1 of Timepoint 0 and ViewId 1 of Timepoint 1. By default, everything will be processed.

***Note:*** `--dryRun` allows the user to test the functionality without writing any data. The Spark implementation parallelizes over pairs of images.

## Detect Interest Points<a name="ip-detect">

Interest-point based registration is generally more reliable and faster than stitching while supporting various transformation models including **(regularized) Translation**, **(regularized) Rigid**, **(regularized) Affine**, and **Non-Rigid**. At the same time parameter selection is more involved. The first step is to detect interest points in the images. A typical command line call that works well on [this example dataset](https://drive.google.com/file/d/16V8RBYP3TNrDVToT9BoRxqclGE15TwKM/view?usp=sharing) looks as follows:

<code>./detect-interestpoints -x ~/SparkTest/IP/dataset.xml -l beads -s 1.8 -t 0.008 -dsxy 2 --minIntensity 0 --maxIntensity 255</code>

The results will be written in the `XML` and the `interestpoints.n5` directory, in order to compute transformation models and apply them to the images you need to run [matching](#ip-match) followed by the [solver](#solver) that computes a global optimization. If you want to inspect the interest points you can open the XML in BigStitcher, go to multiview mode, right click, start the **Interest Point Explorer** and click on the number of detections that will then be overlaid onto the images (see screenshot below).

Please run `detect-interestpoints` without parameters to get help for all command line arguments. `-s` and `-t` define the sigma and threshold of the Difference-of-Gaussian, respectively and `-l` specifies the label for the interest points. `--minIntensity` and `--maxIntensity` set the intensity range in which all processed blocks are normalized to `[0...1]`; these values are mandatory since each individual Spark job is unable to figure out correct min/max values of the images. You could find good guesses for all these values by starting the interactive interest point detection in BigStitcher. `-dsxy` and `-dsz` define the downsampling at which interest point detection is performed. Using `--localization` you can specify the type of subpixel localization, either `NONE` or `QUADRATIC`. `--type` allows to set which type of intensity peaks should be identified; `MIN`, `MAX` or `BOTH`. Finally, `--blockSize` sets the blocksize that will be processed in each Spark job.

`--overlappingOnly` is a feature that will only identify interest points in areas of each image that is currently overlapping with another image. `--storeIntensities` will extract intensities of each interest point and store it in the `interestpoints.n5` directory as extra datasets. `--prefetch` will use parallel threads to pre-load all image data blocks ahead of the computation, which is desirable for cloud execution.

You can choose which Tiles `--tileId`, Channels `--channelId`, Iluminations `--illuminationId`, Angles `--angleId` and Timepoints `--timepointId` will be processed, a typical choice could be `--timepointId 18 --tileId 0,1,2,3,6,7,8` to only process the timepoint 18 and select Tiles. If you would like to choose Views more fine-grained, you can specify their ViewIds directly, e.g. `-vi '0,0' -vi '0,1' -vi '1,1'` to process ViewId 0 & 1 of Timepoint 0 and ViewId 1 of Timepoint 1. By default, everything will be processed.

***Note:*** `--dryRun` allows the user to test the functionality without writing any data. The Spark implementation parallelizes over user-defined block across all processed images at once.

<img align="left" src="https://github.com/JaneliaSciComp/BigStitcher-Spark/blob/main/src/main/resources/BigStitcher-interestpoints.jpg" alt="Visualizing interest points in BigStitcher">
&nbsp;

## Match Interest Points<a name="ip-match">

After interest points are detected they are pair-wise matching between all views/images (***Note: this also works for Stitching, try it out***). Several point cloud matching methods and ways how views can be grouped are supported, which will be explained below. Importantly, matching & solving can be performed once or iteratively; typical workflows that match & solve more than once are 1) to first align each timepoint of a series using affine models individually followed by registration across time using translation models or 2) to first align using geometric descriptor matching to then subsequently refine the result using Iterative Closest Point (ICP) that only works once the current transformation is very good (***Note:*** ICP alignment creates many corresponding interest points that might be desirable for [Non-Rigid fusion](#nonrigid-fusion) where all corresponding interest points are perfectly matched on top of each other).

A typical, simple command line call to register each timepoint alignment individually using [this example](https://drive.google.com/file/d/1we4Iif17bdS4PiWsgRTi3TLNte8scG4u/view?usp=sharing) looks like:

<code>./match-interestpoints -x ~/SparkTest/IP/dataset.xml -l beads -m FAST_ROTATION --clearCorrespondences</code>

Please run `match-interestpoints` without parameters to get help for all command line arguments. `-l` defines the label of the detected interest points used for matching. `-tm` specifies the transformation model to be used (`TRANSLATION`, `RIGID` or (default)`AFFINE`), `-rm` defines the regularization model (`NONE`, `IDENTITY`, `TRANSLATION`, (default)`RIGID` or `AFFINE`) and `--lambda` `[0..1]` is the lambda for the regularization model, which is set to `0.1` by default. `-vr` defines which views/images will be matched; `OVERLAPPING_ONLY` or `ALL_AGAINST_ALL`. `--clearCorrespondence` removes potentially existing, stored matches between views, if it is not called the identified matches will be added to the existing ones.

`-m` defines the matching method; `FAST_ROTATION`, `FAST_TRANSLATION`, `PRECISE_TRANSLATION` or `ICP`.
* `FAST_ROTATION` is a rotation invariant method that uses geometric hashing and can find corresponding constellation of points even if they are significantly rotated relative to each other.
  * `-s` defines the significance during descriptor matching, to establish a correspondence the best matching descriptor has to be `s` times better than the second best matching descriptor.
  * `-r` is the level of redundancy during descriptor matching, it adds extra neighbors to each descriptor and tests all combinations of neighboring points. 
* `FAST_TRANSLATION` is a translation invariant method that uses geometric hashing and can find corresponding constellation of points irrespective of their location in the image. It tolerates small rotatation of up to a few degrees.
  * supports the same parameters `-r`, `-s` as FAST_ROTATION above
* `PRECISE_TRANSLATION` is a translation invariant method that uses exhaustive search to find corresponding constellation of points irrespective of their location in the image. It tolerates small rotatation of up to a few degrees.
  * supports the same parameters `-r`, `-s` as FAST_ROTATION above, and additionally support `-n` to specify the number of neighboring points that are used to build geometric descriptors
* `ICP` is a method that iteratively assignes closest pairs of points between two images until convergence and can be used for fine alignment.
  * `-ime` is the ICP maximum allowed error, `-iit` defines the number of ICP iterations and `--icpUseRANSAC` enables RANSAC at every ICP iteration

All methods use RANSAC to robustly identify a set of corresponding points in the set or correspondence candidates (optional for ICP). `-rit` defines the number of RANSAC iterations (increasing might help to find more correspondences), `-rme` the maximum error (epsilon) for RANSAC (increasing might help to find more correspondences), `-rmir` the minimum inlier ratio (setting to `0.0` might help to find more correspondences), and `-rmif` defines the minimum inlier factor for RANSAC (i.e. how many times the minimal number of inliers required by the transformation model need to be identified so a pair is valid).

By default, all views/images are matched individually. However, under certain conditions it may be useful to group certain views together (see illustration below). `--splitTimepoints` groups all angles/channels/illums/tiles that belong to the same timepoint as one single View, e.g. for stabilization across time. `--groupChannels` groups all channels that belong to the same angle/illumination/tile/timepoint together as one view, e.g. to register all channels together as one. `--groupTiles` groups all tiles that belong to the same angle/channel/illumination/timepoint together as one view, e.g. to align across angles. `--groupIllums` groups all illumination directions that belong to the same angle/channel/tile/timepoint together as one view, e.g. to register illumation directions together. Importantly, interest points in overlapping areas of grouped views need to be merged; `--interestPointMergeDistance` allows to set the merge distance. ***Note:*** You do not need to group views for interest point matching in order to group views during [solving](#solver), these are two independent operations. However, it (usually) is not advisable to only group during matching.

You can choose which Tiles `--tileId`, Channels `--channelId`, Iluminations `--illuminationId`, Angles `--angleId` and Timepoints `--timepointId` will be processed, a typical choice could be `--timepointId 18 --tileId 0,1,2,3,6,7,8` to only process the timepoint 18 and select Tiles. If you would like to choose Views more fine-grained, you can specify their ViewIds directly, e.g. `-vi '0,0' -vi '0,1' -vi '1,1'` to process ViewId 0 & 1 of Timepoint 0 and ViewId 1 of Timepoint 1. By default, everything will be processed.

<img align="left" src="https://github.com/JaneliaSciComp/BigStitcher-Spark/blob/main/src/main/resources/grouping.png" alt="Grouping in BigStitcher">
&nbsp;

When performing timeseries alignment, grouping is often a good choice (`--splitTimepoints`) and further details regarding matching across time need to be specified. ***Important:*** if you are running a second (or third) round of matching, you always need to [solve](#solver) in between to bake in the resulting transformations. `-rtp` defines the type of time series registration; `TIMEPOINTS_INDIVIDUALLY` (i.e. no registration across time), `TO_REFERENCE_TIMEPOINT`, `ALL_TO_ALL` or `ALL_TO_ALL_WITH_RANGE`. Depending on your choice you may need to define the range of timepoints `--rangeTP` or the reference timepoint `--referenceTP`. Below is an example command line call for aligning all views against all across time:

<code>./match-interestpoints -x ~/SparkTest/IP/dataset.xml -l beads -m FAST_ROTATION --clearCorrespondences -rtp ALL_TO_ALL --splitTimepoints</code>

***Note:*** `--dryRun` allows the user to test the functionality without writing any data. The Spark implementation parallelizes over pairs of images.

## Solver<a name="#solver">

The Solver computes a globally optimized result (one transformation per view/image) using all pairwise matches (interest points or stitching), specifically by minimizing the distance between all corresponding points (paiwise stitching is also expressed as a set of corresponding points) across all images/views. A typical call for running the solver on **stitching** results is (e.g. [this dataset](https://drive.google.com/file/d/1we4Iif17bdS4PiWsgRTi3TLNte8scG4u/view?usp=sharing)):

<code>./solver -x ~/SparkTest/Stitching/dataset.xml -s STITCHING</code>

and when using matched **interestpoints** individually per timepoint it is (e.g. [this dataset](https://drive.google.com/file/d/1Ew9NZaOjz7unkQYCOM5f9D6sdKtFz8Fc/view?usp=sharing)):

<code>./solver -x ~/SparkTest/IP/dataset.xml -s IP -l beads</code>

Please run `solver` without parameters to get help for all command line arguments. `-s` switches between `STITCHING` and `IP` (interest points) mode, `-l` defines the interest point label in the latter case. By default the first view of each timepoint will be fixed, `-fv` allows to specify certain views to be fixed and `--disableFixedViews` will not fix any views (in this case make sure to not use plain affine models). `-tm` specifies the transformation model to be used (`TRANSLATION`, `RIGID` or (default)`AFFINE`), `-rm` defines the regularization model (`NONE`, `IDENTITY`, `TRANSLATION`, (default)`RIGID` or `AFFINE`) and `--lambda` `[0..1]` is the lambda for the regularization model, which is set to `0.1` by default.

`--maxError` sets the maximum allowed error for the solve (it will iterate at least until it is under that value), `--maxIterations` defines the maximum number of iterations, and `--maxPlateauwidth` defines the number of iterations that are used to estimate if the solve converged (and is thus also the minimal number of iterations). 

There are several types of solvers available; `--method` allows to choose `ONE_ROUND_SIMPLE`, `ONE_ROUND_ITERATIVE`, `TWO_ROUND_SIMPLE` or `TWO_ROUND_ITERATIVE`. Two round handles unconnected tiles (it moves them into their approximate right location using metadata using a second solve where all views that are connected are grouped together), while iterative tries to identify and remove wrong/inconsistent links between pairs of views. The iterative strategies are parameterized by `--relativeThreshold` (relative error threshold, how many times worse than the average error a link between a pair of views needs to be) and the `--absoluteThreshold` (absoluted error threshold to drop a link between a pair of views - in pixels). The error is computed as the difference between the pairwise alignment of two views and their alignment after running the solve.

You can choose which Tiles `--tileId`, Channels `--channelId`, Iluminations `--illuminationId`, Angles `--angleId` and Timepoints `--timepointId` will be processed, a typical choice could be `--timepointId 18 --tileId 0,1,2,3,6,7,8` to only process the timepoint 18 and select Tiles. If you would like to choose Views more fine-grained, you can specify their ViewIds directly, e.g. `-vi '0,0' -vi '0,1' -vi '1,1'` to process ViewId 0 & 1 of Timepoint 0 and ViewId 1 of Timepoint 1. By default, everything will be processed.

By default, all views/images are matched individually. However, under certain conditions it may be useful to group certain views together (see illustration above). `--splitTimepoints` groups all angles/channels/illums/tiles that belong to the same timepoint as one single View, e.g. for stabilization across time. `--groupChannels` groups all channels that belong to the same angle/illumination/tile/timepoint together as one view, e.g. to register all channels together as one. `--groupTiles` groups all tiles that belong to the same angle/channel/illumination/timepoint together as one view, e.g. to align across angles. `--groupIllums` groups all illumination directions that belong to the same angle/channel/tile/timepoint together as one view, e.g. to register illumation directions together.

When performing timeseries alignment, grouping is usually a good choice (`--splitTimepoints`) and further details regarding matching across time need to be specified. ***Important:*** if you are running a second (or third) round of matching, you usually need to [match interest points](#ip-match) again. `-rtp` defines the type of time series registration; `TIMEPOINTS_INDIVIDUALLY` (i.e. no registration across time), `TO_REFERENCE_TIMEPOINT`, `ALL_TO_ALL` or `ALL_TO_ALL_WITH_RANGE`. Depending on your choice you may need to define the range of timepoints `--rangeTP` or the reference timepoint `--referenceTP`. Below is an example command line call for aligning all views against all across time:

When using interestpoints (for timeseries alignment with grouping all views of a timepoint together) a tyical call could look like that:

<code>./solver -x ~/SparkTest/IP/dataset.xml -s IP -l beads -rtp ALL_TO_ALL_WITH_RANGE --splitTimepoints</code>

***Note:*** `--dryRun` allows the user to test the functionality without writing any data. The solver currently only runs multi-threaded.

## Affine Fusion<a name="affine-fusion">

Performs **fusion using affine transformation models** computed by the [solve](#solver) (also supports translations, rigid, interpolated models) that are stored in the XML (*Warning: not tested on 2D*). By default the affine fusion will create an output image that encompasses all transformed input views/images. While this is good in some cases such as tiled stitching tasks, the output volume can be unnecessarily large for e.g. multi-view datasets. Thus, prior to running the fusion it might be useful to [**define a custom bounding box**](https://imagej.net/plugins/bigstitcher/boundingbox) in BigStitcher.

### Create Fusion Container<a name="create-fusion-container">

The first step for fusing a dataset is to create an empty **output container** that also contains all metadata and empty multi-resolution pyramids. By default an **OME-ZARR** is created, **N5** and **HDF5** are also supported (note that HDF5 only works if Spark is run multi-threaded on a local computer, i.e. not on a cluster or the cloud). By default, this will create an output container that contains a 3D volume for each channel and timepoint present in the dataset (i.e. `numChannels x numTimepoints` 3D volumes). In the case of OME-ZARR, it is represented as a single 5D volume, for N5 and HDF5 it is a series of 3D volumes. ***Note: if you do NOT want to export the entire project, or want to specify fusion assignments (which views/images are fused into which volume), please check the details below. In short, you can specify the dimensions of the output container here, and the fusion assignments in the affine-fusion step below.***

A typical call for creating an output container for e.g. the **stitching** dataset is (e.g. [this dataset](https://drive.google.com/file/d/1ajjk4piENbRrhPWlR6HqoUfD7U7d9zlZ/view?usp=sharing)):

<code>./create-fusion-container -x ~/SparkTest/Stitching/dataset.xml -o ~/SparkTest/Stitching/Stitching/fused.zarr --preserveAnisotropy --multiRes -d UINT8</code>

The **output container** for the [dataset that was aligned using interest points](https://drive.google.com/file/d/13b0UzWuvpT_qL7JFFuGY9WWm-VEiVNj7/view?usp=sharing) can be created in the same way, except that we choose to use the bounding box `embryo` that was specified using BigStitcher and we choose to save as an BDV/BigStitcher project using N5 as underlying export data format:

<code>./create-fusion-container -x ~/SparkTest/IP/dataset.xml -o ~/SparkTest/IP/fused.n5 -xo ~/SparkTest/IP/dataset-fused.xml -s N5 -b embryo --bdv --multiRes -d UINT8</code>

Running `create-fusion-container` without parameters lists help for all command line arguments. `-x` specifies the XML, `-o` defines the output volume location and `-s` the output type `OME-ZARR`, `N5`, or `HDF5` (latter only when running on a local computer). `--multiRes` will create multiresolution pyramids of the fused image(s), `-ds` allows to optionally specify the downsampling steps for the multiresolution pyramid manually. `--bdv` will create fused volumes together with an XML that can be directly opened by **BigStitcher** or **BigDataViewer**, where `-xo` defines the location of the XML for the fused dataset.

You can specify the *datatype* for the fused image using `-d`, `UINT8` *[0..255]*, `UINT16` *[0..65535]* or `FLOAT32` (default) are supported. When selecting UINT8 or UINT16 you can set `--minIntensity` and `--maxIntensity`, which define the range of input intensities that will be mapped to *[0..255]* for `UINT8` or *[0..65535]* for `UINT16`, respectively. The default values are *[0..255]* for `UINT8` or *[0..65535]* for `UINT16`, which works well if input and output datatype are the same.

You can specify a bounding box using `-b`. `--preserveAnisotropy` will preserve the anisotropy of the input dataset, which is a recommended setting if all views/images are taken in the same orientation, e.g. when processing a tiled dataset. The factor will be computed from the data by default, or can be specified using `--anisotropyFactor`.

`-c` allows to specify the compression method, `-cl` set the compression level if supported by the compression method. `--blockSize` defaults to `128x128x128`, which you might want to reduce when using HDF5.

`--numChannels` allows you to override the number of channels in the fused output dataset, by default it will be as many channels as present in the BigStitcher project/XML. Similarly, `--numTimepoints` allows you to override the number of timepoint in the fused noutput dataset, by default it will be as many timepoints as present in the BigStitcher project/XML. ***Note: If (and only if) you specify `--numChannels` or `--numTimepoints` you MUST specify in the `affine-fusion` which ViewIds are fused into which 3D volume, since the assignment deviates from the BigStitcher project/XML. By default, all tiles/angles/illuminations for each channel and timepoint will be fused together.**

`--s3Region` allows to specify a specifc AWS region when saving to AWS S3.

***Note: creating the container for fusion is NOT Spark code (i.e. not distributed), just plain Java code, the fusion itself below is Spark code.***

### Run Affine Fusion<a name="run-affine-fusion">

This is not updated yet, in short you just need to run e.g. <code>./affine-fusion -o ~/SparkTest/Stitching/Stitching/fused.zarr</code> that points to the container you created before. 

Importantly, one can fuse several volumes into the same N5, ZARR or HDF5 container by running the fusion consecutively and specifying different folders or BDV ViewIds.

You can choose which Tiles `--tileId`, Channels `--channelId`, Iluminations `--illuminationId`, Angles `--angleId` and Timepoints `--timepointId` will be processed. For fusion  one normally chooses a specific timepoint and channel, e.g. `--timepointId 18 --channelId 0` to only fuse timepoint 18 and Channel 0 into a single volume. If you would like to choose Views more fine-grained, you can specify their ViewIds directly, e.g. `-vi '0,0' -vi '0,1'` to process ViewId 0 & 1 of Timepoint 0. 
`--blockScale` defines how many blocks to fuse in a single processing step, e.g. 4,4,1 means for blockSize of 128,128,64 that each spark thread processes 512,512,64 blocks.

***Note:*** `--dryRun` allows the user to test the functionality without writing any data. It scales to large datasets as it tests for each block that is written which images are overlapping. For cloud execution one can additionally pre-fetch all input data for each compute block in parallel. You need to specify the `XML` of a BigSticher project and decide which channels, timepoints, etc. to fuse. 

OLD DOCUMENTATION:

A typical set of calls (because it is three channels) for affine fusion into a multi-resolution ZARR using only translations on the **stitching** dataset is (e.g. [this dataset](https://drive.google.com/file/d/1ajjk4piENbRrhPWlR6HqoUfD7U7d9zlZ/view?usp=sharing)):

<code>./affine-fusion -x ~/SparkTest/Stitching/dataset.xml -o ~/SparkTest/Stitching/fused.zarr -d /ch0/s0 -s ZARR --multiRes --preserveAnisotropy --UINT8 --minIntensity 0 --maxIntensity 255 --channelId 0</code>

<code>./affine-fusion -x ~/SparkTest/Stitching/dataset.xml -o ~/SparkTest/Stitching/fused.zarr -d /ch1/s0 -s ZARR --multiRes --preserveAnisotropy --UINT8 --minIntensity 0 --maxIntensity 255 --channelId 1</code>

<code>./affine-fusion -x ~/SparkTest/Stitching/dataset.xml -o ~/SparkTest/Stitching/fused.zarr -d /ch2/s0 -s ZARR --multiRes --preserveAnisotropy --UINT8 --minIntensity 0 --maxIntensity 255 --channelId 2</code>

You can open the ZARR in [Fiji](https://fiji.sc) (**File > Import > HDF5/N5/ZARR/OME-NGFF** or **Plugins > BigDataViewer > HDF5/N5/ZARR/OME-NGFF**), using `n5-view` in the [n5-utils package](https://github.com/saalfeldlab/n5-utils) (`./n5-view -i ~/SparkTest/Stitching/fused.zarr -d /ch0`) or in [Napari](https://napari.org/stable/) (simply drag&drop e.g. the `ch0` or a `s0` folder).

The [dataset that was aligned using interest points](https://drive.google.com/file/d/13b0UzWuvpT_qL7JFFuGY9WWm-VEiVNj7/view?usp=sharing) can be fused in a similar way, except that here we choose to use the bounding box `embryo` that was specified using BigStitcher and we choose to save as an BDV/BigStitcher project using N5 as underlying export data format:

<code>./affine-fusion -x ~/SparkTest/IP/dataset.xml -o ~/SparkTest/IP/fused.n5 -xo ~/SparkTest/IP/dataset-fused.xml -s N5 -b embryo --bdv 18,0 --multiRes --UINT8 --minIntensity 0 --maxIntensity 255 --timepointId 18</code>

<code>./affine-fusion -x ~/SparkTest/IP/dataset.xml -o ~/SparkTest/IP/fused.n5 -xo ~/SparkTest/IP/dataset-fused.xml -s N5 -b embryo --bdv 30,0 --multiRes --UINT8 --minIntensity 0 --maxIntensity 255 --timepointId 30</code>

In additon to the opening methods mentioned above, you can also directly open the `dataset-fused.xml` in **BigStitcher** or **BigDataViewer**; unfortunately opening of N5's in a vanilla Napari is not supported.

***Note: since both acquisitions have more than one channel or timepoint it is important to fuse these into seperate output volumes, respectively.***

***Note:*** `--dryRun` allows the user to test the functionality without writing any data. It scales to large datasets as it tests for each block that is written which images are overlapping. For cloud execution one can additionally pre-fetch all input data for each compute block in parallel. You need to specify the `XML` of a BigSticher project and decide which channels, timepoints, etc. to fuse. 

## Non-Rigid Fusion<a name="nonrigid-fusion">

`nonrigid-fusion` performs **non-rigid distributed fusion** using `net.preibisch.bigstitcher.spark.SparkNonRigidFusion`. The arguments are identical to the [Affine Fusion](#affine-fusion), and one needs to additionally define the corresponding **interest points**, e.g. `-ip beads` that will be used to compute the non-rigid transformation.
