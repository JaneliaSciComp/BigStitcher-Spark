# BigStitcher-Spark

[![install4j](https://www.ej-technologies.com/images/product_banners/install4j_small.png)](https://www.ej-technologies.com/products/install4j/overview.html)

This package allows you to run compute-intense parts of BigStitcher distributed on your workstation, a cluster or the cloud using Apache Spark. The following modules are currently available in BigStitcher-Spark listed as `JavaClassName`/`cmd-line-tool-name` (you can find documentation below, but a good start is also to just check out the cmd-line arguments, they mostly follow the BigStitcher GUI; each module takes an existing XML):

* `SparkResaveN5`/`resave` (resave an XML dataset you defined in BigStitcher - use virtual loading only - into N5 for processing)
* `SparkInterestPointDetection`/`detect-interestpoints` (detect interest points for alignment)
* `SparkGeometricDescriptorMatching`/`match-interestpoints` (perform pair-wise interest point matching)
* `SparkPairwiseStitching`/`stitching` (run pairwise stitching between overlapping tiles)
* `Solver`/`solver` (perform the global solve, works with interest points and stitching)
* `SparkAffineFusion`/`affine-fusion` (fuse the aligned dataset using affine models, including translation)
* `SparkNonRigidFusion`/`nonrigid-fusion` (fuse the aligned dataset using non-rigid models)

Additonally there are some utility methods:
* `SparkDownsample`/`downsample` (perform downsampling of existing volumes)
* `ClearInterestPoints`/`clear-interestpoints` (clears interest points)
* `ClearRegistrations`/`clear-registrations` (clears registrations)

***Note: BigStitcher-Spark is designed to work hand-in-hand with BigStitcher.** You can always verify the results of each step BigStitcher-Spark step interactively using BigStitcher by simply opening the XML. You can of course also run certain steps in BigStitcher, and others in BigStitcher-Spark. Not all functionality is 100% identical between BigStitcher and BigStitcher-Spark; important differences in terms of capabilities is described in the respective module documentation below (typically BigStitcher-Spark supports a specific feature that was hard to implement in BigStitcher and vice-versa).*

### Content

* [**Install and Run**](#install)
  * [Local](#installlocal)
  * [Cluster & Cloud](#installremote)
* [**Example Datasets**](#examples)
* [**Usage**](#usage)
  * [Resave Dataset](#resave)
  * [Pairwise Stitching](#stitching)
  * [Detect Interest Points](#ip-detect)
  * [Match Interest Points](#ip-match)
  * [Solver](#solver)
  * [Affine Fusion](#affine-fusion)
  * [Non-Rigid Fusion](#nonrigid-fusion)

<img align="left" src="https://github.com/JaneliaSciComp/BigStitcher-Spark/blob/main/src/main/resources/bs-spark.png" alt="Overview of the BigStitcher-Spark pipeline">

## Install and Run<a name="install">

### To run it on your local computer<a name="installlocal">

* Prerequisites:  Java and maven must be installed.
* Clone the repo and `cd` into `BigStitcher-Spark`
* Run the included bash script `./install -t <num-cores> -m <mem-in-GB> ` specifying the number of cores and available memory in GB for running locally. This should build the project and create the executable `resave`, `detect-interestpoints`, `register-interestpoints`, `stitching`, `solver`, `affine-fusion`, `nonrigid-fusion`, `downsample`, `clear-interestpoints` and `clear-registrations` in the working directory.

If you run the code directly from your IDE, you will need to add JVM paramters for the local Spark execution (e.g. 8 cores, 50GB RAM):
```
-Dspark.master=local[8] -Xmx50G
```
### To run it on the cluster or the cloud<a name="installremote">

`mvn clean package -P fatjar` builds `target/BigStitcher-Spark-0.0.1-SNAPSHOT.jar` for distribution.

Ask your sysadmin for help how to run it on your **cluster**. To get you started there is a [tutorial on YouTube](https://youtu.be/D3Y1Rv_69xI?si=mp_57Jby0T2ETP0p&t=5520) by [@trautmane](https://github.com/trautmane) that explains how we run it on the Janelia cluster. ***Importantly, if you use HDF5 as input data in a distributed scenario, you need to set a common path for extracting the HDF5 binaries (see solved issue [here](https://github.com/PreibischLab/BigStitcher-Spark/issues/8)), e.g.***
```
--conf spark.executor.extraJavaOptions=-Dnative.libpath.jhdf5=/groups/spruston/home/moharb/libjhdf5.so
```

For running the fatjar on the **cloud** check out services such as [Amazon EMR](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark.html). An implementations of image readers and writers that support cloud storage can be found [here](https://github.com/bigdataviewer/bigdataviewer-omezarr). Note that running it on the cloud is an ongoing effort with [@kgabor](https://github.com/kgabor), [@tpietzsch](https://github.com/tpietzsch) and the AWS team that currently works as a prototype but is further being optimized. We will provide an updated documentation in due time.

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
  *  [As N5/XML](https://drive.google.com/file/d/1X6JW7WeHA7LR71kXgJV0tHlbY8EMrfRF/view?usp=sharing) (aligned)

## Usage<a name="usage">

### Resave Dataset<a name="resave">
Resaving the stitching dataset:
```
./resave -x ~/SparkTest/Stitching/dataset.xml -xo ~/SparkTest/Stitching/dataset.xml
```
Resaving the interest point dataset:
```
./resave -x ~/SparkTest/IP/dataset.xml -xo ~/SparkTest/IP/dataset.xml
```
### Pairwise Stitching<a name="stitching">
```
./stitching -x ~/SparkTest/Stitching/dataset.xml --dryRun
```

### Detect Interest Points<a name="ip-detect">
```
./detect-interestpoints -x ~/SparkTest/IP/dataset.xml -l beads -s 1.8 -t 0.008
```

### Match Interest Points<a name="ip-match">
Per timepoint alignemnt:
```
./match-interestpoints -x ~/SparkTest/IP/dataset.xml -l beads -m FAST_ROTATION --clearCorrespondences
```
For timeseries alignment, grouping all views of a timepoint together:
```
./match-interestpoints -x ~/SparkTest/IP/dataset.xml -l beads -m FAST_ROTATION --clearCorrespondences -rtp ALL_TO_ALL --splitTimepoints
```

### Solver<a name="#solver">
When using pairwise stitching:
```
./solver -x ~/SparkTest/Stitching/dataset.xml -s STITCHING --dryRun
```

When using interestpoints (per timepoint):
```
./solver -x ~/SparkTest/IP/dataset.xml -s IP -l beads
```
When using interestpoints (for timeseries alignment with grouping all views of a timepoint together)
```
./solver -x ~/SparkTest/IP/dataset.xml -s IP -l beads -rtp ALL_TO_ALL --splitTimepoints
```

### Affine Fusion<a name="affine-fusion">

`affine-fusion` performs **fusion with affine transformation models** (including translations of course). It scales to large datasets as it tests for each block that is written which images are overlapping. For cloud execution one can additionally pre-fetch all input data for each compute block in parallel. You need to specify the `XML` of a BigSticher project and decide which channels, timepoints, etc. to fuse. *Warning: not tested on 2D yet.*

Here is am example config for this [example dataset](https://drive.google.com/file/d/1ajjk4piENbRrhPWlR6HqoUfD7U7d9zlZ/view?usp=sharing) for the main class `net.preibisch.bigstitcher.spark.SparkAffineFusion`:

```
./affine-fusion 
-x ~/SparkTest/Stitching/dataset.xml 
-o ~/SparkTest/Stitching/fused.n5 
-d /ch0/s0
--multiRes
--preserveAnisotropy
--UINT8
--minIntensity 1
--maxIntensity 254
--channelId 0
```
*Note: here I save it as UINT8 [0..255] and scale all intensities between `1` and `254` to that range (so it is more obvious what happens). If you omit `UINT8`, it'll save as `FLOAT32` and no `minIntensity` and `maxIntensity` are required. `UINT16` [0..65535] is also supported.*

***Importantly: since we have more than one channel, I specified to use channel 0, otherwise the channels are fused together, which is most likely not desired. Same applies if multiple timepoints are present.***

Calling it with `--multiRes` will create a multiresolution pyramid of the fused image.
The blocksize defaults to `128x128x128`, and can be changed with `--blockSize 64,64,64` for example.

You can open the N5 in Fiji (`File > Import > N5`) or by using `n5-view` from the [n5-utils package](https://github.com/saalfeldlab/n5-utils).

### Non-Rigid Fusion<a name="nonrigid-fusion">

`nonrigid-fusion` performs **non-rigid distributed fusion** using `net.preibisch.bigstitcher.spark.SparkNonRigidFusion`. The arguments are identical to the [Affine Fusion](#affine-fusion), and one needs to additionally define the corresponding **interest points**, e.g. `-ip beads` that will be used to compute the non-rigid transformation.
