# BigStitcher-Spark

[![install4j](https://www.ej-technologies.com/images/product_banners/install4j_small.png)](https://www.ej-technologies.com/products/install4j/overview.html)

This package allows you to run compute-intense parts of BigStitcher distributed on your workstation, a cluster or the cloud using Apache Spark. The following modules are currently available in BigStitcher-Spark listed as `JavaClassName`/`cmd-line-tool-name` (you can find documentation below, but a good start is also to just check out the cmd-line arguments, they mostly follow the BigStitcher GUI; each module takes an existing XML):

* `SparkResaveN5`/`resave` (resave an XML dataset you defined in BigStitcher - use virtual loading only - into N5 for processing)
* `SparkInterestPointDetection`/`detect-interestpoints` (detect interest points for alignment)
* `SparkGeometricDescriptorRegistration`/`register-interestpoints` (perform pair-wise interest point registration)
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
* [**Usage**](#usage)

## Install and Run<a name="install">

### To run it on your local computer:<a name="installlocal">

* Prerequisites:  Java and maven must be installed.
* Clone the repo and `cd` into `BigStitcher-Spark`
* Run the included bash script `./install -t <num-cores> -m <mem-in-GB> ` specifying the number of cores and available memory in GB for running locally. This should build the project and create the executable `resave`, `detect-interestpoints`, `register-interestpoints`, `stitching`, `solver`, `affine-fusion`, `nonrigid-fusion`, `downsample`, `clear-interestpoints` and `clear-registrations` in the working directory.

If you run the code directly from your IDE, you will need to add JVM paramters for the local Spark execution (e.g. 8 cores, 50GB RAM):
```
-Dspark.master=local[8] -Xmx50G
```
### To run it on the cluster or the cloud:<a name="installremote">

`mvn clean package -P fatjar` builds `target/BigStitcher-Spark-0.0.1-SNAPSHOT.jar` for distribution.

Ask your sysadmin for help how to run it on your **cluster**. To get you started there is a [tutorial on YouTube](https://youtu.be/D3Y1Rv_69xI?si=mp_57Jby0T2ETP0p&t=5520) by [@trautmane](https://github.com/trautmane) that explains how we run it on the Janelia cluster. ***Importantly, if you use HDF5 as input data in a distributed scenario, you need to set a common path for extracting the HDF5 binaries (see solved issue [here](https://github.com/PreibischLab/BigStitcher-Spark/issues/8)), e.g.***
```
--conf spark.executor.extraJavaOptions=-Dnative.libpath.jhdf5=/groups/spruston/home/moharb/libjhdf5.so
```

For running the fatjar on the **cloud** check out services such as [Amazon EMR](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark.html). An implementations of image readers and writers that support cloud storage can be found [here](https://github.com/bigdataviewer/bigdataviewer-omezarr). Note that running it on the cloud is an ongoing effort with [@kgabor](https://github.com/kgabor), [@tpietzsch](https://github.com/tpietzsch) and the AWS team that currently works as a prototype but is further being optimized. We will provide an updated documentation in due time.

## Usage<a name="usage">

For now we support **fusion with affine transformation models** (including translations of course). It should scale very well to large datasets as it tests for each block that is written which images are overlapping. You simply need to specify the `XML` of a BigSticher project and decide which channels, timepoints, etc. to fuse. *Warning: not tested on 2D yet.*

Here is my example config for this [example dataset](https://drive.google.com/file/d/13cz9HTqTwd9xoN2o7U7UyZrHylr8TNTA/view?usp=sharing) for the main class `net.preibisch.bigstitcher.spark.AffineFusion`:

```
-x '~/test/dataset.xml'
-o '~/test/test-spark.n5'
-d '/ch488/s0'
--UINT8
--minIntensity 1
--maxIntensity 254
--channelId 0
```
*Note: here I save it as UINT8 [0..255] and scale all intensities between `1` and `254` to that range (so it is more obvious what happens). If you omit `UINT8`, it'll save as `FLOAT32` and no `minIntensity` and `maxIntensity` are required. `UINT16` [0..65535] is also supported.*

***Importantly: since we have more than one channel, I specified to use channel 0, otherwise the channels are fused together, which is most likely not desired. Same applies if multiple timepoints are present.***

The blocksize defaults to `128x128x128`, and can be changed with `--blockSize 64,64,64` for example.

You can open the N5 in Fiji (`File > Import > N5`) or by using `n5-view` from the n5-utils package (https://github.com/saalfeldlab/n5-utils).

You can create a multiresolution pyramid of this data using https://github.com/saalfeldlab/n5-spark


***Update: now there is support for non-rigid distributed fusion using `net.preibisch.bigstitcher.spark.NonRigidFusionSpark`***
In order to run it one needs to additionally define the corresponding **interest points**, e.g. `-ip beads` that will be used to compute the non-rigid transformation.
