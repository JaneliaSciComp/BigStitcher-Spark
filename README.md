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

For running the fatjar on the **cloud** check out services such as [Amazon EMR](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark.html). An implementations of image readers and writers that support cloud storage can be found [here](https://github.com/bigdataviewer/bigdataviewer-omezarr). Note that running it on the cloud is an ongoing effort with [@kgabor](https://github.com/kgabor), [@tpietzsch](https://github.com/tpietzsch) and the AWS team that currently works as a prototype but is further being optimized. We will provide an updated documentation in due time. Note that some modules support prefetching `--prefetch`, which is important for cloud execution due to its delays as it pre-loads all image blocks in parallel before processing.

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

For this tutorial I extracted the Stitching dataset into `~/SparkTest/Stitching` and the dataset for experimenting with interest points into `~/SparkTest/IP`.

## Usage<a name="usage">

### Resave Dataset<a name="resave">

When working with BigStitcher the first step is to [define a dataset](https://imagej.net/plugins/bigstitcher/define-new-dataset), where the goal is to provide sufficient meta-data in order to allow BigStitcher (and BigDataViewer) to load your images. This step is typically done in the BigStitcher GUI, but some people have written scripts to automatically generate XML's for their datasets (e.g. [here](https://github.com/nvladimus/npy2bdv/tree/v1.0.0)). If you want to start testing an entire BigStitcher(-Spark) pipeline from scratch, please use [this dataset for stitching](https://drive.google.com/file/d/15xSQCBHcpEvJWd6YD5iKJzuc0IRpWB8a/view?usp=sharing) or [this one using interest points](https://drive.google.com/file/d/1VFT2APVPItBCyzrQ7dPWBNILyNh6yDKB/view?usp=sharing).

After the dataset is defined one usually re-saved the input data (TIFF, CZI, ...) into a multi-resolution format that makes it possible to interactively display and work with the data the image as various resolution levels, and is essential for distributed processing. Right now, we use the N5 format for temporary storage of the input data. This resaving process can take substantial amounts of time if your input is large and can be distributed using Spark. Importantly, you need to define your dataset using the **Automatic Loader (Bioformats based)** and select to **Load data directly** and **load data virtually**.

For testing the re-saving (and multi-resolution pyramid creation) with Spark use your defined dataset(s), or download [this dataset for stitching](https://drive.google.com/file/d/1-nqzBbtff8u93LGbCTPRwikWJH6o6B46/view?usp=sharing) or [this dataset for interest points](https://drive.google.com/file/d/1Qs3juqQgYlDc2KglbcFTFKzdAQxgS9zc/view?usp=sharing)

The command for resaving the stitching dataset could look like this and will overwrite the input XML, a backup XML will be automatically created:

<code>./resave -x ~/SparkTest/Stitching/dataset.xml -xo ~/SparkTest/Stitching/dataset.xml</code>

It is analogous for the interest point dataset:

<code>./resave -x ~/SparkTest/IP/dataset.xml -xo ~/SparkTest/IP/dataset.xml</code>

Please run `resave` without parameters to get help for all command line arguments. Using `--blockSize` you can change the blocksize of the N5, and `--blockScale` defines how many blocks at once will be processed by a Spark job. With `-ds` you can define your own downsampling steps if the automatic ones are not well suited. 

***Note:*** `--dryRun` allows the user to test the functionality without writing any data. The Spark parallelization is written so it parallelizes over user-defined blocks across all input images at once, so also few, very big images will be processed efficiently.

### Pairwise Stitching<a name="stitching">

To perform classical stitching (translation only), first pair-wise stitching between all overlapping tiles needs to be computed. So far we only support standard grouping where all channels and illuminations of a specific Tile will be grouped together as one image and stitching is performed individually per Timepoint and Angle. To run the stitching with default paramters you can run the following command on this [example dataset](https://drive.google.com/file/d/1Q2SCJW_tCVKFzrdMrgVrFDyiF6nUN5-B/view?usp=sharing):

<code>./stitching -x ~/SparkTest/Stitching/dataset.xml</code>

The results will be written in to the XML, in order to compute transformation models and apply them to the images you need to run the [solver](#solver) that computes a global optimization next.

Please run `stitching` without parameters to get help for all command line arguments. `-ds` sets the resolution at which cross correlation is performed; `2,2,1` is the default and usually superior to `1,1,1` due to suppressed noise, even higher resolution levels typically work well too since by default the peaks are located with subpixel accuracy. `--disableSubpixelResolution` disables subpixel accurate shifts. `-p` sets the number of phase correlation peaks that are checked with cross-correlation (incrementing this number can help with stitching issues). `--minR` and `--maxR` are filters that specify the accepted range for cross correlation for any pair of overlapping tiles, reducing `--minR` may be useful to accept more pair and excluding a `--maxR` of `1` may be useful too if you get wrong links with `r=1.0`. `--maxShiftX/Y/Z` and `--maxShiftTotal` set the maximal allowed shift between any pair of images relative to their current location; limiting it if the current position is close to the correct one might be useful. If your dataset contains multiple channels or illumination directions per Tile, you can select how they will be combined for the pair-wise stitching process using `--channelCombine` and `--illumCombine`, which can be either `AVERAGE` or `PICK_BRIGHTEST`. 

You can choose which Tiles `--tileId`, Channels `--channelId`, Iluminations `--illuminationId`, Angles `--angleId` and Timepoints `--timepointId` will be processed, a typical choice could be `--timepointId 18 --tileId 0,1,2,3,6,7,8` to only process the timepoint 18 and select Tiles. If you would like to choose Views more fine-grained, you can specify their ViewIds directly, e.g. `-vi '0,0' -vi '0,1' -vi '1,1'` to process ViewId 0 & 1 of Timepoint 0 and ViewId 1 of Timepoint 1. By default, everything will be processed.

***Note:*** `--dryRun` allows the user to test the functionality without writing any data. The Spark parallelization is written so it parallelizes over pairs of images.

### Detect Interest Points<a name="ip-detect">

Interest-point based registration is generally more reliable and faster than stitching while supporting various transformation models including **(regularized) Translation**, **(regularized) Rigid**, **(regularized) Affine**, and **Non-Rigid**. At the same time parameter selection is more involved. The first step is to detect interest points in the images. A typical command line call that works well on [this example dataset](https://drive.google.com/file/d/16V8RBYP3TNrDVToT9BoRxqclGE15TwKM/view?usp=sharing) looks as follows:

<code>./detect-interestpoints -x ~/SparkTest/IP/dataset.xml -l beads -s 1.8 -t 0.008 -dsxy 2 --minIntensity 0 --maxIntensity 255</code>

The results will be written in the `XML` and the `interestpoints.n5` directory, in order to compute transformation models and apply them to the images you need to run [matching](#ip-match) followed by the [solver](#solver) that computes a global optimization. If you want to inspect the interest points you can open the XML in BigStitcher, go to multiview mode, right click, start the **Interest Point Explorer** and click on the number of detections that will then be overlaid onto the images (see screenshot below).

Please run `detect-interestpoints` without parameters to get help for all command line arguments. `-s` and `-t` define the sigma and threshold of the Difference-of-Gaussian, respectively and `-l` specifies the label for the interest points. `--minIntensity` and `--maxIntensity` set the intensity range in which all processed blocks are normalized to `[0...1]`; these values are mandatory since each individual Spark job is unable to figure out correct min/max values of the images. You could find good guesses for all these values by starting the interactive interest point detection in BigStitcher. `-dsxy` and `-dsz` define the downsampling at which interest point detection is performed. Using `--localization` you can specify the type of subpixel localization, either `NONE` or `QUADRATIC`. `--type` allows to set which type of intensity peaks should be identified; `MIN`, `MAX` or `BOTH`. Finally, `--blockSize` sets the blocksize that will be processed in each Spark job.

`--overlappingOnly` is a feature that will only identify interest points in areas of each image that is currently overlapping with another image. `--storeIntensities` will extract intensities of each interest point and store it in the `interestpoints.n5` directory as extra datasets. `--prefetch` will use parallel threads to pre-load all image data blocks ahead of the computation, which is desirable for cloud execution.

You can choose which Tiles `--tileId`, Channels `--channelId`, Iluminations `--illuminationId`, Angles `--angleId` and Timepoints `--timepointId` will be processed, a typical choice could be `--timepointId 18 --tileId 0,1,2,3,6,7,8` to only process the timepoint 18 and select Tiles. If you would like to choose Views more fine-grained, you can specify their ViewIds directly, e.g. `-vi '0,0' -vi '0,1' -vi '1,1'` to process ViewId 0 & 1 of Timepoint 0 and ViewId 1 of Timepoint 1. By default, everything will be processed.

***Note:*** `--dryRun` allows the user to test the functionality without writing any data. The Spark parallelization is written so it parallelizes over user-defined block across all processed images at once.

<img align="left" src="https://github.com/JaneliaSciComp/BigStitcher-Spark/blob/main/src/main/resources/BigStitcher-interestpoints.jpg" alt="Visualizing interest points in BigStitcher">

### Match Interest Points<a name="ip-match">

Once 

Per timepoint alignment:

<code>./match-interestpoints -x ~/SparkTest/IP/dataset.xml -l beads -m FAST_ROTATION --clearCorrespondences</code>

For timeseries alignment, grouping all views of a timepoint together:

<code>./match-interestpoints -x ~/SparkTest/IP/dataset.xml -l beads -m FAST_ROTATION --clearCorrespondences -rtp ALL_TO_ALL --splitTimepoints</code>

### Solver<a name="#solver">
When using pairwise stitching:

<code>./solver -x ~/SparkTest/Stitching/dataset.xml -s STITCHING</code>

When using interestpoints (per timepoint):

<code>./solver -x ~/SparkTest/IP/dataset.xml -s IP -l beads</code>

When using interestpoints (for timeseries alignment with grouping all views of a timepoint together):

<code>./solver -x ~/SparkTest/IP/dataset.xml -s IP -l beads -rtp ALL_TO_ALL --splitTimepoints</code>

### Affine Fusion<a name="affine-fusion">

`affine-fusion` performs **fusion with affine transformation models** (including translations of course). It scales to large datasets as it tests for each block that is written which images are overlapping. For cloud execution one can additionally pre-fetch all input data for each compute block in parallel. You need to specify the `XML` of a BigSticher project and decide which channels, timepoints, etc. to fuse. *Warning: not tested on 2D yet.*

Here is an example config for this [example dataset](https://drive.google.com/file/d/1ajjk4piENbRrhPWlR6HqoUfD7U7d9zlZ/view?usp=sharing) for the main class `net.preibisch.bigstitcher.spark.SparkAffineFusion`:

<code>./affine-fusion -x ~/SparkTest/Stitching/dataset.xml -o ~/SparkTest/Stitching/fused.n5 -d /ch0/s0 -s ZARR --multiRes --preserveAnisotropy --UINT8 --minIntensity 1 --maxIntensity 254 --channelId 0</code>

*Note: here I save it as UINT8 [0..255] and scale all intensities between `1` and `254` to that range (so it is more obvious what happens). If you omit `UINT8`, it'll save as `FLOAT32` and no `minIntensity` and `maxIntensity` are required. `UINT16` [0..65535] is also supported.*

***Importantly: since we have more than one channel, I specified to use channel 0, otherwise the channels are fused together, which is most likely not desired. Same applies if multiple timepoints are present.***

Calling it with `--multiRes` will create a multiresolution pyramid of the fused image.
The blocksize defaults to `128x128x128`, and can be changed with `--blockSize 64,64,64` for example.

You can open the N5 in Fiji (`File > Import > HDF5/N5/ZARR/OME-NGFF`) or by using `n5-view` from the [n5-utils package](https://github.com/saalfeldlab/n5-utils).

### Non-Rigid Fusion<a name="nonrigid-fusion">

`nonrigid-fusion` performs **non-rigid distributed fusion** using `net.preibisch.bigstitcher.spark.SparkNonRigidFusion`. The arguments are identical to the [Affine Fusion](#affine-fusion), and one needs to additionally define the corresponding **interest points**, e.g. `-ip beads` that will be used to compute the non-rigid transformation.
