# BigStitcher-Spark
Running compute-intense parts of BigStitcher distributed

Here is my example config:

```
-x '/Users/spreibi/Documents/Microscopy/Stitching/Truman/standard/dataset.xml'
-o '/Users/spreibi/Documents/Microscopy/Stitching/Truman/standard/test-spark.n5'
-d '/ch488/s0'
--UINT8
--minIntensity 0
--maxIntensity 255
--channelId 0
```

And for local spark you need JVM paramters (8 cores, 50GB RAM):

```
-Dspark.master=local[8] -Xmx50G
```
