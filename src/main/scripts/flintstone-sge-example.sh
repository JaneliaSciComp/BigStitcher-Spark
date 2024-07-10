#!/bin/bash

###
# #%L
# Spark-based parallel BigStitcher project.
# %%
# Copyright (C) 2021 - 2024 Developers.
# %%
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as
# published by the Free Software Foundation, either version 2 of the
# License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public
# License along with this program.  If not, see
# <http://www.gnu.org/licenses/gpl-2.0.html>.
# #L%
###

# --------------------------------------------------------------------
# Example wrapper script for launching a BigStitcher-Spark job on SGE using
# https://github.com/JaneliaSciComp/spark-janelia

RUN_TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Define BigStitcher arguments (e.g. input, output, ...) that are not related to Spark.
# These are ones for the demo data set from the repo.
ARGV="\
-x input/dataset.xml \
-o output/dataset_${RUN_TIMESTAMP}.n5 \
-d /ch488/s0 \
--blockSize 128,128,128 \
--UINT8 \
--minIntensity 1 \
--maxIntensity 254 \
--channelId 0"

# Add group write access to output files so that they can easily be removed by others.
umask 0002

# --------------------------------------------------------------------
# Spark Standalone Cluster Setup (11 cores per worker)
# --------------------------------------------------------------------
# export N_EXECUTORS_PER_NODE=2        # number of executor JVMs living on each worker
# export N_CORES_PER_EXECUTOR=5        # number of concurrent tasks each executor can run
# export N_OVERHEAD_CORES_PER_WORKER=1 # number of additional cores to leave on each worker for executor management

# --------------------------------------------------------------------
# Spark Standalone Cluster Setup (5 cores per worker - for DEMO)
# --------------------------------------------------------------------
export N_EXECUTORS_PER_NODE=2        # number of executor JVMs living on each worker
export N_CORES_PER_EXECUTOR=2        # number of concurrent tasks each executor can run
export N_OVERHEAD_CORES_PER_WORKER=1 # number of additional cores to leave on each worker for executor management

# Note: N_CORES_PER_WORKER=$(( (N_EXECUTORS_PER_NODE * N_CORES_PER_EXECUTOR) + N_OVERHEAD_CORES_PER_WORKER ))

# The BigStitcher-Spark fusion drivers do very little work themselves,
# so we only need to allocate one core for the driver.
export N_CORES_DRIVER=1

# The 'LSF_PROJECT' variable will be used as the account_string (-A option) for SGE qsub calls.
# We are required to explicitly identify HPC accounts at Janelia so that labs can be billed for cluster time.
# If your institute does something similar, uncomment and set this variable to an appropriate value.
#export LSF_PROJECT="<lab-or-group-to-bill>"

# Tell spark-janelia that you need to generate SGE scripts instead of LSF scripts.
export SPARK_JANELIA_ARGS="--hpc_type sge"

# It can be useful to write Spark logs to a centralized (mounted) filesystem so that you don't need to connect
# to many different worker machines to look at logs.
# If your institute's HPC has network mounted filesystems and you would like this convenience,
# uncomment the following line and set the --run_parent_dir accordingly.
#export SPARK_JANELIA_ARGS="${SPARK_JANELIA_ARGS} --consolidate_logs --run_parent_dir <network-storage-root-dir>/logs/spark"

# Only generate the spark scripts so they can be manually launched later (the script prints out a line telling you how).
# Comment this line out if you want to generate the scripts and then immediately launch the spark jobs
# (presumably after you are confident the SGE scripts work).
export SPARK_JANELIA_TASK="generate-run"

# If your data is HDF5, uncomment and set the H5_LIBPATH value below, then uncomment the export SUBMIT_ARGS line below.

# Avoid "Could not initialize class ch.systemsx.cisd.hdf5.CharacterEncoding" exceptions
# (see https://github.com/PreibischLab/BigStitcher-Spark/issues/8 ).
#H5_LIBPATH="-Dnative.libpath.jhdf5=<path-to-jhdf5-lib>/lib/jhdf5/native/jhdf5/amd64-Linux/libjhdf5.so"
#export SUBMIT_ARGS="--conf spark.executor.extraJavaOptions=${H5_LIBPATH} --conf spark.driver.extraJavaOptions=${H5_LIBPATH}"

# Number of worker nodes - update this to whatever you like (or make it a parameter to the script).
N_NODES=2

# Update this to the fully-qualified path of your built BigStitcher-Spark fat jar.
# Note that the jar file needs to live on a filesystem that can be read by the launch job.
JAR=".../target/BigStitcher-Spark-0.0.2-SNAPSHOT.jar"

# Update this to the path of the flintstone script from spark-janelia:
FLINTSTONE="spark-janelia/flintstone.sh"

# Update this to the BigStitcher component you want to run:
CLASS="net.preibisch.bigstitcher.spark.AffineFusion"

# Create a log of the launch process so you can go back and find everything else later.
# Change the log file path if you don't like this one.
LOG_FILE="logs/run.${RUN_TIMESTAMP}.out"
mkdir -p "$(dirname "${LOG_FILE}")"

# use shell group to tee all output to log file
{
  echo """
Running with arguments:
${ARGV}
"""
  # shellcheck disable=SC2086
  ${FLINTSTONE} ${N_NODES} ${JAR} ${CLASS} ${ARGV}

} 2>&1 | tee -a "${LOG_FILE}"
