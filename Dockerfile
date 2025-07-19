ARG SPARK_VERSION=3.3.2-scala2.12-java17-ubuntu24.04
ARG BIGSTITCHER_SPARK_GIT_HASH=46451ed

FROM ghcr.io/janeliascicomp/spark:${SPARK_VERSION}
ARG BIGSTITCHER_SPARK_GIT_HASH

LABEL \
    org.opencontainers.image.title="BigStitcher Spark" \
    org.opencontainers.image.description="Spark version of BigStitcher" \
    org.opencontainers.image.authors="rokickik@janelia.hhmi.org,preibischs@janelia.hhmi.org,goinac@janelia.hhmi.org" \
    org.opencontainers.image.licenses="BSD-3-Clause" \
    org.opencontainers.image.version=${BIGSTITCHER_SPARK_GIT_HASH}

USER root

RUN apt update -y; \
    apt-get install -y \
        libblosc1 libblosc-dev \
        libzstd1 libzstd-dev libhdf5-dev;

WORKDIR /app
COPY LICENSE /app/LICENSE
COPY target/BigStitcher-Spark-0.1.0-SNAPSHOT.jar /app/app.jar
