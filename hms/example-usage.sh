#!/bin/bash

# HMS_VERSION=${1:-4.12}
HMS_VERSION=${1:-4.13}
# HMS_VERSION=${1:-4.13-beta.6}
# HMS_VERSION=${1:-4.14-beta.1}

IMAGE=auto-fpt-hms:$HMS_VERSION

docker build -t $IMAGE --build-arg HMS_VERSION=$HMS_VERSION .
# docker pull ghcr.io/dewberry/auto-fpt-hms:$HMS_VERSION || true

LOCAL_MODEL_DIR=/home/username/samples/castro
HMS_MODEL_NAME=castro.hms
SIMULATION='Current'

CONTAINER_MODEL_DIR=/mnt/model
s
docker run \
    -v $LOCAL_MODEL_DIR:$CONTAINER_MODEL_DIR \
    $IMAGE \
    $CONTAINER_MODEL_DIR/$HMS_MODEL_NAME \
    $SIMULATION
