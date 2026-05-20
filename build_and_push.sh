#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <path_to_dockerfile> <full_tag>"
    echo "Example: $0 Dockerfile.usgs 14128755615476.dkr.ecr.us-east-1.amazonaws.com/usgs-reaper:v0.1"
    exit 1
fi

DOCKERFILE="$1"
FULL_TAG="$2"

# Assuming the build context is the current directory
BUILD_CONTEXT="."

echo "=================================================="
echo " Building and Pushing Docker Image"
echo "=================================================="
echo " Dockerfile : ${DOCKERFILE}"
echo " Image Tag  : ${FULL_TAG}"
echo " Context    : ${BUILD_CONTEXT}"
echo "=================================================="

docker buildx build \
    --provenance=false \
    --platform linux/amd64 \
    -f "${DOCKERFILE}" \
    -t "${FULL_TAG}" \
    --push \
    "${BUILD_CONTEXT}"

echo "=================================================="
echo " Successfully built and pushed: ${FULL_TAG}"
echo "=================================================="