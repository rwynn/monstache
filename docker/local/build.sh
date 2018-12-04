#!/bin/bash

MONSTACHE_SOURCE_CODE_PATH=${MONSTACHE_SOURCE_CODE_PATH:-../..}

# # Build a docker image
docker build \
"$MONSTACHE_SOURCE_CODE_PATH" \
-f ./Dockerfile \
-t monstache-build

# Start a container from the newly built docker image
docker run \
--rm \
-d \
monstache-build \
tail -f /app/monstache.go

# Get the container id of the last created container
CONTAINER_ID=$(docker ps -l -q)

# If the folder docker-build exists locally
# TODO add a step to confirm from the user before removing the existing folder
if [ -d docker-build ] ; then
 # Then remove it
 rm -r docker-build
fi

# Copy the build folder from the last created container to the folder docker-build locally
docker cp "$CONTAINER_ID":/app/build ./docker-build
# docker cp "$CONTAINER_ID":/tmp/build ./docker-build

# Stop the container (it'll be removed automatically once stopped, as we used `--rm`)
docker stop "$CONTAINER_ID"
