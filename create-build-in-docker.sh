#!/bin/bash

# # Build a docker image
# docker build . -f Dockerfile-build -t monstache-build
docker build . -f Dockerfile-build-from-scratch -t monstache-build

# Start a container from the newly built docker image
docker run --rm -d monstache-build tail -f /go/src/app/monstache.go

# Get the container id of the last created container
CONTAINER_ID=$(docker ps -l -q)

# If the folder docker-build exists locally
if [ -d docker-build ] ; then
 # Then remove it
 rm -r docker-build
fi

# Copy the build folder from the last created container to the folder docker-build locally
docker cp "$CONTAINER_ID":/go/src/app/build ./docker-build
# docker cp "$CONTAINER_ID":/tmp/build ./docker-build

# Stop the container (it'll be removed automatically once stopped, as we used `--rm`)
docker stop "$CONTAINER_ID"