#!/bin/bash

# docker build . -f Dockerfile-build -t monstache-build
docker build . -f Dockerfile-build-from-scratch -t monstache-build

docker run --rm -d monstache-build tail -f /go/src/app/monstache.go

CONTAINER_ID=$(docker ps -l -q)

if [ -d docker-build ] ; then
 rm -r docker-build
fi

docker cp "$CONTAINER_ID":/go/src/app/build ./docker-build
# docker cp "$CONTAINER_ID":/tmp/build ./docker-build

docker stop "$CONTAINER_ID"