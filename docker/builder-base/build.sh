#!/bin/bash

# docker build . -f Dockerfile-builder-base -t rwynn/monstache-builder-base:0.0.1

tag=$(<.tag)

docker build . -f Dockerfile-builder-base -t "$tag"