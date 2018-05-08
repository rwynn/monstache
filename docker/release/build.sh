#!/bin/bash

tag=$(<.tag)
version=$(<.version)
url=$(<.url)

docker build --build-arg VCS_REF="$(git rev-parse --short HEAD)" \
             --build-arg BUILD_DATE="$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
             --build-arg VSC_URL="$url" \
             --build-arg BUILD_VERSION="$version" \
	     -f ./Dockerfile -t "$tag" ../..
