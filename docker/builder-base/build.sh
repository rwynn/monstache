#!/bin/bash

tag=$(<.tag)

docker build -t "$tag" .
