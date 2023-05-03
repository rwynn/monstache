#!/bin/bash

go mod download

make clean
make all

rm -rf ./build/*.zip ./build/**/*.txt