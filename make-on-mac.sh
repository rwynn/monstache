#!/bin/bash

# This script aliases 2 missing functions on Mac
# To enable the Makefile to run on Mac

# function sha256sum() { openssl sha256 "$@" | awk '{print $2}'; }
function sha256sum() { shasum -a 256 "$@"; };
export -f sha256sum;

function md5sum() { md5 "$@"; };
export -f md5sum;

make "$@";
