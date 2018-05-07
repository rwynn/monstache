#!/bin/bash

echo "************************************************************"
echo "Setting up database"
echo "************************************************************"

set -eo pipefail;

./mongo-engine-wait.sh

./mongo-rep-set-wait.sh

