#!/bin/bash

echo "************************************************************"
echo "Setting up database"
echo "************************************************************"

set -eo pipefail;

./mongo-engine-wait.sh

./mongo-rep-set-wait.sh

# TODO Create an empty database with the name $MONGO_DB_NAME
