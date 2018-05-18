#!/bin/bash

set -eo pipefail

waitForMongo() {
  echo 'Mongo: Pinging ..';

  while true; do
    if $(./mongo-engine-is-up.sh "$1"); then
      echo 'Mongo: OK';
      return 0;
    fi

    echo -n '.';
    sleep 1;
  done;
}

waitForMongo "$1";

exit $?;
