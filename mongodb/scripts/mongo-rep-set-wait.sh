#!/bin/bash

set -eo pipefail;

waitUntilReplicaSetIsUp() {
  echo 'ReplicaSet: Checking if status is ok and primary is up ..';

  while true ; do
    # if $(./mongo-rep-set-is-up.sh) ; then
    if $(./mongo-rep-set-is-ok.sh) ; then
      echo 'ReplicaSet: OK';
      return 0;
    fi

    echo -n '.';
    sleep 1;
  done;
}

waitUntilReplicaSetIsUp;

exit $?;
