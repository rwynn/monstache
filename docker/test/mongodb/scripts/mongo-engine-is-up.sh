#!/bin/bash

set -eo pipefail;

mongoIsUp() {
  if [ -z "$1" ] || [ "$1" != "no-ssl" ] ; then
    if mongo  --quiet "localhost:27017/test" --eval 'quit(db.runCommand({ ping: 1 }).ok ? 0 : 1)' > /dev/null 2>&1; then
      # echo 'Mongo-Ping: OK';
      return 0;
    else
      # echo 'Mongo-Ping: Not OK';
      return 1;
    fi
  else
    if mongo --quiet "localhost:27017/test" --eval 'quit(db.runCommand({ ping: 1 }).ok ? 0 : 1)' > /dev/null 2>&1; then
      # echo 'Mongo-Ping: OK';
      return 0;
    else
      # echo 'Mongo-Ping: Not OK';
      return 1;
    fi
  fi
}

mongoIsUp "$1";

exit $?;
