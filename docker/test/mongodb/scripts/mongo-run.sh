#!/bin/bash

set -m;

# if [ "$MONGO_ROLE" == "primary" ] ; then
./mongo-users-setup.sh;
# fi

mongodb_cmd="mongod"

cmd="$mongodb_cmd"

# # https://docs.mongodb.com/v3.4/reference/program/mongod/#cmdoption-noprealloc
# if [ "$MONGO_NOPREALLOC" == true ] ; then
# cmd="$cmd --noprealloc"
# fi

# # https://docs.mongodb.com/v3.4/reference/program/mongod/#cmdoption-smallfiles
# if [ "$MONGO_SMALLFILES" == true ] ; then
# cmd="$cmd --smallfiles"
# fi

## Rest & HttpInterface are removed in 3.6 => https://docs.mongodb.com/manual/release-notes/3.6-compatibility/#http-interface-and-rest-api
# # if [ "$MONGO_HTTP_INTERFACE" == true ] ; then
# cmd="$cmd --rest --httpinterface"
# # fi

# if [ "$MONGO_AUTH" == true ] ; then
cmd="$cmd --auth"
# else
#   cmd="$cmd --noauth"
# fi

# # --nojournal is deprecated in 3.6 => https://docs.mongodb.com/manual/tutorial/manage-journaling/#disable-journaling
# # if [ "$MONGO_JOURNALING" == false ] ; then
# cmd="$cmd --nojournal"
# # fi

# TODO Add an option to change this value in runtime
cmd="$cmd --storageEngine wiredTiger"

# if [[ "$MONGO_OPLOG_SIZE" ]] ; then
  # cmd="$cmd --oplogSize $OPLOG_SIZE"
# fi
cmd="$cmd --oplogSize 128"

# if [[ "$MONGO_BIND_IP" ]] ; then
  # echo "MONGO_BIND_IP: Adding --bind_ip $MONGO_BIND_IP";
  # cmd="$cmd --bind_ip $MONGO_BIND_IP"
cmd="$cmd --bind_ip 0.0.0.0"
# cmd="$cmd --bind_ip_all"
# else
#   echo "MONGO_BIND_IP: Ignoring --bind_ip option";
# fi

if [[ "$MONGO_REPLICA_SET_NAME" ]] ; then
  cmd="$cmd --replSet $MONGO_REPLICA_SET_NAME"
fi

cmd="$cmd --dbpath /data/db"

# openssl req -new -newkey rsa:2048 -days 3650 -nodes -x509 -subj '/CN=localhost' -keyout /etc/ssl/mongodb-cert.key -out /etc/ssl/mongodb-cert.crt
# cat /etc/ssl/mongodb-cert.key /etc/ssl/mongodb-cert.crt > /etc/ssl/mongodb.pem

# cmd="$cmd --sslMode requireSSL --sslPEMKeyFile /etc/ssl/mongodb.pem"

$cmd &

# if [ "$MONGO_ROLE" == "primary" ]; then
./mongo-rep-set-setup.sh
# fi

./mongo-db-setup.sh

# Create the health.check file indicating healthy
MONGO_CONTAINER_HEALTHCHECK_FILE_PATH=${MONGO_CONTAINER_HEALTHCHECK_FILE_PATH:-/data/health.check}
echo '1' >> "$MONGO_CONTAINER_HEALTHCHECK_FILE_PATH"

fg
