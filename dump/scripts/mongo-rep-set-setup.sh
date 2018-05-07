#!/bin/bash

echo "************************************************************"
echo " Begin setting up replica set"
echo "************************************************************"

# MONGO_REPLICA_SET_MEMBERS="[{'_id':0,'host':'localhost:27017','priority':1}]"

# mongo admin --eval "help" > /dev/null 2>&1
# RET=$?

# while [[ RET -ne 0 ]]; do
#   echo "Waiting for MongoDB to start..."
#   mongo admin --eval "help" > /dev/null 2>&1
#   RET=$?
#   sleep 1

#   if [[ -f /data/db/mongod.lock ]]; then
#     echo "Removing Mongo lock file"
#     rm /data/db/mongod.lock
#   fi
# done

./mongo-engine-wait.sh

# Login as root and configure replica set
# # https://docs.mongodb.com/manual/reference/method/rs.initiate/#rs.initiate
mongo  admin -u "$MONGO_USER_ROOT_NAME" -p "$MONGO_USER_ROOT_PASSWORD" --eval "rs.initiate({ '_id': '$MONGO_REPLICA_SET_NAME', 'version': 1, 'members': $MONGO_REPLICA_SET_MEMBERS });"
# mongo  --host localhost admin -u "$MONGO_USER_ROOT_NAME" -p "$MONGO_USER_ROOT_PASSWORD" --eval "rs.initiate({ '_id': '$MONGO_REPLICA_SET_NAME', 'version': 1, 'members': $MONGO_REPLICA_SET_MEMBERS });"
# mongo  --host mongo-0 admin -u "$MONGO_USER_ROOT_NAME" -p "$MONGO_USER_ROOT_PASSWORD" --eval "rs.initiate({ '_id': '$MONGO_REPLICA_SET_NAME', 'members': $MONGO_REPLICA_SET_MEMBERS });"

echo "************************************************************"
echo " End setting up replica set"
echo "************************************************************"
