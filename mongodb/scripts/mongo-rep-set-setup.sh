#!/bin/bash

echo "************************************************************"
echo " Begin setting up replica set"
echo "************************************************************"

./mongo-engine-wait.sh

# Login as root and configure replica set
# # https://docs.mongodb.com/manual/reference/method/rs.initiate/#rs.initiate
mongo  admin -u "$MONGO_USER_ROOT_NAME" -p "$MONGO_USER_ROOT_PASSWORD" --eval "rs.initiate({ '_id': '$MONGO_REPLICA_SET_NAME', 'version': 1, 'members': $MONGO_REPLICA_SET_MEMBERS });"

echo "************************************************************"
echo " End setting up replica set"
echo "************************************************************"
