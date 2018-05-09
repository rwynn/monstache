#!/bin/bash

echo "************************************************************"
echo "Begin setting up users..."
echo "************************************************************"

mongodb_setup_cmd="mongod"

mongodb_setup_cmd="$mongodb_setup_cmd --dbpath /data/db"

echo "Starting the server";
$mongodb_setup_cmd &

fg

echo "Waiting for engine to start";
./mongo-engine-wait.sh no-ssl

# mongo admin --eval "help" > /dev/null 2>&1
# RET=$?

# while [[ RET -ne 0 ]]; do
#   echo "Waiting for MongoDB to start..."
#   mongo admin --eval "help" > /dev/null 2>&1
#   RET=$?
#   sleep 1
# done

# create root user
if [ ! -z "${MONGO_USER_ROOT_NAME+x}" ] && [ ! -z "${MONGO_USER_ROOT_PASSWORD+x}" ] ; then
  mongo admin --eval "db.createUser({user: '$MONGO_USER_ROOT_NAME', pwd: '$MONGO_USER_ROOT_PASSWORD', roles:[{ role: 'root', db: 'admin' }]});"
else
  echo 'ERROR: Mongo root user credentials are not provided!';
  exit 1;
fi

# create admin user
if [ ! -z "${MONGO_USER_ADMIN_NAME+x}" ] && [ ! -z "${MONGO_USER_ADMIN_PASSWORD+x}" ] ; then
  mongo admin --eval "db.createUser({user: '$MONGO_USER_ADMIN_NAME', pwd: '$MONGO_USER_ADMIN_PASSWORD', roles: [{ role: 'dbAdminAnyDatabase', db: 'admin' }, { role: 'dbAdmin', db: 'local' }]});"
else
  echo 'WARNING: Mongo admin user credentials are not provided!';
fi

# create app user
if [ ! -z "${MONGO_USER_APP_NAME+x}" ] && [ ! -z "${MONGO_USER_APP_PASSWORD+x}" ] ; then
  # mongo admin --eval "db.createUser({ user: '$MONGO_USER_APP_NAME', pwd: '$MONGO_USER_APP_PASSWORD', roles: [{ role: 'readWrite', db: '$MONGO_DB_NAME' }, { role: 'read', db: 'local' }]});"
  mongo "$MONGO_DB_NAME" --eval "db.createUser({ user: '$MONGO_USER_APP_NAME', pwd: '$MONGO_USER_APP_PASSWORD', roles: [{ role: 'readWrite', db: '$MONGO_DB_NAME' }, { role: 'read', db: 'local' }]});"
else
  echo 'WARNING: Mongo app user credentials are not provided!';
fi

# create oplogger user
if [ ! -z "${MONGO_USER_OPLOGGER_NAME+x}" ] && [ ! -z "${MONGO_USER_OPLOGGER_PASSWORD+x}" ] ; then
  # mongo admin --eval "db.createUser({ user: '$MONGO_USER_APP_NAME', pwd: '$MONGO_USER_APP_PASSWORD', roles: [{ role: 'readWrite', db: '$MONGO_DB_NAME' }, { role: 'read', db: 'local' }]});"
  mongo admin --eval "db.createUser({ user: '$MONGO_USER_OPLOGGER_NAME', pwd: '$MONGO_USER_OPLOGGER_PASSWORD', roles: [{ role: 'read', db: 'local' }]});"
  # mongo "local" -u "$MONGO_USER_ROOT_NAME" -p "$MONGO_USER_ROOT_PASSWORD" --eval "db.createUser({ user: '$MONGO_USER_OPLOGGER_NAME', pwd: '$MONGO_USER_OPLOGGER_PASSWORD', roles: [{ role: 'read', db: 'local' }]});"

  # mongo "admin" -u "$MONGO_USER_ROOT_NAME" -p "$MONGO_USER_ROOT_PASSWORD" --eval "db.createUser({ user: '$MONGO_USER_OPLOGGER_NAME', pwd: '$MONGO_USER_OPLOGGER_PASSWORD', roles: [{ role: 'read', db: 'local' }]});"
  # mongo admin --eval "db.createUser({ user: '$MONGO_USER_OPLOGGER_NAME', pwd: '$MONGO_USER_OPLOGGER_PASSWORD', roles: [], otherDBRoles: { local: [ 'read' ]}})"
else
  echo 'WARNING: Mongo oplogger user credentials are not provided!';
fi

# create backup user
if [ ! -z "${MONGO_USER_APP_NAME+x}" ] && [ ! -z "${MONGO_USER_APP_PASSWORD+x}" ] ; then
  mongo admin --eval "db.createUser({ user: '$MONGO_USER_BACKUP_NAME', pwd: '$MONGO_USER_BACKUP_PASSWORD', roles: [{ role: 'backup', db: 'admin' }]});"
else
  echo 'WARNING: Mongo backup user credentials are not provided!';
fi

echo "Shutting down...";
mongo admin --eval "db.shutdownServer();";

echo 'Sleeping 1 second...';
sleep 1;

echo "************************************************************"
echo "End Setting up users..."
echo "************************************************************"
