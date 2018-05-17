# Monstache test runner

You can use the `run-tests.sh` script in this folder to run integration tests. You will need to have installed
`docker` and `docker-compose`.  The script starts services for MongoDB, Elasticsearch, and Monstache itself, and
then runs the tests in `monstache_test.go`.
