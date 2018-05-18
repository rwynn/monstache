#!/bin/bash

export COMPOSE_FILE=docker-compose.test.yml
export COMPOSE_PROJECT_NAME=monstache

# The network created by docker-compose will be called ${COMPOSE_PROJECT_NAME}_test as we have the network test in docker-compose

docker-compose down --remove-orphans ; docker-compose up --force-recreate --build --abort-on-container-exit --exit-code-from sut
