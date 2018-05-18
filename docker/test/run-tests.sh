#!/bin/bash

export COMPOSE_FILE=docker-compose.test.yml

docker-compose down --remove-orphans ; docker-compose up --force-recreate --build --abort-on-container-exit --exit-code-from sut
