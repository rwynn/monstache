#!/bin/bash

export COMPOSE_FILE=docker-compose.test.yml

docker-compose down --remove-orphans ; docker-compose up