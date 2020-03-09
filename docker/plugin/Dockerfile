FROM rwynn/monstache-builder-cache:1.0.24 AS build-app

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download

ARG PLUGIN

WORKDIR /app/docker/plugin

RUN go mod download

RUN go build -buildmode=plugin -o $PLUGIN.so $PLUGIN.go
