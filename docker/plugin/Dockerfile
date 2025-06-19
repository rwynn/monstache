FROM golang:1.24.4-alpine3.21 AS build-app

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download

ARG PLUGIN

WORKDIR /app/docker/plugin

RUN go mod download

RUN go build -buildmode=plugin -o $PLUGIN.so $PLUGIN.go
