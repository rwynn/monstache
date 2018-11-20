FROM rwynn/monstache-builder-cache:1.0.10 AS build-app

WORKDIR /go/src/cache-app

COPY . .

ARG PLUGIN

RUN go build -buildmode=plugin -o $PLUGIN.so $PLUGIN.go
