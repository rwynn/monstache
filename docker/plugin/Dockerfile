FROM rwynn/monstache-builder-cache-rel3:1.0.9 AS build-app

WORKDIR /go/src/cache-app

COPY . .

ARG PLUGIN

RUN go build -buildmode=plugin -o $PLUGIN.so $PLUGIN.go
