FROM rwynn/monstache-builder:1.0.2 AS build-app

WORKDIR /go/src/app

COPY . .

RUN go get -d -v ./...

ARG PLUGIN

RUN go build -buildmode=plugin -o $PLUGIN.so $PLUGIN.go
