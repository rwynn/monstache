FROM golang:alpine

RUN apk add --no-cache gcc go git musl-dev make zip

WORKDIR /go/src/app

COPY . .

RUN go get -d -v ./...

ARG PLUGIN

RUN go build -buildmode=plugin -o $PLUGIN.so $PLUGIN.go
