FROM golang:1.26-alpine3.22

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download
