FROM golang:1.24.4-alpine3.21

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download
