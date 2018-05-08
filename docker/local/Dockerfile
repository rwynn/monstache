FROM rwynn/monstache-builder:1.0.0 AS build-app

WORKDIR /go/src/app

COPY . .

RUN go get -d -v ./...

RUN make all
