FROM rwynn/monstache-builder:1.0.2

ARG PLUGIN

WORKDIR /go/src/app

COPY . .

RUN go get -d -v ./...

RUN go build -buildmode=plugin -o $PLUGIN.so $PLUGIN.go
