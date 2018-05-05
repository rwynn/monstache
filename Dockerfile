# FROM rwynn/monstache-builder-base:0.0.1 AS monstache-builder-base
FROM golang:alpine AS base

# Step 1 Create a base builder image

# https://github.com/kelseyhightower/confd/issues/609
# ENV CGO_ENABLED=0

# ENV GOPATH /go

RUN apk add --no-cache gcc go git musl-dev make zip

FROM base AS deps-cache

# Step 2 use go to install packages

WORKDIR /go/src/app

COPY . .

RUN go get -d -v ./...

# RUN go install

# RUN go build

# RUN make

# FROM golang:alpine
# FROM rwynn/monstache-builder-base:0.0.1 AS monstache-builder-make
# FROM deps-cache AS make-app
FROM base AS make-app

# Step 3 run make to build the app

COPY --from=deps-cache /go /go
COPY --from=deps-cache /usr/lib/go /usr/lib/go

WORKDIR /go/src/app

COPY . .

RUN make

# FROM golang:alpine
# Use the following with CGO_ENABLED=0 builds
FROM alpine:3.7 AS final

# # Use the following with CGO_ENABLED=1 builds (e.g. you need to use the go plugin feature)
# FROM debian:latest

# Step 4 copy output build file to an alpine image

RUN apk --no-cache add ca-certificates

ENTRYPOINT ["/bin/monstache"]

# COPY --from=monstache-builder-make /go/src/app/build /app/build

# ADD monstache /bin/monstache
COPY --from=make-app /go/src/app/build/linux-amd64/monstache /bin/monstache
