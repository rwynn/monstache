# use the following with CGO_ENABLED=0 builds
FROM alpine:latest
RUN apk --no-cache add ca-certificates
# use the following with CGO_ENABLED=1 builds (e.g. you need to use the go plugin feature)
#FROM debian:latest

ENTRYPOINT ["/bin/monstache"]

ADD monstache /bin/monstache
