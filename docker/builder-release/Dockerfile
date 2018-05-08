FROM golang:alpine AS base

RUN apk add --no-cache gcc go git musl-dev make zip

FROM base AS deps-cache

WORKDIR /go/src/app

COPY . .

RUN go get -d -v ./...

FROM base AS make-app

COPY --from=deps-cache /go /go
COPY --from=deps-cache /usr/lib/go /usr/lib/go

WORKDIR /go/src/app

COPY . .

RUN make release

FROM alpine:latest AS final

ARG BUILD_DATE

ARG VCS_REF

ARG VSC_URL

ARG BUILD_VERSION

LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.vcs-url=$VSC_URL \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.schema-version=$BUILD_VERSION

RUN apk --no-cache add ca-certificates

ENTRYPOINT ["/bin/monstache"]

COPY --from=make-app /go/src/app/build/linux-amd64/monstache /bin/monstache
