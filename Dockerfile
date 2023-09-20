FROM --platform=$BUILDPLATFORM golang:1.20.4-alpine3.17 AS build
WORKDIR /src
ARG TARGETOS TARGETARCH
RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
	go mod download; \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/monstache .

FROM alpine:3.17
RUN apk --no-cache add ca-certificates
ENTRYPOINT ["/bin/monstache"]
COPY --from=build /out/monstache /bin
