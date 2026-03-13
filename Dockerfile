FROM --platform=$BUILDPLATFORM golang:1.26-alpine3.22 AS build
WORKDIR /src
ARG TARGETOS TARGETARCH
RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
	go mod download; \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/monstache .

FROM alpine:3.22
RUN apk --no-cache add ca-certificates
ENTRYPOINT ["/bin/monstache"]
COPY --from=build /out/monstache /bin
