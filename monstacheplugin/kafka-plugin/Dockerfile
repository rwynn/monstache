FROM golang:1.25-alpine3.22 AS builder
# Install build dependencies
RUN apk add --no-cache gcc g++ make git

WORKDIR /app

# Clone monstache repo with no history for smaller size and faster clone
RUN git clone https://github.com/rwynn/monstache.git -b rel6 --depth 1
WORKDIR /app/monstache

# Build plugin binary so
RUN go get github.com/segmentio/kafka-go
RUN go get github.com/shopspring/decimal
RUN go mod download

FROM builder AS builder-binary
# Build kafka plugin
WORKDIR /app/monstache

# Copy kafka plugin code
COPY kafka_plugin.go plugins/kafka_plugin.go

RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
    go build -buildmode=plugin -o kafka_plugin.so plugins/kafka_plugin.go

# Build monstache binary with plugin included
RUN go build -o monstache .

FROM alpine:3.22 AS runtime
COPY --from=builder-binary /app/monstache/monstache /usr/local/bin/monstache
COPY --from=builder-binary /app/monstache/kafka_plugin.so /monstache/

WORKDIR /monstache
# Copy config + scripts
COPY monstache.toml /monstache/monstache.toml
COPY scripts /monstache/scripts

CMD ["monstache" , "-f", "/monstache/monstache.toml", "-mapper-plugin-path", "/monstache/kafka_plugin.so"]