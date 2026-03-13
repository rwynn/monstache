####################################################################################################
# Step 1: Build the app
####################################################################################################

FROM golang:1.26-alpine3.22 AS build-app

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download

RUN make release

####################################################################################################
# Step 2: Copy output build file to an alpine image
####################################################################################################

FROM alpine:3.22 AS final

RUN apk --no-cache add ca-certificates curl wget

ARG BUILD_DATE

ARG VCS_REF

ARG VSC_URL

ARG BUILD_VERSION

LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.vcs-url=$VSC_URL \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.schema-version=$BUILD_VERSION

ENTRYPOINT ["/bin/monstache"]

COPY --from=build-app /app/build/linux-amd64/monstache /bin/monstache
