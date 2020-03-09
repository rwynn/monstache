####################################################################################################
# Step 1: Build the app
####################################################################################################

FROM rwynn/monstache-builder-cache:1.0.24 AS build-app

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download

RUN make release

####################################################################################################
# Step 2: Copy output build file to an alpine image
####################################################################################################

FROM rwynn/monstache-alpine:3.11.3 AS final

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
