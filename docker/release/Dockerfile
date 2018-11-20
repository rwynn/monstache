####################################################################################################
# Step 1: Build the app
####################################################################################################

FROM rwynn/monstache-builder-cache:1.0.10 AS build-app

WORKDIR /go/src/cache-app

COPY . .

RUN make release

####################################################################################################
# Step 2: Copy output build file to an alpine image
####################################################################################################

FROM quadric/alpine-certs:3.7 AS final

ARG BUILD_DATE

ARG VCS_REF

ARG VSC_URL

ARG BUILD_VERSION

LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.vcs-url=$VSC_URL \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.schema-version=$BUILD_VERSION

ENTRYPOINT ["/bin/monstache"]

COPY --from=build-app /go/src/cache-app/build/linux-amd64/monstache /bin/monstache
