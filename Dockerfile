####################################################################################################
# Step 1: Build the app
####################################################################################################

FROM rwynn/monstache-builder-cache-rel3:1.0.12 AS build-app

WORKDIR /go/src/cache-app

COPY . .

RUN go build -ldflags="-s -w" -v -o build/linux-amd64/monstache

####################################################################################################
# Step 2: Copy output build file to an alpine image
####################################################################################################

FROM rwynn/monstache-alpine:3.8

ENTRYPOINT ["/bin/monstache"]

COPY --from=build-app /go/src/cache-app/build/linux-amd64/monstache /bin/monstache
