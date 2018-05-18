####################################################################################################
# Step 1: Build the app
####################################################################################################

FROM rwynn/monstache-builder-cache:1.0.2 AS build-app

WORKDIR /go/src/cache-app

COPY . .

RUN go get -d -v ./...

RUN make release
# RUN go build -ldflags="-s -w" -v -o build/linux-amd64/monstache

####################################################################################################
# Step 2: Copy output build file to an alpine image
####################################################################################################

FROM quadric/alpine-certs:3.7

ENTRYPOINT ["/bin/monstache"]

COPY --from=build-app /go/src/cache-app/build/linux-amd64/monstache /bin/monstache
