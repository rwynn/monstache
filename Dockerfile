####################################################################################################
# Step 1: Build the app
####################################################################################################

FROM rwynn/monstache-builder:1.0.0 AS build-app
# FROM rwynn/monstache-builder-cache:1.0.0 AS build-app

WORKDIR /go/src/app

COPY . .

RUN go get -d -v ./...

# RUN make
RUN go build -ldflags="-s -w" -v -o build/linux-amd64/monstache

####################################################################################################
# Step 2: Copy output build file to an alpine image
####################################################################################################

FROM quadric/alpine-certs:3.7

ENTRYPOINT ["/bin/monstache"]

COPY --from=build-app /go/src/app/build/linux-amd64/monstache /bin/monstache
