####################################################################################################
# Step 1: Build the app
####################################################################################################

FROM rwynn/monstache-builder-cache-rel3:1.0.18 AS build-app

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download

RUN make release

####################################################################################################
# Step 2: Copy output build file to an alpine image
####################################################################################################

FROM rwynn/monstache-alpine:3.9

ENTRYPOINT ["/bin/monstache"]

COPY --from=build-app /app/build/linux-amd64/monstache /bin/monstache
