
####################################################################################################
# Step 1: Use go to install packages 
####################################################################################################

FROM rwynn/monstache-builder:1.0.0 AS deps-cache

WORKDIR /go/src/app

COPY . .

RUN go get -d -v ./...

# RUN go install

# RUN go build

# RUN make

####################################################################################################
# Step 2: Run make to build the app
####################################################################################################

FROM rwynn/monstache-builder:1.0.0 AS make-app

COPY --from=deps-cache /go /go
COPY --from=deps-cache /usr/lib/go /usr/lib/go

WORKDIR /go/src/app

COPY . .

RUN make

####################################################################################################
# Step 3: Copy output build file to an alpine image
####################################################################################################

FROM alpine:3.7 AS final

RUN apk --no-cache add ca-certificates

ENTRYPOINT ["/bin/monstache"]

COPY --from=make-app /go/src/app/build/linux-amd64/monstache /bin/monstache
