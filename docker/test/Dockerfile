FROM rwynn/monstache-builder-cache-rel3:1.0.14

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download
