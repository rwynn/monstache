FROM rwynn/monstache-builder-cache-rel6:1.0.8

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download
