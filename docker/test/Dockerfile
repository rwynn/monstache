FROM rwynn/monstache-builder-cache-rel6:1.0.6

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download
