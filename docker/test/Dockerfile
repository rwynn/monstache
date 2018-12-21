FROM rwynn/monstache-builder-cache-rel3:1.0.15

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download
