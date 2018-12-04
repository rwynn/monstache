FROM rwynn/monstache-builder-cache-rel3:1.0.13

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download
