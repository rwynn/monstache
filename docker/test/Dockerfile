FROM rwynn/monstache-builder-cache:1.0.20

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download
