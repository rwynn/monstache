FROM rwynn/monstache-builder-cache-rel5:1.0.0

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download
