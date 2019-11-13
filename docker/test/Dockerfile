FROM rwynn/monstache-builder-cache:1.0.22

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download
