FROM rwynn/monstache-builder:1.0.15

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download

RUN make all
