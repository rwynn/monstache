FROM rwynn/monstache-builder:1.0.13

RUN mkdir /app

WORKDIR /app

COPY . .

RUN go mod download

RUN make all
