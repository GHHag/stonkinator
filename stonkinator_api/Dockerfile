FROM golang:1.21 AS build

WORKDIR /app

COPY go.mod go.sum /app/

RUN go mod download

COPY . /app/

# RUN go build -o /bin/stonkinator_api
RUN CGO_ENABLED=0 GOOS=linux go build -o /stonkinator_api

CMD ["/stonkinator_api"]
