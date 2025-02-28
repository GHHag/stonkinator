FROM golang:1.23 AS build

WORKDIR /app

COPY /src/go.mod /src/go.sum /app/

RUN go mod download

COPY ./src /app/

# RUN go build -o /bin/stonkinator_rpc_service
RUN CGO_ENABLED=0 GOOS=linux go build -o /stonkinator_rpc_service

CMD ["/stonkinator_rpc_service"]
