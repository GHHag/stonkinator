FROM golang:1.23 AS build

WORKDIR /app

COPY /src/go.mod /src/go.sum /app/

RUN go mod download

COPY ./src /app/

COPY stonkify.crt /app
COPY stonkify.key /app

# RUN go build -o /bin/stonkinator
RUN CGO_ENABLED=0 GOOS=linux go build -o /stonkinator

CMD ["/stonkinator"]
