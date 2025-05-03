FROM golang:1.23 AS build

WORKDIR /app
COPY src/go.mod src/go.sum /app/
RUN go mod download

COPY src /app/

COPY stonkinator.key /etc/ssl/private
COPY stonkinator.crt /etc/ssl

# RUN go build -o /bin/stonkinator
RUN CGO_ENABLED=0 GOOS=linux go build -o /stonkinator_web_api

CMD ["/stonkinator_web_api"]
