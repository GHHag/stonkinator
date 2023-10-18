FROM golang:latest

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

#RUN go build -o server
RUN CGO_ENABLED=0 GOOS=linux go build -o /server

CMD ["/server"]
