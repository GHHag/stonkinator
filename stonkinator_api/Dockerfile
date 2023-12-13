FROM golang:1.21 AS build

WORKDIR /app

COPY go.mod go.sum /app/

RUN go mod download

COPY . /app/

# RUN go build -o /bin/tet_api
RUN CGO_ENABLED=0 GOOS=linux go build -o /tet_api

CMD ["/tet_api"]

# FROM scratch
# COPY --from=build /bin/app /bin/app
# ENTRYPOINT ["/bin/app"]
# CMD ["--help"]

# FROM build AS run
#
# FROM gcr.io/distroless/base-debian11 AS build-release
#
# WORKDIR /
#
# COPY --from=build /app /app
#
# EXPOSE 5800
#
# USER nonroot:nonroot
#
# ENTRYPOINT ["/tet_api"]
