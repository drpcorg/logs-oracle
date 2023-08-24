# syntax=docker/dockerfile:1

FROM golang:1.21

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY db/  /app/db
COPY *.go /app/ 
RUN go build .

ENTRYPOINT ["/app/drpc-logs-oracle"]
