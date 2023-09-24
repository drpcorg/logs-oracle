FROM golang:alpine as build-stage

RUN apk update && \
    apk add --no-cache git tzdata g++ ca-certificates

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

ADD . /app
RUN --mount=type=cache,target=/root/.cache \
    --mount=type=cache,target=/tmp/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=1 GOOS=linux go build .

FROM alpine:3.17

RUN apk add --no-cache ca-certificates libstdc++ tzdata

ARG UID=1000
ARG GID=1000
RUN adduser -D -u $UID -g $GID nonroot
USER nonroot:nonroot

COPY --from=build-stage /app/drpc-logs-oracle /drpc-logs-oracle

EXPOSE 8000

ENTRYPOINT ["/drpc-logs-oracle"]
