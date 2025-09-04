FROM golang:1.24 AS builder
WORKDIR /src
COPY go.sum go.mod ./
RUN go mod download
COPY . .
RUN go build -o /bin/app .

FROM ubuntu:latest
RUN apt-get update && apt-get -y upgrade && apt-get install -y --no-install-recommends \
  libssl-dev \
  ca-certificates \
  python3 \
  wget \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /bin/app /clickhouse-proto-gen
ENTRYPOINT ["/clickhouse-proto-gen"]
