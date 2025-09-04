FROM debian:latest
RUN apt-get update && apt-get -y upgrade && apt-get install -y --no-install-recommends \
  libssl-dev \
  ca-certificates \
  python3 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
COPY clickhouse-proto-gen* /clickhouse-proto-gen
ENTRYPOINT ["/clickhouse-proto-gen"]
