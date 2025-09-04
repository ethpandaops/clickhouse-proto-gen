FROM gcr.io/distroless/static-debian12:latest
COPY clickhouse-proto-gen* /clickhouse-proto-gen
ENTRYPOINT ["/clickhouse-proto-gen"]
