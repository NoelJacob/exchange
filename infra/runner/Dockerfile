FROM minio/mc:latest AS mc-builder

FROM ubuntu:24.04
COPY --from=mc-builder /usr/bin/mc /usr/local/bin/mc
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates curl netcat-openbsd && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /app/binary
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
EXPOSE 9090 8080
ENTRYPOINT ["/app/entrypoint.sh"]