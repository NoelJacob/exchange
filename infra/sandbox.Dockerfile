FROM debian:bookworm-slim AS runner
RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 libgcc-s1 ca-certificates && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /tmp /binary && chmod 777 /tmp /binary
USER 1000:1000
COPY contestant /binary/contestant
ENTRYPOINT ["/binary/contestant"]
