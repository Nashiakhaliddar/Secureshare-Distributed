# ---------- build both binaries (API node + storage node) ----------
FROM rust:1.91-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src
RUN cargo build --release

# ---------- tiny runtime image ----------
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/secureshare_backend /usr/local/bin/secureshare_backend
COPY --from=builder /app/target/release/storage_node /usr/local/bin/storage_node
# default = API node; docker-compose overrides the command for storage nodes
CMD ["secureshare_backend"]
