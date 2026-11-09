# Packaging stub (SYNTHETIC DEMO). Toolchain pinned to rust-toolchain.toml.
FROM rust:1.99.1-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake make g++ pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN useradd -u 10001 -r processor
COPY --from=build /src/target/release/telemetry-processor /usr/local/bin/telemetry-processor
USER 10001
EXPOSE 9100
ENTRYPOINT ["/usr/local/bin/telemetry-processor"]
