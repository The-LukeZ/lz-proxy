# ---- builder ----
FROM rust:1-slim AS builder
RUN rustup target add x86_64-unknown-linux-musl && \
    apt-get update && apt-get install -y musl-tools pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
# cache deps: build a stub so the dependency layer is reused across source changes
RUN mkdir src && echo 'fn main(){}' > src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl || true
COPY . .
# bust the stub's mtime so cargo rebuilds the real binary
RUN touch src/main.rs && cargo build --release --target x86_64-unknown-linux-musl

# ---- runtime ----
FROM alpine:3
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/lz-proxy /usr/local/bin/lz-proxy
EXPOSE 8080 9000
ENTRYPOINT ["/usr/local/bin/lz-proxy"]
