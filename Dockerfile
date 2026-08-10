# klend-indexer container: compile against the full toolchain, ship a slim runtime.
#
# Runtime trust stores, two of them, on purpose:
#   - The gRPC client uses rustls with_native_roots(), which reads the OS trust
#     store at runtime, so the runtime image MUST carry ca-certificates.
#   - The ClickHouse TLS side uses webpki-roots baked into the binary and needs
#     nothing from the OS.
# Drop ca-certificates and the Alchemy connection fails while ClickHouse still works.

FROM rust:1-bookworm AS builder
WORKDIR /build

# Copy manifests first and compile a dummy main, so the dependency build is cached
# in its own layer and only re-runs when Cargo.toml/Cargo.lock change.
COPY Cargo.toml Cargo.lock ./
# Copy vendored dependencies alongside manifests so cargo can resolve path deps.
COPY vendor ./vendor
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Now the real source. Touch main so cargo rebuilds it over the cached dummy.
COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/klend-indexer /usr/local/bin/klend-indexer
# The one-shot RPC snapshot writer (Phase 2, Option A). Same image on purpose: it
# shares the decoders and the ClickHouse connect path with the indexer, and it has
# to run on the VM because the VM is the only host on ClickHouse's IP access list.
# Reached with `docker run --entrypoint /usr/local/bin/klend-snapshot`.
COPY --from=builder /build/target/release/snapshot /usr/local/bin/klend-snapshot
# The cold path. Both binaries ship for the same reason as klend-snapshot: the VM
# is the only host on ClickHouse Cloud's IP access list, and it is an e2-micro that
# cannot compile Rust. Without them in the image the export simply cannot be run
# anywhere, which is the actual blocker on the 2026-08-19 credit expiry, not the
# export code itself.
#   docker run --entrypoint /usr/local/bin/klend-parquet-export ... -v /var/klend:/out
#   docker run --entrypoint /usr/local/bin/klend-coldquery ...
COPY --from=builder /build/target/release/parquet_export /usr/local/bin/klend-parquet-export
COPY --from=builder /build/target/release/coldquery /usr/local/bin/klend-coldquery

# Run unprivileged. The indexer needs no filesystem writes; all its state is in
# ClickHouse. The export does write, so it needs a bind-mounted directory that
# uid 10001 can write to (COS mounts /var noexec, which blocks execution, not
# writes, so /var/klend is a valid target for the Parquet output).
RUN useradd -r -u 10001 klend
USER klend

# No KLEND_SAMPLE_SLOTS: the container runs unattended, so the reconnect loop is
# active and a bounded sample is never entered. Secrets and endpoints come from the
# runtime environment (see deploy/klend-indexer.env.example), never baked into the image.
ENTRYPOINT ["/usr/local/bin/klend-indexer"]
