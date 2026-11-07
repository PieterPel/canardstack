# syntax=docker/dockerfile:1.7

FROM rust:1.92-slim-bookworm AS chef

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-chef --locked

WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY crates ./crates
# Cargo validates every declared target, including the bench in Cargo.toml.
COPY benches ./benches
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

ARG TARGETARCH
# Optional cap on parallel compile jobs: `--build-arg CARGO_BUILD_JOBS=2`.
ARG CARGO_BUILD_JOBS

# By default link a prebuilt libduckdb instead of compiling DuckDB's C++ (about 25 cold minutes)
# through the `bundled` cargo feature. The version must equal the one the `duckdb` crate pins
# (crate 1.10506.x = DuckDB 1.5.6), because DuckDB extensions only load into the exact version they
# were built for. To compile DuckDB into the binary instead: `--build-arg DUCKDB_BUNDLED=1`.
ARG DUCKDB_VERSION=1.5.6
ARG DUCKDB_BUNDLED=
ARG DUCKDB_SHA256_AMD64=b845005f5132a7d8180057c35e14a7626632258782f871a90861b19c1c03841b
ARG DUCKDB_SHA256_ARM64=b72ed9f05003f5e9d2015f7ceada6416b377d9dd33169cdde3c9e33897856eee
RUN mkdir -p /opt/duckdb \
    && if [ -z "$DUCKDB_BUNDLED" ]; then \
        case "$TARGETARCH" in \
            amd64) sha="$DUCKDB_SHA256_AMD64" ;; \
            arm64) sha="$DUCKDB_SHA256_ARM64" ;; \
            *) echo "unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
        esac \
        && apt-get update && apt-get install -y --no-install-recommends curl unzip \
        && rm -rf /var/lib/apt/lists/* \
        && curl -fsSL -o /tmp/libduckdb.zip \
            "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-linux-${TARGETARCH}.zip" \
        && echo "$sha  /tmp/libduckdb.zip" | sha256sum -c - \
        && unzip -q /tmp/libduckdb.zip -d /opt/duckdb && rm /tmp/libduckdb.zip; \
    fi
ENV DUCKDB_LIB_DIR=/opt/duckdb

COPY --from=planner /app/recipe.json recipe.json

# Compile dependencies. This is the cacheable layer: it rebuilds only when recipe.json changes, and
# the compiled output stays in the image layer so the type=gha layer cache reuses it across CI runs.
# A --mount=type=cache target dir would NOT persist in GitHub Actions.
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    [ -n "$CARGO_BUILD_JOBS" ] || unset CARGO_BUILD_JOBS; \
    if [ -n "$DUCKDB_BUNDLED" ]; then flags=""; else flags="--no-default-features"; fi; \
    cargo chef cook --release --locked $flags --features tls --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY crates ./crates
COPY benches ./benches

RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    [ -n "$CARGO_BUILD_JOBS" ] || unset CARGO_BUILD_JOBS; \
    if [ -n "$DUCKDB_BUNDLED" ]; then flags=""; else flags="--no-default-features"; fi; \
    cargo build --release --locked $flags --features tls \
    && mkdir -p /out/lib \
    && cp /app/target/release/canardstack /out/canardstack \
    && if [ -f /opt/duckdb/libduckdb.so ]; then cp /opt/duckdb/libduckdb.so /out/lib/; fi

FROM debian:bookworm-slim AS runtime

ARG CANARDSTACK_UID=10001
ARG CANARDSTACK_GID=10001

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid "${CANARDSTACK_GID}" canardstack \
    && useradd \
        --uid "${CANARDSTACK_UID}" \
        --gid "${CANARDSTACK_GID}" \
        --home-dir /var/lib/canardstack \
        --create-home \
        --shell /usr/sbin/nologin \
        canardstack \
    && mkdir -p /var/lib/canardstack/storage \
    && chown -R canardstack:canardstack /var/lib/canardstack

COPY --from=builder /out/canardstack /usr/local/bin/canardstack
# Empty when DuckDB is compiled in (`DUCKDB_BUNDLED=1`); otherwise holds the libduckdb.so linked above.
COPY --from=builder /out/lib/ /usr/local/lib/

ENV LD_LIBRARY_PATH=/usr/local/lib \
    HOME=/var/lib/canardstack \
    CANARDSTACK_BIND=0.0.0.0:9090 \
    CANARDSTACK_DATA_DIR=/var/lib/canardstack

USER 10001:10001
EXPOSE 9090

HEALTHCHECK --interval=5s --timeout=5s --start-period=20s --retries=12 \
    CMD ["canardstack", "healthcheck", "--endpoint", "http://127.0.0.1:9090/healthz"]

CMD ["canardstack", "serve"]
