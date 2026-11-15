# Developer Notes

canardstack is now a query-only Rust binary. It attaches DuckDB to a DuckLake
catalog and serves bounded Prometheus, Loki, and Tempo-compatible HTTP routes.

## Local Checks

```bash
cargo check --all-targets
cargo test
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo fmt --all -- --check
```

## Run Locally

```bash
cp config/example.env .env
set -a && . ./.env && set +a
cargo run -- serve
cargo run -- serve --listen 127.0.0.1:4320
cargo run -- healthcheck --endpoint http://127.0.0.1:4320/healthz
```

Telemetry writes are handled outside this binary. Use
[duckdb-otlp](https://github.com/smithclay/duckdb-otlp) in a DuckDB process to
write the DuckLake tables, then configure canardstack with the same catalog and
data path.

To validate the local extension checkout end to end:

```bash
scripts/e2e-duckdb-otlp-local.py
```

See [`docs/e2e-duckdb-otlp.md`](e2e-duckdb-otlp.md) for the exact shape and the
local file-lock caveat.

## Source Map

- `src/main.rs` dispatches `serve`, `healthcheck`, and `--version`.
- `src/http/` contains the Axum/Hyper HTTP/1.1 transport and compatibility route table.
- `src/compat/` adapts Prometheus, Loki, and Tempo-style HTTP requests.
- `src/query/` builds bounded SQL over the DuckLake tables.
- `src/storage/` owns DuckDB/DuckLake attach, health probes, schema fencing, and
  per-query connection cloning.
- `src/metadata.rs` serves discovery routes from `metadata_summary`, with a
  bounded raw-table fallback for Prometheus metric discovery.
- `src/metrics.rs` renders the process-local `/metrics` surface.

## Constraints

- Keep synchronous DuckDB/storage work in bounded `spawn_blocking` tasks, with owned admission permits acquired asynchronously before spawning.
- Keep platform-specific launch behavior in deployment files. The application uses its ordinary bind, TLS, and data-directory settings on every host.
- Do not add OTLP ingest, gRPC, Kafka, a bundled catalog service, or a second
  long-running process to this binary.
- Do not expose arbitrary SQL through the compatibility APIs.
- Keep query paths bounded by time range, row limit, timeout, DuckDB memory
  limit, and admission caps.

## Transport and build notes

Tokio handles connections, admission waits, body deadlines, shutdown signals,
and graceful draining. Hyper parses HTTP/1.1; Axum dispatches requests to the
synchronous compatibility adapters. Query task cancellation interrupts an
active scoped DuckDB connection, and the blocking task retains its permit until
it returns. A disconnected HTTP client cannot release capacity while its query
continues running. Operator requests have one separate bounded blocking slot.

`CANARDSTACK_SOCKET_READ_TIMEOUT_SECS` bounds header completion, body completion,
and idle reads. `CANARDSTACK_SOCKET_WRITE_TIMEOUT_SECS` bounds stalled socket
writes. Bodies are size-limited even with chunked transfer encoding. Requests
acquire execution capacity before buffering bodies. At the connection limit,
new sockets are closed; query admission returns 429 with `Retry-After`.

TLS uses the same public listener. Remove `CANARDSTACK_TLS_BACKEND_BIND` or
`server.tls.backend_bind` from older configurations; there is no plaintext
loopback proxy listener.

The first native build compiles bundled DuckDB C++. The development profile
disables debug symbols for `libduckdb-sys` to reduce archive size and build time;
Rust application debug symbols remain enabled. Reuse `target/` between checks.

Azure Functions hosting considerations are in
[`azure-functions.md`](azure-functions.md).
