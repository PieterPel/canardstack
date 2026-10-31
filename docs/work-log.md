# Work log

Updated: 2026-10-31

## 1. Compose on this Mac

Status: startup and initial log smoke passed; broader verification found an unresolved storage-compatibility blocker (R1).

- Replaced shell-based startup for the distroless `duckdb-otlp` image with its native CLI and an init SQL template.
- Pinned `duckdb-otlp` to `0.7.3` after observing incompatible serialization with the DuckDB 2.0 `latest` image and canardstack's DuckDB 1.5.x client.
- Removed the forced amd64 platform so Apple Silicon uses native arm64 images.
- Added a TLS proxy for the older Quack listener and initialization for certificates, SQL rendering, and shared-volume permissions.
- Matched canardstack's runtime UID to the ingest image after an accepted log failed to commit because canardstack-created directories blocked the writer.
- Removed the obsolete catalog Dockerfile and shell startup scripts; updated the README.
- Created a gitignored local `.env` with `OTLP_PORT=14318`, `CANARDSTACK_PORT=19090`, and `GRAFANA_PORT=13000`; local Prometheus and Node already occupied the usual query/UI ports.

Verification performed:

- `docker compose config` and `podman-compose config` passed.
- A clean `podman-compose down -v` followed by `podman-compose up -d` succeeded on the newly created example volumes.
- OTLP accepted a test log; after background commit, Loki returned `clean compose run works`.
- canardstack `/healthz` reported healthy storage; Grafana `/api/health` returned HTTP 200.
- `git diff --check` passed. No Rust code changed in this work.

Follow-up verification changed the conclusion: two gauge datapoints were accepted, but querying them returned HTTP 503 with a missing `ducklake_inlined_data_3_3` table. The earlier Loki selector then also returned HTTP 503 for missing `ducklake_inlined_data_1_1`. `/healthz` still returned healthy. The earlier successful smoke is historical evidence, not sufficient proof that this image pairing works reliably.

Local endpoints:

| Service | URL |
| --- | --- |
| OTLP/HTTP | http://localhost:14318 |
| canardstack | http://localhost:19090 |
| Grafana | http://localhost:13000 |

Remaining checks:

- [x] Review startup ordering, permissions, persistent state, and configuration edge cases in the revised Compose stack; unresolved findings below.
- [x] R1: reader pinned to `duckdb =1.10506.1` (DuckDB 1.5.6, ducklake `ac7595b0`, matching the writer). Aligning versions alone did not fix reads after `ducklake_flush_inlined_data`; the attached catalog kept stale inlined-table references. `Storage::with_query_execution` now re-attaches the catalog and retries once on that error. `external_writer_survives_schema_changes_flush_and_restarts` passes (6 rounds incl. flush). Not yet checked against a DuckDB 2.0 writer or the Compose images.
- [ ] Resolve the benchmark/tutorial flush instructions, which still assume a shell and SQL FIFO in the ingest container.
- [ ] Add repeatable CI smoke coverage for ingest, commit, and query; config parsing alone does not prove the stack works.

## 2. Why not Axum?

Status: Axum/Tokio migration approved and implemented; see section 5 for verification. The assessment and history below describe the baseline that motivated it.

- Confirmed repository guidance explicitly requires synchronous std-library HTTP and forbids an async runtime (`AGENTS.md`, `docs/developer.md:51–58`).
- The implementation uses an accept-polling loop, one OS thread per accepted connection, and a custom HTTP/1 parser (`src/http/server.rs:37–111,166–283`). DuckDB queries are synchronous.
- Those choices keep the runtime small and make direct blocking DuckDB calls straightforward. I found no benchmark or decision record in the files reviewed demonstrating that they outperform or simplify a maintained HTTP stack overall.
- Synchronous DuckDB does **not** require custom synchronous HTTP. Axum can serve HTTP asynchronously while a bounded blocking executor runs DuckDB work. Axum/Tokio would still fit in one binary and one process.
- The current cost is concrete: custom framing/version handling accepts malformed requests (R2), socket timeouts are not total request deadlines (R6), and idle keep-alive connections retain OS threads.

History checked after the initial review:

- `7a9996b` (initial commit) already contains the handwritten parser and thread-per-connection server. The parser was not introduced by a later migration away from Axum.
- `f716e6a` (2026-05-13) adds the explicit no-Tokio/synchronous constraint to `AGENTS.md`, documenting an implementation that already existed. Its commit message does not explain a comparative transport decision.
- `acfced7` (2026-05-20) enables keep-alive by default, extending the responsibilities of that original implementation.
- `35d1a6c` (2026-05-25) adds optional Tokio/Tonic for gRPC ingestion. The project did use an async runtime behind a feature; the constraint was not technically immutable.
- `ce64d05` (2026-05-30) removes that gRPC/Tokio feature during the query-only cutover while retaining the existing HTTP server.

The evidence supports an early synchronous implementation becoming a preserved architectural rule. A preference for fewer dependencies and straightforward blocking execution is plausible, but the history inspected does not establish the original author's motivation or a measured advantage over Axum.

Recommendation: use Axum/Hyper for the public HTTP transport if we approve changing the no-async constraint. Preserve query planning, storage ownership, compatibility adapters, authentication semantics, bounded admission, and protocol-specific error envelopes. Do not run blocking DuckDB work directly on Tokio worker threads. Hold admission permits until the actual blocking query finishes, including when its HTTP client disconnects; timing out an async future alone does not stop DuckDB.

If synchronous transport remains a hard requirement, the custom parser still needs replacement or substantial hardening. The repository constraint explains why Axum is absent; it does not justify the parser's correctness gaps.

Next actions:

- [x] Record an explicit transport decision and update `AGENTS.md`/developer constraints if Axum is selected.
- [ ] Compare representative Grafana workloads, idle connections, malformed requests, cancellation, shutdown, latency, and memory before/after.
- [x] Preserve Loki/Prometheus error envelopes and request/body/concurrency limits in transport regression tests.
- [x] Include the optional TLS feature and health/metrics routes in the migration boundary.

## 3. Azure Blob support

Status: initial gap assessment complete; implementation and live Azure verification pending.

Current code:

- `src/storage/ducklake.rs:45–105` detects only S3/GCS URI schemes and configures their extension/credential secrets. Azure URIs return no object-store kind, so they skip explicit Azure setup.
- `CANARDSTACK_DUCKLAKE_DATA_PATH` already accepts a string passed through to DuckLake, so the data-path configuration can carry an Azure URI.
- There is no Azure-specific extension/identity/account configuration or documented, tested Azure path in the inspected storage/config code. DuckDB autoloading and pre-provisioned secrets may allow some scenarios already; that is not verified end-to-end support.

DuckDB 1.5's documented Azure extension supports:

| Storage | URI form | Preferred authentication |
| --- | --- | --- |
| Blob | `az://account.blob.core.windows.net/container/prefix/` | Azure credential-chain secret; managed identity in Azure |
| Blob alias | `azure://account.blob.core.windows.net/container/prefix/` | Same |
| ADLS Gen2 | `abfss://account.dfs.core.windows.net/filesystem/prefix/` | Credential chain; workload identity where deployed |

`INSTALL azure; LOAD azure;` provides the filesystem. A secret uses `TYPE azure, PROVIDER credential_chain`; documented chains include `cli`, `env`, `managed_identity`, and `workload_identity`. Fully qualified paths can avoid a separate account-name setting. A user-assigned managed identity can be selected with the managed-identity provider's `CLIENT_ID`. Scope matching needs a trailing slash. Prefer identity/RBAC over application-managed connection strings.

Source checked: [DuckDB 1.5 Azure extension documentation](https://duckdb.org/docs/1.5/core_extensions/azure.html), 2026-10-29. These are documented capabilities, not live verification of canardstack's exact build.

Implementation plan:

- [ ] Add Azure URI classification alongside `ObjectStore::S3`/`Gcs`; explicitly load `azure` and configure a scoped secret before DuckLake attach.
- [ ] Decide the smallest identity configuration: fully qualified data URI plus credential chain by default; an explicit user-assigned identity when required. Preserve operator-provisioned secrets rather than silently replacing them.
- [ ] Integrate settings consistently into TOML, environment overrides, validation, example config, and runtime documentation.
- [ ] Verify extension availability for the actual DuckDB version and both Linux image architectures; choose an explicit image-cache/offline extension policy.
- [ ] Configure the external writer independently with the same catalog/data URI and appropriate Azure credentials. Reader support alone is insufficient.
- [ ] Resolve startup writes before promising read-only Blob RBAC: `Storage::open` currently creates tables, sets partitioning, and writes schema metadata (R4). A query server with only read permission can fail boot.
- [ ] Add URI/secret-generation tests, real extension-load coverage, and an integration round trip. Use an emulator for filesystem/auth mechanics where supported; verify managed/workload identity against a real Azure deployment.
- [ ] Test missing permissions, expired credentials, account/container scope, restart, bounded cancellation, and actual query error envelopes.

No live Azure smoke was run: this session has not established a test storage account, catalog, or identity. No claim of working Azure support yet.

## 4. Further critical code review

Status: baseline critical review complete. Findings below describe pre-migration code; subsequent fixes and remaining gaps are tracked in section 5.

- [x] Review HTTP parsing and resource limits.
- [x] Review query admission, timeouts, SQL generation, and storage/schema behavior.
- [x] Review the Compose changes critically, including known benchmark compatibility issues.
- [x] Record prioritized findings with file references, impact, evidence, and next actions.

Scope: the changed Compose stack, HTTP server/router, query engine and Prometheus adapter/parser, storage attach/schema/query connections/health, and admission control. This is a targeted review, not an exhaustive audit of every adapter or feature. P1 = high-priority correctness/availability issue; P2 = significant follow-up. Runtime observations use the published Compose image; code references use this checkout (the published canardstack image was previously inspected as revision `652de5a`).

### R1 · P1 · The revised Compose pairing still fails on evolving DuckLake data

**Evidence: live reproduction.** Logs initially ingested and queried successfully. A subsequent OTLP/HTTP metrics POST accepted two `review_grouped_gauge` datapoints for `review-a` and `review-b`. `GET /api/v1/query?query=sum by (service_name) (review_grouped_gauge)` returned `query_storage_unavailable` with a missing `ducklake_inlined_data_3_3` table. Repeating the prior Loki query then failed on `ducklake_inlined_data_1_1` as well. The error suggested differently named inlined-data tables exist.

**Location:** `compose.yaml` image pairing; `Cargo.lock` DuckDB `1.10503.1`; the pinned writer reports DuckDB 1.5.6. The exact source of the inlined-table mismatch has not been isolated. Do not assume matching only the major/minor DuckDB version guarantees compatibility, or call this proven data loss.

**Impact:** accepted telemetry cannot reliably be queried through the current stack. The prior log-only smoke and image pin do not close compatibility work.

- [ ] Identify actual DuckDB and DuckLake extension versions on both sides, reproduce snapshot/inlined-data behavior, and establish a tested version combination.
- [ ] Cover logs, gauges, sums, spans, repeated writes, mixed signals, multiple readers, and process restarts in a repeatable smoke.

### R2 · P1 · HTTP framing is ambiguous and protocol versions are ignored

**Location:** `src/http/server.rs:189–233,252–259`.

**Evidence: source inspection plus three live raw-socket probes.** Each of these reached `/healthz` and returned `HTTP/1.1 200 OK`:

- Two different `Content-Length` headers (`5`, then `0`). The HashMap keeps only the last.
- `Transfer-Encoding: chunked` together with `Content-Length: 0`. Transfer encoding is never validated.
- A request line ending in `NOTHTTP` instead of an HTTP version. Only method and target are parsed.

**Impact:** request boundaries can disagree with an upstream proxy; the keep-alive implementation can parse unread bytes as another request. This establishes a framing vulnerability surface, not a demonstrated proxy-specific request-smuggling exploit.

- [x] Replace the custom parser with a maintained HTTP stack.
- [x] Add raw-socket regression cases, including pipelining and chunked framing.
- [ ] Verify the deployed upstream-proxy integration, including Azure Functions.

### R3 · P1 · Public metrics collection bypasses query bounds and holds the connection-cloning mutex

**Location:** `src/http/router.rs:81–89,132–133`; `src/storage/health.rs:25–45,97–145`; `src/storage/query_conn.rs:14–16,67–74`.

**Evidence: source inspection.** `/metrics` calls `storage.health()` without authentication or query admission. That runs all-history `max(...)` and `count(*)` queries on the parent reader through `with_conn`, without the query watchdog. New query connections must lock that same reader before their watchdog is installed. Operator storage-health queries have the same unbounded execution path, even though they require authentication.

**Impact:** slow catalog/object-store access or repeated scrapes can block query setup and readiness outside advertised query deadlines. DuckDB may optimize some aggregates using metadata, but there is no enforced latency/resource bound here.

- [ ] Collect bounded operator statistics separately, cache snapshots for scrapes, and avoid holding the shared connection-cloning mutex during remote queries.
- [ ] Include connection acquisition/setup in deadline handling and prove an expensive scrape cannot starve compatibility requests.

### R4 · P1 · Startup mutates the catalog before checking its schema fence

**Location:** `src/storage/mod.rs:114–117`; `src/storage/schema.rs:22–61,232–240`.

**Evidence: source inspection.** `create_tables_on` creates missing tables and issues partition-changing `ALTER TABLE` statements before `enforce_schema_version_on` can reject an incompatible catalog. An unversioned catalog is stamped with the current generation without validating existing table column types first; `CREATE TABLE IF NOT EXISTS` does not validate them.

**Impact:** a supposedly rejected startup can already have changed an externally owned catalog. A structurally incompatible unversioned catalog can be stamped as compatible and fail later. This also prevents a straightforward read-only query-server deployment.

- [ ] Read the existing fence before mutations and validate the physical schema before adopting an unversioned catalog.
- [ ] Separate bootstrap/migration ownership from read-side startup; explicitly define any writes that remain necessary.
- [ ] Test that rejecting a newer/older catalog leaves its tables and partitioning unchanged.

### R5 · P1 · Accepted Prometheus expressions silently lose semantics

**Location:** `src/compat/prometheus.rs:31–63`; `src/query/prometheus.rs:23–38`; `src/query/mod.rs:150–151,202–206`.

**Evidence: source inspection.** Instant queries pass `limit: 1` even when grouping is requested, then return a vector containing only one row. Nested `sum(rate(...))` is reduced to a single aggregation field: the inner rate overwrites the outer sum. The range selector is stripped, and the rate SQL calculates `(max(value)-min(value))/elapsed` per bucket without counter-reset/series-identity handling.

**Impact:** valid-looking responses can drop grouped series or report incorrect rates. A deliberately bounded subset should reject unsupported semantics instead of accepting them with a different meaning.

The attempted live two-series instant-query reproduction was blocked by R1, so grouped-result truncation and rate correctness are not claimed as live-verified here.

- [ ] Return all bounded instant-vector groups; distinguish the per-series latest sample from a global row limit.
- [ ] Represent supported nested expressions/windows explicitly, or reject them with the Prometheus error envelope.
- [ ] Add regression cases for two grouped series, different range windows, counter resets, multiple service instances, and sum-of-rates.

### R6 · P2 · Per-read socket timeouts do not impose a total request deadline

**Location:** `src/http/server.rs:59–60,166–249`.

**Evidence: source inspection.** Read timeouts apply to individual socket reads. A peer can continue a partial header/body by sending data before each read timeout expires. Each accepted connection retains an OS thread; the connection cap bounds the count but does not prevent slow clients occupying every slot indefinitely.

- [x] Add absolute header/body deadlines and bound body buffering through execution admission before collection. The baseline 1,024 connections × 8 MiB body cap could otherwise approach 8 GiB in body buffers alone.
- [ ] Test trickled headers/bodies and prove normal queries retain service under the configured connection limit.

### R7 · P2 · Readiness does not establish that visible telemetry is readable

**Location:** `src/storage/health.rs:136–145`.

**Evidence: live observation and source inspection.** Readiness runs `SELECT 1` and prepares `SELECT * FROM otlp_logs LIMIT 0`. During R1, `/healthz` returned healthy while both metric and log queries returned storage failures.

**Impact:** container dependencies and operators see a ready query server that cannot serve stored data. A readiness check should remain cheap, but this check only demonstrates a minimal connection/schema condition.

- [ ] Define separate liveness/readiness/degraded-storage semantics and track recent bounded query failures without doing a full scan on every probe.
- [ ] Test catalog outage, inaccessible data files, and the inlined-data failure against the reported status.

### R8 · P2 · Benchmark and tutorial still rely on the removed ingest FIFO

**Location:** `scripts/bench-compose-logs.py:264–279`; `site/src/content/docs/tutorials/local-observability-stack.md:93`; README benchmark command.

**Evidence: source inspection.** The benchmark executes `sh` inside the ingest image and writes SQL to `/tmp/duckdb-otlp-ingest.sql`, which the native distroless daemon does not consume. Its flush SQL also names port 4319 while the Compose listener is 4318. The script uses Docker commands, so it does not operate the running Podman stack as written.

- [ ] Replace the flush mechanism and documented command with a supported administrative path; do not merely remove `--flush` and imply durability was verified.
- [ ] Make the selected container runtime explicit and exercise the documented command in CI.

### R9 · P2 · Compose initialization adds configuration and reproducibility debt

**Location:** `compose.yaml` init service and mutable auxiliary images; `config/compose/duckdb-otlp-ingest.sql`.

**Evidence: source inspection.** The init container renders SQL with `sed` and rejects several token characters; newline/carriage-return handling is not validated. It recursively marks catalog, data, and canardstack files world-writable, rather than assigning only required ownership/permissions. Auxiliary images still use mutable `latest` tags. Quack's former published host port is no longer exposed by this Compose revision.

- [ ] Replace ad hoc substitution with a predictable validated configuration path, including control-character cases.
- [ ] Initialize narrowly scoped ownership/permissions for the shared runtime UID instead of recursive `0777` on every start.
- [ ] Pin and test the complete image/extension matrix and explicitly decide whether host Quack access should be preserved.
- [ ] Add startup/readiness ordering coverage and bounded retry behavior for a slow catalog or TLS front end.

## Next implementation order

1. Fix R1 and establish repeatable cross-signal storage compatibility verification.
2. Address R2/R3/R4: transport correctness, bounded operator queries, and schema fencing before mutation.
3. Correct or reject the Prometheus semantics in R5; tighten deadlines/readiness in R6/R7.
4. Integrate Azure Blob with a defined startup-write/RBAC contract and real round-trip verification.
5. Finish Compose/benchmark reproducibility work in R8/R9. The Axum decision has been implemented in section 5.

Baseline review-session verification: three raw HTTP probes, a two-gauge OTLP POST, subsequent Prometheus/Loki queries, and a readiness check. That documentation-only review did not run Rust checks or implement fixes. Subsequent migration verification follows.

## 5. Axum/Tokio migration

Status: implemented and locally verified, 2026-10-31. No commit, push, or Azure deployment performed.

Completed:

- [x] Replace custom request/response framing and thread-per-connection HTTP with Axum/Hyper HTTP/1.1 on Tokio.
- [x] Preserve synchronous compatibility adapters, JSON/protobuf responses, authentication, and protocol error envelopes.
- [x] Replace blocking admission waits with separate Tokio cheap/heavy semaphores.
- [x] Acquire capacity before buffering request bodies or spawning blocking work. Blocking closures own their permits until they actually finish, even after an HTTP deadline or dropped request future.
- [x] Bound non-query/operator dispatch with a separate one-slot semaphore.
- [x] Enforce fixed/chunked body size limits, absolute header/body read deadlines, idle I/O timeouts, and a connection cap.
- [x] Add per-request execution deadlines and scoped DuckDB interruption. Retain the synchronous DuckDB watchdog; use a spurious-wakeup-safe condition wait.
- [x] Implement Tokio signal handling and bounded graceful drain of connections and tracked blocking tasks.
- [x] Serve optional TLS directly with tokio-rustls; use the shared PEM library. Remove the proxy pump and obsolete TLS backend bind setting. Older configurations must remove `CANARDSTACK_TLS_BACKEND_BIND` / `server.tls.backend_bind`.
- [x] Update repository/developer guidance to permit async HTTP while keeping blocking database work bounded and off runtime workers.
- [x] Keep Azure Functions adaptation outside Rust application code. Initial Azure-specific branches in `Config` and `main` were removed following feedback; source search finds no Functions-specific branches in `src/`.
- [x] Add deployment-only templates under `deploy/azure-functions/` and document requirements in `docs/azure-functions.md`.

Build investigation:

- The cold build was compiling bundled DuckDB C++, rather than waiting in HTTP code. The first foreground all-features check was interrupted before application verification.
- Disabled development debug symbols for `libduckdb-sys` only, reducing native object/archive overhead while retaining Rust application debug symbols.
- Reused build outputs. A later all-target/all-features typecheck completed in about five seconds; subsequent feature/profile checks can still trigger a separate native build.
- The Mac's Cargo wrapper did not include Rustfmt/Clippy on PATH. Those tools were supplied with `nix shell nixpkgs#rustfmt nixpkgs#clippy`.

Verification completed:

- `cargo check --all-targets --all-features` passed.
- `cargo check --locked` passed with default features.
- `cargo test --all-features --locked` passed: 56 library tests, 2 binary tests, 14 integration tests; one existing extension-download test remains ignored.
- `cargo clippy --all-targets --all-features --locked -- -D warnings` passed via the Nix tool environment.
- Rust formatting and `git diff --check` passed.
- Deployment JSON parsing and `sh -n deploy/azure-functions/launch.sh` passed.
- `ASTRO_TELEMETRY_DISABLED=1 npm run build` in `site/` was not run: its dependencies are not installed (`site/node_modules` is absent). Site edits only update runtime terminology and an existing diagram label; no frontend implementation changed.

Transport regression coverage includes conflicting content lengths, invalid HTTP versions, duplicate auth headers, pipelining, chunked framing/body limits, trickled body deadlines, idle shutdown, 429/`Retry-After`, form POST decoding, auth/error envelopes, Tempo JSON/protobuf negotiation, and a large-body TLS round trip. Storage coverage proves active DuckDB interruption; a blocked-connection test proves a timed-out request retains admission until its blocking work returns.

Hyper safely parses transfer-encoding framing instead of using the old last-header-wins behavior. The tests cover its normalized chunked-plus-content-length behavior; they do not claim that every such request is rejected.

Remaining limitations and next work:

- [x] R1: resolved in section 1 (reader/writer version alignment plus re-attach on stale inlined-data errors). The note below predates that fix. Local transport tests use a local DuckLake catalog, not the previously failing published-image pairing.
- [ ] R3: operator work now has bounded dispatch and HTTP response deadlines, but its existing storage-health queries still use the shared parent connection. Those calls do not gain scoped-query interruption; finish cached/bounded operator statistics and eliminate mutex starvation.
- [ ] R4/R5/R7: startup mutation/schema fencing, Prometheus semantic issues, and data-readiness detection remain open.
- [ ] R8/R9: benchmark FIFO assumptions and Compose reproducibility/permission debt remain open.
- [ ] Measure before/after performance with representative Grafana workloads. Functional tests do not establish a throughput improvement.
- [ ] Run Functions Core Tools against the deployment templates, then verify on the actual Linux Function App. Core Tools is not installed locally; the template has not been host-validated or deployed.
- [ ] Implement Azure Blob support and resolve cold-start, remote-catalog, RBAC/startup-write, memory, and scale-out requirements from `docs/azure-functions.md` before production deployment.

The running Compose containers still use the published images. This migration's verified artifacts are the locally compiled code/tests; the Compose images were not rebuilt as part of this migration.

## 6. Comparative observability benchmarks

Status: local single-node harness implemented in `bench/` (see `bench/README.md`); first results below. Azure Functions track, cost accounting, Grafana-in-path, resource limits, and scale sweeps remain open. Requested comparison: standard LGTM, Victoria, and canardstack on Azure Functions.

Candidate configurations:

| Configuration | Components to include |
| --- | --- |
| LGTM | Loki, Tempo, Mimir, Grafana, collectors, and required storage/catalog infrastructure |
| Victoria | VictoriaMetrics, VictoriaLogs, VictoriaTraces, Grafana, collectors, and required storage infrastructure; confirm exact products/topology before pinning |
| Serverless canardstack | Azure Functions custom handler, external duckdb-otlp writer, DuckLake catalog, Blob/object storage, Grafana, and collectors |

Use two comparison tracks: equal-resource controlled infrastructure for execution efficiency, and realistic documented deployment configurations for end-to-end latency and cost. Record single-node versus distributed topology explicitly. Standalone components and complete stacks need separately labeled results.

Benchmark contract:

- [ ] Pin images, extensions, deployment definitions, region, CPU/memory, local disk, object-store configuration, retention, replication/durability, and Azure hosting plan/scale/minimum-instance settings.
- [ ] Generate deterministic logs, traces, gauges, and sums with shared labels, timestamps, seed, payload sizes, cardinality, and trace shapes. Track the full source/export/ingest counts.
- [ ] Publish a query capability matrix. Compare common queries against an independent expected result; record unsupported semantics separately. Do not count an empty, incorrect, partial, or rejected result as a fast successful query.
- [ ] Keep common Grafana dashboard intent consistent while using each backend's supported query language/adapter. Keep Grafana/collector overhead consistent and record client concurrency.
- [ ] Measure accepted ingestion, committed/durable ingestion where observable, ingest-to-query visibility, loss/duplication, and backlog recovery independently; acceptance is not durability.
- [ ] Measure query p50/p95/p99, throughput, errors, throttling, and result correctness across recent/historical data, cardinality, concurrent dashboards, and mixed ingest/query workloads.
- [ ] Separate warm-process/cold-cache queries, warm-cache queries, actual platform cold starts, warm Functions invocations, and scale-out. Record observed cold starts instead of assuming an idle interval guarantees one.
- [ ] Include idle/sporadic use, steady production load, bursts, and sustained saturation; measure at several retained-data sizes.
- [ ] Account for the whole system: hosting, writer/catalog compute, minimum/warm instances, persistent storage, Blob request operations, data transfer, and monitoring. Publish raw billing quantities and price date/region, then compute monthly cost for explicit workload scenarios.
- [ ] Include process restarts, scale-in/out, catalog outages, and memory pressure as separately labeled resilience tests.
- [ ] Store raw requests/results, correctness checks, repeated-run samples, configuration manifests, and report-generation code in the repository. Run a small smoke in CI and a scheduled/manual full suite on controlled infrastructure.

Publication prerequisites: resolve R1, run the actual Azure Functions host/deployment smoke, establish the Azure storage/identity path, and repair or replace the old FIFO-based benchmark. A one-off local throughput number is not evidence for the serverless comparison.

Deliverables: reproducible deployment profiles, a shared workload/query manifest with expected results, a runner, raw versioned result artifacts, and a generated performance/cost report with capability and reliability limitations visible alongside the charts.

### First local results (2026-10-31)

Workload: 5 services x 2,000 records per signal, 100 runs per query at 1 and 4 clients, local Podman VM (4 CPU / 8 GiB), one stack at a time, no resource limits, an unrelated idle compose stack also running. Raw JSON is produced by `bench/run.py` into `bench/results/`; regenerate tables with `bench/report.py`.

- All three stacks returned correct answers for every query intent at 1 client. Victoria and LGTM were also correct at 4 clients.
- Victoria and LGTM answer in about 1-15 ms p50 (LGTM `log_lines` ~316 ms p50 on the single-client run, with a long tail). canardstack answers in ~310-400 ms p50 for every intent, regardless of result size.
- Ingest-to-visible: LGTM 0.6-1.1 s; Victoria logs 2.3 s, metrics 9.9 s, traces 22.8 s; canardstack logs 12.2 s, metrics 17.9 s, traces 8.1 s (writer commit timing).
- With 4 clients, canardstack instant metric queries queue behind a single heavy-admission slot (~4x latency); `sum_last` failed 27/100 on the 1 s admission wait. This is bounded-concurrency behavior on top of the per-query cost.
- Bugs found and fixed by this work: PromQL instant queries aggregated over the lookback window instead of the latest sample per series, range queries excluded `end`, and the fix's empty-result path. Regression tests are in `tests/integration.rs`.
- The ~310 ms per-query floor was the socat TLS proxy in front of the Quack catalog, not canardstack code. A DuckLake query makes ~8 small sequential catalog requests; without `nodelay` on both socat legs, Nagle plus delayed ACK added ~40 ms to each. Fixed in `compose.yaml` (and `bench/canardstack/compose.yaml`) with `nodelay`. Diagnosis: same native binary on plain loopback Quack 26 ms vs through the proxy 370 ms for an empty Loki query; after `nodelay`, the containerized stack went from 354 ms to 28 ms.
- Rerun after the fix (same workload): canardstack p50 33-44 ms at 1 client for every intent, 69-160 ms at 4 clients, 0 wrong and 0 errors (the 4-client `sum_last` failures disappeared). Visibility 6-7 s for all signals. Victoria and LGTM remain 15-40x faster on the small queries (0.8-6 ms).
- Open: the remaining ~30 ms is catalog round trips through the TLS hop (native binary on a plain relay: 13-20 ms; plain loopback: ~2 ms). A freshly created catalog with no data was much slower (200-360 ms per empty query) than the same stack after ingest; not explained. A deployment whose TLS terminator lacks TCP_NODELAY would see the same stall.
