# Serverless writer experiments

canardstack only queries. The writer that fills the DuckLake catalog is a separate component, and
for the stack to be serverless it must be serverless too. These scripts test whether that is
feasible with parts that exist today, and what it does to the catalog.

## The idea

duckdb-otlp ships table functions (`read_otlp_logs`, `read_otlp_traces`, `read_otlp_metrics_gauge`,
`read_otlp_metrics_sum`, ...) that read OTLP JSON, NDJSON or protobuf into flat tables. The `otlp`
extension is a community extension that installs into plain DuckDB. So a writer needs no server: one
invocation opens DuckDB, attaches the catalog, runs `INSERT ... SELECT ... FROM read_otlp_*(file)`
in one transaction, and exits. `writer.py` is exactly that.

## Run it

```bash
uv run --project bench bench/serverless_writer/experiment.py            # 40 cold invocations, two modes, ~1 minute
uv run --project bench bench/serverless_writer/bake_extensions.py ./ext  # bake extensions and prove they load offline
uv run --project bench bench/serverless_writer/test_eventhub_ingest.py   # JSON and protobuf events, redelivery, rollback
```

The `serverless` dependency group in `bench/pyproject.toml` pins `duckdb==1.5.6`, which is installed
by default; it must match the DuckDB the stack uses because extension binaries are version-specific.

## What it showed (local SQLite catalog, extensions cached, one laptop)

40 cold invocations, each committing 2,500 rows (500 logs, 1,000 spans, 500 gauge, 500 sum points):

| state | files | snapshots | inlined rows | one-batch query | newest 1,000 rows |
| --- | --- | --- | --- | --- | --- |
| default inlining limit (10 rows) | 160 | 41 | 0 | 7.1 ms | 7.3 ms |
| after merge + expire + cleanup (0.16 s) | 4 | 1 | 0 | 6.0 ms | 8.1 ms |
| inlining on (limit 5,000) | 0 | 41 | 100,000 | 13.0 ms | 14.4 ms |
| after `ducklake_flush_inlined_data` (0.28 s) | 4 | 42 | 0 | 5.8 ms | 8.2 ms |

| mode | cold invocation | load extensions | insert + commit |
| --- | --- | --- | --- |
| default | 0.22 s | 0.07 s | 0.05 s |
| inlining on | 0.47 s | 0.07 s | 0.30 s |

- A one-shot writer is cheap, and data is queryable the moment the transaction commits (no
  group-commit delay like the always-on server has).
- Every invocation is one snapshot, and with the default inlining limit of 10 rows every real batch
  becomes its own Parquet file: 40 invocations leave 160 files. DuckLake does not compact on its own;
  `CHECKPOINT`, or `ducklake_merge_adjacent_files` + `ducklake_expire_snapshots` +
  `ducklake_cleanup_old_files`, must be run by something. That something can be a timer-triggered
  function. Maintenance took well under a second here.
- Inlining (`CALL lake.set_option('data_inlining_row_limit', N)`, persisted in the catalog) keeps
  small writes out of files until a flush. It makes inserts slower (0.30 s against 0.05 s here) and
  reads of inlined rows more expensive. With a remote catalog the inlined rows travel over the network
  on every query (earlier measurements showed whole-column reads with no filter pushdown), so keep the
  limit small and flush often.
- **Correction:** an earlier run of an unconsolidated script reported a 50 ms one-batch query with 160
  small files. That did not reproduce (7 ms here, for every service tried; the first query on a
  connection costs 20-28 ms), so do not rely on it. At this local scale small files cost almost nothing
  in query time. The file and snapshot counts are solid; the latency penalty, if any, belongs to object
  storage, where each small file is its own request. That has not been measured.

## Baking the extensions into a function

`INSTALL` downloads an extension into `<extension_directory>/<duckdb version>/<platform>/`, and
`LOAD` reads it from there. A function must not download at cold start, so install at build time and
ship the directory.

```bash
python bake_extensions.py ./ext      # installs otlp, ducklake, sqlite, postgres; then verifies
```

The verification runs a child process with an empty `HOME` and auto-install off, so `ok` means nothing
was downloaded. At run time:

```python
con.execute("SET extension_directory='/home/site/wwwroot/ext'")
con.execute("SET autoinstall_known_extensions=false")
con.execute("SET autoload_known_extensions=false")
con.execute("LOAD otlp"); con.execute("LOAD ducklake"); con.execute("LOAD postgres")
```

- Extensions are specific to the DuckDB version and the platform. Run the bake on the platform the
  function runs on (a linux container in CI for Azure Functions on Linux), not on a laptop, and keep
  the DuckDB version identical between build and run time. Four extensions are 112 MB on macOS; drop
  the ones you do not use (a Postgres catalog does not need `sqlite`).
- **Rust / canardstack.** canardstack bundles DuckDB core in its binary; extensions are still
  separate files. It honours `CANARDSTACK_DUCKDB_EXTENSION_DIR` and runs `LOAD` first, falling back to
  `INSTALL` only if `LOAD` fails, so a pre-populated directory works with no network. The shipped
  Dockerfile does not populate one yet, so a cold container downloads `ducklake`, `postgres` and
  `quack` on first attach. The fix is a build stage that bakes into `/opt/duckdb/extensions` with the
  same DuckDB version as the `duckdb` crate (1.5.6 for crate 1.10506.x) and sets the environment
  variable. That Dockerfile change is not made or tested here.
- A Rust writer function would work the same way: the extension is a file loaded at run time with
  `LOAD`, not something linked in.

## Event Hubs as the source

Yes, and it fits well. The OpenTelemetry Collector's `azureeventhub` exporter writes OTLP (JSON or
protobuf) to an event hub, and an Event Hubs trigger gives the function batches, partition-based
scale-out, checkpointing and replay. One batch becomes one DuckLake transaction, which also controls
the snapshot rate.

```text
SDKs / Collector --azureeventhub exporter--> Event Hub (one hub per signal)
                                               | Event Hubs trigger (cardinality: many)
                                               v
                          function: ingest_batch(...) -> DuckLake (catalog + object storage)
        timer trigger: CHECKPOINT lake   (merge, expire, cleanup; also flush if inlining is on)
```

`eventhub_ingest.py` is the testable core. Event Hubs delivers at least once, so the batch's
(partition, signal, last sequence number) is written to `ingest_offsets` in the same transaction as the
rows, and a redelivered batch is skipped. The test shows that a redelivery writes nothing, that offsets
are per partition, that a bad batch rolls back rows and offsets together, and that both JSON (as
NDJSON) and protobuf (as `.pb` files) events ingest.

An untested sketch of the Azure binding (Python v2 model; check how the partition id is exposed in
your trigger version):

```python
@app.event_hub_message_trigger(arg_name="events", event_hub_name="otlp-logs",
                               connection="EVENTHUB", cardinality="many")
def ingest_logs(events: list[func.EventHubEvent]):
    bodies = [e.get_body() for e in events]
    ingest_batch(connection(), "logs", bodies, partition=PARTITION_ID,
                 first_seq=events[0].sequence_number, last_seq=events[-1].sequence_number)
```

Things to settle before relying on it:

- Event size limits (about 1 MB on Standard) cap the Collector's batch size.
- Parallelism is bounded by the partition count, and each invocation is a catalog commit, so tune
  `maxEventBatchSize` and `prefetchCount` in `host.json` to trade latency against snapshot churn.
- Metrics need both a gauge and a sum reader over the same events; `READERS` in `eventhub_ingest.py`
  already does that.
- Event Hubs Capture to Blob is an alternative source, but it writes Avro, so the OTLP bodies would
  need unwrapping before `read_otlp_*` can read them.

## Not measured

Everything above used a local SQLite catalog on one laptop. Not measured: a Postgres catalog over a
network, object-storage latency, real Azure Functions cold starts, Event Hubs throughput, concurrent
writers on one catalog, and behaviour at volume.
