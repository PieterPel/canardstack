# Upstream PromQL compliance suite

Runs [prometheus/compliance](https://github.com/prometheus/compliance)'s PromQL suite (about 660
queries over a deterministic 70-minute dataset) with canardstack as the target and a Prometheus
3.x binary as the reference, and diffs every result.

```bash
cargo build                                   # target/debug/canardstack (or set CANARDSTACK_BINARY)
cd bench/compat/promql
nix-shell -p go --run 'go test -run TestCompliance -timeout 30m -json ./... > results.json'
```

How canardstack takes part: `bridge.py` scrapes the suite's replayer (OpenMetrics text with explicit
timestamps) and commits each scrape with `reedlake ingest` into a local SQLite-catalog
DuckLake, the way a Collector's prometheus receiver would feed it. A native canardstack
serves that catalog, behind a small reverse proxy that adds the API key the suite's client lacks.
`DEBUG=1` streams the child processes; `KEEP_DIR=1` keeps the catalog for inspection.

The bridge reads the replayer's protobuf exposition (the text format cannot carry native
histograms) and stores what a Collector's prometheus receiver would: counters as monotonic sums,
gauges as gauges, classic histograms as OTLP explicit-bucket histograms and native histograms as
OTLP exponential histograms, so the suite exercises the histogram code as well.

## Result

| | queries | pass |
| --- | --- | --- |
| everything | 661 | 661 (100%) |

Two of those queries call `label_replace`/`label_join` with a destination label `"\xff"`, which
Prometheus rejects as invalid UTF-8. The parser used here hands over `ÿ`, so canardstack rejects a
byte escape of 0x80 or more in those functions by looking at the query text.
Before histograms were stored, 546 passed: the 113 native-histogram queries could not be answered.

Setup notes that matter when reading results: the reference gets a 2h out-of-order window (the
replay is 70 minutes of history delivered in seconds, and a default Prometheus drops the oldest
snapshots as "too old", leaving it with less data than the target); the target's queries are held
until the bridge has committed the whole replay; and the target runs with raised query concurrency
because the suite fires several hundred queries at once.
