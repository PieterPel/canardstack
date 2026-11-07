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
timestamps) and commits each scrape through `read_otlp_metrics_*` into a local SQLite-catalog
DuckLake, the way a Collector's prometheus receiver would feed duckdb-otlp. A native canardstack
serves that catalog, behind a small reverse proxy that adds the API key the suite's client lacks.
`DEBUG=1` streams the child processes; `KEEP_DIR=1` keeps the catalog for inspection.

Native histograms cannot go through a text scrape and canardstack stores no histograms, so those
queries (about 110) are expected failures for now.

## Result

| | queries | pass |
| --- | --- | --- |
| everything | 661 | 546 (83%) |
| excluding native histograms | 548 | 546 (99.6%) |

The 113 native-histogram queries fail because the text scrape cannot carry them and canardstack
stores no histograms (they would map to OTel exponential histograms). The other two failures are
`label_replace`/`label_join` called with a destination label `"\xff"`: Prometheus rejects it as
invalid UTF-8, but the parser hands us the string `ÿ`, which Rust accepts.

Setup notes that matter when reading results: the reference gets a 2h out-of-order window (the
replay is 70 minutes of history delivered in seconds, and a default Prometheus drops the oldest
snapshots as "too old", leaving it with less data than the target); the target's queries are held
until the bridge has committed the whole replay; and the target runs with raised query concurrency
because the suite fires several hundred queries at once.
