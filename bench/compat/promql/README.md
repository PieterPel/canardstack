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
