# OTel histograms against Prometheus

OpenTelemetry explicit-bucket histograms are stored in `otlp_metrics_histogram` and read as the
classic Prometheus series: `<name>_bucket{le}` (cumulative, with a `+Inf` bucket), `<name>_sum` and
`<name>_count`, with the name translated like Prometheus' OTLP receiver does (`http.server.duration`
in seconds becomes `http_server_duration_seconds`).

`compare.py` checks that against the real thing. It generates cumulative histograms (two services,
three routes, 60 scrapes 15 s apart, one counter reset) and sends them to a Prometheus through its
OTLP receiver and to a DuckLake through the benchmark writer (`duckdb-otlp`'s
`read_otlp_metrics_histogram`), which a native canardstack serves. 26 queries then run as instant and
range queries on both and the answers are compared (values to 1e-6 relative): selectors on each series
kind and on `le`, `rate`/`increase` over buckets, sums and ratios, `histogram_quantile` over plain
and rated buckets, grouped by `le`, `job` and route, and including a counter reset.

```bash
cargo build --release
uv run bench/compat/histogram/compare.py
```

`PROMETHEUS_BINARY` and `CANARDSTACK_BINARY` override the binaries (the Prometheus one is the
download of the PromQL suite in `../promql/bin`). A query that returns nothing on both sides fails,
so a missing dataset cannot pass.

Result: 52 of 52 comparisons match.

Not covered: delta histograms, exponential histograms (Prometheus native histograms), summaries.
