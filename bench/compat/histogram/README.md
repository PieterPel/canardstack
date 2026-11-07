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

## Native histograms

The same script sends OTel exponential histograms (four series at scales 2, 3 and 4, positive and
negative buckets, zero observations, a counter reset) to a Prometheus started with
`--enable-feature=native-histograms`, which stores them as native histograms, and compares 56 more
queries: the sample itself with its buckets, `offset`, `rate`/`increase`/`delta`/`irate`/`idelta`,
`resets`, `changes`, the `*_over_time` functions, `absent_over_time`, every `histogram_*` function
(quantiles at 0, 0.1, 0.5, 0.99, 1 and out of range, fractions, count, sum, avg, stddev, stdvar),
`sum`/`avg`/`count`/`group` with `by` and `without`, and arithmetic and comparisons between
histograms and with scalars. Histogram samples are compared bucket by bucket.

Result: 164 of 166 comparisons match. The two others are `changes(...[10m])` on one series: Prometheus
compares the span layout its storage may pad, so it counts a change between two identical samples,
where canardstack (which compares the buckets) does not. They are listed as known differences.

Not covered: delta histograms, custom-bucket native histograms, summaries.

The upstream PromQL suite covers native histograms too (`../promql/README.md`).
