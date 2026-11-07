# TraceQL comparison against Tempo

Runs TraceQL search queries against a real Tempo (the reference) and against canardstack with the
same generated traces, and compares the responses. The queries are `queries.yml` (written for the
generated data) plus every valid query in Tempo's own `test_examples.yaml`. Search queries go to
`/api/search`, metrics queries (`rate()`, `*_over_time`, `topk`/`bottomk`, filters, arithmetic) to
`/api/metrics/query_range` (`compare()` included); queries with a sampling hint are left out because they are random.

```bash
cargo build                                   # target/debug/canardstack (or set CANARDSTACK_BINARY)
cd bench/compat/traceql
nix-shell -p go --run 'go test -run TestCompliance -timeout 30m -count=1 -v .'
```

The Tempo binary (3.1.1; `TEMPO_VERSION` overrides) is downloaded into `bin/`. `DEBUG=1` streams the
child processes, `KEEP_DIR=1` keeps their temp directories.

How the data gets in: the generator makes 60 deterministic checkout traces (frontend, checkout and
payment services; typed span attributes; errors, events, links) one second apart. They go to Tempo
as OTLP/HTTP, and to canardstack as OTLP trace exports committed to a local SQLite-catalog DuckLake.
Tempo 3 only serves recent traces and reports ready about 15 seconds before its live-store partition
turns active, so the harness waits before pushing and generates fresh timestamps.

Per search query, a trace is compared on: the root service and name, the duration in ms and the
set of spans in its spansets. Per metrics query, the series (by labels) and their value at every
step, to 1e-6 relative, with a missing sample counting as 0 and exemplars ignored. Order is not
compared. A query the reference rejects is skipped. `DUMP=file` appends the reference responses to
`file` and canardstack's to `file.got`.

## Results

| | queries | pass |
| --- | --- | --- |
| search: spanset filters, pipelines, aggregates, structural operators | 266 | 256 |
| metrics: `rate`, `count/min/max/avg/sum/quantile/histogram_over_time`, `by()`, `topk`, `bottomk`, filters, arithmetic, `compare()` | 119 | 116 |

The 9 search queries that do not pass (one more is rejected by Tempo and skipped) are all behaviour of Tempo's storage layer, not of TraceQL, which is why
canardstack does not copy them (the query with `childCount` is rejected by Tempo and counted as a
skip, not a pass):

- `{ .http.method = nil }`: Tempo returns the traces whose only spans have `http.method`; the answer
  is every trace. `http.method`, `http.status_code` and a few more are dedicated columns in Tempo's
  block format, and the other differences below involve those columns.
- `{ !(.http.method = "GET") }`, `{ !(.http.status_code = 200) }`: Tempo returns nothing; a negated
  comparison only sees the spans that matched the comparison.
- `{ } | max(.http.status_code) ...` (4 queries): Tempo drops about two thirds of the traces.
- `{ .amount < 100 }`, `{ .order.items > 3.5 }`: Tempo does not compare an integer with a float
  attribute, while its expression engine does (`{ 1 <= 1.1 }` is true). canardstack compares numbers
  of any type.

## Metrics differences from Tempo

- Tempo's frontend cuts the end of a metrics range back to 30 seconds before now
  (`query_end_cutoff`). canardstack evaluates the whole range, so the harness waits until its window
  is older than that.
- Like Tempo, the first step is labelled with the aligned start (the range gets one extra leading
  step), steps cover `(t - step, t]`, and `quantile_over_time` interpolates over power-of-two
  buckets.
- Sampling hints (`with(sample=...)`) are not evaluated.
- `compare()` counts each span once. The comparer drops what comes from Tempo's storage format:
  attributes in its dedicated columns get a series for the spans that lack them (value `nil`) and
  totals over every span, so those series and totals are ignored on both sides, and when several
  values are equally frequent Tempo's pick for the top N is arbitrary, so equal counts compare equal.
  Three `compare()` queries still differ, for two reasons in Tempo: an attribute that also appears in
  the query's filter is counted two or more times (`{ name = "payment.charge" } | compare(...)` has
  every `name` count doubled while its totals are right), and the `nil` series takes a place in the
  top N (canardstack has no `nil` series, so one more real value fits).
