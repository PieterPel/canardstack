# TraceQL comparison against Tempo

Runs TraceQL search queries against a real Tempo (the reference) and against canardstack with the
same generated traces, and compares the responses. The queries are `queries.yml` (written for the
generated data) plus every valid query in Tempo's own `test_examples.yaml`, minus metrics queries
(`rate()`, `*_over_time`, `compare()`, `topk`/`bottomk`), which are not evaluated yet.

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

Per query, a trace is compared on: the root service and name, the duration in ms and the set of
spans in its spansets. Order is not compared. A query the reference rejects is skipped.

## Results

| | queries | pass |
| --- | --- | --- |
| spanset filters, pipelines, aggregates, structural operators | 262 | 256 |

The 6 that do not pass are all behaviour of Tempo's storage layer, not of TraceQL, which is why
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
