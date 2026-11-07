# LogQL comparison against Loki

Expands oteldb's LogQL query templates (`queries.yml`, about 2,400 queries once the variants are
filled in), runs each against a real Loki (the reference) and against canardstack with the same
generated log lines, and compares the responses.

```bash
cargo build                                   # target/debug/canardstack (or set CANARDSTACK_BINARY)
cd bench/compat/logql
nix-shell -p go --run 'go test -run TestCompliance -timeout 30m -count=1 -json ./... > results.json'
```

The Loki binary (3.0.0, the version oteldb's suite targets; `LOKI_VERSION` overrides) is downloaded
into `bin/`. `DEBUG=1` streams the child processes, `KEEP_DIR=1` keeps their temp directories.

How the data gets in: the generator makes 480 JSON log lines (the shape oteldb's generator uses:
method, status, took, size, ips, ids) in three streams labelled `{filename, job="varlogs"}`. They go
to Loki through its JSON push API, and to canardstack as OTLP log exports (the stream labels as
resource attributes) committed to a local SQLite-catalog DuckLake with `reedlake ingest`.

Comparison follows oteldb's comparer: `service_name`, `detected_level` and `level` are stripped
from both sides (attribute discovery is implementation-defined), series are sorted by label set,
numbers are compared to 1e-5 relative, NaN equals NaN, and a query marked `should_be_empty` or
`should_fail` must behave that way on both. A query the reference rejects is skipped.

## Results

| | queries | pass |
| --- | --- | --- |
| first run, before the metric engine | 2,408 | 46 |
| with the metric engine | 2,408 | 2,403 |
| with `line_format` / `label_format` templates | 2,408 | 2,406 |

The 2 that do not pass: one `topk by (method)` where tied values are picked in a different order
than Loki picks them (the order of equal values is implementation-defined in both), and one
`absent_over_time` that the reference answers unexpectedly and the harness therefore skips.

Loki evaluates a range query on multiples of the step (the start rounds down, the end rounds up).
canardstack does the same for metric queries, so results do not depend on the second a run starts.
The harness runs Loki with `TZ=UTC` because `{{ __timestamp__ }}` prints the server's local zone and
canardstack prints UTC.
