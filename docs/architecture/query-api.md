# Compatibility Query API

Canardstack v0 exposes bounded compatibility adapters for existing
observability clients. It does not define a Canardstack-specific product query
protocol and does not expose arbitrary SQL through the normal HTTP API.

All telemetry query paths use the internal query engine protections:

- Required or server-bounded time ranges.
- Result limits.
- Server-owned timeouts.
- DuckDB memory limits.
- Query concurrency guards.

Telemetry writes happen outside canardstack, typically through
[`duckdb-otlp`](https://github.com/smithclay/duckdb-otlp). Query results reflect
whatever rows are visible in the attached DuckLake catalog.

## Discovery Metadata Spine

Prometheus label values, Prometheus series, Prometheus metric metadata, Loki
label values, Loki series, and Tempo tag values prefer the shared
`metadata_summary` table when the writer side maintains it. Prometheus metric
label and series discovery can fall back to bounded raw `otlp_metrics_*` table
scans so query-only deployments backed by external OTLP writers still expose
usable metric discovery. The query server reads canonical OTAP-style columns
directly where available and derives compatibility labels such as
`deployment_environment`, `http_route`, and `http_method` from canonical JSON
attribute columns.

These reads still run through the bounded `QueryEngine` path with normal
concurrency, timeout, memory, range, and result-limit controls. An in-process
cache stores only bounded discovery responses, keyed by API family, metadata
kind, label/tag name, and exact normalized time range.

## Prometheus Metrics Subset

Endpoints:

- `GET/POST /api/v1/query`
- `GET/POST /api/v1/query_range`
- `GET /api/v1/labels`
- `GET /api/v1/label/{name}/values`
- `GET /api/v1/series`
- `GET /api/v1/metadata`

Responses use Prometheus-style envelopes:

```json
{"status":"success","data":{"resultType":"matrix","result":[]}}
```

Errors use:

```json
{"status":"error","errorType":"unsupported_promql","error":"..."}
```

Supported PromQL subset:

- A bare metric name such as `smoke.gauge`.
- A metric selector such as `smoke.gauge{service_name="checkout"}`.
- Equality filters over supported labels such as `service_name` and
  `deployment_environment`.
- `avg`, `min`, `max`, `sum`, `count`, and `rate` around a single selector.
- `avg`, `min`, `max`, `sum`, and `count` with explicit `by(...)` or
  `without(...)` grouping over supported labels such as `service_name` and
  `deployment_environment`.

Instant queries (`/api/v1/query`) take the latest sample per series inside a 5-minute
lookback ending at `time`, then aggregate across series, so `max()` is the maximum of the
latest values and `count()` is the number of series. Range queries (`/api/v1/query_range`)
include a sample exactly at `end`.

Not implemented: full PromQL expression evaluation, joins, binary operators,
subqueries, histograms, exemplars, rule evaluation, staleness semantics, and
remote read/write.

## Loki Logs Subset

Endpoints:

- `GET /loki/api/v1/query_range`
- `GET /loki/api/v1/query`
- `GET /loki/api/v1/labels`
- `GET /loki/api/v1/label/{name}/values`
- `GET /loki/api/v1/series`

Responses use Loki-style stream results:

```json
{
  "status": "success",
  "data": {
    "resultType": "streams",
    "result": [{"stream": {"service_name": "checkout"}, "values": []}]
  }
}
```

Supported LogQL (see `src/query/logql/` and `crates/logql-syntax/`):

- Stream selectors with equality, inequality and regex matchers. Only `service_name` equality narrows
  the database scan; other matchers are applied to the labels of each stream after the rows are read.
- `start`, `end`, `limit`, and `direction`.
- Line filters (`|=`, `!=`, `|~`, `!~`), parsers and label stages (`json`, `line_format`, label filters
  and so on), and `unwrap`.
- Metric queries: range aggregations (`count_over_time`, `rate`, `bytes_over_time`, `bytes_rate`,
  `sum_over_time`, `quantile_over_time`, `absent_over_time` and the rest of the families the parser
  accepts), vector aggregations (`sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `topk`,
  `bottomk`, `sort`, `sort_desc`), binary operators and `label_replace`.

Not implemented: recording rules, and LogQL features the parser rejects.

### How a LogQL query is evaluated, and what bounds it

Every query is bounded by the interactive query timeout and memory limit. On top of that, how much of the
log table a query reads depends on its shape:

| Shape | How it is evaluated | Bound |
| --- | --- | --- |
| `count_over_time`, `rate`, `bytes_over_time`, `bytes_rate` over a selector with no stage except plain `\|=` / `!=` text filters | Counted inside DuckDB: `GROUP BY` stream and time bucket. No line is read into memory. | Streams times buckets, at most `CANARDSTACK_QUERY_MAX_LOG_LINES` (default 1,000,000). Not limited by the number of lines. |
| A log query (not a metric) | Reads the newest (or, with `direction=forward`, the oldest) rows in the database with `ORDER BY ... LIMIT`, a few times `limit` at first, and more only when a stage dropped too many to fill the limit. | At most `CANARDSTACK_QUERY_MAX_LOG_LINES` rows read; a query that cannot find `limit` matching lines within that is refused. |
| Any other metric query (regex filters, parsers, `unwrap`, other range functions) | Every matching line is read into memory and evaluated per line. | More than `CANARDSTACK_QUERY_MAX_LOG_LINES` matching lines is refused with HTTP 422 and "narrow the selector or the time range". |

Tests: `loki_counted_range_aggregations_match_reading_the_lines` runs the counted shapes and the
line-reading path on the same data and requires equal answers;
`loki_line_cap_bounds_reading_but_not_counting_or_limits` runs with a cap smaller than the data.
Before this was written down, the cap was an undocumented constant of 1,000,000 that no test reached.

## Tempo Traces Subset

Endpoints:

- `GET /api/v2/traces/{traceID}`
- `GET /api/traces/{traceID}`
- `GET /api/search`
- `GET /api/search/tags`
- `GET /api/search/tag/{tag}/values`
- `GET /api/v2/search/tags`
- `GET /api/v2/search/tag/{tag}/values`

Grafana probe shims:

- `GET /api/status/buildinfo`

Supported search filters map to canonical span/resource columns or derived
attribute labels:

- `service.name` or `service_name`.
- Span `name` or `span_name`.
- `http.route`.
- `status.code` or `status_code`.
- `traceID` or `trace_id`.

Trace lookup returns a Tempo-compatible JSON shape containing batches and spans.
Search returns trace summaries. TraceQL is not implemented.

## Non-API Surfaces

The remaining HTTP endpoints are operational endpoints:

- `GET /`
- `GET /metrics`
- `GET /api/admin/health/storage`
- `GET /api/admin/health/queries`

Direct DuckDB/DuckLake SQL access is available outside Canardstack through
DuckDB CLI, MotherDuck, or SQL clients. That path is intentionally separate from
the normal HTTP product surface.

## Prometheus labels and metric names

Series follow the OpenTelemetry-to-Prometheus convention, computed from the stored rows:

- Labels: `job` (`service.namespace/service.name`, or the name alone), `instance`
  (`service.instance.id`), every metric attribute with its name escaped to `[a-zA-Z0-9_]`, and
  `otel_scope_name` / `otel_scope_version` when the scope has them. Other resource attributes are
  not labels.
- Names: characters outside `[a-zA-Z0-9_:]` become `_`; a known unit adds a suffix (`s` ->
  `_seconds`, `By` -> `_bytes`, `By/s` -> `_bytes_per_second`); a gauge with unit `1` gets
  `_ratio`; a monotonic sum gets `_total`. `CANARDSTACK_PROMETHEUS_NAME_SUFFIXES=false` keeps only
  the escaping.

Label names, label values, series and metadata are read from the raw metric tables in the requested
window (capped), not from `metadata_summary`.
