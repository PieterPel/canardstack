# Scaling benchmark: canardstack against LGTM, 1,000 to 200,000 records per service

`bench/sweep.py` runs the shared workload at several sizes with every query intent, and `bench/scale_report.py`
turns the results into the tables below (see `bench/README.md`). One laptop, single-node containers with 2 CPUs and
3 GiB each, one client, 15 timed runs per cell. **Run-to-run noise is about 50%: only differences of 2x or more
mean anything.** Identical datasets differ by that much between the two sweeps below.

- **Stacks:** canardstack with a Postgres DuckLake catalog (`canardstack-pg`) against Loki 3.7.9, Mimir 2.17.9 and
  Tempo 2.9.5 (`lgtm`). Tempo's live-trace limit is raised in `bench/lgtm/tempo.yaml`, as the Loki and Mimir limits
  already were: at the default it silently dropped 80% of the spans.
- **Sizes:** 5 services, each with N logs, N gauge points, N counter points and 2N spans. At 200,000 that is 1M log
  lines, 2M metric samples and 2M spans per stack.
- **Correctness first:** every cell is checked against a closed-form answer derived from the workload seed, validated
  against Loki, Mimir and Tempo. A wrong or failed answer is shown as `wrong` or `err`, never as a latency. In both tables
  every cell is correct.
- **Data density:** a dataset is packed into 25 minutes ending two minutes ago, so sample spacing shrinks as N grows
  (1 s at 1,000, 7.5 ms at 200,000). Spreading it over hours or days made Mimir, Loki and Tempo serve part of it from
  flushed blocks that take minutes to become queryable, which looked like data loss. The consequence: N is also sample
  *density*, and a 5-minute instant-query lookback covers more samples as N grows.
- **Cardinality:** `--users 1000` gives each log line an attribute with 1,000 distinct values. Loki stores it as structured
  metadata; canardstack makes every attribute part of a stream's identity. It changes only the logs.
- **Not measured:** concurrent clients, mixed ingest and query load, a multi-node deployment, cost, and anything beyond
  200,000 records per service. A plain `count_over_time` without a `sum` over high-cardinality attributes returns a series per
  stream, which is not among the intents.

## Without attributes

```
p50 in ms, clients 1, log attribute cardinality 0, records per service

query                    stack                1000    10000    50000   200000   slope  scales
---------------------------------------------------------------------------------------------
log_lines                canardstack-pg         18       12       19       19    0.03  flat
log_lines                lgtm                  9.1       27       80       10    0.13  flat

log_error_lines          canardstack-pg         16       21       19       18    0.02  flat
log_error_lines          lgtm                  5.7       17       27       15    0.21  sublinear

gauge_range_max          canardstack-pg         18       24       67      161    0.42  sublinear
gauge_range_max          lgtm                  3.3      5.9      8.7       19    0.31  sublinear

gauge_last               canardstack-pg         17       20       26       44    0.17  sublinear
gauge_last               lgtm                  2.6      4.2      6.6      7.3    0.21  sublinear

sum_last                 canardstack-pg         19       15       50       45    0.21  sublinear
sum_last                 lgtm                  2.4      6.9      5.0      6.1    0.15  sublinear

gauge_points             canardstack-pg         19       26       85      171    0.43  sublinear
gauge_points             lgtm                  2.6      9.0       12       18    0.35  sublinear

trace_by_id              canardstack-pg         13       20       37      102    0.38  sublinear
trace_by_id              lgtm                   11       88       41       98    0.36  sublinear

log_count_over_time      canardstack-pg         34       24       54       69    0.16  sublinear
log_count_over_time      lgtm                  5.5       19       60      155    0.63  LINEAR+

log_count_all            canardstack-pg         26       20       65       78    0.24  sublinear
log_count_all            lgtm                  7.4       60      458      922    0.95  LINEAR+

log_count_by_service     canardstack-pg         74       17       62       81    0.06  flat
log_count_by_service     lgtm                  8.6       70      400     2755    1.08  LINEAR+

log_regex_count          canardstack-pg         26       18       56      188    0.37  sublinear
log_regex_count          lgtm                  4.3       13       50      373    0.82  LINEAR+

log_newest               canardstack-pg         15       12       19       68    0.27  sublinear
log_newest               lgtm                  4.4       15       13       26    0.30  sublinear

metric_sum_over_time     canardstack-pg         16       15       37      146    0.40  sublinear
metric_sum_over_time     lgtm                  2.3      4.4      7.8       33    0.47  sublinear

metric_count_over_time   canardstack-pg         14       16       50      124    0.41  sublinear
metric_count_over_time   lgtm                  2.3      4.4      6.8       24    0.42  sublinear

metric_max_counter       canardstack-pg         19       17       40      144    0.37  sublinear
metric_max_counter       lgtm                  2.1      5.7      6.6       26    0.43  sublinear

metric_sum_all           canardstack-pg         21       23       43      122    0.32  sublinear
metric_sum_all           lgtm                  3.2      4.4      7.5       43    0.45  sublinear

trace_search             canardstack-pg         28       37      139      601    0.57  sublinear
trace_search             lgtm                  5.8      8.0       15       26    0.29  sublinear

canardstack-over-LGTM p50 ratio at 200000 records per service (above 1: canardstack slower)
  log_lines                canardstack-pg 1.8x
  log_error_lines          canardstack-pg 1.2x
  gauge_range_max          canardstack-pg 8.6x
  gauge_last               canardstack-pg 6.0x
  sum_last                 canardstack-pg 7.4x
  gauge_points             canardstack-pg 9.4x
  trace_by_id              canardstack-pg 1.0x
  log_count_over_time      canardstack-pg 0.4x
  log_count_all            canardstack-pg 0.1x
  log_count_by_service     canardstack-pg 0.0x
  log_regex_count          canardstack-pg 0.5x
  log_newest               canardstack-pg 2.7x
  metric_sum_over_time     canardstack-pg 4.5x
  metric_count_over_time   canardstack-pg 5.1x
  metric_max_counter       canardstack-pg 5.6x
  metric_sum_all           canardstack-pg 2.8x
  trace_search             canardstack-pg 23.4x
```

## With 1,000 distinct users

```
p50 in ms, clients 1, log attribute cardinality 1000, records per service

query                    stack               10000    50000   200000   slope  scales
------------------------------------------------------------------------------------
log_lines                canardstack-pg         21       21       30    0.12  flat
log_lines                lgtm                   35       35       36    0.01  flat

log_error_lines          canardstack-pg         12       32       17    0.14  flat
log_error_lines          lgtm                   13       19      552    1.22  LINEAR+

gauge_range_max          canardstack-pg         20       55      222    0.80  LINEAR+
gauge_range_max          lgtm                  4.7      5.8       11    0.28  sublinear

gauge_last               canardstack-pg         15       20       52    0.40  sublinear
gauge_last               lgtm                  2.2      3.6      4.1    0.21  sublinear

sum_last                 canardstack-pg         13       25       53    0.46  sublinear
sum_last                 lgtm                  2.5      5.0      3.4    0.11  flat

gauge_points             canardstack-pg         24      123      171    0.67  LINEAR+
gauge_points             lgtm                  3.0      6.1     10.0    0.40  sublinear

trace_by_id              canardstack-pg         16       43      112    0.64  LINEAR+
trace_by_id              lgtm                   67       47       59   -0.05  flat

log_count_over_time      canardstack-pg         22       42       77    0.42  sublinear
log_count_over_time      lgtm                  9.8       27      109    0.80  LINEAR+

log_count_all            canardstack-pg         20       39       83    0.48  sublinear
log_count_all            lgtm                   36      154      551    0.91  LINEAR+

log_count_by_service     canardstack-pg         19       49       88    0.51  sublinear
log_count_by_service     lgtm                   48      178      760    0.92  LINEAR+

log_regex_count          canardstack-pg         18       82      204    0.81  LINEAR+
log_regex_count          lgtm                  8.2       25       93    0.81  LINEAR+

log_newest               canardstack-pg         13       27       49    0.45  sublinear
log_newest               lgtm                   27       89       13   -0.22  flat

metric_sum_over_time     canardstack-pg         16       44      128    0.70  LINEAR+
metric_sum_over_time     lgtm                  3.1      7.2       12    0.45  sublinear

metric_count_over_time   canardstack-pg         14       38      140    0.77  LINEAR+
metric_count_over_time   lgtm                  3.3      6.5       11    0.41  sublinear

metric_max_counter       canardstack-pg         15       41      213    0.88  LINEAR+
metric_max_counter       lgtm                  2.6      5.6       11    0.48  sublinear

metric_sum_all           canardstack-pg         18       39      148    0.69  LINEAR+
metric_sum_all           lgtm                  2.3      4.6       12    0.54  sublinear

trace_search             canardstack-pg         39      133      522    0.86  LINEAR+
trace_search             lgtm                  5.6      8.2       11    0.24  sublinear

canardstack-over-LGTM p50 ratio at 200000 records per service (above 1: canardstack slower)
  log_lines                canardstack-pg 0.8x
  log_error_lines          canardstack-pg 0.0x
  gauge_range_max          canardstack-pg 20.2x
  gauge_last               canardstack-pg 12.7x
  sum_last                 canardstack-pg 15.7x
  gauge_points             canardstack-pg 17.2x
  trace_by_id              canardstack-pg 1.9x
  log_count_over_time      canardstack-pg 0.7x
  log_count_all            canardstack-pg 0.2x
  log_count_by_service     canardstack-pg 0.1x
  log_regex_count          canardstack-pg 2.2x
  log_newest               canardstack-pg 3.8x
  metric_sum_over_time     canardstack-pg 10.8x
  metric_count_over_time   canardstack-pg 12.4x
  metric_max_counter       canardstack-pg 19.6x
  metric_sum_all           canardstack-pg 12.4x
  trace_search             canardstack-pg 45.8x
```

## What changed while building this

The first sweep (before the changes below, commit `93380d3`) found these at 200,000 records, in milliseconds:

| Query | First sweep | Now | LGTM |
|---|---|---|---|
| `trace_search` | error (span cap) | 601 | 26 |
| `gauge_range_max` | 374 | 161 | 19 |
| `gauge_points` | 261 | 171 | 18 |
| `gauge_last` | 174 | 44 | 7.3 |
| `sum_last` | 88 | 45 | 6.1 |
| `metric_sum_over_time` / `_count_over_time` | 235 / 224 | 146 / 124 | 33 / 24 |
| `metric_max_counter` / `metric_sum_all` | 256 / 292 | 144 / 122 | 26 / 43 |
| `log_error_lines` | 161 | 18 | 15 |

- `trace_search` pages through candidate traces newest first (a `GROUP BY ... LIMIT` over three columns, then the spans of
  only those traces) instead of loading every trace of the window. The 300,000-span cap is now `CANARDSTACK_QUERY_MAX_SPANS`
  and bounds a page, not the window.
- An instant selector keeps only the newest sample per series and step in the database, instead of reading every sample of
  the lookback window into Rust.
- Earlier changes (LogQL counting and limits in SQL, fractional epoch times rounded to the millisecond) are in
  `docs/architecture/query-api.md` and the commit history.

## What it shows

- **Aggregations over logs are where canardstack is ahead**, with and without attributes: `log_count_all` and
  `log_count_by_service` take 78 and 81 ms at 200,000 records against 922 ms and 2,755 ms for Loki, whose time grows in
  proportion to the data (slope 0.95 and 1.08 against 0.24 and 0.06). A log attribute with 1,000 values did not change that
  (83 and 88 ms), because under a `sum` the streams are reduced to the labels the query uses. `log_error_lines` is flat
  for canardstack and 552 ms for Loki with attributes present.
- **Metric lookups are still 3 to 9 times slower, and `trace_search` 23 to 46 times.** `gauge_last`, `sum_last`, the points
  and `*_over_time` queries take 15 to 25 ms at the smallest size against 2 to 3 ms (a fixed cost of catalog round trips) and
  44 to 171 ms at 200,000 against 6 to 33 ms. `trace_search` takes 601 ms against 26 ms, and its slope is 0.57 to 0.86.
- **Nothing scales worse than linearly**, and most slopes are 0.2 to 0.5. The widest gaps are where canardstack scans rows
  that Mimir and Tempo find through an index.

## Next

1. `*_over_time` and the other range functions over a matrix selector: aggregate in SQL as the LogQL counts do, instead of
   loading every sample of the window.
2. `trace_search`: the candidate query still aggregates every span of the window; order the scan by time and stop early.
3. A plain `count_over_time` over high-cardinality attributes (a series per stream), and concurrent clients.
