# Scaling benchmark: canardstack against LGTM at 1,000 to 200,000 records per service

`bench/sweep.py` runs the shared workload at several sizes with every query intent, and
`bench/scale_report.py` turns the results into the table below (see `bench/README.md`). This is the first run:
one laptop, single-node containers with 2 CPUs and 3 GiB each, one client, 15 timed runs per cell. Treat a
difference under about 2x as noise.

- **Stacks:** canardstack with a Postgres DuckLake catalog (`canardstack-pg`) and Loki 3.7.9, Mimir 2.17.9 and
  Tempo 2.9.5 (`lgtm`). Tempo's live-trace limit is raised in `bench/lgtm/tempo.yaml`, as the Loki and Mimir limits
  already were: at the default it silently dropped 80% of the spans.
- **Sizes:** 5 services, each with N logs, N gauge points, N counter points and 2N spans. At 200,000 that is 1M log
  lines, 2M metric samples and 2M spans per stack.
- **Correctness first:** every cell is checked against a closed-form answer derived from the workload seed, and
  the answers were validated against Loki, Mimir and Tempo. A wrong or failed answer is shown as `wrong` or `err`,
  never as a latency. All cells below except one are correct.
- **Data density:** a dataset is packed into 25 minutes ending two minutes ago, so sample spacing shrinks as N grows
  (1 s at 1,000, 7.5 ms at 200,000). Spreading it over hours or days instead made Mimir, Loki and Tempo serve part of
  it from flushed blocks that take minutes to become queryable, which looked like data loss. The consequence: N here is
  also sample *density*, and a "last value" lookback of 5 minutes covers more samples as N grows.
- **Not measured:** log attribute cardinality (`--users`, not run yet), concurrent clients, mixed ingest and query
  load, a multi-node deployment, cost, and anything beyond 200,000 records per service.

## Result

```
p50 in ms, clients 1, log attribute cardinality 0, records per service

query                    stack                1000    10000    50000   200000   slope  scales
---------------------------------------------------------------------------------------------
log_lines                canardstack-pg         18       24       38       36    0.15  flat
log_lines                lgtm                  9.1       27       80       10    0.13  flat

log_error_lines          canardstack-pg         16       29       34      161    0.39  sublinear
log_error_lines          lgtm                  5.7       17       27       15    0.21  sublinear

gauge_range_max          canardstack-pg         18       43       79      374    0.54  sublinear
gauge_range_max          lgtm                  3.3      5.9      8.7       19    0.31  sublinear

gauge_last               canardstack-pg         17       32       44      174    0.40  sublinear
gauge_last               lgtm                  2.6      4.2      6.6      7.3    0.21  sublinear

sum_last                 canardstack-pg         19       26       44       88    0.28  sublinear
sum_last                 lgtm                  2.4      6.9      5.0      6.1    0.15  sublinear

gauge_points             canardstack-pg         19       41       94      261    0.49  sublinear
gauge_points             lgtm                  2.6      9.0       12       18    0.35  sublinear

trace_by_id              canardstack-pg         13       32       68      179    0.49  sublinear
trace_by_id              lgtm                   11       88       41       98    0.36  sublinear

log_count_over_time      canardstack-pg         34       50       74      108    0.22  sublinear
log_count_over_time      lgtm                  5.5       19       60      155    0.63  LINEAR+

log_count_all            canardstack-pg         26       44       71      185    0.36  sublinear
log_count_all            lgtm                  7.4       60      458      922    0.95  LINEAR+

log_count_by_service     canardstack-pg         74       37       66      208    0.18  sublinear
log_count_by_service     lgtm                  8.6       70      400     2755    1.08  LINEAR+

log_regex_count          canardstack-pg         26       26       91      290    0.46  sublinear
log_regex_count          lgtm                  4.3       13       50      373    0.82  LINEAR+

log_newest               canardstack-pg         15       18       41       85    0.34  sublinear
log_newest               lgtm                  4.4       15       13       26    0.30  sublinear

metric_sum_over_time     canardstack-pg         16       33       90      235    0.50  sublinear
metric_sum_over_time     lgtm                  2.3      4.4      7.8       33    0.47  sublinear

metric_count_over_time   canardstack-pg         14       31       87      224    0.52  sublinear
metric_count_over_time   lgtm                  2.3      4.4      6.8       24    0.42  sublinear

metric_max_counter       canardstack-pg         19       52      119      256    0.49  sublinear
metric_max_counter       lgtm                  2.1      5.7      6.6       26    0.43  sublinear

metric_sum_all           canardstack-pg         21       45      117      292    0.50  sublinear
metric_sum_all           lgtm                  3.2      4.4      7.5       43    0.45  sublinear

trace_search             canardstack-pg         28      197      800      err    0.86  LINEAR+
trace_search             lgtm                  5.8      8.0       15       26    0.29  sublinear

canardstack-over-LGTM p50 ratio at 200000 records per service (above 1: canardstack slower)
  log_lines                canardstack-pg 3.5x
  log_error_lines          canardstack-pg 10.7x
  gauge_range_max          canardstack-pg 20.0x
  gauge_last               canardstack-pg 23.6x
  sum_last                 canardstack-pg 14.5x
  gauge_points             canardstack-pg 14.4x
  trace_by_id              canardstack-pg 1.8x
  log_count_over_time      canardstack-pg 0.7x
  log_count_all            canardstack-pg 0.2x
  log_count_by_service     canardstack-pg 0.1x
  log_regex_count          canardstack-pg 0.8x
  log_newest               canardstack-pg 3.3x
  metric_sum_over_time     canardstack-pg 7.2x
  metric_count_over_time   canardstack-pg 9.2x
  metric_max_counter       canardstack-pg 10.0x
  metric_sum_all           canardstack-pg 6.7x
  trace_search             canardstack-pg n/a

* partial, err and wrong cells (first failure):
  canardstack-pg trace_search at 200000: HTTP 422: {"error":"query reads more than 300000 spans; narrow the time range","errorType":"execution","status":"error"}
```

## What it shows

- **Aggregations are where canardstack is now ahead.** `log_count_all` and `log_count_by_service` take 185 and 208
  ms at 200,000 records against 922 ms and 2,755 ms for Loki, whose time grows in proportion to the data (slope 0.95
  and 1.08, against 0.36 and 0.18). They run in DuckDB since the LogQL change in this repository counts without loading lines.
  Loki's per-service count and regex filter also grow roughly linearly.
- **Lookups are slower and the gap widens.** `gauge_last`, `gauge_range_max`, `sum_last`, the points and `*_over_time`
  metric queries take 17 to 21 ms at 1,000 records against 2 to 3 ms for LGTM, and 88 to 374 ms at 200,000 against 6 to
  43 ms (7 to 24 times). The slopes are 0.3 to 0.5 against 0.15 to 0.45. Mimir finds the newest sample through an index;
  canardstack scans the rows of the lookback window, so with this dense data it reads far more than with a 15-second scrape.
- **`trace_search` is the worst case, and it fails.** 28, 197 and 800 ms at 1,000, 10,000 and 50,000 records (slope 0.86), and at
  200,000 it returns HTTP 422, "query reads more than 300000 spans; narrow the time range". Tempo answers in 6 to 26 ms.
  The search loads every span of every matching trace for the whole window; the 300,000 is a hardcoded constant, the same
  kind of cap the LogQL path had.
- **The fixed per-query cost** (about 15 ms against 2 to 3 ms) is visible at every size. It is catalog round trips, not data.

## Bugs this found

- canardstack rounded a fractional epoch `time` down instead of to the millisecond, so an instant query at a sample's own
  timestamp returned the previous sample (fixed: `fix(compat): round a fractional epoch time to the millisecond`).
- The harness itself had several flaws that looked like backend failures: back-dated data hitting each stack's block-flush
  delay, services sent out of step (Mimir rejects what is too old against its head), Tempo's default live-trace limit,
  the Podman VM's clock falling hours behind when the Mac sleeps (now checked at the start and end of every run), and
  an expectation that ignored a query's own limit.

## Next

1. `trace_search`: choose the matching trace ids with a limit before loading spans, instead of loading the window.
2. Instant-query lookback: read the newest sample per series, not every row of the window.
3. Run the sweep with `--users 1000` to measure high-cardinality attributes, where canardstack makes each attribute part of a
   stream and Loki does not.
