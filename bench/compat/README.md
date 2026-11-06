# Query compatibility suite

canardstack answers PromQL, LogQL and Tempo requests with a hand-written subset. This suite measures
how much of the real languages it covers, at the level of **values**, not just "did the request
succeed", so a query that is accepted but answered wrongly counts against it.

```bash
uv run --project bench bench/compat/run.py --target canardstack-pg                  # score a stack
uv run --project bench bench/compat/run.py --target canardstack-pg --check-baseline # CI: fail on regressions
uv run --project bench bench/compat/run.py --target lgtm                            # reference engines
uv run --project bench bench/compat/run.py --target canardstack-pg --lang logql --only line_contains
```

The target must be running (`bench/demo.py`, or `bench/run.py --manage --keep-up`). The suite
ingests its own small deterministic dataset under a fresh run id (unique service names and metric
names, so data left by other runs never mixes in), waits until it is queryable, then runs every
case in `cases.py` and prints a table plus a per-language score. `results/<target>.json` keeps the
latest run.

## How a case is judged

Every expected value is computed from the workload formulas in `bench/workload.py`, never from a
backend. Each case ends as one of:

| status | meaning |
| --- | --- |
| PASS | answered, and the answer matches the golden value |
| WRONG | answered successfully but differently: the dangerous kind |
| UNSUPPORTED | the engine rejected the query (4xx or an error status) |
| ERROR | 5xx, timeout, or an unparseable response |

The goldens are validated by running the same cases against **Mimir, Loki and Tempo** (`--target
lgtm`), which pass all of them. A case the reference engines fail has a wrong golden or tests
something outside the language; fix the case before trusting results for canardstack. Victoria
(`--target victoria`) runs the PromQL cases as a second opinion and passes all 48.

The runs are deterministic: the dataset's last sample is aligned to a 60 s boundary so time buckets
are the same on every run, and Tempo search is unwindowed because a windowed Tempo search only sees
blocks it has already flushed. If you add a case, run it a few times against both a reference engine
and canardstack before committing a baseline.

## The ratchet

`baselines/<target>.json` lists the cases that must keep passing. `--check-baseline` exits non-zero
if any of them stops passing and prints cases that newly pass; `--update-baseline` records the
current passes, so each fix locks in a gain. `--fail-under promql=60` adds a floor on a language's
pass rate. The goal is simply for `canardstack-pg`'s baseline to approach the reference's 74.

## Scoreboard (canardstack on a Postgres catalog)

| language | pass | wrong | unsupported | pass rate |
| --- | --- | --- | --- | --- |
| PromQL | 12 | 13 | 23 | 25% (12/48) |
| LogQL | 6 | 2 | 8 | 38% (6/16) |
| Tempo / TraceQL | 9 | 1 | 0 | 90% (9/10) |
| reference (Mimir, Loki, Tempo) | 74 | 0 | 0 | 100% (74/74) |

## What is failing, by root cause

Wrong answers come first because they are worse than errors: a dashboard shows a plausible number
that is not the right one.

**Silently wrong**
- Instant queries return **one unlabeled series** for a selector over several series
  (`selector_all`, `rate`, the matcher cases), and a `by (label)` aggregation returns **one group**
  instead of all (`agg_by`, `count_by`, `sum_rate_by`).
- `sum(rate(counter[1m]))` returns the rate of the whole counter set, not the sum of per-series rates
  (1.0 instead of 3.0).
- A range `sum()` without `by` returns one series per service instead of collapsing to one.
- A range query reports, at each step, the last sample in a bucket aligned to the epoch, not the
  value at the step time (`range_value_at_step`), and a bucket holding one sample yields a rate of 0
  (`range_rate_last_step_single_sample`).
- LogQL chained line filters (`|= "a" != "b"`) return nothing (`chained_filters`).
- TraceQL duration filters are ignored: `{ duration > 1h }` returns every trace.
- Loki label values do not include recent services (`logql.meta_label_values`, a stale metadata cache).

**Rejected with an explicit error**
- PromQL: `quantile`, `increase`, `irate`, `*_over_time`, arithmetic and comparisons (scalar and
  between series), `topk`/`bottomk`, `offset`, `absent`, `clamp`, `abs`, `label_replace`, `sort`.
- LogQL: `!=` and `!~` line filters, `| line_format`, every metric query (`count_over_time`, `rate`,
  `bytes_over_time`, `sum by (...)`), and parentheses around filtered metric queries.

Not covered by any case yet: histograms (the storage layer has none), subqueries, `@` modifiers,
recording and alerting rules, `| json` / `| logfmt` parsers, `label_format`, TraceQL structural
operators and aggregates, and the exemplar, log-volume and tail endpoints.

## Adding a case

Append a `Case` to the right list in `cases.py`. Use the `$GAUGE`, `$COUNTER`, `$LBL`, `$S0..` and
`$RUN` placeholders, and give either `expect` (a dict compared within `tol`) or a `check`
(`(ctx, observed) -> (ok, detail)`). Run it against `--target lgtm` first.
