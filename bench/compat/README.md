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
latest run locally; it is not committed because timings, run ids and some phase-dependent values
vary from run to run (the baselines are the contract).

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
lgtm`), which passed the original 74. A case the reference engines fail has a wrong golden or tests
something outside the language; fix the case before trusting results for canardstack. Victoria
(`--target victoria`) runs the PromQL cases as a second opinion and passes all 59 of the current ones.

**Not yet validated against a reference engine:** the LogQL cases (`line_case_sensitive`,
`line_contains_substring`, `line_literal_keywords`, `line_literal_parens`, `metric_bytes_rate`,
`metric_negated_filter`) and the TraceQL case `traceql_duration_name_service`
were added with the evaluator work and have only been checked against their goldens by hand. Run
them with `--target lgtm` before baselining them. A PromQL `changes()` case was dropped because
VictoriaMetrics and Prometheus count the change into the window differently.

The runs are deterministic: the dataset's last sample is aligned to a 60 s boundary so time buckets
are the same on every run, and Tempo search is unwindowed because a windowed Tempo search only sees
blocks it has already flushed. If you add a case, run it a few times against both a reference engine
and canardstack before committing a baseline.

## The ratchet

`baselines/<target>.json` lists the cases that must keep passing. `--check-baseline` exits non-zero
if any of them stops passing and prints cases that newly pass; `--update-baseline` records the
current passes, so each fix locks in a gain. `--fail-under promql=60` adds a floor on a language's
pass rate. The goal is to keep `canardstack-pg`'s baseline at every case in the suite.

## Scoreboard (canardstack on a Postgres catalog)

| language | pass | wrong | unsupported | pass rate |
| --- | --- | --- | --- | --- |
| PromQL | 18 | 0 | 0 | 100% (18/18) |
| LogQL | 22 | 0 | 0 | 100% (22/22) |
| Tempo / TraceQL | 11 | 0 | 0 | 100% (11/11) |

`baselines/canardstack-pg.json` lists all 51 cases.

**PromQL is covered by the upstream suite.** The 18 PromQL cases here are a smoke test and the
checks specific to canardstack (labels, discovery endpoints, resource caps). Breadth comes from
[`promql/`](promql/README.md), which runs prometheus/compliance (546 of 548 queries pass without
native histograms). LogQL and TraceQL have no upstream suite, so [`logql/`](logql/README.md)
(2,406 of 2,408 queries) and [`traceql/`](traceql/README.md) (256 of 266 search queries, 116 of 119 metrics queries) run the same queries
against a real Loki and Tempo and compare the answers.

## What the suite does not cover

Histograms (the storage layer has none), subqueries, `@` modifiers, recording and alerting rules,
`| json` / `| logfmt` parsers, `label_format`, `line_format` templates, LogQL `topk`/`offset`
and per-stream (unaggregated) metric queries, TraceQL `select()` attributes in search results, and the exemplar,
log-volume and tail endpoints. canardstack rejects the
unsupported ones with an explicit `unsupported_promql` / `unsupported_logql` error rather than a
wrong answer. Known semantic gaps: `changes()` follows Prometheus (the change into the window is
not counted), and Loki streams split by severity, so unaggregated log metric queries differ from
Loki's per-stream series.

## Adding a case

Append a `Case` to the right list in `cases.py`. Use the `$GAUGE`, `$COUNTER`, `$LBL`, `$S0..` and
`$RUN` placeholders, and give either `expect` (a dict compared within `tol`) or a `check`
(`(ctx, observed) -> (ok, detail)`). Run it against `--target lgtm` first.
