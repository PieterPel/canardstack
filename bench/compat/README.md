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
[`promql/`](promql/README.md), which runs prometheus/compliance (all 661 queries pass). LogQL and TraceQL have no upstream suite, so [`logql/`](logql/README.md)
(2,406 of 2,408 queries) and [`traceql/`](traceql/README.md) (256 of 266 search queries, 116 of 119 metrics queries) run the same queries
against a real Loki and Tempo and compare the answers.

## What the suite does not cover

Summaries, which the storage layer does not hold; native histograms with custom buckets; the `@`
modifier, recording and alerting rules (canardstack rejects `@` with an explicit
`unsupported_promql` error rather than a wrong answer); TraceQL `select()` attributes in search
results; and the exemplar, log-volume and tail endpoints. Explicit-bucket and exponential histograms
are checked against Prometheus by [`histogram/`](histogram/README.md) and by the upstream suite.

## Adding a case

Append a `Case` to the right list in `cases.py`. Use the `$GAUGE`, `$COUNTER`, `$LBL`, `$S0..` and
`$RUN` placeholders, and give either `expect` (a dict compared within `tol`) or a `check`
(`(ctx, observed) -> (ok, detail)`). Run it against `--target lgtm` first.

## Latency against the reference engines

The three differential harnesses can also time every query on both servers. With `TIMING=<file>`
each request is repeated four times (the first is cold) and every duration is appended to the file;
run with `-parallel 1` and a release build so requests do not compete and the binary is the one you
would deploy. `SCALE=<n>` multiplies the LogQL lines and `TRACES=<n>` sets the TraceQL traces (the
PromQL data comes from the upstream suite and cannot be scaled).

```bash
cargo build --release
CANARDSTACK_BINARY=$PWD/target/release/canardstack TIMING=$PWD/bench/compat/results/latency-logql.jsonl \
  sh -c 'cd bench/compat/logql && nix-shell -p go --run "go test -parallel 1 -run TestCompliance -timeout 90m -count=1 ."'
uv run --project bench bench/compat/latency_report.py bench/compat/results/latency-*.jsonl
```

The report gives, per language, the ratio of canardstack's median warm latency to the reference's,
by feature, the worst ratios, and the first-request penalty.

What it showed (release build, one machine, medians over three warm rounds):

| | data | queries | ratio p50 | p90 | notes |
| --- | --- | --- | --- | --- | --- |
| PromQL vs Prometheus | upstream suite, ~70 min | 649 | 19.5 | 40 | every query costs 6–7 ms (even `min(nonexistent)`), Prometheus 0.1–0.5 ms |
| LogQL vs Loki | 480 lines | 2,408 | 1.7 | 2.6 | nothing above 10x |
| LogQL vs Loki | 20x lines | 1,874 | 1.4 | 2.0 | the gap shrinks as data grows |
| TraceQL vs Tempo | 60 traces | 384 | 3.3 | 6.0 | |
| TraceQL vs Tempo | 900 traces | 384 | 7.3 | 15.8 | before the optimisations below: 119 queries over 10x; a plain filter takes 13 ms against 1.6 ms |
| TraceQL vs Tempo | 900 traces, optimised | 384 | 3.8 | 9.9 | 38 queries over 10x |

- **The PromQL floor is DuckLake's, not ours.** A filtered scan of one DuckLake table on a SQLite
  catalog takes 3–6 ms in plain DuckDB, even when the table is empty (`SELECT 1` is 0.06 ms; canardstack's
  own HTTP and planning cost for `1+1` is 0.3 ms). Things that did not lower it: holding one
  transaction open (7.7 to 6.0 ms), flushing inlined rows to Parquet, merging files (the span table
  is a single file), and reading one table instead of both for a metric whose table is known (no
  change across the 649 queries, so that routing cache was not kept). What would: fewer catalog
  round trips per scan, which is DuckLake's to improve, or caching whole results.
- **TraceQL scaled with spans and was the weak spot; three changes cut it.** Loading a trace set now
  takes one scan (window functions mark the traces that touch the window and meet each condition),
  the filters of the query are pushed into that scan as SQL (`pushdown.rs`, conservative: anything
  not understood is left to the evaluator), and the wide JSON columns a query cannot read are not
  selected (reading them was most of the cost: 12 ms against 6 ms for 4,270 spans). At 900 traces
  the median ratio fell from 7.3 to 3.8 and the queries over 10x from 119 to 38. What remains is
  mostly the same per-scan cost; a plain `{ } | rate()` still reads every span of the window, where
  Tempo answers from an index.
- **LogQL holds up.** Its engine reads only the matching rows, and the gap to Loki narrows with data.
