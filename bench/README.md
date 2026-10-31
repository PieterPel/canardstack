# Comparative observability benchmark

Compares canardstack (external `duckdb-otlp` writer + DuckLake + canardstack) with a
standard LGTM stack (Loki, Tempo, Mimir) and the single-node Victoria trio
(VictoriaMetrics, VictoriaLogs, VictoriaTraces) on one shared, deterministic workload.
Status and open work live in `docs/work-log.md` section 6.

## Run

```bash
python3 -m venv bench/.venv && bench/.venv/bin/pip install -r bench/requirements.txt
# one stack at a time; --manage wipes volumes, boots the stack and tears it down after
bench/.venv/bin/python bench/run.py --stack victoria --manage --services 5 --records 1000
bench/.venv/bin/python bench/run.py --stack lgtm --manage
bench/.venv/bin/python bench/run.py --stack canardstack --manage   # needs canardstack:local (see below)
bench/.venv/bin/python bench/report.py
```

Images are pinned in `bench/versions.env`. The canardstack profile uses a locally built
image: `podman build -t canardstack:local .` from the repo root.

## What is measured

| Phase | What it records |
| --- | --- |
| ingest | batches accepted per signal, HTTP status counts, accepted rows/s |
| visibility | seconds from ingest accepted to every expected row being queryable (accepted is not durable) |
| queries | per query intent: p50/p95/p99, qps, and `ok` / `wrong` / `errors` counts at 1 and N clients |
| containers | CPU and memory sampled from `podman stats` for the stack's containers |

Expected answers are derived from the workload seed (`workload.py`), never from a backend.
An empty, wrong, or rejected response is counted as `wrong`/`errors` and excluded from the
latency percentiles. Each run uses a unique run id in service names and trace ids, so a
reused stack cannot mix datasets.

Query intents (`backends.py`): `log_lines`, `log_error_lines` (text filter), `gauge_last`
and `sum_last` (instant at the last sample), `gauge_points` (1 s range query), and
`trace_by_id`. They are limited to what canardstack supports (no `count_over_time`, no
TraceQL). Each stack uses its native dialect: LogQL (Loki, canardstack), LogsQL
(VictoriaLogs), PromQL/MetricsQL, Tempo and Jaeger trace APIs.

## Known limits of this harness

- Single-node containers in one Podman VM (4 CPU / 8 GiB), run one stack at a time with no
  CPU or memory limits. This is an execution-efficiency comparison, not a cost model.
- Direct OTLP/HTTP protobuf ingest and direct backend queries. No Grafana and no collector in
  the measured path, so client-side overhead is not included.
- Not yet covered: Azure Functions hosting, cold starts, scale-out, Blob storage, cost
  accounting, restarts and outages, mixed ingest/query load, retained-data size sweeps, and
  durability (as opposed to acceptance) of ingested data.
- Mimir exposes `service.name` as `job`; VictoriaMetrics keeps the dotted label
  `"service.name"`; canardstack and Loki use `service_name`.
