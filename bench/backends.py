"""Per-stack ingest endpoints and query adapters for the same dashboard intents.

Each query asks one question about service `svc-i`; `expected` comes from the
workload, never from a backend.
"""

from __future__ import annotations

import json
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from workload import Workload

q = urllib.parse.quote


@dataclass
class Query:
    id: str
    signal: str  # logs | metrics | traces
    build: Callable[[Workload, int], Tuple[str, Dict[str, str]]]  # -> (url, headers)
    parse: Callable[[str], Optional[float]]  # response body -> observed value
    expected: Callable[[Workload, int], float]
    # Core intents are cheap lookups that also decide when ingested data is visible. The others
    # read or count every record of a service, so their latency grows with the data (`--intents`).
    core: bool = True


@dataclass
class Backend:
    name: str
    ingest: Dict[str, Tuple[str, Dict[str, str]]]
    ready: List[str]
    queries: List[Query] = field(default_factory=list)
    compose_dir: str = ""
    project: str = ""
    unsupported: Dict[str, str] = field(default_factory=dict)


# ---- response parsers -------------------------------------------------------


def loki_lines(body: str) -> Optional[float]:
    data = json.loads(body)
    if data.get("status") != "success":
        return None
    return float(sum(len(s["values"]) for s in data["data"]["result"]))


def ndjson_lines(body: str) -> Optional[float]:
    return float(sum(1 for line in body.splitlines() if line.strip()))


def prom_instant(body: str) -> Optional[float]:
    data = json.loads(body)
    result = data.get("data", {}).get("result", [])
    if data.get("status") != "success" or not result:
        return None
    return float(result[0]["value"][1])


def prom_range_points(body: str) -> Optional[float]:
    data = json.loads(body)
    if data.get("status") != "success":
        return None
    return float(sum(len(s["values"]) for s in data["data"]["result"]))


def prom_range_max(body: str) -> Optional[float]:
    """Largest value over every point of every series in a range response."""
    data = json.loads(body)
    if data.get("status") != "success":
        return None
    values = [float(v[1]) for s in data["data"]["result"] for v in s["values"]]
    return max(values) if values else None


def vector_sum(body: str) -> Optional[float]:
    """Sum of the values of an instant-query vector (Loki and Prometheus alike)."""
    data = json.loads(body)
    result = data.get("data", {}).get("result", [])
    if data.get("status") != "success" or not result:
        return None
    return float(sum(float(s["value"][1]) for s in result))


def search_traces(body: str) -> Optional[float]:
    return float(len(json.loads(body).get("traces") or []))


def tempo_spans(body: str) -> Optional[float]:
    data = json.loads(body)
    batches = data.get("batches") or data.get("trace", {}).get("resourceSpans") or []
    total = 0
    for batch in batches:
        for scope in batch.get("scopeSpans") or batch.get("instrumentationLibrarySpans") or []:
            total += len(scope.get("spans", []))
    return float(total) if total else None


def jaeger_spans(body: str) -> Optional[float]:
    data = json.loads(body).get("data") or []
    total = sum(len(trace.get("spans", [])) for trace in data)
    return float(total) if total else None


# ---- query builders ---------------------------------------------------------


def _window(w: Workload) -> Tuple[int, int]:
    return w.base_ns // 10**9 - 60, w.ts(w.records - 1) // 10**9 + 60


def _last(w: Workload) -> int:
    return w.ts(w.records - 1) // 10**9


def _prom_queries(base: str, label: str, prefix: str, headers: Dict[str, str],
                  counter: str = "bench_requests") -> List[Query]:
    """Latest gauge/sum value (instant at the last sample) and gauge points (range)."""

    def sel(name: str, i: int, w: Workload) -> str:
        return f'{name}{{{label}="{w.service(i)}"}}'

    def instant(name: str, fn: str):
        return lambda w, i: (
            f"{base}{prefix}/api/v1/query?time={_last(w)}&query={q(fn + '(' + sel(name, i, w) + ')')}",
            headers,
        )

    def points(w: Workload, i: int):
        start = w.base_ns // 10**9
        return (
            f"{base}{prefix}/api/v1/query_range?start={start}&end={_last(w)}&step={w.points_step}"
            f"&query={q(sel('bench_gauge', i, w))}",
            headers,
        )

    def coarse_points(w: Workload, i: int):
        # A 15 s step spans 15 samples per bucket, so a stack that averages a bucket instead of
        # taking the newest sample cannot reach the sawtooth peak.
        return (
            f"{base}{prefix}/api/v1/query_range?start={w.base_ns // 10**9}&end={_last(w)}&step={w.peak_step}"
            f"&query={q(sel('bench_gauge', i, w))}",
            headers,
        )

    return [
        Query("gauge_range_max", "metrics", coarse_points, prom_range_max,
              lambda w, i: w.expected(i)["gauge_range_max"]),
        Query("gauge_last", "metrics", instant("bench_gauge", "max"), prom_instant,
              lambda w, i: w.expected(i)["gauge_last"]),
        Query("sum_last", "metrics", instant(counter, "max"), prom_instant,
              lambda w, i: w.expected(i)["sum_last"]),
        Query("gauge_points", "metrics", points, prom_range_points,
              lambda w, i: w.expected(i)["gauge_count"]),
    ]


def _loki_queries(base: str, headers: Dict[str, str]) -> List[Query]:
    def lines(selector_tail: str):
        def build(w: Workload, i: int):
            s, e = _window(w)
            expr = '{service_name="%s"}%s' % (w.service(i), selector_tail)
            return (
                f"{base}/loki/api/v1/query_range?query={q(expr)}&limit={w.query_limit}"
                f"&start={s}&end={e}&direction=forward",
                headers,
            )

        return build

    return [
        Query("log_lines", "logs", lines(""), loki_lines,
              lambda w, i: w.expected(i)["log_count"]),
        Query("log_error_lines", "logs", lines(' |= "error"'), loki_lines,
              lambda w, i: w.expected(i)["log_error_count"]),
    ]


def _scale_queries(loki: str, prom: str, prom_prefix: str, tempo: str, headers: Dict[str, str],
                   counter: str) -> List[Query]:
    """Shapes whose cost grows with the data: each reads or counts every record of a service, or
    of all of them. Same questions and closed-form answers for every stack that speaks LogQL, PromQL
    and TraceQL. `loki`, `prom` and `tempo` are the base URLs of the three APIs."""

    def logql(expr: Callable[[Workload, int], str]):
        return lambda w, i: (
            f"{loki}/loki/api/v1/query?time={_last(w) + 1}&query={q(expr(w, i))}", headers)

    def promql(expr: Callable[[Workload, int], str]):
        return lambda w, i: (
            f"{prom}{prom_prefix}/api/v1/query?time={_last(w)}&query={q(expr(w, i))}", headers)

    def one(w: Workload, i: int) -> str:
        return '{service_name="%s"}' % w.service(i)

    def every(w: Workload, i: int) -> str:
        return '{service_name=~"%s"}' % w.service_regex

    def newest(w: Workload, i: int):
        s, e = _window(w)
        return (f"{loki}/loki/api/v1/query_range?query={q(one(w, i))}&limit=100&start={s}&end={e}"
                f"&direction=backward", headers)

    def search(w: Workload, i: int):
        # The window ends after now, not after the last span. Tempo dates a block by when its traces
        # arrived, so a window that ends at the (hours-old) synthetic timestamp of the last span skips
        # the blocks that hold them; real telemetry is stamped about when it arrives.
        s, _ = _window(w)
        e = int(time.time()) + 60
        expr = '{ resource.service.name = "%s" }' % w.service(i)
        return f"{tempo}/api/search?q={q(expr)}&limit=20&start={s}&end={e}", headers

    def series(name: str, w: Workload, i: int) -> str:
        return f'{name}{{job="{w.service(i)}"}}'

    def ex(key: str):
        return lambda w, i: w.expected(i)[key]

    return [
        Query("log_count_over_time", "logs",
              logql(lambda w, i: f"sum(count_over_time({one(w, i)}[{w.window_s}s]))"),
              vector_sum, ex("log_count_over_time"), core=False),
        Query("log_count_all", "logs",
              logql(lambda w, i: f"sum(count_over_time({every(w, i)}[{w.window_s}s]))"),
              vector_sum, ex("log_count_all"), core=False),
        Query("log_count_by_service", "logs",
              logql(lambda w, i: f"max(sum by (service_name) (count_over_time({every(w, i)}[{w.window_s}s])))"),
              vector_sum, ex("log_count_over_time"), core=False),
        Query("log_regex_count", "logs",
              logql(lambda w, i: f'sum(count_over_time({one(w, i)} |~ "event [0-9]*5$" [{w.window_s}s]))'),
              vector_sum, ex("log_regex_count"), core=False),
        Query("log_newest", "logs", newest, loki_lines, ex("log_newest"), core=False),
        Query("metric_sum_over_time", "metrics",
              promql(lambda w, i: f"sum_over_time({series('bench_gauge', w, i)}[{w.window_s}s])"),
              vector_sum, ex("metric_sum_over_time"), core=False),
        Query("metric_count_over_time", "metrics",
              promql(lambda w, i: f"count_over_time({series('bench_gauge', w, i)}[{w.window_s}s])"),
              vector_sum, ex("metric_count_over_time"), core=False),
        Query("metric_max_counter", "metrics",
              promql(lambda w, i: f"max_over_time({series(counter, w, i)}[{w.window_s}s])"),
              vector_sum, ex("metric_max_counter"), core=False),
        Query("metric_sum_all", "metrics",
              promql(lambda w, i: 'sum(bench_gauge{job=~"%s"})' % w.service_regex),
              vector_sum, ex("metric_sum_all"), core=False),
        Query("trace_search", "traces", search, search_traces, ex("trace_search"), core=False),
    ]


# ---- stacks -----------------------------------------------------------------


def canardstack() -> Backend:
    auth = {"Authorization": "Bearer dev-canardstack-key"}
    otlp = {"Authorization": "Bearer dev-otlp-token-123456"}
    ingest = "http://localhost:14338"
    query = "http://localhost:19091"

    def trace(w: Workload, i: int):
        return f"{query}/api/traces/{w.trace_id(i, w.records - 1)}", {**auth, "Accept": "application/json"}

    return Backend(
        name="canardstack",
        ingest={s: (f"{ingest}/v1/{s}", otlp) for s in ("logs", "metrics", "traces")},
        ready=[f"{query}/healthz"],
        queries=_loki_queries(query, auth)
        + _prom_queries(query, "job", "", auth, "bench_requests_total")
        + [Query("trace_by_id", "traces", trace, tempo_spans, lambda w, i: 2)]
        + _scale_queries(query, query, "", query, auth, "bench_requests_total"),
        compose_dir="canardstack",
        project="bench-canardstack",
    )


def canardstack_pg() -> Backend:
    """Same query surface as `canardstack`, with a Postgres DuckLake catalog instead of Quack."""
    base = canardstack()
    auth = {"Authorization": "Bearer dev-canardstack-key"}
    otlp = {"Authorization": "Bearer dev-otlp-token-123456"}
    ingest = "http://localhost:14348"
    query = "http://localhost:19092"

    def trace(w: Workload, i: int):
        return f"{query}/api/traces/{w.trace_id(i, w.records - 1)}", {**auth, "Accept": "application/json"}

    return Backend(
        name="canardstack-pg",
        ingest={s: (f"{ingest}/v1/{s}", otlp) for s in ("logs", "metrics", "traces")},
        ready=[f"{query}/healthz"],
        queries=_loki_queries(query, auth)
        + _prom_queries(query, "job", "", auth, "bench_requests_total")
        + [Query("trace_by_id", "traces", trace, tempo_spans, lambda w, i: 2)]
        + _scale_queries(query, query, "", query, auth, "bench_requests_total"),
        compose_dir="canardstack-pg",
        project="bench-canardstack-pg",
        unsupported=base.unsupported,
    )


def canardstack_sqlite() -> Backend:
    """Same query surface as `canardstack`, with a SQLite DuckLake catalog on a shared volume."""
    auth = {"Authorization": "Bearer dev-canardstack-key"}
    otlp = {"Authorization": "Bearer dev-otlp-token-123456"}
    ingest = "http://localhost:14358"
    query = "http://localhost:19093"

    def trace(w: Workload, i: int):
        return f"{query}/api/traces/{w.trace_id(i, w.records - 1)}", {**auth, "Accept": "application/json"}

    return Backend(
        name="canardstack-sqlite",
        ingest={s: (f"{ingest}/v1/{s}", otlp) for s in ("logs", "metrics", "traces")},
        ready=[f"{query}/healthz"],
        queries=_loki_queries(query, auth)
        + _prom_queries(query, "job", "", auth, "bench_requests_total")
        + [Query("trace_by_id", "traces", trace, tempo_spans, lambda w, i: 2)]
        + _scale_queries(query, query, "", query, auth, "bench_requests_total"),
        compose_dir="canardstack-sqlite",
        project="bench-canardstack-sqlite",
    )


def lgtm() -> Backend:
    loki, mimir, tempo = "http://localhost:13100", "http://localhost:19009", "http://localhost:13200"

    def trace(w: Workload, i: int):
        return f"{tempo}/api/traces/{w.trace_id(i, w.records - 1)}", {}

    return Backend(
        name="lgtm",
        ingest={
            "logs": (f"{loki}/otlp/v1/logs", {}),
            "metrics": (f"{mimir}/otlp/v1/metrics", {}),
            "traces": ("http://localhost:14328/v1/traces", {}),
        },
        ready=[f"{loki}/ready", f"{tempo}/ready", f"{mimir}/ready"],
        queries=_loki_queries(loki, {})
        + _prom_queries(mimir, "job", "/prometheus", {})
        + [Query("trace_by_id", "traces", trace, tempo_spans, lambda w, i: 2)]
        + _scale_queries(loki, mimir, "/prometheus", tempo, {}, "bench_requests"),
        compose_dir="lgtm",
        project="bench-lgtm",
    )


def victoria() -> Backend:
    vm, vl, vt = "http://localhost:18428", "http://localhost:19428", "http://localhost:10428"

    def lines(extra: str):
        def build(w: Workload, i: int):
            expr = f'"service.name":"{w.service(i)}"{extra}'
            return f"{vl}/select/logsql/query?query={q(expr)}&limit={w.query_limit}", {}

        return build

    def trace(w: Workload, i: int):
        return f"{vt}/select/jaeger/api/traces/{w.trace_id(i, w.records - 1)}", {}

    return Backend(
        name="victoria",
        ingest={
            "logs": (f"{vl}/insert/opentelemetry/v1/logs", {}),
            "metrics": (f"{vm}/opentelemetry/v1/metrics", {}),
            "traces": (f"{vt}/insert/opentelemetry/v1/traces", {}),
        },
        ready=[f"{vm}/health", f"{vl}/health", f"{vt}/health"],
        queries=[
            Query("log_lines", "logs", lines(""), ndjson_lines,
                  lambda w, i: w.expected(i)["log_count"]),
            Query("log_error_lines", "logs", lines(" error"), ndjson_lines,
                  lambda w, i: w.expected(i)["log_error_count"]),
        ]
        + _prom_queries(vm, '"service.name"', "", {})
        + [Query("trace_by_id", "traces", trace, jaeger_spans, lambda w, i: 2)],
        compose_dir="victoria",
        project="bench-victoria",
        unsupported={"query_language": "logs use LogsQL, not LogQL; traces use the Jaeger API"},
    )


BACKENDS = {"canardstack": canardstack, "canardstack-pg": canardstack_pg, "canardstack-sqlite": canardstack_sqlite, "lgtm": lgtm, "victoria": victoria}
