"""Per-stack ingest endpoints and query adapters for the same dashboard intents.

Each query asks one question about service `svc-i`; `expected` comes from the
workload, never from a backend.
"""

from __future__ import annotations

import json
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
            f"{base}{prefix}/api/v1/query_range?start={start}&end={_last(w)}&step=1"
            f"&query={q(sel('bench_gauge', i, w))}",
            headers,
        )

    def coarse_points(w: Workload, i: int):
        # A 15 s step spans 15 samples per bucket, so a stack that averages a bucket instead of
        # taking the newest sample cannot reach the sawtooth peak.
        return (
            f"{base}{prefix}/api/v1/query_range?start={w.base_ns // 10**9}&end={_last(w)}&step=15"
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
        + [Query("trace_by_id", "traces", trace, tempo_spans, lambda w, i: 2)],
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
        + [Query("trace_by_id", "traces", trace, tempo_spans, lambda w, i: 2)],
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
        + [Query("trace_by_id", "traces", trace, tempo_spans, lambda w, i: 2)],
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
        + [Query("trace_by_id", "traces", trace, tempo_spans, lambda w, i: 2)],
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
