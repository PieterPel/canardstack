#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["opentelemetry-proto", "protobuf"]
# ///
"""Compare PromQL over OTel explicit-bucket histograms between Prometheus and canardstack.

The same cumulative histograms are sent to Prometheus through its OTLP receiver (which translates
them to classic `_bucket`/`_sum`/`_count` series) and committed to a DuckLake that canardstack
serves. A set of queries runs against both and the answers are compared.

    REEDLAKE_BINARY=... uv run bench/compat/histogram/compare.py   # needs target/release/canardstack (or CANARDSTACK_BINARY)
    PROMETHEUS_BINARY=... CANARDSTACK_BINARY=... REEDLAKE_BINARY=... uv run bench/compat/histogram/compare.py
"""

from __future__ import annotations

import json
import math
import os
import random
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from google.protobuf.json_format import MessageToJson
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.metrics.v1 import metrics_pb2

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "bench" / "compat"))
from reedlake import ingest  # noqa: E402

API_KEY = "histogram-compare"
BOUNDS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
SCRAPE = 15  # seconds between points
POINTS = 60

QUERIES = [
    "http_server_duration_seconds_bucket",
    'http_server_duration_seconds_bucket{le="0.5"}',
    'http_server_duration_seconds_bucket{le="+Inf", http_route="/a"}',
    "http_server_duration_seconds_count",
    "http_server_duration_seconds_sum",
    "sum by (le) (http_server_duration_seconds_bucket)",
    "rate(http_server_duration_seconds_count[2m])",
    "sum(rate(http_server_duration_seconds_count[2m]))",
    "increase(http_server_duration_seconds_bucket[5m])",
    "sum(rate(http_server_duration_seconds_sum[2m])) / sum(rate(http_server_duration_seconds_count[2m]))",
    "histogram_quantile(0.5, http_server_duration_seconds_bucket)",
    "histogram_quantile(0.9, sum by (le) (rate(http_server_duration_seconds_bucket[2m])))",
    "histogram_quantile(0.99, sum by (le) (rate(http_server_duration_seconds_bucket[2m])))",
    "histogram_quantile(0.9, sum by (le, http_route) (rate(http_server_duration_seconds_bucket[2m])))",
    "histogram_quantile(0.5, sum by (le, job) (rate(http_server_duration_seconds_bucket[5m])))",
    "histogram_quantile(0.95, rate(http_server_duration_seconds_bucket[3m]))",
    "histogram_quantile(0, sum by (le) (rate(http_server_duration_seconds_bucket[2m])))",
    "histogram_quantile(1, sum by (le) (rate(http_server_duration_seconds_bucket[2m])))",
    'sum by (le) (rate(http_server_duration_seconds_bucket{job="api"}[2m]))',
    'sum(rate(http_server_duration_seconds_bucket{le="0.25"}[2m])) / sum(rate(http_server_duration_seconds_count[2m]))',
    "count by (le) (http_server_duration_seconds_bucket)",
    "max by (http_route) (http_server_duration_seconds_count)",
    'http_server_duration_seconds_bucket{le=~"0.1|0.25"}',
    'http_server_duration_seconds_count{http_route!="/a"}',
    "topk(2, sum by (http_route) (rate(http_server_duration_seconds_count[2m])))",
    "http_server_duration_seconds_count offset 2m",
]


# Queries whose correct answer is nothing (absent data, or a comparison that never holds).
EXPECT_EMPTY = {
    "absent_over_time(http_client_duration_seconds[1m])",
    "http_client_duration_seconds != http_client_duration_seconds",
    "http_client_duration_seconds + 1",
}

NATIVE_NAME = "http_client_duration_seconds"
N = NATIVE_NAME

# Known differences that are Prometheus' doing, not ours: its histogram equality compares the span
# layout its storage may pad, so `changes()` can count a change between two identical samples.
KNOWN_DIFFERENT = {f"changes({NATIVE_NAME}[10m])": "equality compares storage span layout"}
NATIVE_QUERIES = [
    N,
    f'{N}{{http_route="/a"}}',
    f"{N} offset 2m",
    f"rate({N}[2m])",
    f"increase({N}[2m])",
    f"delta({N}[2m])",
    f"irate({N}[2m])",
    f"idelta({N}[2m])",
    f"resets({N}[10m])",
    f"changes({N}[10m])",
    f"changes({N}[20s])",
    f"count_over_time({N}[2m])",
    f"last_over_time({N}[2m])",
    f"sum_over_time({N}[2m])",
    f"avg_over_time({N}[2m])",
    f"absent_over_time({N}[1m])",
    "absent_over_time(no_such_native[1m])",
    f"histogram_count({N})",
    f"histogram_sum({N})",
    f"histogram_avg(rate({N}[2m]))",
    f"histogram_stddev(rate({N}[2m]))",
    f"histogram_stdvar(rate({N}[2m]))",
    f"histogram_fraction(0, 0.5, rate({N}[2m]))",
    f"histogram_fraction(-Inf, +Inf, rate({N}[2m]))",
    f"histogram_fraction(0.1, 1, {N})",
    f"histogram_quantile(0.5, rate({N}[2m]))",
    f"histogram_quantile(0.9, rate({N}[2m]))",
    f"histogram_quantile(0.99, rate({N}[2m]))",
    f"histogram_quantile(0.1, rate({N}[2m]))",
    f"histogram_quantile(0, rate({N}[2m]))",
    f"histogram_quantile(1, rate({N}[2m]))",
    f"histogram_quantile(1.5, rate({N}[2m]))",
    f"histogram_quantile(-0.5, rate({N}[2m]))",
    f"histogram_quantile(0.9, sum by (job) (rate({N}[2m])))",
    f"histogram_quantile(0.5, sum(rate({N}[5m])))",
    f"sum({N})",
    f"sum by (job) ({N})",
    f"sum without (instance) ({N})",
    f"avg({N})",
    f"avg by (job) ({N})",
    f"count({N})",
    f"count by (job) ({N})",
    f"group({N})",
    f"sum by (job) (rate({N}[2m]))",
    f"{N} * 2",
    f"2 * {N}",
    f"{N} / 2",
    f"{N} + {N}",
    f"{N} - {N}",
    f"{N} == {N}",
    f"{N} != {N}",
    f"{N} == bool {N}",
    f"{N} != bool {N}",
    f"-{N}",
    f"sum({N}) / count({N})",
    f"{N} + 1",
    f"{N} * on (job) group_left () sum by (job) (rate(http_server_duration_seconds_count[2m]))",
]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def attr(key: str, value: str) -> KeyValue:
    return KeyValue(key=key, value=AnyValue(string_value=value))


def series_defs():
    for service in ("web", "api"):
        for route in ("/a", "/b", "/c"):
            yield service, f"{service}-1", route


def build_requests(start: int):
    """One export request per scrape, as a Collector would send: every series at that time."""
    rng = random.Random(42)
    state = {}  # series -> (count, sum, bucket_counts)
    requests = []
    for point in range(POINTS):
        t = start + point * SCRAPE
        request = ExportMetricsServiceRequest()
        for service, instance, route in series_defs():
            key = (service, route)
            count, total, buckets = state.get(key, (0, 0.0, [0] * (len(BOUNDS) + 1)))
            if service == "api" and route == "/c" and point == POINTS // 2:
                count, total, buckets = 0, 0.0, [0] * (len(BOUNDS) + 1)  # a counter reset
            for _ in range(rng.randint(0, 40)):
                v = rng.lognormvariate(-1.8 + 0.4 * (route == "/b") + 0.8 * (service == "api"), 1.0)
                count += 1
                total += v
                buckets[next((i for i, b in enumerate(BOUNDS) if v <= b), len(BOUNDS))] += 1
            state[key] = (count, total, buckets)
            rm = request.resource_metrics.add()
            rm.resource.attributes.extend([attr("service.name", service), attr("service.instance.id", instance)])
            sm = rm.scope_metrics.add()
            sm.scope.name = "compare"
            metric = sm.metrics.add()
            metric.name = "http.server.duration"
            metric.unit = "s"
            metric.description = "request duration"
            h = metric.histogram
            h.aggregation_temporality = metrics_pb2.AGGREGATION_TEMPORALITY_CUMULATIVE
            dp = h.data_points.add()
            dp.start_time_unix_nano = (start - 60) * 10**9
            dp.time_unix_nano = t * 10**9
            dp.count = count
            dp.sum = total
            dp.bucket_counts.extend(buckets)
            dp.explicit_bounds.extend(BOUNDS)
            dp.attributes.append(attr("http.route", route))
        requests.append((t, request))
    return requests


NATIVE_SERIES = [  # service, instance, route, scale
    ("web", "web-1", "/a", 3),
    ("web", "web-1", "/b", 4),
    ("api", "api-1", "/a", 3),
    ("api", "api-1", "/c", 2),
]


def otlp_index(value: float, scale: int) -> int:
    """The OTLP bucket index of a positive value: bucket i holds (base^i, base^(i+1)]."""
    return math.ceil(math.log2(value) * 2**scale) - 1


def build_native_requests(start: int):
    rng = random.Random(7)
    state = {}  # series -> [count, sum, zero, pos{idx: n}, neg{idx: n}]
    requests = []
    for point in range(POINTS):
        t = start + point * SCRAPE
        request = ExportMetricsServiceRequest()
        for service, instance, route, scale in NATIVE_SERIES:
            key = (service, route)
            st = state.setdefault(key, [0, 0.0, 0, {}, {}])
            if service == "api" and route == "/c" and point == POINTS // 2:
                st[:] = [0, 0.0, 0, {}, {}]  # a counter reset
            for _ in range(rng.randint(0, 30)):
                v = rng.lognormvariate(-1.5 + 0.5 * (route == "/b"), 1.2) * rng.choice([1, 1, 1, -1])
                if rng.random() < 0.05:
                    v = 0.0
                st[0] += 1
                st[1] += v
                if v == 0.0:
                    st[2] += 1
                else:
                    side = st[3] if v > 0 else st[4]
                    idx = otlp_index(abs(v), scale)
                    side[idx] = side.get(idx, 0) + 1
            rm = request.resource_metrics.add()
            rm.resource.attributes.extend([attr("service.name", service), attr("service.instance.id", instance)])
            sm = rm.scope_metrics.add()
            sm.scope.name = "compare"
            metric = sm.metrics.add()
            metric.name = "http.client.duration"
            metric.unit = "s"
            h = metric.exponential_histogram
            h.aggregation_temporality = metrics_pb2.AGGREGATION_TEMPORALITY_CUMULATIVE
            dp = h.data_points.add()
            dp.start_time_unix_nano = (start - 60) * 10**9
            dp.time_unix_nano = t * 10**9
            dp.count = st[0]
            dp.sum = st[1]
            dp.scale = scale
            dp.zero_count = st[2]
            for side, buckets in ((dp.positive, st[3]), (dp.negative, st[4])):
                if buckets:
                    low, high = min(buckets), max(buckets)
                    side.offset = low
                    side.bucket_counts.extend(buckets.get(i, 0) for i in range(low, high + 1))
            dp.attributes.append(attr("http.route", route))
        requests.append((t, request))
    return requests


def wait_ready(url: str, what: str, timeout: float = 90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=2).read()
            return
        except Exception:
            time.sleep(0.3)
    raise SystemExit(f"{what} did not become ready")


def get(base: str, path: str, params: dict, headers=None):
    req = urllib.request.Request(f"{base}{path}?{urllib.parse.urlencode(params)}", headers=headers or {})
    try:
        return json.load(urllib.request.urlopen(req, timeout=60))
    except urllib.error.HTTPError as e:
        return {"status": "error", "error": e.read().decode()[:200]}


def normalise(resp: dict):
    """label set -> list of (timestamp, value) for the series of a vector or matrix result."""
    if resp.get("status") != "success":
        return None
    out = {}
    for s in resp["data"]["result"]:
        labels = tuple(sorted((k, v) for k, v in s["metric"].items() if k not in ("otel_scope_name", "otel_scope_version", "otel_scope_schema_url")))
        points = []
        for t, v in s.get("values") or ([s["value"]] if "value" in s else []):
            points.append((float(t), float(v)))
        for t, h in s.get("histograms") or ([s["histogram"]] if "histogram" in s else []):
            buckets = tuple((int(b[0]), float(b[1]), float(b[2]), float(b[3])) for b in h.get("buckets", []))
            points.append((float(t), ("H", float(h["count"]), float(h["sum"]), buckets)))
        out[labels] = sorted(points, key=lambda p: p[0])
    return out


def same(a, b) -> bool:
    """Whether two samples (floats or histograms) agree."""
    if isinstance(a, tuple) or isinstance(b, tuple):
        if not (isinstance(a, tuple) and isinstance(b, tuple)):
            return False
        if not (close(a[1], b[1]) and close(a[2], b[2])) or len(a[3]) != len(b[3]):
            return False
        return all(x[0] == y[0] and close(x[1], y[1]) and close(x[2], y[2]) and close(x[3], y[3]) for x, y in zip(a[3], b[3]))
    return close(a, b)


def explain(want, got) -> str:
    """What differs between two samples, short enough to read."""
    if not (isinstance(want, tuple) and isinstance(got, tuple)):
        return f"got {got!r}, want {want!r}"
    if not close(want[1], got[1]):
        return f"count {got[1]}, want {want[1]}"
    if not close(want[2], got[2]):
        return f"sum {got[2]}, want {want[2]}"
    if len(want[3]) != len(got[3]):
        have = {round(b[1], 12): b for b in got[3]}
        wanted = {round(b[1], 12): b for b in want[3]}
        extra = [have[k] for k in have if k not in wanted][:2]
        missing = [wanted[k] for k in wanted if k not in have][:2]
        return f"{len(got[3])} buckets, want {len(want[3])}: extra {extra}, missing {missing}"
    for x, y in zip(want[3], got[3]):
        if not (x[0] == y[0] and close(x[1], y[1]) and close(x[2], y[2]) and close(x[3], y[3])):
            return f"bucket {y}, want {x}"
    return "equal"


def close(a: float, b: float) -> bool:
    if math.isnan(a) and math.isnan(b):
        return True
    if math.isinf(a) or math.isinf(b):
        return a == b
    return abs(a - b) <= 1e-6 * max(1.0, abs(a), abs(b))


def diff(ref, got) -> str:
    if ref is None or got is None:
        return f"one side failed (reference ok: {ref is not None}, canardstack ok: {got is not None})"
    if set(ref) != set(got):
        missing = [dict(k) for k in list(set(ref) - set(got))[:2]]
        extra = [dict(k) for k in list(set(got) - set(ref))[:2]]
        return f"series differ: missing {missing}; unexpected {extra}; {len(ref)} vs {len(got)}"
    for key in ref:
        a, b = dict(ref[key]), dict(got[key])
        if set(a) != set(b):
            return f"{dict(key)}: timestamps differ ({len(a)} vs {len(b)})"
        bad = [t for t in sorted(a) if not same(a[t], b[t])]
        if bad:
            more = f" (and {len(bad) - 1} more steps: {bad[1:4]})" if len(bad) > 1 else ""
            return f"{dict(key)} at {bad[0]}: {explain(a[bad[0]], b[bad[0]])}{more}"
    return ""


def main():
    prometheus = os.environ.get("PROMETHEUS_BINARY") or str(next((ROOT / "bench/compat/promql/bin").glob("prometheus-*")))
    binary = os.environ.get("CANARDSTACK_BINARY") or str(ROOT / "target/release/canardstack")
    # Whole scrapes ending a minute ago.
    start = int(time.time()) - POINTS * SCRAPE - 60
    start -= start % SCRAPE
    requests = build_requests(start)
    native_requests = build_native_requests(start)
    end = requests[-1][0]
    work = Path(tempfile.mkdtemp(prefix="histogram-compare-"))
    processes = []
    try:
        # --- Prometheus, fed over OTLP
        prom_port = free_port()
        (work / "prom.yml").write_text("storage:\n  tsdb:\n    out_of_order_time_window: 2h\n")
        processes.append(subprocess.Popen(
            [prometheus, f"--web.listen-address=127.0.0.1:{prom_port}", f"--storage.tsdb.path={work/'prom'}",
             f"--config.file={work/'prom.yml'}", "--web.enable-otlp-receiver", "--enable-feature=native-histograms"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        prom = f"http://127.0.0.1:{prom_port}"
        wait_ready(prom + "/-/ready", "prometheus")
        for _, request in requests + native_requests:
            req = urllib.request.Request(prom + "/api/v1/otlp/v1/metrics", data=request.SerializeToString(),
                                         headers={"Content-Type": "application/x-protobuf"})
            urllib.request.urlopen(req, timeout=30).read()

        # --- canardstack, fed through the writer
        catalog, data = work / "catalog.sqlite", work / "data"
        data.mkdir()
        sqlite3.connect(catalog).execute("PRAGMA journal_mode=WAL").close()
        events = [MessageToJson(r, indent=None).encode() for _, r in requests + native_requests]
        print(ingest(catalog, data, "metrics", "histogram-compare", 0, len(events) - 1, events), f"from {len(events)} requests")
        cs_port = free_port()
        processes.append(subprocess.Popen(
            [binary, "serve"], cwd=work, stdout=open(work / "canardstack.log", "w"), stderr=subprocess.STDOUT,
            env={**os.environ, "CANARDSTACK_BIND": f"127.0.0.1:{cs_port}", "CANARDSTACK_API_KEY": API_KEY,
                 "CANARDSTACK_ADMIN_API_KEY": API_KEY + "-admin", "CANARDSTACK_DATA_DIR": str(work / "cs"),
                 "CANARDSTACK_DUCKLAKE_ATTACH_URI": f"ducklake:sqlite:{catalog}",
                 "CANARDSTACK_DUCKLAKE_DATA_PATH": f"{data}/"}))
        cs = f"http://127.0.0.1:{cs_port}"
        try:
            wait_ready(cs + "/healthz", "canardstack", timeout=30)
        except SystemExit:
            print((work / "canardstack.log").read_text()[-3000:])
            raise
        auth = {"Authorization": f"Bearer {API_KEY}"}

        if os.environ.get("DEBUG_PAIR"):
            i = int(os.environ["DEBUG_PAIR"])
            for k in (i - 1, i):
                t = start + k * SCRAPE
                for name, base, hdr in (("prom", prom, None), ("cs", cs, auth)):
                    r = get(base, "/api/v1/query", {"query": f'{NATIVE_NAME}{{http_route="/b", job="web"}}', "time": t}, hdr)
                    h = r["data"]["result"][0]["histogram"][1] if r.get("data", {}).get("result") else None
                    print(name, k, h and (h["count"], h["sum"], len(h.get("buckets", []))))
            for name, base, hdr in (("prom", prom, None), ("cs", cs, auth)):
                r = get(base, "/api/v1/query", {"query": f'changes({NATIVE_NAME}{{http_route="/b", job="web"}}[20s])', "time": start + i * SCRAPE}, hdr)
                print(name, "changes at pair", r.get("data", {}).get("result"))
        failures = known = 0
        only = os.environ.get("ONLY")
        for q in [q for q in QUERIES + NATIVE_QUERIES if not only or only in q]:
            for kind, path, params in (
                ("instant", "/api/v1/query", {"query": q, "time": end - 30}),
                ("range", "/api/v1/query_range", {"query": q, "start": start + 300, "end": end - 30, "step": 30}),
            ):
                ref = normalise(get(prom, path, params))
                got = normalise(get(cs, path, params, auth))
                problem = diff(ref, got)
                if not problem and not ref and q not in EXPECT_EMPTY:
                    # Every query here selects data, so two empty answers mean the data is missing.
                    problem = "both answers are empty"
                if problem and q in KNOWN_DIFFERENT:
                    print(f"known {kind:7} {q}: {KNOWN_DIFFERENT[q]}")
                    known += 1
                elif problem:
                    failures += 1
                    print(f"FAIL {kind:7} {q}\n     {problem}")
        total = len([q for q in QUERIES + NATIVE_QUERIES if not only or only in q]) * 2
        print(f"{total - failures - known}/{total} comparisons match" + (f", {known} known differences" if known else ""))
        sys.exit(1 if failures else 0)
    finally:
        for p in processes:
            p.terminate()
        for p in processes:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()


if __name__ == "__main__":
    main()
