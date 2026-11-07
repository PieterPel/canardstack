#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.6", "opentelemetry-proto", "protobuf"]
# ///
"""Compare PromQL over OTel explicit-bucket histograms between Prometheus and canardstack.

The same cumulative histograms are sent to Prometheus through its OTLP receiver (which translates
them to classic `_bucket`/`_sum`/`_count` series) and committed to a DuckLake that canardstack
serves. A set of queries runs against both and the answers are compared.

    uv run bench/compat/histogram/compare.py            # needs target/release/canardstack (or CANARDSTACK_BINARY)
    PROMETHEUS_BINARY=... CANARDSTACK_BINARY=... uv run bench/compat/histogram/compare.py
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
sys.path.insert(0, str(ROOT / "bench" / "serverless_writer"))
from eventhub_ingest import ingest_batch  # noqa: E402

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
        values = s.get("values") or [s["value"]]
        out[labels] = [(float(t), float(v)) for t, v in values]
    return out


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
        for t in a:
            if not close(a[t], b[t]):
                return f"{dict(key)} at {t}: {b[t]}, want {a[t]}"
    return ""


def main():
    prometheus = os.environ.get("PROMETHEUS_BINARY") or str(next((ROOT / "bench/compat/promql/bin").glob("prometheus-*")))
    binary = os.environ.get("CANARDSTACK_BINARY") or str(ROOT / "target/release/canardstack")
    # Whole scrapes ending a minute ago.
    start = int(time.time()) - POINTS * SCRAPE - 60
    start -= start % SCRAPE
    requests = build_requests(start)
    end = requests[-1][0]
    work = Path(tempfile.mkdtemp(prefix="histogram-compare-"))
    processes = []
    try:
        # --- Prometheus, fed over OTLP
        prom_port = free_port()
        (work / "prom.yml").write_text("storage:\n  tsdb:\n    out_of_order_time_window: 2h\n")
        processes.append(subprocess.Popen(
            [prometheus, f"--web.listen-address=127.0.0.1:{prom_port}", f"--storage.tsdb.path={work/'prom'}",
             f"--config.file={work/'prom.yml'}", "--web.enable-otlp-receiver"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        prom = f"http://127.0.0.1:{prom_port}"
        wait_ready(prom + "/-/ready", "prometheus")
        for _, request in requests:
            req = urllib.request.Request(prom + "/api/v1/otlp/v1/metrics", data=request.SerializeToString(),
                                         headers={"Content-Type": "application/x-protobuf"})
            urllib.request.urlopen(req, timeout=30).read()

        # --- canardstack, fed through the writer
        import duckdb
        catalog, data = work / "catalog.sqlite", work / "data"
        data.mkdir()
        sqlite3.connect(catalog).execute("PRAGMA journal_mode=WAL").close()
        con = duckdb.connect()
        for ext, repo in (("otlp", "community"), ("ducklake", "core"), ("sqlite", "core")):
            con.execute(f"INSTALL {ext} FROM {repo}")
            con.execute(f"LOAD {ext}")
        con.execute(f"ATTACH 'ducklake:sqlite:{catalog}' AS lake (DATA_PATH '{data}/')")
        events = [MessageToJson(r, indent=None).encode() for _, r in requests]
        rows = ingest_batch(con, "metrics", events, "histogram-compare", 0, len(events) - 1)
        print(f"committed {rows} histogram rows from {len(events)} requests")
        con.close()
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

        failures = 0
        for q in QUERIES:
            for kind, path, params in (
                ("instant", "/api/v1/query", {"query": q, "time": end - 30}),
                ("range", "/api/v1/query_range", {"query": q, "start": start + 300, "end": end - 30, "step": 30}),
            ):
                ref = normalise(get(prom, path, params))
                got = normalise(get(cs, path, params, auth))
                problem = diff(ref, got)
                if not problem and not ref:
                    # Every query here selects data, so two empty answers mean the data is missing.
                    problem = "both answers are empty"
                if problem:
                    failures += 1
                    print(f"FAIL {kind:7} {q}\n     {problem}")
        total = len(QUERIES) * 2
        print(f"{total - failures}/{total} comparisons match")
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
