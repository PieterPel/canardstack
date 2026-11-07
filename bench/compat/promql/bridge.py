#!/usr/bin/env python3
"""Scrape a Prometheus/OpenMetrics endpoint and commit the samples to DuckLake as OTLP metrics.

    bridge.py init CATALOG                       create the SQLite catalog in WAL mode
    bridge.py run  CATALOG DATA --scrape HOST:PORT --interval SECONDS --job NAME

This is what an OpenTelemetry Collector's prometheus receiver feeds duckdb-otlp: one OTLP export
per scrape, one resource per `instance`, the remaining labels as metric attributes, counters as
monotonic cumulative sums, everything else as gauges. Samples keep the timestamps the target
exposes. The rows go through `read_otlp_metrics_*`, so the stored shape is the real ingest shape.
"""

import argparse
import os
import re
import traceback
from decimal import Decimal
import json
import queue
import sqlite3
import sys
import threading
import time
import urllib.request
from pathlib import Path

import duckdb
from prometheus_client.openmetrics.parser import text_string_to_metric_families

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "serverless_writer"))
from eventhub_ingest import ingest_batch  # noqa: E402

BATCH_SCRAPES = 12  # scrapes committed per DuckLake transaction


def attr(key, value):
    return {"key": key, "value": {"stringValue": value}}


_EXPONENT_TIMESTAMP = re.compile(r"^(?P<head>[^#].*\s)(?P<ts>[0-9.]+[eE][+-]?[0-9]+)$")


def expand_timestamps(text: str) -> str:
    """Rewrite exponent timestamps (1.79e+09) as decimals; the OpenMetrics parser misreads them."""
    lines = []
    for line in text.split("\n"):
        m = _EXPONENT_TIMESTAMP.match(line)
        if m:
            line = m.group("head") + format(Decimal(m.group("ts")), "f")
        lines.append(line)
    return "\n".join(lines)


def nanos(timestamp) -> int:
    """Sample timestamps arrive as a float or as the parser's Timestamp(sec, nsec)."""
    if hasattr(timestamp, "sec"):
        return timestamp.sec * 10**9 + timestamp.nsec
    return int(round(timestamp * 1e9))


def to_otlp(text: str, job: str) -> dict | None:
    """One scrape (OpenMetrics text) as an OTLP/JSON metrics export request."""
    by_instance: dict[str, dict[str, dict]] = {}
    for family in text_string_to_metric_families(expand_timestamps(text)):
        monotonic = family.type == "counter" or family.type in ("histogram", "summary")
        for sample in family.samples:
            if sample.timestamp is None:
                continue
            labels = dict(sample.labels)
            instance = labels.pop("instance", "")
            ts = str(nanos(sample.timestamp))
            point = {"timeUnixNano": ts, "startTimeUnixNano": ts, "asDouble": float(sample.value),
                     "attributes": [attr(k, v) for k, v in sorted(labels.items())]}
            metric = by_instance.setdefault(instance, {}).setdefault(
                sample.name, {"name": sample.name, "monotonic": monotonic, "points": []})
            metric["points"].append(point)
    if not by_instance:
        return None
    resources = []
    for instance, metrics in by_instance.items():
        otlp_metrics = []
        for m in metrics.values():
            if m["monotonic"]:
                body = {"sum": {"dataPoints": m["points"], "aggregationTemporality": 2, "isMonotonic": True}}
            else:
                body = {"gauge": {"dataPoints": m["points"]}}
            otlp_metrics.append({"name": m["name"], **body})
        attrs = [attr("service.name", job)] + ([attr("service.instance.id", instance)] if instance else [])
        resources.append({"resource": {"attributes": attrs},
                          "scopeMetrics": [{"scope": {"name": ""}, "metrics": otlp_metrics}]})
    return {"resourceMetrics": resources}


def scrape(url: str) -> str:
    request = urllib.request.Request(url, headers={"Accept": "application/openmetrics-text; version=1.0.0"})
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.read().decode()


def run(args) -> None:
    con = duckdb.connect()
    for ext, repo in (("otlp", "community"), ("ducklake", "core"), ("sqlite", "core")):
        con.execute(f"INSTALL {ext} FROM {repo}")
        con.execute(f"LOAD {ext}")
    con.execute(f"ATTACH 'ducklake:sqlite:{args.catalog}' AS lake (DATA_PATH '{args.data}/')")
    pending: "queue.Queue[bytes]" = queue.Queue()

    def writer() -> None:
        seq = 0
        try:
            while True:
                events = [pending.get()]
                deadline = time.monotonic() + 0.3
                while len(events) < BATCH_SCRAPES and time.monotonic() < deadline:
                    try:
                        events.append(pending.get(timeout=0.05))
                    except queue.Empty:
                        pass
                ingest_batch(con, "metrics", events, "bridge", seq, seq + len(events) - 1)
                seq += len(events)
                for _ in events:
                    pending.task_done()
        except Exception:
            traceback.print_exc()
            os._exit(1)  # a silent writer would leave the suite querying incomplete data

    threading.Thread(target=writer, daemon=True).start()
    url = f"http://{args.scrape}/metrics"
    seen_samples = signalled = False
    while True:
        request = to_otlp(scrape(url), args.job)
        if request is not None:
            seen_samples = True
            pending.put(json.dumps(request, separators=(",", ":")).encode())
        elif seen_samples and not signalled and args.done_file:
            # The replay is exhausted (the target now serves empty scrapes): once everything
            # scraped is committed, tell the harness the data is complete.
            pending.join()
            Path(args.done_file).touch()
            signalled = True
        time.sleep(args.interval)


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    init = sub.add_parser("init")
    init.add_argument("catalog")
    r = sub.add_parser("run")
    r.add_argument("catalog")
    r.add_argument("data")
    r.add_argument("--scrape", required=True)
    r.add_argument("--interval", type=float, default=0.015)
    r.add_argument("--job", default="demo")
    r.add_argument("--done-file", help="touched once the scrape source is exhausted and all committed")
    args = parser.parse_args()
    if args.cmd == "init":
        connection = sqlite3.connect(args.catalog)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.close()
    else:
        run(args)


if __name__ == "__main__":
    main()
