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
import struct
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
        # Only counters are sums. Histogram and summary components (`_bucket`, `_count`, `_sum`)
        # have no OTLP counterpart here, so they are stored as gauges and keep their names.
        monotonic = family.type == "counter"
        for sample in family.samples:
            if sample.timestamp is None:
                continue
            labels = dict(sample.labels)
            instance = labels.pop("instance", "")
            ts = str(nanos(sample.timestamp))
            point = {"timeUnixNano": ts, "startTimeUnixNano": ts, "asDouble": float(sample.value),
                     "attributes": [attr(k, v) for k, v in sorted(labels.items())]}
            metric = by_instance.setdefault(instance, {}).setdefault(
                sample.name,
                {"name": sample.name, "monotonic": monotonic and sample.name.endswith("_total"), "points": []},
            )
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


# --- The Prometheus protobuf exposition (delimited MetricFamily messages). The text format cannot
# carry native histograms, so the bridge reads protobuf, which has everything the text has.

PROTO_ACCEPT = "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited"


def _varint(buf: bytes, pos: int) -> tuple[int, int]:
    shift = result = 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7


def _fields(buf: bytes):
    """The (field number, wire type, value) triples of one message."""
    pos = 0
    while pos < len(buf):
        key, pos = _varint(buf, pos)
        number, wire = key >> 3, key & 7
        if wire == 0:
            value, pos = _varint(buf, pos)
        elif wire == 1:
            value, pos = buf[pos:pos + 8], pos + 8
        elif wire == 2:
            length, pos = _varint(buf, pos)
            value, pos = buf[pos:pos + length], pos + length
        elif wire == 5:
            value, pos = buf[pos:pos + 4], pos + 4
        else:
            raise ValueError(f"unsupported protobuf wire type {wire}")
        yield number, wire, value


def _zigzag(n: int) -> int:
    return (n >> 1) ^ -(n & 1)


def _double(raw: bytes) -> float:
    return struct.unpack("<d", raw)[0]


def _signed_list(items) -> list[int]:
    """A repeated sint64/sint32 field, packed or not."""
    out = []
    for wire, value in items:
        if wire == 2:
            pos = 0
            while pos < len(value):
                n, pos = _varint(value, pos)
                out.append(_zigzag(n))
        else:
            out.append(_zigzag(value))
    return out


def _spans(messages) -> list[tuple[int, int]]:
    spans = []
    for raw in messages:
        offset = length = 0
        for number, _, value in _fields(raw):
            if number == 1:
                offset = _zigzag(value)
            elif number == 2:
                length = value
        spans.append((offset, length))
    return spans


def _dense(spans: list[tuple[int, int]], deltas: list[int]) -> tuple[int, list[int]]:
    """Absolute counts of native histogram buckets as (index of the first one, counts)."""
    counts: dict[int, int] = {}
    index = cumulative = at = 0
    for offset, length in spans:
        index += offset
        for _ in range(length):
            cumulative += deltas[at]
            at += 1
            counts[index] = cumulative
            index += 1
    if not counts:
        return 0, []
    low, high = min(counts), max(counts)
    return low, [counts.get(i, 0) for i in range(low, high + 1)]


def _histogram(raw: bytes) -> dict:
    h = {"buckets": [], "pos_spans": [], "neg_spans": [], "pos_delta": [], "neg_delta": [], "schema": None,
         "zero_threshold": 0.0, "zero_count": 0, "count": 0, "sum": 0.0}
    pos_delta, neg_delta, pos_spans, neg_spans = [], [], [], []
    for number, wire, value in _fields(raw):
        if number == 1:
            h["count"] = value
        elif number == 2:
            h["sum"] = _double(value)
        elif number == 3:
            bucket = {"count": 0, "bound": float("inf")}
            for n, _, v in _fields(value):
                if n == 1:
                    bucket["count"] = v
                elif n == 2:
                    bucket["bound"] = _double(v)
            h["buckets"].append(bucket)
        elif number == 5:
            h["schema"] = _zigzag(value)
        elif number == 6:
            h["zero_threshold"] = _double(value)
        elif number == 7:
            h["zero_count"] = value
        elif number == 9:
            neg_spans.append(value)
        elif number == 10:
            neg_delta.append((wire, value))
        elif number == 12:
            pos_spans.append(value)
        elif number == 13:
            pos_delta.append((wire, value))
    h["pos_spans"], h["neg_spans"] = _spans(pos_spans), _spans(neg_spans)
    h["pos_delta"], h["neg_delta"] = _signed_list(pos_delta), _signed_list(neg_delta)
    return h


def _family(raw: bytes) -> dict:
    family = {"name": "", "type": 3, "metrics": []}
    for number, _, value in _fields(raw):
        if number == 1:
            family["name"] = value.decode()
        elif number == 3:
            family["type"] = value
        elif number == 4:
            metric = {"labels": {}, "kind": None, "value": 0.0, "histogram": None, "timestamp_ms": None}
            for n, _, v in _fields(value):
                if n == 1:
                    name = val = ""
                    for ln, _, lv in _fields(v):
                        if ln == 1:
                            name = lv.decode()
                        elif ln == 2:
                            val = lv.decode()
                    metric["labels"][name] = val
                elif n in (2, 3, 5):  # gauge, counter, untyped: all start with `double value = 1`
                    metric["kind"] = {2: "gauge", 3: "counter", 5: "untyped"}[n]
                    for vn, _, vv in _fields(v):
                        if vn == 1:
                            metric["value"] = _double(vv)
                elif n == 7:
                    metric["histogram"] = _histogram(v)
                elif n == 6:
                    metric["timestamp_ms"] = v
            family["metrics"].append(metric)
    return family


def _families(payload: bytes) -> list[dict]:
    families, pos = [], 0
    while pos < len(payload):
        length, pos = _varint(payload, pos)
        families.append(_family(payload[pos:pos + length]))
        pos += length
    return families


def to_otlp_proto(payload: bytes, job: str) -> dict | None:
    """One scrape (protobuf) as an OTLP/JSON metrics export request: counters as monotonic sums,
    gauges as gauges, classic histograms as explicit-bucket histograms and native histograms as
    exponential histograms."""
    by_instance: dict[str, dict[str, dict]] = {}
    for family in _families(payload):
        for metric in family["metrics"]:
            if metric["timestamp_ms"] is None:
                continue
            labels = dict(metric["labels"])
            instance = labels.pop("instance", "")
            ts = str(metric["timestamp_ms"] * 10**6)
            attributes = [attr(k, v) for k, v in sorted(labels.items())]
            hist = metric["histogram"]
            if hist is not None and hist["schema"] is not None:
                kind, name = "exp", family["name"]
                p_off, p_counts = _dense(hist["pos_spans"], hist["pos_delta"])
                n_off, n_counts = _dense(hist["neg_spans"], hist["neg_delta"])
                point = {"timeUnixNano": ts, "startTimeUnixNano": ts, "count": str(hist["count"]), "sum": hist["sum"],
                         "scale": hist["schema"], "zeroCount": str(hist["zero_count"]), "zeroThreshold": hist["zero_threshold"],
                         # OTLP bucket i covers (base^i, base^(i+1)]; Prometheus index i covers (base^(i-1), base^i].
                         "positive": {"offset": p_off - 1, "bucketCounts": [str(c) for c in p_counts]},
                         "negative": {"offset": n_off - 1, "bucketCounts": [str(c) for c in n_counts]},
                         "attributes": attributes}
            elif hist is not None:
                kind, name = "histogram", family["name"]
                finite = [b for b in hist["buckets"] if b["bound"] != float("inf")]
                cumulative = [b["count"] for b in finite]
                counts = [c - (cumulative[i - 1] if i else 0) for i, c in enumerate(cumulative)]
                counts.append(hist["count"] - (cumulative[-1] if cumulative else 0))
                point = {"timeUnixNano": ts, "startTimeUnixNano": ts, "count": str(hist["count"]), "sum": hist["sum"],
                         "bucketCounts": [str(c) for c in counts], "explicitBounds": [b["bound"] for b in finite],
                         "attributes": attributes}
            else:
                name = family["name"]
                kind = "sum" if metric["kind"] == "counter" and name.endswith("_total") else "gauge"
                point = {"timeUnixNano": ts, "startTimeUnixNano": ts, "asDouble": float(metric["value"]), "attributes": attributes}
            entry = by_instance.setdefault(instance, {}).setdefault((name, kind), {"name": name, "kind": kind, "points": []})
            entry["points"].append(point)
    if not by_instance:
        return None
    resources = []
    for instance, metrics in by_instance.items():
        otlp_metrics = []
        for m in metrics.values():
            points = m["points"]
            if m["kind"] == "sum":
                body = {"sum": {"dataPoints": points, "aggregationTemporality": 2, "isMonotonic": True}}
            elif m["kind"] == "histogram":
                body = {"histogram": {"dataPoints": points, "aggregationTemporality": 2}}
            elif m["kind"] == "exp":
                body = {"exponentialHistogram": {"dataPoints": points, "aggregationTemporality": 2}}
            else:
                body = {"gauge": {"dataPoints": points}}
            otlp_metrics.append({"name": m["name"], **body})
        attrs = [attr("service.name", job)] + ([attr("service.instance.id", instance)] if instance else [])
        resources.append({"resource": {"attributes": attrs},
                          "scopeMetrics": [{"scope": {"name": ""}, "metrics": otlp_metrics}]})
    return {"resourceMetrics": resources}


def scrape_proto(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"Accept": PROTO_ACCEPT})
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.read()


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
        if os.environ.get("BRIDGE_FORMAT") == "text":
            request = to_otlp(scrape(url), args.job)
        else:
            request = to_otlp_proto(scrape_proto(url), args.job)
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
