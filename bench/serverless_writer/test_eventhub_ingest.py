#!/usr/bin/env python3
"""Local test for eventhub_ingest.py: no Azure needed.

    uv run --project bench bench/serverless_writer/test_eventhub_ingest.py

Checks that JSON and protobuf events ingest, that redelivering a batch (same partition and
sequence numbers) writes nothing, and that a bad batch rolls back rows and offsets together.
"""

import json
import sys
import tempfile
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from eventhub_ingest import ingest_batch  # noqa: E402
from workload import Workload  # noqa: E402

root = Path(tempfile.mkdtemp())
(root / "data").mkdir()
con = duckdb.connect()
for ext, repo in (("otlp", "community"), ("ducklake", "core"), ("sqlite", "core")):
    con.execute(f"INSTALL {ext} FROM {repo}")
    con.execute(f"LOAD {ext}")
con.execute(f"ATTACH 'ducklake:sqlite:{root}/lake.sqlite' AS lake (DATA_PATH '{root}/data/')")
count = lambda table: con.execute(f"SELECT count(*) FROM lake.{table}").fetchone()[0]  # noqa: E731

w = Workload(1, 1, 400, (int(time.time()) - 900) * 10**9, run="eh")
json_events = [json.dumps(p).encode() for p in w.logs()]          # 2 requests x 200 records
expected = sum(len(p["resourceLogs"][0]["scopeLogs"][0]["logRecords"]) for p in w.logs())

wrote = ingest_batch(con, "logs", json_events, partition="0", first_seq=100, last_seq=101)
assert wrote == expected == count("otlp_logs"), (wrote, expected, count("otlp_logs"))
print(f"JSON batch: {wrote} rows written")

again = ingest_batch(con, "logs", json_events, partition="0", first_seq=100, last_seq=101)
assert again == 0 and count("otlp_logs") == expected
print("redelivered batch: skipped, row count unchanged")

other = ingest_batch(con, "logs", json_events, partition="1", first_seq=100, last_seq=101)
assert other == expected and count("otlp_logs") == 2 * expected
print("same sequence numbers on another partition: written (offsets are per partition)")

before = count("otlp_logs"), count("ingest_offsets")
try:
    ingest_batch(con, "logs", [b'{"resourceLogs": [not json'], partition="0", first_seq=200, last_seq=200)
    raise SystemExit("bad batch should have raised")
except duckdb.Error:
    pass
assert (count("otlp_logs"), count("ingest_offsets")) == before
print("bad batch: raised, rows and offsets unchanged")

try:
    from otlp import encode  # opentelemetry-proto is in bench/pyproject.toml
except ImportError:
    print("protobuf check skipped: opentelemetry-proto is not installed in this interpreter")
else:
    pb_events = [encode("traces", p) for p in Workload(1, 1, 100, (int(time.time()) - 900) * 10**9, run="pb").traces()]
    spans = ingest_batch(con, "traces", pb_events, partition="0", first_seq=1, last_seq=1)
    assert spans == 200 and count("otlp_traces") == 200, spans
    print(f"protobuf batch: {spans} spans written")
print("all checks passed")
