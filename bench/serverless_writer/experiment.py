#!/usr/bin/env python3
"""Cold one-shot writers against DuckLake: what happens to files, snapshots and query time?

    python experiment.py [--invocations 40] [--records 500]

Runs the same N cold invocations (each a fresh process, see writer.py) twice against a local
SQLite catalog: once with DuckLake's default inlining limit (10 rows) and once with inlining on.
After each it counts live files, snapshots and inlined rows, times three queries, runs the
maintenance that fits (merge + expire + cleanup, or flush), and measures again.

Run it with `uv run --project bench bench/serverless_writer/experiment.py`. Cold-start numbers are for the machine you run it on
with extensions already installed; a real function adds a remote catalog and object-store latency.
"""

import argparse
import json
import os
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from workload import Workload  # noqa: E402  (stdlib-only OTLP/JSON generator shared with bench/)

EXTENSIONS = (("otlp", "community"), ("ducklake", "core"), ("sqlite", "core"))
QUERIES = {
    "one batch, service filter": "SELECT time_unix_nano::VARCHAR, body FROM lake.otlp_logs WHERE service_name = '{svc}' ORDER BY time_unix_nano DESC LIMIT 1001",
    "newest 1,000 of everything": "SELECT time_unix_nano::VARCHAR, body FROM lake.otlp_logs ORDER BY time_unix_nano DESC LIMIT 1001",
}


def make_batches(directory: Path, invocations: int, records: int) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    base = (int(time.time()) - 3600) * 10**9
    keys = {"logs": "resourceLogs", "metrics": "resourceMetrics", "traces": "resourceSpans"}
    for n in range(invocations):
        w = Workload(1, 1, records, base + n * records * 10**9, run=f"sl{n}")
        for signal, make in (("logs", w.logs), ("metrics", w.metrics), ("traces", w.traces)):
            payloads = list(make())
            merged = payloads[0]
            for extra in payloads[1:]:
                merged[keys[signal]].extend(extra[keys[signal]])
            (directory / f"{n}.{signal}.json").write_text(json.dumps(merged))


def connect(root: Path, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    for ext, repo in EXTENSIONS:
        con.execute(f"INSTALL {ext} FROM {repo}")
        con.execute(f"LOAD {ext}")
    mode = ", READ_ONLY" if read_only else ""
    con.execute(f"ATTACH 'ducklake:sqlite:{root}/lake.sqlite' AS lake (DATA_PATH '{root}/data/'{mode})")
    return con


def run_invocations(root: Path, invocations: int) -> dict:
    walls, phases = [], []
    for n in range(invocations):
        started = time.perf_counter()
        out = subprocess.run([sys.executable, str(HERE / "writer.py"), str(n), str(root)], capture_output=True, text=True)
        walls.append(time.perf_counter() - started)
        if out.returncode:
            sys.exit(f"invocation {n} failed:\n{out.stderr[-500:]}")
        phases.append([float(x) for x in out.stdout.strip().splitlines()[-1].split(",")[1:]])
    return {
        "wall_median_s": statistics.median(walls[1:] or walls),  # first call may download extensions
        "first_wall_s": walls[0],
        "load_s": statistics.median(p[0] for p in phases),
        "insert_commit_s": statistics.median(p[2] for p in phases),
    }


def inspect(root: Path, svc: str) -> dict:
    meta = sqlite3.connect(root / "lake.sqlite")
    snapshots = meta.execute("select count(*) from ducklake_snapshot").fetchone()[0]
    files = meta.execute("select count(*) from ducklake_data_file where end_snapshot is null").fetchone()[0]
    inlined = sum(
        meta.execute(f'select count(*) from "{name}" where end_snapshot is null').fetchone()[0]
        for (name,) in meta.execute("select name from sqlite_master where name glob 'ducklake_inlined_data_[0-9]*_[0-9]*'")
    )
    meta.close()
    con = connect(root, read_only=True)
    con.execute("SET disabled_optimizers='late_materialization'")
    timings = {}
    for label, sql in QUERIES.items():
        samples = []
        for _ in range(15):
            cursor = con.cursor()
            started = time.perf_counter()
            cursor.execute(sql.format(svc=svc)).fetchall()
            samples.append((time.perf_counter() - started) * 1000)
        timings[label] = statistics.median(samples)
    return {"snapshots": snapshots, "files": files, "inlined_rows": inlined, "queries_ms": timings}


def run_mode(label: str, base: Path, batches: Path, invocations: int, inlining: bool) -> list:
    root = base / label
    (root / "data").mkdir(parents=True)
    (root / "batches").symlink_to(batches)
    if inlining:
        con = connect(root)
        con.execute("CALL lake.set_option('data_inlining_row_limit', 5000)")  # persisted in the catalog
        con.close()
    timing = run_invocations(root, invocations)
    rows = [(f"{label}: after {invocations} invocations", timing, inspect(root, "svc-sl5-0"))]
    con = connect(root)
    started = time.perf_counter()
    if inlining:
        con.execute("CALL ducklake_flush_inlined_data('lake')").fetchall()
        step = "flush inlined data"
    else:
        con.execute("CALL ducklake_merge_adjacent_files('lake')").fetchall()
        con.execute("CALL ducklake_expire_snapshots('lake', older_than => now() + INTERVAL 1 MINUTE)").fetchall()
        con.execute("CALL ducklake_cleanup_old_files('lake', cleanup_all => true)").fetchall()
        step = "merge + expire + cleanup"
    took = time.perf_counter() - started
    con.close()
    rows.append((f"{label}: after {step} ({took:.2f} s)", None, inspect(root, "svc-sl5-0")))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--invocations", type=int, default=40)
    parser.add_argument("--records", type=int, default=500, help="records per signal per invocation")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        batches = base / "batches"
        make_batches(batches, args.invocations, args.records)
        rows = []
        for label, inlining in (("default (inlining limit 10)", False), ("inlining on (limit 5000)", True)):
            rows += run_mode(label, base, batches, args.invocations, inlining)
    print("\n| state | files | snapshots | inlined rows | one-batch query ms | newest-1,000 query ms |")
    print("| --- | --- | --- | --- | --- | --- |")
    for label, _, info in rows:
        q = info["queries_ms"]
        print(f"| {label} | {info['files']} | {info['snapshots']} | {info['inlined_rows']} | "
              f"{q['one batch, service filter']:.1f} | {q['newest 1,000 of everything']:.1f} |")
    print("\n| mode | cold invocation wall (median) | first (installs extensions) | load extensions | insert + commit |")
    print("| --- | --- | --- | --- | --- |")
    for label, timing, _ in rows:
        if timing:
            print(f"| {label.split(':')[0]} | {timing['wall_median_s']:.2f} s | {timing['first_wall_s']:.2f} s | "
                  f"{timing['load_s']:.2f} s | {timing['insert_commit_s']:.2f} s |")


if __name__ == "__main__":
    main()
