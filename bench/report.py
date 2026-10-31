#!/usr/bin/env python3
"""Render result JSONs (latest per stack, or the files given) as Markdown tables.

    bench/.venv/bin/python bench/report.py [results/a.json results/b.json ...]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List

HERE = Path(__file__).parent


def load(paths: List[str]) -> List[dict]:
    if paths:
        return [json.loads(Path(p).read_text()) for p in paths]
    latest: Dict[str, Path] = {}
    for path in sorted((HERE / "results").glob("*.json")):
        latest[path.name.split("-")[0]] = path
    return [json.loads(p.read_text()) for p in latest.values()]


def fmt(value, digits=1, unit=""):
    return "n/a" if value is None else f"{value:.{digits}f}{unit}"


def main() -> None:
    reports = load(sys.argv[1:])
    if not reports:
        raise SystemExit("no results found")
    first = reports[0]["workload"]
    print(f"Workload: seed={first['seed']}, {first['services']} services x {first['records']} records "
          f"per signal ({first['totals']})\n")

    print("## Ingest (accepted, not durable) and visibility\n")
    print("| stack | signal | accepted rows/s | visible after accept (s) |")
    print("| --- | --- | --- | --- |")
    for r in reports:
        for signal in ("logs", "metrics", "traces"):
            ing, vis = r["ingest"][signal], r["visibility"][signal]
            seen = "timed out" if vis["timed_out"] else fmt(vis["visible_seconds_after_accept"], 2)
            print(f"| {r['stack']} | {signal} | {fmt(ing['accepted_rows_per_s'], 0)} | {seen} |")

    for clients in sorted({c for r in reports for c in r["queries"]}):
        print(f"\n## Queries, {clients.replace('_', ' ')}\n")
        print("| query | stack | ok/runs | wrong | errors | p50 ms | p95 ms | p99 ms | qps |")
        print("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        names = [q for q in reports[0]["queries"][clients]]
        for name in names:
            for r in reports:
                q = r["queries"].get(clients, {}).get(name)
                if q is None:
                    print(f"| {name} | {r['stack']} | not supported | | | | | | |")
                    continue
                lat = q["latency_ms"]
                print(f"| {name} | {r['stack']} | {q['ok']}/{q['runs']} | {q['wrong']} | {q['errors']} | "
                      f"{fmt(lat['p50'])} | {fmt(lat['p95'])} | {fmt(lat['p99'])} | {fmt(q['throughput_qps'])} |")

    print("\n## Container resource use (whole run)\n")
    print("| stack | container | cpu mean % | cpu max % | mem max MB |")
    print("| --- | --- | --- | --- | --- |")
    for r in reports:
        for name, c in r["containers"].items():
            print(f"| {r['stack']} | {name} | {fmt(c['cpu_mean_pct'])} | {fmt(c['cpu_max_pct'])} | "
                  f"{fmt(c['mem_max_mb'], 0)} |")

    print("\nCaveats: single-node local containers, no CPU/memory limits, Grafana and collectors not in the "
          "measured path, query languages differ per stack (see bench/README.md).")


if __name__ == "__main__":
    main()
