#!/usr/bin/env python3
"""Render result JSONs as Markdown, aggregating trials per stack.

    uv run --project bench bench/report.py [results/a.json results/b.json ...]

With several result files for one stack (trials), every number is the median
across trials and the [min-max] range shows the spread between trials.
"""

from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

HERE = Path(__file__).parent


def load(paths: List[str]) -> Dict[str, List[dict]]:
    files = [Path(p) for p in paths] or sorted((HERE / "results").glob("*.json"))
    by_stack: Dict[str, List[dict]] = defaultdict(list)
    for path in files:
        report = json.loads(path.read_text())
        by_stack[report["stack"]].append(report)
    return dict(by_stack)


def span(values: List[float], digits: int = 1) -> str:
    values = [v for v in values if v is not None]
    if not values:
        return "n/a"
    med = statistics.median(values)
    if len(values) == 1:
        return f"{med:.{digits}f}"
    return f"{med:.{digits}f} [{min(values):.{digits}f}-{max(values):.{digits}f}]"


def main() -> None:
    stacks = load(sys.argv[1:])
    if not stacks:
        raise SystemExit("no results found")
    first = next(iter(stacks.values()))[0]["workload"]
    trials = {name: len(reports) for name, reports in stacks.items()}
    print(f"Workload: seed={first['seed']}, {first['services']} services x {first['records']} records "
          f"per signal. Trials per stack: {trials}. Numbers are median [min-max] across trials.\n")

    print("## Ingest (accepted, not durable) and visibility\n")
    print("| stack | signal | accepted rows/s | visible after accept (s) |")
    print("| --- | --- | --- | --- |")
    for name, reports in stacks.items():
        for signal in ("logs", "metrics", "traces"):
            rate = [r["ingest"][signal]["accepted_rows_per_s"] for r in reports]
            vis = [r["visibility"][signal]["visible_seconds_after_accept"] for r in reports]
            timed_out = sum(r["visibility"][signal]["timed_out"] for r in reports)
            note = f" ({timed_out} timed out)" if timed_out else ""
            print(f"| {name} | {signal} | {span(rate, 0)} | {span(vis, 2)}{note} |")

    for clients in sorted({c for reports in stacks.values() for r in reports for c in r["queries"]}):
        print(f"\n## Queries, {clients.replace('_', ' ')}\n")
        print("| query | stack | ok/runs | wrong | errors | p50 ms | p95 ms | qps |")
        print("| --- | --- | --- | --- | --- | --- | --- | --- |")
        names = list(next(iter(stacks.values()))[0]["queries"][clients])
        for query in names:
            for name, reports in stacks.items():
                rows = [r["queries"][clients][query] for r in reports if query in r["queries"].get(clients, {})]
                if not rows:
                    print(f"| {query} | {name} | not supported | | | | | |")
                    continue
                print(f"| {query} | {name} | {sum(q['ok'] for q in rows)}/{sum(q['runs'] for q in rows)} | "
                      f"{sum(q['wrong'] for q in rows)} | {sum(q['errors'] for q in rows)} | "
                      f"{span([q['latency_ms']['p50'] for q in rows])} | "
                      f"{span([q['latency_ms']['p95'] for q in rows])} | "
                      f"{span([q['throughput_qps'] for q in rows], 0)} |")

    print("\n## Container resource use (peak per run)\n")
    print("| stack | container | cpu mean % | cpu max % | mem max MB |")
    print("| --- | --- | --- | --- | --- |")
    for name, reports in stacks.items():
        for container in reports[0]["containers"]:
            c = [r["containers"][container] for r in reports if container in r["containers"]]
            print(f"| {name} | {container} | {span([x['cpu_mean_pct'] for x in c])} | "
                  f"{span([x['cpu_max_pct'] for x in c])} | {span([x['mem_max_mb'] for x in c], 0)} |")

    noise = [r["environment"].get("background_cpu_pct_mean") for reports in stacks.values() for r in reports]
    noise = [n for n in noise if n is not None]
    print(f"\nBackground CPU from containers outside the stack: mean {statistics.fmean(noise):.1f}% "
          f"(max {max(noise):.1f}%)." if noise else "")
    print("\nCaveats: single-node local containers (2.0 CPU / 3 GiB per stack), Grafana and collectors not in the "
          "measured path, query languages differ per stack (see bench/README.md).")


if __name__ == "__main__":
    main()
