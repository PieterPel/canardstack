#!/usr/bin/env python3
"""Tabulate sweep results: p50 per query and size, how it scales, and canardstack against LGTM.

    uv run --project bench bench/scale_report.py [--users 0] [--clients 1]

For each (stack, attribute cardinality) the newest result per size is used. The slope is the fitted
exponent of p50 against records on log-log axes over the sizes that answered correctly: about 0 means
latency does not depend on the data, about 1 means it grows in proportion. Cells that did not answer
correctly show `err` or `wrong` with the first failure below the table.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

HERE = Path(__file__).parent


def slope(points: List[Tuple[float, float]]) -> Optional[float]:
    points = [(math.log(n), math.log(ms)) for n, ms in points if ms and ms > 0]
    if len(points) < 3:
        return None
    mx = sum(x for x, _ in points) / len(points)
    my = sum(y for _, y in points) / len(points)
    var = sum((x - mx) ** 2 for x, _ in points)
    return sum((x - mx) * (y - my) for x, y in points) / var if var else None


def verdict(b: Optional[float]) -> str:
    if b is None:
        return "?"
    return "flat" if b < 0.15 else "sublinear" if b < 0.6 else "LINEAR+" if b < 1.3 else "SUPERLINEAR"


def cell(entry: Optional[dict]) -> str:
    if entry is None:
        return "-"
    p50 = (entry.get("latency_ms") or {}).get("p50")
    if entry["ok"] == 0:
        return "err" if entry["errors"] >= entry["wrong"] else "wrong"
    text = f"{p50:.0f}" if p50 >= 10 else f"{p50:.1f}"
    return text + ("*" if entry["ok"] < entry["runs"] else "")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, default=0)
    ap.add_argument("--clients", default="clients_1")
    ap.add_argument("--results", type=Path, default=HERE / "results")
    args = ap.parse_args()
    clients = args.clients if args.clients.startswith("clients_") else f"clients_{args.clients}"

    # (stack, users) -> records -> (started, query results)
    runs: Dict[Tuple[str, int], Dict[int, Tuple[str, dict]]] = defaultdict(dict)
    for path in sorted(args.results.glob("*.json")):
        data = json.loads(path.read_text())
        workload = data.get("workload", {})
        if workload.get("intents") not in ("all", "scale") or clients not in data.get("queries", {}):
            continue
        key = (data["stack"], workload.get("users", 0))
        runs[key][workload["records"]] = (data["started"], data["queries"][clients])

    stacks = sorted(s for (s, u) in runs if u == args.users)
    if not stacks:
        raise SystemExit(f"no sweep results with --users {args.users} in {args.results}")
    sizes = sorted({n for s in stacks for n in runs[(s, args.users)]})
    queries: List[str] = []
    for s in stacks:
        for _, qs in runs[(s, args.users)].values():
            queries += [q for q in qs if q not in queries]

    head = f"{'query':24s} {'stack':16s}" + "".join(f"{n:>9d}" for n in sizes) + "   slope  scales"
    print(f"p50 in ms, {clients.replace('_', ' ')}, log attribute cardinality {args.users}, records per service\n")
    print(head)
    print("-" * len(head))
    failures: Dict[str, str] = {}
    for q in queries:
        for s in stacks:
            per = {n: runs[(s, args.users)].get(n, ("", {}))[1].get(q) for n in sizes}
            if not any(per.values()):
                continue
            pts = [(n, e["latency_ms"]["p50"]) for n, e in per.items() if e and e["ok"] == e["runs"]]
            b = slope(pts)
            print(f"{q:24s} {s:16s}" + "".join(f"{cell(per[n]):>9s}" for n in sizes)
                  + f"   {'' if b is None else format(b, '5.2f')}  {verdict(b)}")
            for n, e in per.items():
                if e and e["ok"] < e["runs"]:
                    why = e.get("first_error") or json.dumps(e.get("first_wrong"))
                    failures.setdefault(f"{s} {q} at {n}", str(why)[:160])
        print()
    if len(stacks) >= 2 and "lgtm" in stacks:
        top = max(n for n in sizes if all(runs[(s, args.users)].get(n) for s in stacks))
        print(f"canardstack-over-LGTM p50 ratio at {top} records per service (above 1: canardstack slower)")
        for q in queries:
            row = []
            for s in stacks:
                if s == "lgtm":
                    continue
                a = runs[(s, args.users)].get(top, ("", {}))[1].get(q)
                b = runs[("lgtm", args.users)].get(top, ("", {}))[1].get(q)
                if a and b and a["ok"] == a["runs"] and b["ok"] == b["runs"]:
                    row.append(f"{s} {a['latency_ms']['p50'] / b['latency_ms']['p50']:.1f}x")
                else:
                    row.append(f"{s} n/a")
            print(f"  {q:24s} " + "  ".join(row))
        print()
    if failures:
        print("* partial, err and wrong cells (first failure):")
        for k, v in failures.items():
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
