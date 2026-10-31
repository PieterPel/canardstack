#!/usr/bin/env python3
"""Run the shared workload against one stack and write a result JSON.

    bench/.venv/bin/python bench/run.py --stack lgtm --services 5 --records 1000

Phases: ingest -> visibility (poll until every expected row is queryable) ->
query suite (latency, throughput, correctness against workload-derived answers).
An empty, wrong, or rejected response is counted separately and never feeds the
latency percentiles.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import subprocess
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from backends import BACKENDS, Backend, Query
from otlp import post
from workload import Workload

HERE = Path(__file__).parent


def http_get(url: str, headers: Dict[str, str], timeout: float = 60) -> Tuple[int, str]:
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as err:
        return err.code, err.read().decode(errors="replace")
    except Exception as err:  # connection reset, timeout
        return 0, str(err)


def percentile(values: List[float], pct: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(pct / 100 * (len(ordered) - 1))))]


def summarize(latencies: List[float]) -> Dict[str, Optional[float]]:
    if not latencies:
        return {"p50": None, "p95": None, "p99": None, "max": None, "mean": None}
    return {
        "p50": percentile(latencies, 50),
        "p95": percentile(latencies, 95),
        "p99": percentile(latencies, 99),
        "max": max(latencies),
        "mean": statistics.fmean(latencies),
    }


# ---- stack management -------------------------------------------------------


def compose(backend: Backend, *args: str) -> None:
    env_file = HERE / "versions.env"
    env = dict(
        line.split("=", 1) for line in env_file.read_text().splitlines()
        if line and not line.startswith("#")
    )
    subprocess.run(
        ["podman-compose", "-p", backend.project, *args],
        cwd=HERE / backend.compose_dir,
        env={**__import__("os").environ, **env},
        check=True,
    )


def start_created(backend: Backend, timeout: float = 120) -> None:
    """podman-compose can start dependents before `service_healthy` holds; start them once it does."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        created = subprocess.run(
            ["podman", "ps", "-a", "--filter", f"name=^{backend.project}_", "--filter", "status=created",
             "--format", "{{.Names}}"], capture_output=True, text=True).stdout.split()
        if not created:
            return
        for name in created:
            subprocess.run(["podman", "start", name], capture_output=True)
        time.sleep(3)


def wait_ready(backend: Backend, timeout: float = 240) -> None:
    deadline = time.time() + timeout
    pending = list(backend.ready)
    while pending and time.time() < deadline:
        pending = [u for u in pending if http_get(u, {}, 5)[0] != 200]
        if pending:
            time.sleep(2)
    if pending:
        raise SystemExit(f"stack not ready: {pending}")


def to_mb(text: str) -> float:
    """'12.3MB' / '1.2GB' / '512kB' / '3MiB' -> megabytes."""
    text = text.strip()
    for unit, scale in (("GiB", 1024), ("MiB", 1), ("kiB", 1 / 1024), ("GB", 1000),
                        ("MB", 1), ("kB", 1 / 1000), ("B", 1e-6)):
        if text.endswith(unit):
            return float(text[: -len(unit)] or 0) * scale
    return 0.0


class StatsSampler(threading.Thread):
    """Samples `podman stats` for the stack's containers until stopped."""

    def __init__(self, project: str):
        super().__init__(daemon=True)
        self.project = project
        self.samples: List[Dict[str, Tuple[float, float]]] = []
        self.stop = threading.Event()

    def run(self) -> None:
        while not self.stop.is_set():
            out = subprocess.run(
                ["podman", "stats", "--no-stream", "--format", "json"],
                capture_output=True, text=True,
            )
            try:
                rows = json.loads(out.stdout)
            except ValueError:
                continue
            self.samples.append({
                row["name"]: (float(str(row["cpu_percent"]).rstrip("%") or 0),
                              to_mb(str(row.get("mem_usage", "0")).split("/")[0]))
                for row in rows if row["name"].startswith(self.project)
            })

    def summary(self) -> Dict[str, Dict[str, float]]:
        names = {n for s in self.samples for n in s}
        return {
            name: {
                "cpu_mean_pct": statistics.fmean(s[name][0] for s in self.samples if name in s),
                "cpu_max_pct": max(s[name][0] for s in self.samples if name in s),
                "mem_max_mb": max(s[name][1] for s in self.samples if name in s),
            }
            for name in sorted(names)
        }


# ---- phases -----------------------------------------------------------------


def ingest(backend: Backend, workload: Workload, concurrency: int) -> Dict[str, dict]:
    sources = {"logs": workload.logs, "metrics": workload.metrics, "traces": workload.traces}
    results: Dict[str, dict] = {}
    for signal, make in sources.items():
        url, headers = backend.ingest[signal]
        payloads = list(make())
        # Payloads are generated service by service; send each service's batches in
        # timestamp order (backends like Mimir reject out-of-order samples within a
        # series) and parallelize across services only.
        per_service = [payloads[n:n + workload.batches_per_service()]
                       for n in range(0, len(payloads), workload.batches_per_service())]
        statuses: Dict[str, int] = {}
        lock = threading.Lock()

        def send_service(batches: List[dict]) -> None:
            for payload in batches:
                status, body = post(url, signal, payload, headers)
                with lock:
                    statuses[str(status)] = statuses.get(str(status), 0) + 1
                    if status >= 300 and statuses[str(status)] == 1:
                        print(f"  ! {signal} HTTP {status}: {body[:160]!r}")

        started = time.perf_counter()
        with ThreadPoolExecutor(concurrency) as pool:
            list(pool.map(send_service, per_service))
        elapsed = time.perf_counter() - started
        rows = {"logs": workload.total()["logs"],
                "metrics": workload.total()["gauge_points"] + workload.total()["sum_points"],
                "traces": workload.total()["spans"]}[signal]
        results[signal] = {
            "batches": len(payloads),
            "statuses": statuses,
            "seconds": elapsed,
            "accepted_rows_per_s": rows / elapsed
            if sum(n for code, n in statuses.items() if code.startswith("2")) == len(payloads) else None,
            "finished_at": time.time(),
        }
        print(f"ingest {signal}: {elapsed:.2f}s {statuses}")
    return results


def run_once(query: Query, workload: Workload, i: int) -> Tuple[str, float, Optional[float]]:
    url, headers = query.build(workload, i)
    started = time.perf_counter()
    status, body = http_get(url, headers)
    latency = (time.perf_counter() - started) * 1000
    if status != 200:
        return "error", latency, None
    try:
        observed = query.parse(body)
    except Exception:
        return "error", latency, None
    expected = query.expected(workload, i)
    return ("ok" if observed == expected else "wrong"), latency, observed


def visibility(backend: Backend, workload: Workload, ingest_done: Dict[str, dict],
               timeout: float) -> Dict[str, dict]:
    """Poll all signals together; record when each first becomes fully queryable."""
    signals = ("logs", "metrics", "traces")
    deadline = time.time() + timeout
    visible_at: Dict[str, float] = {}
    while len(visible_at) < len(signals) and time.time() < deadline:
        for signal in signals:
            if signal in visible_at:
                continue
            queries = [qy for qy in backend.queries if qy.signal == signal]
            if all(run_once(qy, workload, i)[0] == "ok"
                   for qy in queries for i in range(workload.services)):
                visible_at[signal] = time.time()
        time.sleep(0.25)
    out = {
        signal: {
            "visible_seconds_after_accept": max(0.0, visible_at[signal] - ingest_done[signal]["finished_at"])
            if signal in visible_at else None,
            "timed_out": signal not in visible_at,
        }
        for signal in signals
    }
    for signal, entry in out.items():
        print(f"visibility {signal}: {entry}")
    return out


def query_suite(backend: Backend, workload: Workload, repeats: int, clients: int) -> Dict[str, dict]:
    results: Dict[str, dict] = {}
    for query in backend.queries:
        outcomes: List[Tuple[str, float, Optional[float]]] = []
        lock = threading.Lock()
        first_wrong: List[dict] = []

        def task(n: int) -> None:
            outcome = run_once(query, workload, n % workload.services)
            with lock:
                outcomes.append(outcome)
                if outcome[0] == "wrong" and not first_wrong:
                    first_wrong.append({"observed": outcome[2],
                                        "expected": query.expected(workload, n % workload.services)})

        run_once(query, workload, 0)  # one untimed warm-up
        started = time.perf_counter()
        with ThreadPoolExecutor(clients) as pool:
            list(pool.map(task, range(repeats)))
        wall = time.perf_counter() - started
        ok = [lat for kind, lat, _ in outcomes if kind == "ok"]
        results[query.id] = {
            "signal": query.signal,
            "clients": clients,
            "runs": len(outcomes),
            "ok": len(ok),
            "wrong": sum(1 for k, _, _ in outcomes if k == "wrong"),
            "errors": sum(1 for k, _, _ in outcomes if k == "error"),
            "first_wrong": first_wrong[0] if first_wrong else None,
            "latency_ms": summarize(ok),
            "throughput_qps": len(ok) / wall if wall else None,
        }
        r = results[query.id]
        print(f"query {query.id:16s} ok={r['ok']}/{r['runs']} wrong={r['wrong']} err={r['errors']} "
              f"p50={r['latency_ms']['p50'] and round(r['latency_ms']['p50'], 1)}ms"
              + (f" first_wrong={r['first_wrong']}" if r["first_wrong"] else ""))
    return results


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stack", choices=BACKENDS, required=True)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--services", type=int, default=5)
    ap.add_argument("--records", type=int, default=1000, help="per service, per signal")
    ap.add_argument("--ingest-concurrency", type=int, default=4)
    ap.add_argument("--repeats", type=int, default=50)
    ap.add_argument("--clients", type=int, nargs="+", default=[1, 4])
    ap.add_argument("--visibility-timeout", type=float, default=120)
    ap.add_argument("--manage", action="store_true", help="down -v, up -d, then wait for readiness")
    ap.add_argument("--keep-up", action="store_true")
    ap.add_argument("--out", type=Path, default=HERE / "results")
    args = ap.parse_args()

    backend = BACKENDS[args.stack]()
    if args.manage:
        compose(backend, "down", "-v")
        try:
            compose(backend, "up", "-d")
        except subprocess.CalledProcessError:
            pass  # dependents that raced their healthcheck are started below
        start_created(backend)
    wait_ready(backend)
    if args.manage:
        time.sleep(20)  # Loki/Tempo/Mimir hold ingesters unready for ~15s after boot

    base_ns = (int(time.time()) - args.records - 120) * 10**9  # whole seconds: no float drift in range queries
    workload = Workload(args.seed, args.services, args.records, base_ns, run=format(base_ns // 10**9, 'x'))
    sampler = StatsSampler(backend.project)
    sampler.start()
    ingest_result = ingest(backend, workload, args.ingest_concurrency)
    visible = visibility(backend, workload, ingest_result, args.visibility_timeout)
    queries = {f"clients_{c}": query_suite(backend, workload, args.repeats, c) for c in args.clients}
    sampler.stop.set()

    report = {
        "stack": backend.name,
        "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "workload": {"seed": args.seed, "services": args.services, "records": args.records,
                     "totals": workload.total()},
        "environment": {
            "host": platform.platform(), "podman_vm": "applehv 4 CPU / 8 GiB",
            "images": (HERE / "versions.env").read_text(),
            "note": "one benchmark stack at a time on the same VM, no CPU/memory limits; unrelated idle containers (a developer compose stack) were also running",
            "grafana_in_path": False, "collector_in_path": False,
        },
        "unsupported": backend.unsupported,
        "ingest": ingest_result,
        "visibility": visible,
        "queries": queries,
        "containers": sampler.summary(),
    }
    args.out.mkdir(exist_ok=True)
    path = args.out / f"{backend.name}-{time.strftime('%Y%m%dT%H%M%S')}.json"
    path.write_text(json.dumps(report, indent=2))
    print(f"wrote {path}")
    if args.manage and not args.keep_up:
        compose(backend, "down", "-v")


if __name__ == "__main__":
    main()
