#!/usr/bin/env python3
"""Generate the Grafana dashboards under bench/grafana/dashboards/.

    bench/.venv/bin/python bench/grafana/build_dashboards.py [--results DIR]

* bench-results.json: the benchmark results (median across trials) as bar charts and
  tables. The numbers are embedded as TestData CSV, so the dashboard needs no plugin and
  no file server; regenerate it after each benchmark run.
* bench-data-<stack>.json: the demo workload (run id "demo", see bench/demo.py) on each
  backend: logs, gauge and sum series and one trace, queried with the stack's own
  datasource. Service names and trace ids are deterministic, so the defaults work.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path
from typing import Dict, List, Optional

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from workload import Workload  # noqa: E402

DEMO = Workload(seed=1, services=5, records=2000, base_ns=0, run="demo")
STACKS = ["canardstack", "canardstack-pg", "canardstack-sqlite", "lgtm", "victoria"]
TESTDATA = {"type": "grafana-testdata-datasource", "uid": "testdata"}


# ---- results dashboard -------------------------------------------------------


def load_results(directory: Path) -> Dict[str, List[dict]]:
    by_stack: Dict[str, List[dict]] = {}
    for path in sorted(directory.glob("*.json")):
        report = json.loads(path.read_text())
        by_stack.setdefault(report["stack"], []).append(report)
    return by_stack


def med(values: List[Optional[float]]) -> Optional[float]:
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def cell(value: Optional[float], digits: int = 2) -> str:
    return "" if value is None else f"{value:.{digits}f}"


def csv(header: List[str], rows: List[List[str]]) -> str:
    return "\n".join([",".join(header)] + [",".join(row) for row in rows])


def csv_panel(pid: int, title: str, kind: str, content: str, x: int, y: int, w: int, h: int,
              *, unit: str = "", log: bool = False, description: str = "") -> dict:
    panel = {
        "id": pid, "type": kind, "title": title, "description": description,
        "datasource": TESTDATA,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [{"refId": "A", "datasource": TESTDATA, "scenarioId": "csv_content", "csvContent": content}],
        "fieldConfig": {"defaults": {"unit": unit, "custom": {}}, "overrides": []},
        "options": {},
    }
    if kind == "barchart":
        panel["fieldConfig"]["defaults"]["custom"] = {
            "scaleDistribution": {"type": "log", "log": 10} if log else {"type": "linear"},
            "axisSoftMin": 0 if not log else None,
            "fillOpacity": 80,
        }
        panel["options"] = {
            "xField": "name", "orientation": "auto", "showValue": "auto", "groupWidth": 0.75,
            "barWidth": 0.95, "stacking": "none", "xTickLabelRotation": 0,
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi", "sort": "none"},
        }
    return panel


def results_dashboard(results: Dict[str, List[dict]]) -> dict:
    stacks = [s for s in STACKS if s in results]
    first = results[stacks[0]][0]
    queries = list(first["queries"][next(iter(first["queries"]))])
    client_keys = sorted(first["queries"], key=lambda k: int(k.split("_")[1]))

    def per_query(metric, clients: str, digits=2) -> str:
        rows = []
        for q in queries:
            row = [q]
            for s in stacks:
                vals = [metric(r["queries"][clients][q]) for r in results[s] if q in r["queries"].get(clients, {})]
                row.append(cell(med(vals), digits))
            rows.append(row)
        return csv(["name"] + stacks, rows)

    def per_signal(metric, digits=2) -> str:
        rows = []
        for sig in ("logs", "metrics", "traces"):
            rows.append([sig] + [cell(med([metric(r, sig) for r in results[s]]), digits) for s in stacks])
        return csv(["name"] + stacks, rows)

    panels: List[dict] = []
    pid = 1
    c1 = client_keys[0]
    cn = client_keys[-1]
    panels.append(csv_panel(pid, f"Query latency p50 (ms), {c1.replace('_', ' ')} (log scale)", "barchart",
                            per_query(lambda q: q["latency_ms"]["p50"], c1), 0, 0, 12, 9, unit="ms", log=True,
                            description="Median across trials of each trial's pooled p50. Lower is better.")); pid += 1
    panels.append(csv_panel(pid, f"Query latency p50 (ms), {cn.replace('_', ' ')} (log scale)", "barchart",
                            per_query(lambda q: q["latency_ms"]["p50"], cn), 12, 0, 12, 9, unit="ms", log=True)); pid += 1
    panels.append(csv_panel(pid, f"Query latency p95 (ms), {c1.replace('_', ' ')} (log scale)", "barchart",
                            per_query(lambda q: q["latency_ms"]["p95"], c1), 0, 9, 12, 9, unit="ms", log=True)); pid += 1
    panels.append(csv_panel(pid, f"Query throughput (qps), {c1.replace('_', ' ')}", "barchart",
                            per_query(lambda q: q["throughput_qps"], c1, 0), 12, 9, 12, 9, unit="reqps")); pid += 1
    panels.append(csv_panel(pid, "Ingest: accepted rows/s (acceptance, not durability)", "barchart",
                            per_signal(lambda r, s: r["ingest"][s]["accepted_rows_per_s"], 0), 0, 18, 12, 8,
                            unit="short")); pid += 1
    panels.append(csv_panel(pid, "Ingest to queryable (seconds after accept, log scale)", "barchart",
                            per_signal(lambda r, s: r["visibility"][s]["visible_seconds_after_accept"]), 12, 18, 12, 8,
                            unit="s", log=True,
                            description="Time from the write being accepted until every expected row is queryable.")); pid += 1

    rows = []
    for ck in client_keys:
        for s in stacks:
            rs = [q for r in results[s] for q in r["queries"].get(ck, {}).values()]
            rows.append([ck.replace("_", " "), s, f'{sum(x["ok"] for x in rs)}/{sum(x["runs"] for x in rs)}',
                         str(sum(x["wrong"] for x in rs)), str(sum(x["errors"] for x in rs))])
    panels.append(csv_panel(pid, "Correctness: every answer checked against workload-derived expectations", "table",
                            csv(["clients", "stack", "ok / runs", "wrong", "errors"], rows),
                            0, 26, 12, 12, description="Summed over all queries and trials. wrong = answered, but not "
                            "the expected value; errors = no answer.")); pid += 1

    rows = []
    for s in stacks:
        for name in results[s][0]["containers"]:
            c = [r["containers"][name] for r in results[s] if name in r["containers"]]
            rows.append([s, name.split("_", 1)[1] if "_" in name else name,
                         cell(med([x["cpu_mean_pct"] for x in c]), 1), cell(med([x["cpu_max_pct"] for x in c]), 1),
                         cell(med([x["mem_max_mb"] for x in c]), 0)])
    panels.append(csv_panel(pid, "Container resource use (median across trials)", "table",
                            csv(["stack", "container", "cpu mean %", "cpu max %", "mem max MB"], rows),
                            12, 26, 12, 12 + max(0, len(rows) - 10))); pid += 1

    env = first["environment"]
    git = env.get("git") or {}
    trials = ", ".join(f"{s}: {len(results[s])}" for s in stacks)
    noise = [r["environment"].get("background_cpu_pct_mean") for s in stacks for r in results[s]]
    noise = [n for n in noise if n is not None]
    text = (
        f"**Workload** seed {first['workload']['seed']}, {first['workload']['services']} services x "
        f"{first['workload']['records']} records per signal.  \n"
        f"**Trials** {trials}. Every number is the median across trials.  \n"
        f"**Budget** {env.get('resource_budget', 'n/a')}.  \n"
        f"**Git** `{(git.get('sha') or 'n/a')[:10]}`{' (dirty)' if git.get('dirty') else ''}.  \n"
        + (f"**Background CPU** outside the stack: mean {statistics.fmean(noise):.1f}%, max {max(noise):.1f}%.  \n"
           if noise else "")
        + "\nSingle-node local containers; Grafana and collectors are not in the measured path; "
          "query languages differ per stack. See `bench/README.md`."
    )
    panels.append({"id": pid, "type": "text", "title": "About this run", "gridPos": {"x": 0, "y": 50, "w": 24, "h": 5},
                   "options": {"mode": "markdown", "content": text}})
    return dashboard("bench-results", "Benchmark results", ["bench"], panels,
                     time={"from": "now-6h", "to": "now"})



# ---- side-by-side comparison --------------------------------------------------

COMPARE = [
    # (benchmark stack name, legend, prometheus uid, label, logs kind/uid, trace kind/uid)
    ("canardstack-pg", "canardstack (Postgres)", "canardstack-pg-prometheus", "service_name",
     ("loki", "canardstack-pg-loki"), ("tempo", "canardstack-pg-tempo")),
    ("lgtm", "LGTM (Mimir)", "lgtm-mimir", "job", ("loki", "lgtm-loki"), ("tempo", "lgtm-tempo")),
    ("victoria", "Victoria (VictoriaMetrics)", "victoria-metrics", '"service.name"',
     ("victoria", "victoria-logs"), ("jaeger", "victoria-traces")),
]


def mixed_timeseries(pid: int, title: str, series: List[tuple], x: int, y: int, w: int, h: int) -> dict:
    """One chart with a query per backend. If the backends agree the lines draw on top of each other."""
    targets = []
    for n, (legend, uid, expr) in enumerate(series):
        d = ds("prometheus", uid)
        targets.append({"refId": chr(65 + n), "datasource": d, "expr": expr, "range": True, "instant": False,
                        "legendFormat": legend, "interval": "15s"})
    return {"id": pid, "type": "timeseries", "title": title,
            "datasource": {"type": "datasource", "uid": "-- Mixed --"},
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets,
            "fieldConfig": {"defaults": {"custom": {"lineWidth": 2, "fillOpacity": 0, "showPoints": "never"}},
                            "overrides": []}}


def compare_dashboard(results: Dict[str, List[dict]]) -> dict:
    present = [c for c in COMPARE if c[0] in results]
    stacks = [c[0] for c in present]
    first = results[stacks[0]][0]
    queries = list(first["queries"][next(iter(first["queries"]))])
    client_keys = sorted(first["queries"], key=lambda k: int(k.split("_")[1]))

    def latency_csv(clients: str) -> str:
        rows = []
        for q in queries:
            row = [q]
            for s in stacks:
                vals = [r["queries"][clients][q]["latency_ms"]["p50"] for r in results[s] if q in r["queries"].get(clients, {})]
                row.append(cell(med(vals), 2))
            rows.append(row)
        return csv(["name"] + stacks, rows)

    panels: List[dict] = []
    panels.append({"id": 1, "type": "text", "title": "What this compares", "gridPos": {"x": 0, "y": 0, "w": 24, "h": 4},
                   "options": {"mode": "markdown", "content":
                               "The same deterministic workload (run id `demo`) queried from three backends: **canardstack** "
                               "(Postgres catalog), **LGTM** (Loki, Mimir, Tempo) and **Victoria**. The top charts are the "
                               "benchmark's measured latency (median across trials). Everything below is live data: where "
                               "the backends agree, the overlay lines coincide. Grafana panel load time is not shown on "
                               "purpose: it goes through Grafana's proxy once per refresh and cannot separate a 1 ms backend "
                               "from a 10 ms one, so latency comes from the benchmark."}})
    pid = 2
    for n, ck in enumerate(client_keys[:2]):
        panels.append(csv_panel(pid, f"Benchmark query latency p50 (ms), {ck.replace('_', ' ')} (log scale)", "barchart",
                                latency_csv(ck), 12 * n, 4, 12, 8, unit="ms", log=True)); pid += 1
    # latest values, side by side
    for n, (_, legend, uid, label, _, _) in enumerate(present):
        panels.append(stat(pid, f"Latest gauge: {legend}", uid, f'bench_gauge{{{label}="$service"}}', 8 * n, 12, 8, 4)); pid += 1
    for n, (_, legend, uid, label, _, _) in enumerate(present):
        panels.append(stat(pid, f"Latest counter: {legend}", uid, f'bench_requests{{{label}="$service"}}', 8 * n, 16, 8, 4)); pid += 1
    # overlays
    panels.append(mixed_timeseries(pid, "bench_gauge, all backends overlaid (a 0-6 sawtooth)",
                                   [(legend, uid, f'bench_gauge{{{label}="$service"}}') for _, legend, uid, label, _, _ in present],
                                   0, 20, 12, 9)); pid += 1
    panels.append(mixed_timeseries(pid, "bench_requests, all backends overlaid (a counter reaching 2000)",
                                   [(legend, uid, f'bench_requests{{{label}="$service"}}') for _, legend, uid, label, _, _ in present],
                                   12, 20, 12, 9)); pid += 1
    # logs and traces, one panel per backend
    for row_y, title, error_only in ((29, "Logs", False), (39, 'Logs containing "error"', True)):
        for n, (_, legend, _, _, (kind, uid), _) in enumerate(present):
            if kind == "loki":
                expr = '{service_name="$service"}' + (' |= "error"' if error_only else "")
                panels.append(logs_loki(pid, f"{title}: {legend}", uid, expr, 8 * n, row_y, 8, 10))
            else:
                expr = '"service.name":"$service"' + (" error" if error_only else "")
                panels.append(logs_victoria(pid, f"{title}: {legend}", uid, expr, 8 * n, row_y, 8, 10))
            pid += 1
    for n, (_, legend, _, _, _, (kind, uid)) in enumerate(present):
        panels.append(trace_panel(pid, f"Trace by id: {legend}", kind, uid, 8 * n, 49, 8, 12)); pid += 1
    sample_service, sample_trace = DEMO.service(0), DEMO.trace_id(0, DEMO.records - 1)
    templating = {"list": [
        {"type": "textbox", "name": "service", "label": "service", "query": sample_service,
         "current": {"text": sample_service, "value": sample_service}, "options": []},
        {"type": "textbox", "name": "trace_id", "label": "trace id", "query": sample_trace,
         "current": {"text": sample_trace, "value": sample_trace}, "options": []},
    ]}
    d = dashboard("bench-compare", "Compare: canardstack vs LGTM vs Victoria", ["bench", "compare"], panels,
                  templating=templating, time={"from": "now-3h", "to": "now"})
    d["graphTooltip"] = 2  # shared tooltip across every chart
    return d


# ---- per-stack data dashboards -----------------------------------------------


def ds(kind: str, uid: str) -> dict:
    return {"type": kind, "uid": uid}


def timeseries(pid: int, title: str, uid: str, expr: str, x: int, y: int, w: int, h: int) -> dict:
    d = ds("prometheus", uid)
    return {"id": pid, "type": "timeseries", "title": title, "datasource": d,
            "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "targets": [{"refId": "A", "datasource": d, "expr": expr, "range": True, "instant": False,
                         "legendFormat": "{{__name__}}", "interval": "15s"}],
            "fieldConfig": {"defaults": {"custom": {"lineWidth": 2, "fillOpacity": 10, "showPoints": "never"}},
                            "overrides": []}}


def stat(pid: int, title: str, uid: str, expr: str, x: int, y: int, w: int, h: int) -> dict:
    p = timeseries(pid, title, uid, expr, x, y, w, h)
    p["type"] = "stat"
    p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "value", "graphMode": "area"}
    return p


def logs_loki(pid: int, title: str, uid: str, expr: str, x: int, y: int, w: int, h: int) -> dict:
    d = ds("loki", uid)
    return {"id": pid, "type": "logs", "title": title, "datasource": d,
            "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "targets": [{"refId": "A", "datasource": d, "expr": expr, "queryType": "range", "maxLines": 200}],
            "options": {"showTime": True, "sortOrder": "Ascending", "wrapLogMessage": False,
                        "enableLogDetails": True, "dedupStrategy": "none"}}


def logs_victoria(pid: int, title: str, uid: str, expr: str, x: int, y: int, w: int, h: int) -> dict:
    d = ds("victoriametrics-logs-datasource", uid)
    return {"id": pid, "type": "logs", "title": title, "datasource": d,
            "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "targets": [{"refId": "A", "datasource": d, "expr": expr, "queryType": "instant",
                         "editorMode": "code", "maxLines": 200}],
            "options": {"showTime": True, "sortOrder": "Ascending", "wrapLogMessage": False}}


def trace_panel(pid: int, title: str, kind: str, uid: str, x: int, y: int, w: int, h: int) -> dict:
    d = ds(kind, uid)
    target = {"refId": "A", "datasource": d, "query": "$trace_id"}
    target["queryType"] = "traceql" if kind == "tempo" else ""
    return {"id": pid, "type": "traces", "title": title, "datasource": d,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": [target]}


CANARDSTACK_VARIANTS = {
    "canardstack": ("canardstack", "Benchmark data: canardstack (Quack catalog)"),
    "canardstack-pg": ("canardstack-pg", "Benchmark data: canardstack (Postgres catalog)"),
    "canardstack-sqlite": ("canardstack-sqlite", "Benchmark data: canardstack (SQLite catalog)"),
}


def data_dashboard(stack: str) -> dict:
    sample_service = DEMO.service(0)
    sample_trace = DEMO.trace_id(0, DEMO.records - 1)
    if stack in CANARDSTACK_VARIANTS:
        prefix, title = CANARDSTACK_VARIANTS[stack]
        prom, label = f"{prefix}-prometheus", "service_name"
        logs = lambda pid, t, e, x, y, w, h: logs_loki(pid, t, f"{prefix}-loki", e, x, y, w, h)  # noqa: E731
        err = '{service_name="$service"} |= "error"'
        all_logs = '{service_name="$service"}'
        trace = ("tempo", f"{prefix}-tempo")
    elif stack == "lgtm":
        prom, label = "lgtm-mimir", "job"
        logs = lambda pid, t, e, x, y, w, h: logs_loki(pid, t, "lgtm-loki", e, x, y, w, h)  # noqa: E731
        err = '{service_name="$service"} |= "error"'
        all_logs = '{service_name="$service"}'
        trace = ("tempo", "lgtm-tempo")
        title = "Benchmark data: LGTM (Loki, Mimir, Tempo)"
    elif stack == "victoria":
        prom, label = "victoria-metrics", '"service.name"'
        logs = lambda pid, t, e, x, y, w, h: logs_victoria(pid, t, "victoria-logs", e, x, y, w, h)  # noqa: E731
        err = '"service.name":"$service" error'
        all_logs = '"service.name":"$service"'
        trace = ("jaeger", "victoria-traces")
        title = "Benchmark data: Victoria (VictoriaMetrics, VictoriaLogs, VictoriaTraces)"
    else:
        raise ValueError(f"unknown stack {stack}")
    sel = lambda name: f'{name}{{{label}="$service"}}'  # noqa: E731
    panels = [
        stat(1, "Latest gauge value", prom, sel("bench_gauge"), 0, 0, 6, 4),
        stat(2, "Latest request counter", prom, sel("bench_requests"), 6, 0, 6, 4),
        {"id": 3, "type": "text", "title": "About", "gridPos": {"x": 12, "y": 0, "w": 12, "h": 4},
         "options": {"mode": "markdown", "content":
                     "Deterministic benchmark workload (run id `demo`, see `bench/demo.py`). Pick a service "
                     f"(`svc-demo-0` to `svc-demo-4`); the trace id defaults to the last trace of `{sample_service}`. "
                     "The data is a few tens of minutes old, so keep the time range wide."}},
        timeseries(4, "bench_gauge (sawtooth, 10*service + k mod 7)", prom, sel("bench_gauge"), 0, 4, 12, 8),
        timeseries(5, "bench_requests (monotonic sum, k + 1)", prom, sel("bench_requests"), 12, 4, 12, 8),
        logs(6, "Logs", all_logs, 0, 12, 12, 10),
        logs(7, 'Logs containing "error" (every 10th record)', err, 12, 12, 12, 10),
        trace_panel(8, "Trace by id (root + child span)", trace[0], trace[1], 0, 22, 24, 10),
    ]
    templating = {"list": [
        {"type": "textbox", "name": "service", "label": "service", "query": sample_service,
         "current": {"text": sample_service, "value": sample_service}, "options": []},
        {"type": "textbox", "name": "trace_id", "label": "trace id", "query": sample_trace,
         "current": {"text": sample_trace, "value": sample_trace}, "options": []},
    ]}
    return dashboard(f"bench-data-{stack}", title, ["bench", stack], panels, templating=templating,
                     time={"from": "now-3h", "to": "now"})


def dashboard(uid: str, title: str, tags: List[str], panels: List[dict], *, templating: Optional[dict] = None,
              time: Optional[dict] = None) -> dict:
    return {
        "uid": uid, "title": title, "tags": tags, "timezone": "browser", "schemaVersion": 39, "version": 1,
        "editable": True, "refresh": "", "panels": panels,
        "templating": templating or {"list": []}, "time": time or {"from": "now-3h", "to": "now"},
        "links": [
            {"type": "dashboards", "title": "Benchmark dashboards", "tags": ["bench"], "asDropdown": True,
             "includeVars": False, "keepTime": False}
        ],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", type=Path, default=HERE.parent / "results")
    args = ap.parse_args()
    out = HERE / "dashboards"
    out.mkdir(exist_ok=True)
    results = load_results(args.results)
    if results:
        (out / "bench-results.json").write_text(json.dumps(results_dashboard(results), indent=2) + "\n")
        print(f"bench-results.json from {sum(len(v) for v in results.values())} result files")
    else:
        print(f"no results in {args.results}; skipping bench-results.json")
    if results:
        (out / "bench-compare.json").write_text(json.dumps(compare_dashboard(results), indent=2) + "\n")
        print("bench-compare.json")
    for stack in STACKS:
        (out / f"bench-data-{stack}.json").write_text(json.dumps(data_dashboard(stack), indent=2) + "\n")
        print(f"bench-data-{stack}.json")


if __name__ == "__main__":
    main()
