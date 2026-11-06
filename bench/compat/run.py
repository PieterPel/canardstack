#!/usr/bin/env python3
"""Run the PromQL / LogQL / TraceQL compatibility suite against one stack.

    uv run --project bench bench/compat/run.py --target canardstack-pg
    uv run --project bench bench/compat/run.py --target lgtm           # reference engines: validates the goldens
    uv run --project bench bench/compat/run.py --target canardstack-pg --check-baseline

The target must already be running (bench/run.py --manage --keep-up, or bench/demo.py). The suite
ingests its own small deterministic dataset under a fresh run id, waits until it is queryable, and
runs every case. Each case ends as one of:

    PASS         answered, and the answer matches the golden value computed from the workload
    WRONG        answered successfully but the answer differs (the dangerous kind)
    UNSUPPORTED  the engine rejected the query with a 4xx / error status
    ERROR        5xx, timeout or an unparseable response

Mimir, Loki and Tempo (target `lgtm`) are the reference: a case they fail has a wrong golden or
tests something outside the language, so fix the case before trusting results for canardstack.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import run as bench_run  # noqa: E402  (bench/run.py: ingest, visibility, http_get)
from backends import BACKENDS, tempo_spans  # noqa: E402
from cases import ALL, Case, Ctx  # noqa: E402
from workload import Workload  # noqa: E402

BEARER = {"Authorization": "Bearer dev-canardstack-key"}


@dataclass
class Target:
    name: str
    stack: str                 # bench backend used for ingest
    prom: Optional[str]
    loki: Optional[str]
    tempo: Optional[str]
    headers: Dict[str, str]
    lbl: str                   # service label as written inside expressions
    lbl_plain: str             # the same label as a bare name (for metadata APIs)


def canardstack(name: str, port: int) -> Target:
    base = f"http://localhost:{port}"
    return Target(name, name, base, base, base, BEARER, "service_name", "service_name")


TARGETS: Dict[str, Target] = {
    "canardstack": canardstack("canardstack", 19091),
    "canardstack-pg": canardstack("canardstack-pg", 19092),
    "canardstack-sqlite": canardstack("canardstack-sqlite", 19093),
    "lgtm": Target("lgtm", "lgtm", "http://localhost:19009/prometheus", "http://localhost:13100",
                   "http://localhost:13200", {}, "job", "job"),
    "victoria": Target("victoria", "victoria", "http://localhost:18428", None, None, {},
                       '"service.name"', "service.name"),
}


class Unsupported(Exception):
    pass


class Broken(Exception):
    pass


class Wrong(Exception):
    pass


# ---- request building ---------------------------------------------------------------

def fill(text: str, tgt: Target, ctx: Ctx) -> str:
    text = text.replace("$GAUGE", f"{ctx.w.metric_prefix}_gauge").replace("$COUNTER", f"{ctx.w.metric_prefix}_requests")
    text = text.replace("$LBLPLAIN", tgt.lbl_plain).replace("$LBL", tgt.lbl).replace("$RUN", ctx.run)
    for i in range(ctx.S):
        text = text.replace(f"$S{i}", ctx.name(i))
    return text


def build(case: Case, tgt: Target, ctx: Ctx) -> Tuple[str, str, Dict[str, str]]:
    """Return (family, url, headers) for a case, or raise Unsupported if the target lacks the API."""
    expr = fill(case.expr, tgt, ctx)
    params = {k: (fill(v, tgt, ctx) if isinstance(v, str) else v) for k, v in (case.params or {}).items()}
    T, R = ctx.T, ctx.R
    q = urllib.parse.urlencode
    if case.lang == "promql":
        if not tgt.prom:
            raise Unsupported("target has no Prometheus API")
        if case.api == "instant":
            return "prom", f"{tgt.prom}/api/v1/query?" + q({"query": expr, "time": T}), tgt.headers
        if case.api == "range":
            step = params["step"]
            start = T - params["start_back"]
            if params.get("align"):
                start = start // step * step
            return "prom", f"{tgt.prom}/api/v1/query_range?" + q(
                {"query": expr, "start": start, "end": T - params.get("end_back", 0), "step": step}), tgt.headers
        if case.api == "names":
            return "names", f"{tgt.prom}{expr}", tgt.headers
    if case.lang == "logql":
        if not tgt.loki:
            raise Unsupported("target has no Loki API")
        if case.api == "logs":
            return "logs", f"{tgt.loki}/loki/api/v1/query_range?" + q(
                {"query": expr, "start": T - R - 120, "end": T + 1, "limit": 1000, "direction": "forward"}), tgt.headers
        if case.api == "logs_instant":
            return "prom", f"{tgt.loki}/loki/api/v1/query?" + q({"query": expr, "time": T + 1}), tgt.headers
        if case.api == "names":
            return "names", f"{tgt.loki}{expr}", tgt.headers
    if case.lang == "traceql":
        if not tgt.tempo:
            raise Unsupported("target has no Tempo API")
        headers = {**tgt.headers, "Accept": "application/json"}
        if case.api == "trace":
            return "trace", f"{tgt.tempo}/api/traces/{ctx.w.trace_id(0, R - 1)}", headers
        if case.api == "search":
            # No time window: a windowed Tempo search only sees blocks it has already flushed, so its
            # result depends on how long ago the data was written; an unwindowed one searches the
            # ingesters and is deterministic.
            query = {"limit": 1000}
            if expr:
                query["q"] = expr
            query.update({k: v for k, v in params.items() if k == "tags"})
            return "search", f"{tgt.tempo}/api/search?" + q(query), headers
        if case.api == "names":
            return "names", f"{tgt.tempo}{expr}", headers
    raise Unsupported(f"no runner for {case.lang}/{case.api}")


# ---- response normalisation ---------------------------------------------------------

def svc_key(metric: Dict[str, str], ctx: Ctx) -> Any:
    for label in ("service_name", "job", "service.name"):
        m = re.fullmatch(rf"svc-{re.escape(ctx.run)}-(\d+)", metric.get(label, ""))
        if m:
            return int(m.group(1))
    return "_"


def load(body: str) -> dict:
    try:
        return json.loads(body)
    except ValueError:
        raise Broken(f"unparseable response: {body[:80]!r}")


def parse(family: str, body: str, ctx: Ctx, case: Case) -> Any:
    d = load(body)
    if family == "trace":
        return int(tempo_spans(body) or 0)
    if family == "search":
        return len(d.get("traces") or [])
    if family == "names":
        names = d.get("data")
        if names is None:
            names = d.get("tagNames")
        if names is None:  # Tempo v2: {"scopes": [{"tags": [...]}]}
            names = [t for s in d.get("scopes", []) for t in s.get("tags", [])]
        return {"names": names, "wanted": []}
    result = d["data"]["result"]
    kind = d["data"].get("resultType")
    if family == "logs":
        lines = [line for s in result for _, line in s["values"]]
        return {"n": len(lines), "lines": lines}
    if kind == "scalar":
        return {"_": float(result[1])}
    if kind == "matrix":
        out: Dict[Any, List[Tuple[float, float]]] = {}
        for s in result:
            key = svc_key(s["metric"], ctx)
            if key in out:
                raise Wrong(f"two series share the key {key!r}")
            out[key] = sorted((float(t), float(v)) for t, v in s["values"])
        return out
    out2: Dict[Any, float] = {}
    for s in result:
        key = svc_key(s["metric"], ctx)
        if key in out2:
            raise Wrong(f"two series share the key {key!r}")
        out2[key] = float(s["value"][1])
    return out2


def compare(case: Case, observed: Any, ctx: Ctx) -> Tuple[bool, str]:
    if case.check:
        return case.check(ctx, observed)
    expected = case.expect(ctx)
    if set(observed) != set(expected):
        return False, f"series {sorted(map(str, observed))}, expected {sorted(map(str, expected))}"
    bad = {k: (observed[k], expected[k]) for k in expected
           if abs(observed[k] - expected[k]) > max(case.tol * abs(expected[k]), 1e-6)}
    return not bad, (f"values differ (got, want): {dict(list(bad.items())[:2])}" if bad else "")


def classify(status: int, body: str) -> Tuple[str, str]:
    snippet = body.strip().replace("\n", " ")[:110]
    try:
        d = json.loads(body)
        snippet = f"{d.get('errorType', '')}: {d.get('error', '')}".strip(": ")[:110] or snippet
    except ValueError:
        pass
    if status in (400, 404, 405, 422, 501):
        return "UNSUPPORTED", snippet
    return "ERROR", f"HTTP {status}: {snippet}"


def run_case(case: Case, tgt: Target, ctx: Ctx) -> Tuple[str, str, float]:
    started = time.perf_counter()
    try:
        family, url, headers = build(case, tgt, ctx)
        status, body = bench_run.http_get(url, headers, timeout=60)
    except Unsupported as err:
        return "UNSUPPORTED", str(err), 0.0
    ms = (time.perf_counter() - started) * 1000
    if status != 200:
        verdict, detail = classify(status, body)
        return verdict, detail, ms
    try:
        d = json.loads(body)
        if isinstance(d, dict) and d.get("status") == "error":
            verdict, detail = classify(400, body)
            return verdict, detail, ms
        observed = parse(family, body, ctx, case)
        if family == "names":
            wanted = [fill(w, tgt, ctx) for w in (case.params or {}).get("wanted", [])]
            observed["wanted"] = wanted
        ok, detail = compare(case, observed, ctx)
        return ("PASS" if ok else "WRONG"), detail, ms
    except Wrong as err:
        return "WRONG", str(err), ms
    except Broken as err:
        return "ERROR", str(err), ms
    except (KeyError, IndexError, TypeError, ValueError) as err:
        return "ERROR", f"unexpected response shape ({err!r})", ms


# ---- dataset ------------------------------------------------------------------------

def prepare(tgt: Target, services: int, records: int) -> Ctx:
    backend = BACKENDS[tgt.stack]()
    now = int(time.time())
    run_id = "c" + format(now, "x")
    last_s = (now - 90) // 60 * 60  # last sample time, a multiple of 60 s: bucket phase is the same every run
    w = Workload(1, services, records, (last_s - (records - 1)) * 10**9, run=run_id, metric_prefix=f"compat_{run_id}")
    bench_run.ingest(backend, w, 4)
    ctx = Ctx(w=w, T=w.ts(records - 1) // 10**9, run=run_id)
    wait_ready(tgt, ctx)
    return ctx


def wait_ready(tgt: Target, ctx: Ctx, timeout: float = 180) -> None:
    """Poll one probe case per signal until the dataset is fully queryable on this target.

    Metrics and logs are readable almost at once; Tempo only makes traces searchable after its
    ingester flushes them, so the search probe is the slow one. A probe the engine rejects outright
    (or an API the target lacks) is not waited on.
    """
    probes = [c for c in ALL if (c.lang, c.id) in {("promql", "agg_count"), ("logql", "selector"),
                                                  ("traceql", "trace_by_id"), ("traceql", "search_by_tag")}]
    deadline = time.time() + timeout
    pending = list(probes)
    while pending and time.time() < deadline:
        pending = [c for c in pending if run_case(c, tgt, ctx)[0] in ("WRONG", "ERROR")]
        if pending:
            time.sleep(2)
    if pending:
        print("warning: still not fully queryable after %ds: %s" % (timeout, [c.id for c in pending]))


# ---- main ---------------------------------------------------------------------------

def summarize(results: List[dict]) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    for r in results:
        lang = out.setdefault(r["lang"], {"PASS": 0, "WRONG": 0, "UNSUPPORTED": 0, "ERROR": 0, "total": 0})
        lang[r["status"]] += 1
        lang["total"] += 1
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", choices=sorted(TARGETS), required=True)
    ap.add_argument("--lang", nargs="+", choices=["promql", "logql", "traceql"])
    ap.add_argument("--only", nargs="+", help="case ids")
    ap.add_argument("--services", type=int, default=3)
    ap.add_argument("--records", type=int, default=300)
    ap.add_argument("--out", type=Path, default=HERE / "results")
    ap.add_argument("--baseline", type=Path, help="default: compat/baselines/<target>.json")
    ap.add_argument("--update-baseline", action="store_true", help="record the currently passing cases")
    ap.add_argument("--check-baseline", action="store_true", help="fail if a baselined case no longer passes")
    ap.add_argument("--fail-under", nargs="*", default=[], metavar="LANG=PCT", help="e.g. promql=60 logql=40")
    args = ap.parse_args()

    tgt = TARGETS[args.target]
    ctx = prepare(tgt, args.services, args.records)
    selected = [c for c in ALL if (not args.lang or c.lang in args.lang) and (not args.only or c.id in args.only)]
    results = []
    for case in selected:
        status, detail, ms = run_case(case, tgt, ctx)
        results.append({"id": f"{case.lang}.{case.id}", "lang": case.lang, "api": case.api, "status": status,
                        "detail": detail, "ms": round(ms, 1), "note": case.note})
    summary = summarize(results)

    print(f"\n## Compatibility: {tgt.name} (dataset {args.services} services x {args.records} records, run {ctx.run})\n")
    print("| case | status | detail |\n| --- | --- | --- |")
    for r in results:
        print(f"| {r['id']} | {r['status']} | {r['detail'] or r['note']} |")
    print("\n| language | pass | wrong | unsupported | error | pass rate |\n| --- | --- | --- | --- | --- | --- |")
    for lang, s in summary.items():
        print(f"| {lang} | {s['PASS']} | {s['WRONG']} | {s['UNSUPPORTED']} | {s['ERROR']} | "
              f"{100 * s['PASS'] / s['total']:.0f}% ({s['PASS']}/{s['total']}) |")

    args.out.mkdir(exist_ok=True)
    (args.out / f"{tgt.name}.json").write_text(json.dumps(
        {"target": tgt.name, "dataset": {"services": args.services, "records": args.records},
         "summary": summary, "cases": results}, indent=2) + "\n")

    failed = False
    baseline_path = args.baseline or HERE / "baselines" / f"{tgt.name}.json"
    passing = sorted(r["id"] for r in results if r["status"] == "PASS")
    if args.update_baseline:
        baseline_path.parent.mkdir(exist_ok=True)
        baseline_path.write_text(json.dumps({"pass": passing}, indent=2) + "\n")
        print(f"\nbaseline written: {len(passing)} passing cases -> {baseline_path}")
    elif args.check_baseline and baseline_path.exists():
        ran = {r["id"] for r in results}  # with --only/--lang, judge just the cases that were run
        required = set(json.loads(baseline_path.read_text())["pass"]) & ran
        regressed = sorted(required - set(passing))
        gained = sorted(set(passing) - required)
        print(f"\nbaseline: {len(required)} required; regressed {len(regressed)}; newly passing {len(gained)}")
        for rid in regressed:
            print(f"  REGRESSION {rid}")
        for rid in gained:
            print(f"  new pass   {rid}  (run with --update-baseline to lock it in)")
        failed = failed or bool(regressed)
    for spec in args.fail_under:
        lang, pct = spec.split("=")
        s = summary.get(lang)
        if s and 100 * s["PASS"] / s["total"] < float(pct):
            print(f"FAIL: {lang} pass rate {100 * s['PASS'] / s['total']:.0f}% is under {pct}%")
            failed = True
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
