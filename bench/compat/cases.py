"""PromQL, LogQL and TraceQL/Tempo compatibility cases with golden answers.

Every expected value is computed from the deterministic workload (bench/workload.py), never read
from a backend, so a case can tell "answered correctly" from "answered something".

Workload recap (per service i, records k = 0..R-1, one sample per second, last sample at time T):
    bench_gauge    10*i + (k mod 7)         a sawtooth
    bench_requests k + 1                    a counter, +1 per second
    logs           "svc-<run>-<i> event k" + " error" when k mod 10 == 0
    traces         one root span and one child span per k (both named bench.root / bench.child)

Expression templates: $GAUGE and $COUNTER are this run's metric names (bench_gauge / bench_requests
with a per-run prefix, so data left by other runs never mixes in), $LBL is the backend's service label (service_name, job or "service.name"),
$S0.. are service names, $RUN is the run id.

A case's `check(ctx, observed)` returns (ok, detail); the default compares `observed` with
`expect(ctx)` (numbers within `tol`, relative). Cases are data: add a Case to grow the suite.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from workload import Workload


@dataclass
class Ctx:
    w: Workload
    T: int                 # last sample time, unix seconds
    run: str

    @property
    def R(self) -> int:
        return self.w.records

    @property
    def S(self) -> int:
        return self.w.services

    def name(self, i: int) -> str:
        return self.w.service(i)

    def g(self, i: int) -> float:
        """$GAUGE for service i at the last sample."""
        return self.w.gauge_value(i, self.R - 1)

    def lines(self, i: int) -> List[str]:
        return [self.w.log_body(i, k) for k in range(self.R)]


@dataclass
class Case:
    id: str
    lang: str            # promql | logql | traceql
    api: str             # instant | range | logs | logs_instant | trace | search | names
    expr: str
    expect: Optional[Callable[[Ctx], Any]] = None
    check: Optional[Callable[[Ctx, Any], Tuple[bool, str]]] = None
    tol: float = 0.0
    note: str = ""
    params: Optional[Dict[str, Any]] = None


def per_service(ctx: Ctx, fn: Callable[[int], float]) -> Dict[Any, float]:
    return {i: fn(i) for i in range(ctx.S)}


# ---- PromQL (instant at T) ---------------------------------------------------------

def _gauge_all(ctx): return per_service(ctx, ctx.g)


PROMQL: List[Case] = [
    Case("selector_one", "promql", "instant", '$GAUGE{$LBL="$S0"}', lambda c: {0: c.g(0)}),
    Case("selector_all", "promql", "instant", "$GAUGE", _gauge_all),
    Case("agg_sum", "promql", "instant", "sum($GAUGE)", lambda c: {"_": sum(c.g(i) for i in range(c.S))}),
    Case("agg_by", "promql", "instant", "sum by ($LBL) ($GAUGE)", _gauge_all),
    Case("count_by", "promql", "instant", "count by ($LBL) ($GAUGE)", lambda c: per_service(c, lambda i: 1.0)),
    Case("rate", "promql", "instant", "rate($COUNTER[1m])", lambda c: per_service(c, lambda i: 1.0), tol=0.05),
    Case("sum_rate_by", "promql", "instant", "sum by ($LBL) (rate($COUNTER[1m]))", lambda c: per_service(c, lambda i: 1.0), tol=0.05),
    Case("cmp_filter", "promql", "instant", "$GAUGE > 12", lambda c: {i: c.g(i) for i in range(c.S) if c.g(i) > 12}),
    Case("vector_add", "promql", "instant", "$GAUGE + $COUNTER", lambda c: per_service(c, lambda i: c.g(i) + c.R)),
    Case("regex_match", "promql", "instant", '$GAUGE{$LBL=~"$S0|$S1"}', lambda c: {i: c.g(i) for i in (0, 1)}),
]


# ---- PromQL range checks -----------------------------------------------------------

def _series(obs: Dict[Any, List[Tuple[float, float]]], key: Any) -> List[Tuple[float, float]]:
    return obs.get(key, [])


def check_range_peak(ctx: Ctx, obs) -> Tuple[bool, str]:
    s = _series(obs, 0)
    peak = max((v for _, v in s), default=None)
    return peak == 6.0, f"{len(s)} points, peak {peak} (a 0-6 sawtooth must reach 6)"


def check_range_single_series(ctx: Ctx, obs) -> Tuple[bool, str]:
    return list(obs) == ["_"], f"series keys {list(obs)}, expected one collapsed series"


def check_range_rate(ctx: Ctx, obs) -> Tuple[bool, str]:
    values = [v for s in obs.values() for _, v in s]
    bad = [v for v in values if abs(v - 1.0) > 0.05]
    return bool(values) and not bad, f"{len(values)} points, {len(bad)} outside 1.0 +/- 5%"


def check_range_exact_values(ctx: Ctx, obs) -> Tuple[bool, str]:
    """PromQL evaluates at each step timestamp t and returns the sample at or before t."""
    base_s = ctx.w.base_ns // 10**9
    bad = []
    for ts, v in _series(obs, 0):
        k = int(round(ts)) - base_s
        if 0 <= k < ctx.R and v != ctx.w.gauge_value(0, k):
            bad.append((int(ts), v, ctx.w.gauge_value(0, k)))
    return not bad, f"{len(bad)} points differ from the sample at their timestamp, e.g. {bad[:2]}"


# The dataset's last sample T is a multiple of 60 s, so with `align` (start rounded down to a multiple
# of the step) the range is a whole number of steps ending exactly on a sample, every bucket is
# complete, and results do not depend on the second the run began at.
RANGE_PARAMS = {"start_back": 240, "step": 15, "align": True}
# Step 10: the last sample of a 10 s bucket is 9 s after the bucket start, and 9 mod 7 != 0, so the
# sawtooth value differs from the sample at the step time (a 14 s offset would coincide: 14 mod 7 == 0).
RANGE_STEP10 = {"start_back": 240, "step": 10, "align": True}
# End one second early: every bucket holds a full step of samples, with no single-sample edge bucket.
RANGE_INTERIOR = {"start_back": 240, "step": 15, "align": True, "end_back": 1}
# End exactly on the grid: the last bucket holds a single sample (the one at T).
RANGE_EDGE = {"start_back": 240, "step": 15, "align": True}
def check_range_sum_rate(ctx: Ctx, obs) -> Tuple[bool, str]:
    values = [v for s in obs.values() for _, v in s]
    bad = [v for v in values if abs(v - ctx.S) > 0.05 * ctx.S]
    return list(obs) == ["_"] and bool(values) and not bad, f"series {list(obs)}, {len(values)} points, {len(bad)} outside {ctx.S} +/- 5%"


PROMQL_RANGE: List[Case] = [
    Case("range_peak", "promql", "range", '$GAUGE{$LBL="$S0"}', check=check_range_peak, params=RANGE_PARAMS,
         note="a smoothed gauge cannot reach the sawtooth peak"),
    Case("range_sum_collapses", "promql", "range", "sum($GAUGE)", check=check_range_single_series, params=RANGE_PARAMS,
         note="sum() without by() must collapse every series into one"),
    Case("range_rate", "promql", "range", "rate($COUNTER[1m])", check=check_range_rate, params=RANGE_INTERIOR),
    Case("range_sum_rate", "promql", "range", "sum(rate($COUNTER[1m]))", check=check_range_sum_rate, params=RANGE_INTERIOR,
         note="one collapsed series whose every point is the number of services (each counter grows 1/s)"),
    Case("range_value_at_step", "promql", "range", '$GAUGE{$LBL="$S0"}', check=check_range_exact_values, params=RANGE_STEP10,
         note="the value reported at step time t is the sample at t, not the last sample of a bucket"),
]


# ---- PromQL metadata APIs ----------------------------------------------------------

def check_contains(ctx: Ctx, obs) -> Tuple[bool, str]:
    wanted = obs["wanted"]
    missing = [w for w in wanted if w not in obs["names"]]
    return not missing, f"missing {missing[:3]} of {len(wanted)}"


PROMQL_META: List[Case] = [
    Case("meta_label_names", "promql", "names", "/api/v1/labels", check=check_contains, params={"wanted": ["$LBLPLAIN"]}),
    Case("meta_metric_names", "promql", "names", "/api/v1/label/__name__/values", check=check_contains,
         params={"wanted": ["$GAUGE", "$COUNTER"]}),
    Case("meta_label_values", "promql", "names", "/api/v1/label/$LBLPLAIN/values", check=check_contains,
         params={"wanted": ["$S0", "$S1", "$S2"]}),
]


# ---- LogQL -------------------------------------------------------------------------

def logs_n(fn: Callable[[Ctx], int]) -> Callable[[Ctx, Any], Tuple[bool, str]]:
    def check(ctx: Ctx, obs) -> Tuple[bool, str]:
        want = fn(ctx)
        return obs["n"] == want, f"{obs['n']} lines, expected {want}"
    return check


def _count_matching(ctx: Ctx, pattern: str, keep: bool = True, services=None) -> int:
    rx = re.compile(pattern)
    services = range(ctx.S) if services is None else services
    return sum(1 for i in services for line in ctx.lines(i) if bool(rx.search(line)) == keep)


def check_line_format(ctx: Ctx, obs) -> Tuple[bool, str]:
    ok = obs["n"] == ctx.R and set(obs["lines"]) == {"L"}
    return ok, f"{obs['n']} lines, distinct bodies {sorted(set(obs['lines']))[:3]}"


LOGQL: List[Case] = [
    Case("selector", "logql", "logs", '{service_name="$S0"}', check=logs_n(lambda c: c.R)),
    Case("line_contains", "logql", "logs", '{service_name="$S0"} |= "error"', check=logs_n(lambda c: _count_matching(c, "error", True, [0]))),
    Case("line_not_contains", "logql", "logs", '{service_name="$S0"} != "error"', check=logs_n(lambda c: _count_matching(c, "error", False, [0]))),
    Case("line_regex", "logql", "logs", '{service_name="$S0"} |~ "event 1[0-9]( |$)"',
         check=logs_n(lambda c: _count_matching(c, r"event 1[0-9]( |$)", True, [0]))),
    Case("line_not_regex", "logql", "logs", '{service_name="$S0"} !~ "error"', check=logs_n(lambda c: _count_matching(c, "error", False, [0]))),
    Case("line_case_sensitive", "logql", "logs", '{service_name="$S0"} |= "ERROR"', check=logs_n(lambda c: 0),
         note="line filters are case-sensitive; the lines only contain lowercase 'error'"),
    Case("line_contains_substring", "logql", "logs", '{service_name="$S0"} |= "event 1"',
         check=logs_n(lambda c: _count_matching(c, "event 1", True, [0]))),
    Case("line_literal_keywords", "logql", "logs", '{service_name="$S0"} |= "event 1 AND event 2"', check=logs_n(lambda c: 0),
         note="the filter is one literal string, not a boolean expression"),
    Case("line_literal_parens", "logql", "logs", '{service_name="$S0"} |= "(event)"', check=logs_n(lambda c: 0)),
    Case("label_regex", "logql", "logs", '{service_name=~"$S0|$S1"}', check=logs_n(lambda c: 2 * c.R)),
    Case("label_not_equal", "logql", "logs", '{service_name=~"svc-$RUN-.+", service_name!="$S0"}', check=logs_n(lambda c: (c.S - 1) * c.R)),
    Case("chained_filters", "logql", "logs", '{service_name="$S0"} |= "event" != "error"', check=logs_n(lambda c: _count_matching(c, "error", False, [0]))),
    Case("line_format", "logql", "logs", '{service_name="$S0"} | line_format "L"', check=check_line_format),
    # Loki keeps INFO and ERROR lines in separate streams, so the raw per-stream form has no single
    # value per service; sum() is the form dashboards use.
    Case("metric_count_over_time", "logql", "logs_instant", 'sum(count_over_time({service_name="$S0"}[10m]))', lambda c: {"_": float(c.R)}),
    Case("metric_rate", "logql", "logs_instant", 'sum(rate({service_name="$S0"}[10m]))', lambda c: {"_": c.R / 600.0}, tol=1e-6),
    Case("metric_sum_by", "logql", "logs_instant", 'sum by (service_name) (count_over_time({service_name=~"svc-$RUN-.+"}[10m]))',
         lambda c: per_service(c, lambda i: float(c.R))),
    Case("metric_bytes_over_time", "logql", "logs_instant", 'sum(bytes_over_time({service_name="$S0"}[10m]))',
         lambda c: {"_": float(sum(len(l) for l in c.lines(0)))}),
    Case("metric_bytes_rate", "logql", "logs_instant", 'sum(bytes_rate({service_name="$S0"}[10m]))',
         lambda c: {"_": float(sum(len(l) for l in c.lines(0))) / 600.0}, tol=1e-6),
    Case("metric_negated_filter", "logql", "logs_instant", 'sum(count_over_time({service_name="$S0"} !~ "error" [10m]))',
         lambda c: {"_": float(_count_matching(c, "error", False, [0]))}),
    Case("metric_filtered_count", "logql", "logs_instant", 'sum(count_over_time({service_name=~"svc-$RUN-.+"} |= "error" [10m]))',
         lambda c: {"_": float(_count_matching(c, "error", True))}),
]
LOGQL_META: List[Case] = [
    Case("meta_label_names", "logql", "names", "/loki/api/v1/labels", check=check_contains, params={"wanted": ["service_name"]}),
    Case("meta_label_values", "logql", "names", "/loki/api/v1/label/service_name/values", check=check_contains,
         params={"wanted": ["$S0", "$S1", "$S2"]}),
]


# ---- Tempo / TraceQL ---------------------------------------------------------------

def count_is(fn: Callable[[Ctx], int]) -> Callable[[Ctx, Any], Tuple[bool, str]]:
    def check(ctx: Ctx, obs) -> Tuple[bool, str]:
        want = fn(ctx)
        return obs == want, f"{obs}, expected {want}"
    return check


TEMPO: List[Case] = [
    Case("trace_by_id", "traceql", "trace", "", check=count_is(lambda c: 2)),
    Case("search_by_tag", "traceql", "search", "", params={"tags": "service.name=$S0"}, check=count_is(lambda c: c.R)),
    Case("search_tag_no_match", "traceql", "search", "", params={"tags": "service.name=does-not-exist"}, check=count_is(lambda c: 0)),
    Case("traceql_service", "traceql", "search", '{ resource.service.name = "$S0" }', check=count_is(lambda c: c.R)),
    Case("traceql_service_no_match", "traceql", "search", '{ resource.service.name = "does-not-exist" }', check=count_is(lambda c: 0)),
    Case("traceql_span_name", "traceql", "search", '{ name = "bench.root" && resource.service.name =~ "svc-$RUN-.+" }', check=count_is(lambda c: c.S * c.R)),
    Case("traceql_duration_gt_none", "traceql", "search", '{ duration > 1h }', check=count_is(lambda c: 0),
         note="a filter nothing can satisfy must return no traces, not every trace"),
    Case("traceql_duration_lt_all", "traceql", "search", '{ duration < 1h && resource.service.name = "$S0" }', check=count_is(lambda c: c.R)),
    Case("traceql_duration_name_service", "traceql", "search",
         '{ name = "bench.child" && duration < 1h && resource.service.name = "$S0" }', check=count_is(lambda c: c.R)),
    Case("traceql_and", "traceql", "search", '{ resource.service.name = "$S0" && name = "bench.child" }', check=count_is(lambda c: c.R)),
    Case("meta_tag_names", "traceql", "names", "/api/search/tags", check=check_contains, params={"wanted": ["service.name"]}),
]

ALL: List[Case] = PROMQL + PROMQL_RANGE + PROMQL_META + LOGQL + LOGQL_META + TEMPO
