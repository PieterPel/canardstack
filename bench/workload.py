"""Deterministic OTLP workload plus the expected query answers.

Everything derives from (seed, services, records, base_ns), so expected results
are computed independently of any backend.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional

# Sample spacing is one second, or less for large workloads so the whole dataset spans at most
# SPAN_NS: a backend serves recent data from its ingesters and older data from blocks that are
# flushed and synced on a schedule (Mimir's store-gateway syncs every 15 minutes, Loki queries
# ingesters only for the last 3 hours, Tempo for 30 minutes), so back-dating hours of data makes
# part of it invisible for a while. Real telemetry arrives about when it is stamped.
STEP_NS = 1_000_000_000
SPAN_NS = 1_500_000_000_000
BATCH = 200


@dataclass(frozen=True)
class Workload:
    seed: int
    services: int
    records: int  # per service, per signal
    base_ns: int
    run: str = ""  # unique per run so a reused stack never mixes datasets
    metric_prefix: str = "bench"  # metrics are <prefix>_gauge and <prefix>_requests
    users: int = 0  # distinct values of a log attribute `bench.user`; 0 adds no attribute

    @property
    def query_limit(self) -> int:
        """Row limit sent with log queries; canardstack bounds `limit` at 1000."""
        return min(self.records, 1000)

    def service(self, i: int) -> str:
        return f"svc-{self.run}-{i}"

    @property
    def service_regex(self) -> str:
        """Matches every service of this run and nothing else."""
        return f"svc-{self.run}-.*"

    @property
    def window_s(self) -> int:
        """Seconds a `[W]` range selector needs to reach every sample from the last one."""
        return self.span_s + 60

    @property
    def points_step(self) -> int:
        """Step in seconds of the full-resolution range query, wide enough to stay under 5,000 points."""
        return max(1, -(-self.span_s // 5000))

    @property
    def points_count(self) -> int:
        """Points that range query returns: one per step from the first sample to the last."""
        return (self.last_ms - self.base_ns // 1_000_000) // (self.points_step * 1000) + 1

    @property
    def peak_step(self) -> int:
        """Step in seconds of the coarse range query; each point is the newest sample at or before it."""
        return 15

    def peak_indices(self) -> List[int]:
        start_ms = self.base_ns // 1_000_000
        count = (self.last_ms - start_ms) // (self.peak_step * 1000) + 1
        return [self.sample_at(start_ms + g * self.peak_step * 1000) for g in range(count)]

    @property
    def step_ns(self) -> int:
        return min(STEP_NS, SPAN_NS // max(1, self.records))

    def ts(self, k: int) -> int:
        return self.base_ns + k * self.step_ns

    @property
    def last_ms(self) -> int:
        """Millisecond timestamp, rounded up, of the last sample: the `time` or `end` of a query
        that must see all of them (Prometheus timestamps have millisecond resolution)."""
        return -(-self.ts(self.records - 1) // 1_000_000)

    @property
    def span_s(self) -> int:
        return -(-(self.records - 1) * self.step_ns // 1_000_000_000)

    def sample_at(self, t_ms: int) -> int:
        """Index of the newest sample at or before `t_ms` (what an instant or range query returns)."""
        return min(self.records - 1, max(0, (t_ms * 1_000_000 - self.base_ns) // self.step_ns))

    def trace_id(self, i: int, k: int) -> str:
        return hashlib.md5(f"{self.seed}:{self.run}:{i}:{k}".encode()).hexdigest()

    def span_id(self, i: int, k: int, which: str) -> str:
        return hashlib.md5(f"{self.seed}:{self.run}:{i}:{k}:{which}".encode()).hexdigest()[:16]

    @staticmethod
    def log_body(i: int, k: int) -> str:
        return f"svc-{i} event {k}" + (" error" if k % 10 == 0 else "")

    @staticmethod
    def gauge_value(i: int, k: int) -> float:
        return float(10 * i + k % 7)

    @staticmethod
    def sum_value(k: int) -> float:
        return float(k + 1)

    def batches_per_service(self) -> int:
        return -(-self.records // BATCH)

    # ---- expected answers -------------------------------------------------

    def expected(self, i: int) -> Dict[str, float]:
        n = self.records
        errors = (n + 9) // 10  # lines with k % 10 == 0
        return {
            "log_count": self.query_limit,
            # The query asks for at most `query_limit` lines.
            "log_error_count": min(errors, self.query_limit),
            "gauge_last": self.gauge_value(i, n - 1),
            # Peak of the sawtooth seen by the coarse range query: every residue of k mod 7 is
            # sampled once there are at least 7 steps.
            "gauge_range_max": max(self.gauge_value(i, k) for k in self.peak_indices()),
            "gauge_count": self.points_count,
            "sum_last": self.sum_value(n - 1),
            "trace_spans": 2,
            # Shapes that read or count every record of the service (see backends.py).
            "log_count_over_time": n,
            "log_count_all": self.services * n,
            "log_regex_count": (n + 4) // 10,  # lines ending in 5: k % 10 == 5
            "log_newest": min(100, n),
            "metric_sum_over_time": 10 * i * n + (n // 7) * 21 + sum(range(n % 7)),
            "metric_count_over_time": n,
            "metric_max_counter": self.sum_value(n - 1),
            "metric_sum_all": sum(self.gauge_value(j, n - 1) for j in range(self.services)),
            "trace_search": min(20, n),
        }

    def total(self) -> Dict[str, int]:
        n = self.services * self.records
        return {"logs": n, "gauge_points": n, "sum_points": n, "spans": 2 * n}

    # ---- OTLP/HTTP JSON payloads ------------------------------------------

    def _resource(self, i: int) -> dict:
        return {
            "attributes": [
                {"key": "service.name", "value": {"stringValue": self.service(i)}},
                {"key": "bench.seed", "value": {"stringValue": str(self.seed)}},
            ]
        }

    def _ranges(self) -> Iterator[range]:
        for start in range(0, self.records, BATCH):
            yield range(start, min(start + BATCH, self.records))

    def logs(self, services: Optional[Iterable[int]] = None) -> Iterator[dict]:
        for i in (range(self.services) if services is None else services):
            for ks in self._ranges():
                records = [
                    {
                        "timeUnixNano": str(self.ts(k)),
                        "severityNumber": 17 if k % 10 == 0 else 9,
                        "severityText": "ERROR" if k % 10 == 0 else "INFO",
                        "body": {"stringValue": self.log_body(i, k)},
                        **(
                            {"attributes": [{"key": "bench.user", "value": {"stringValue": f"u{k % self.users}"}}]}
                            if self.users
                            else {}
                        ),
                    }
                    for k in ks
                ]
                yield {
                    "resourceLogs": [
                        {
                            "resource": self._resource(i),
                            "scopeLogs": [{"logRecords": records}],
                        }
                    ]
                }

    def metrics(self, services: Optional[Iterable[int]] = None) -> Iterator[dict]:
        for i in (range(self.services) if services is None else services):
            for ks in self._ranges():
                gauge = [
                    {"timeUnixNano": str(self.ts(k)), "asDouble": self.gauge_value(i, k)}
                    for k in ks
                ]
                total = [
                    {
                        "startTimeUnixNano": str(self.base_ns),
                        "timeUnixNano": str(self.ts(k)),
                        "asDouble": self.sum_value(k),
                    }
                    for k in ks
                ]
                yield {
                    "resourceMetrics": [
                        {
                            "resource": self._resource(i),
                            "scopeMetrics": [
                                {
                                    "metrics": [
                                        {"name": f"{self.metric_prefix}_gauge", "gauge": {"dataPoints": gauge}},
                                        {
                                            "name": f"{self.metric_prefix}_requests",
                                            "sum": {
                                                "aggregationTemporality": 2,
                                                "isMonotonic": True,
                                                "dataPoints": total,
                                            },
                                        },
                                    ]
                                }
                            ],
                        }
                    ]
                }

    def traces(self, services: Optional[Iterable[int]] = None) -> Iterator[dict]:
        for i in (range(self.services) if services is None else services):
            for ks in self._ranges():
                spans: List[dict] = []
                for k in ks:
                    tid = self.trace_id(i, k)
                    root = self.span_id(i, k, "root")
                    spans.append(
                        {
                            "traceId": tid,
                            "spanId": root,
                            "name": "bench.root",
                            "kind": 2,
                            "startTimeUnixNano": str(self.ts(k)),
                            "endTimeUnixNano": str(self.ts(k) + 50_000_000),
                        }
                    )
                    spans.append(
                        {
                            "traceId": tid,
                            "spanId": self.span_id(i, k, "child"),
                            "parentSpanId": root,
                            "name": "bench.child",
                            "kind": 1,
                            "startTimeUnixNano": str(self.ts(k) + 5_000_000),
                            "endTimeUnixNano": str(self.ts(k) + 45_000_000),
                        }
                    )
                yield {
                    "resourceSpans": [
                        {
                            "resource": self._resource(i),
                            "scopeSpans": [{"spans": spans}],
                        }
                    ]
                }
