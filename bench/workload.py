"""Deterministic OTLP workload plus the expected query answers.

Everything derives from (seed, services, records, base_ns), so expected results
are computed independently of any backend.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Dict, Iterator, List

STEP_NS = 1_000_000_000
BATCH = 200


@dataclass(frozen=True)
class Workload:
    seed: int
    services: int
    records: int  # per service, per signal
    base_ns: int
    run: str = ""  # unique per run so a reused stack never mixes datasets
    metric_prefix: str = "bench"  # metrics are <prefix>_gauge and <prefix>_requests

    @property
    def query_limit(self) -> int:
        """Row limit sent with log queries; canardstack bounds `limit` at 1000."""
        return min(self.records, 1000)

    def service(self, i: int) -> str:
        return f"svc-{self.run}-{i}"

    def ts(self, k: int) -> int:
        return self.base_ns + k * STEP_NS

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
        ks = range(self.records)
        return {
            "log_count": self.query_limit,
            "log_error_count": sum(1 for k in ks if k % 10 == 0),
            "gauge_last": self.gauge_value(i, self.records - 1),
            # Peak of the sawtooth seen by a 15 s step range query: every residue of k mod 7 is
            # sampled once there are at least 7 steps (records >= 105).
            "gauge_range_max": max(self.gauge_value(i, k) for k in range(0, self.records, 15)),
            "gauge_count": self.records,
            "sum_last": self.sum_value(self.records - 1),
            "trace_spans": 2,
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

    def logs(self) -> Iterator[dict]:
        for i in range(self.services):
            for ks in self._ranges():
                records = [
                    {
                        "timeUnixNano": str(self.ts(k)),
                        "severityNumber": 17 if k % 10 == 0 else 9,
                        "severityText": "ERROR" if k % 10 == 0 else "INFO",
                        "body": {"stringValue": self.log_body(i, k)},
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

    def metrics(self) -> Iterator[dict]:
        for i in range(self.services):
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

    def traces(self) -> Iterator[dict]:
        for i in range(self.services):
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
