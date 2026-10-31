"""Encode the workload's JSON-shaped OTLP payloads as protobuf and POST them."""

from __future__ import annotations

import base64
import copy
import urllib.error
import urllib.request

from google.protobuf.json_format import ParseDict
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

MESSAGES = {
    "logs": ExportLogsServiceRequest,
    "metrics": ExportMetricsServiceRequest,
    "traces": ExportTraceServiceRequest,
}


def _hex_ids_to_base64(payload: dict) -> dict:
    """OTLP/JSON carries trace/span ids as hex; protobuf JSON parsing wants base64."""
    for resource in payload.get("resourceSpans", []):
        for scope in resource.get("scopeSpans", []):
            for span in scope.get("spans", []):
                for key in ("traceId", "spanId", "parentSpanId"):
                    if key in span:
                        span[key] = base64.b64encode(bytes.fromhex(span[key])).decode()
    return payload


def encode(signal: str, payload: dict) -> bytes:
    if signal == "traces":
        payload = _hex_ids_to_base64(copy.deepcopy(payload))
    return ParseDict(payload, MESSAGES[signal]()).SerializeToString()


def post(url: str, signal: str, payload: dict, headers=None, timeout=60):
    """Return (status, body) without raising on HTTP errors."""
    request = urllib.request.Request(
        url,
        encode(signal, payload),
        {"content-type": "application/x-protobuf", **(headers or {})},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as err:
        return err.code, err.read()
