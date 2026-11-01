"""Commit one batch of OTLP events (as delivered by an Event Hubs trigger) to DuckLake.

The Azure binding is a thin wrapper around `ingest_batch`: the trigger hands the function a list
of event bodies plus the partition id and sequence numbers, the function calls `ingest_batch`, and
returning without an exception lets the host advance the checkpoint.

Event Hubs delivers at least once. If the function commits to DuckLake and then dies before the
checkpoint is written, the same batch is delivered again. To make the effect exactly-once, the
batch's (partition, signal, last sequence number) is recorded in `ingest_offsets` inside the SAME
DuckLake transaction as the rows, and a batch whose offsets are already recorded is skipped.

Each event body is one OTLP export request, either JSON or protobuf (the OpenTelemetry Collector's
azureeventhub exporter can produce either). A batch holds a single signal.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import List

READERS = {
    "logs": [("otlp_logs", "read_otlp_logs")],
    "traces": [("otlp_traces", "read_otlp_traces")],
    "metrics": [("otlp_metrics_gauge", "read_otlp_metrics_gauge"), ("otlp_metrics_sum", "read_otlp_metrics_sum")],
}


def _is_json(body: bytes) -> bool:
    return body.lstrip()[:1] == b"{"


def _write_events(events: List[bytes], directory: Path) -> str:
    """Write the events where `read_otlp_*` can read them; return the path or glob to read."""
    if all(_is_json(e) for e in events):
        # NDJSON: one export request per line (a JSON request must not contain a raw newline).
        lines = [e.decode().replace("\r", " ").replace("\n", " ") for e in events]
        path = directory / "batch.ndjson"
        path.write_text("\n".join(lines) + "\n")
        return str(path)
    if not any(_is_json(e) for e in events):
        for n, body in enumerate(events):
            (directory / f"{n:06d}.pb").write_bytes(body)
        return str(directory / "*.pb")
    raise ValueError("a batch must be all JSON or all protobuf")


def ensure_schema(con, signal: str, sample_path: str, catalog: str = "lake") -> None:
    for table, reader in READERS[signal]:
        con.execute(f"CREATE TABLE IF NOT EXISTS {catalog}.{table} AS SELECT * FROM {reader}('{sample_path}') LIMIT 0")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {catalog}.ingest_offsets "
        "(partition_id VARCHAR, signal VARCHAR, first_seq BIGINT, last_seq BIGINT)"
    )


def ingest_batch(con, signal: str, events: List[bytes], partition: str, first_seq: int, last_seq: int,
                 catalog: str = "lake") -> int:
    """Commit the batch once. Returns rows written, or 0 if this batch was already committed."""
    if not events:
        return 0
    with tempfile.TemporaryDirectory() as tmp:
        path = _write_events(events, Path(tmp))
        con.execute("BEGIN")
        try:
            ensure_schema(con, signal, path, catalog)
            already = con.execute(
                f"SELECT count(*) FROM {catalog}.ingest_offsets WHERE partition_id = ? AND signal = ? AND last_seq >= ?",
                [partition, signal, last_seq],
            ).fetchone()[0]
            if already:
                con.execute("ROLLBACK")
                return 0
            rows = 0
            for table, reader in READERS[signal]:
                rows += con.execute(f"INSERT INTO {catalog}.{table} SELECT * FROM {reader}('{path}')").fetchone()[0]
            con.execute(
                f"INSERT INTO {catalog}.ingest_offsets VALUES (?, ?, ?, ?)", [partition, signal, first_seq, last_seq]
            )
            con.execute("COMMIT")
            return rows
        except Exception:
            con.execute("ROLLBACK")
            raise
