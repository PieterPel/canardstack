"""Commit OTLP requests to a DuckLake the way a deployment does: with the `reedlake` binary.

The harnesses feed their data through this instead of a writer of their own, so what they test is
what runs. Set REEDLAKE_BINARY to a built `reedlake` (`cargo build -p reedlake` in the private-mono
checkout that holds it); it must be built against the same DuckDB as canardstack, because DuckDB
extensions only load into the exact version they were built for.
"""

import os
import subprocess
import tempfile
from pathlib import Path


def binary() -> str:
    path = os.environ.get("REEDLAKE_BINARY")
    if not path or not Path(path).is_file():
        raise SystemExit("set REEDLAKE_BINARY to a built reedlake binary (cargo build -p reedlake)")
    return path


def ingest(catalog, data, signal: str, stream: str, first_seq: int, last_seq: int, payloads: list[bytes]) -> str:
    """Commit one batch (one OTLP request per payload, all JSON or all protobuf) exactly once per
    (stream, sequence range) and return what reedlake reported."""
    with tempfile.TemporaryDirectory() as tmp:
        files = []
        for n, payload in enumerate(payloads):
            path = Path(tmp) / f"{n:06d}"
            path.write_bytes(payload)
            files.append(str(path))
        result = subprocess.run(
            [binary(), "--catalog", f"ducklake:sqlite:{catalog}", "--data-path", str(data), "ingest",
             "--signal", signal, "--stream", stream, "--first-seq", str(first_seq), "--last-seq", str(last_seq), *files],
            capture_output=True, text=True,
        )
    if result.returncode:
        raise RuntimeError(f"reedlake ingest failed: {result.stderr.strip()}")
    return result.stdout.strip()
