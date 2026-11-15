#!/usr/bin/env python3
"""Commit OTLP/JSON trace export requests (one per line of a file) to a DuckLake catalog.

    ingest_traces.py init CATALOG                 create the SQLite catalog in WAL mode
    ingest_traces.py run  CATALOG DATA FILE       commit the requests in FILE with reedlake
"""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reedlake import ingest  # noqa: E402


def main() -> None:
    if sys.argv[1] == "init":
        connection = sqlite3.connect(sys.argv[2])
        connection.execute("PRAGMA journal_mode=WAL")
        connection.close()
        return
    _, _, catalog, data, path = sys.argv
    events = [line.encode() for line in Path(path).read_text().splitlines() if line.strip()]
    print(ingest(catalog, data, "traces", "harness", 0, len(events) - 1, events), f"from {len(events)} requests")


if __name__ == "__main__":
    main()
