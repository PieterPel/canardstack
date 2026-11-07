#!/usr/bin/env python3
"""Commit OTLP/JSON trace export requests (one per line of a file) to a DuckLake catalog.

    ingest_traces.py init CATALOG                 create the SQLite catalog in WAL mode
    ingest_traces.py run  CATALOG DATA FILE       attach the catalog and commit the requests in FILE
"""

import sqlite3
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "serverless_writer"))
from eventhub_ingest import ingest_batch  # noqa: E402


def main() -> None:
    if sys.argv[1] == "init":
        connection = sqlite3.connect(sys.argv[2])
        connection.execute("PRAGMA journal_mode=WAL")
        connection.close()
        return
    _, _, catalog, data, path = sys.argv
    events = [line.encode() for line in Path(path).read_text().splitlines() if line.strip()]
    con = duckdb.connect()
    for ext, repo in (("otlp", "community"), ("ducklake", "core"), ("sqlite", "core")):
        con.execute(f"INSTALL {ext} FROM {repo}")
        con.execute(f"LOAD {ext}")
    con.execute(f"ATTACH 'ducklake:sqlite:{catalog}' AS lake (DATA_PATH '{data}/')")
    rows = ingest_batch(con, "traces", events, "harness", 0, len(events) - 1)
    print(f"committed {rows} rows from {len(events)} requests")


if __name__ == "__main__":
    main()
