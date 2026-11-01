"""One serverless-function invocation: read OTLP files, commit them to DuckLake, exit.

    writer.py BATCH_INDEX ROOT      (ROOT holds batches/, data/ and lake.sqlite)

Set EXT_DIR to a directory of pre-installed extensions (see bake_extensions.py) to run
without any network access: nothing is downloaded and nothing is auto-installed.
"""

import os
import sys
import time

import duckdb

n, root = sys.argv[1], sys.argv[2]
ext_dir = os.environ.get("EXT_DIR")
t0 = time.perf_counter()
con = duckdb.connect()
if ext_dir:
    con.execute(f"SET extension_directory='{ext_dir}'")
    con.execute("SET autoinstall_known_extensions=false")
    con.execute("SET autoload_known_extensions=false")
for ext, repo in (("otlp", "community"), ("ducklake", "core"), ("sqlite", "core")):
    if not ext_dir:
        con.execute(f"INSTALL {ext} FROM {repo}")
    con.execute(f"LOAD {ext}")
t1 = time.perf_counter()
con.execute(f"ATTACH 'ducklake:sqlite:{root}/lake.sqlite' AS lake (DATA_PATH '{root}/data/')")
t2 = time.perf_counter()
sources = [
    ("otlp_logs", "read_otlp_logs", f"{root}/batches/{n}.logs.json"),
    ("otlp_traces", "read_otlp_traces", f"{root}/batches/{n}.traces.json"),
    ("otlp_metrics_gauge", "read_otlp_metrics_gauge", f"{root}/batches/{n}.metrics.json"),
    ("otlp_metrics_sum", "read_otlp_metrics_sum", f"{root}/batches/{n}.metrics.json"),
]
con.execute("BEGIN")  # one transaction, so one snapshot, per invocation
rows = 0
for table, reader, path in sources:
    con.execute(f"CREATE TABLE IF NOT EXISTS lake.{table} AS SELECT * FROM {reader}('{path}') LIMIT 0")
    rows += con.execute(f"INSERT INTO lake.{table} SELECT * FROM {reader}('{path}')").fetchone()[0]
con.execute("COMMIT")
t3 = time.perf_counter()
print(f"{rows},{t1 - t0:.3f},{t2 - t1:.3f},{t3 - t2:.3f}")
