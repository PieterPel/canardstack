#!/usr/bin/env python3
"""Pre-install the DuckDB extensions a writer function needs, and prove they load offline.

    python bake_extensions.py ./ext            install into ./ext, then verify offline loading
    python bake_extensions.py ./ext --verify   only verify

Ship ./ext inside the function package or image and point `extension_directory` at it. Extension
binaries are specific to the DuckDB version AND the platform (linux_amd64, linux_arm64, osx_arm64,
...): run this on the platform the function runs on (for example in a linux container in CI), not
on a laptop. The verification step runs in a child process with HOME pointed at an empty
directory and auto-install switched off, so a success means no download happened.
"""

import subprocess
import sys
import tempfile

import duckdb

EXTENSIONS = (("otlp", "community"), ("ducklake", "core"), ("sqlite", "core"), ("postgres", "core"))


def install(ext_dir: str) -> None:
    con = duckdb.connect()
    con.execute(f"SET extension_directory='{ext_dir}'")
    for name, repo in EXTENSIONS:
        con.execute(f"INSTALL {name} FROM {repo}")
        print(f"installed {name} from {repo}")


def verify(ext_dir: str) -> bool:
    code = f"""
import duckdb
con = duckdb.connect()
con.execute("SET extension_directory='{ext_dir}'")
con.execute("SET autoinstall_known_extensions=false")
con.execute("SET autoload_known_extensions=false")
for name in {[n for n, _ in EXTENSIONS]!r}:
    con.execute("LOAD " + name)
print("ok", duckdb.__version__)
"""
    with tempfile.TemporaryDirectory() as empty_home:
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env={"HOME": empty_home, "PATH": ""})
    print("offline load:", out.stdout.strip() or out.stderr.strip()[-300:])
    return out.returncode == 0


if __name__ == "__main__":
    target = sys.argv[1]
    if "--verify" not in sys.argv:
        install(target)
    sys.exit(0 if verify(target) else 1)
