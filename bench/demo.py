#!/usr/bin/env python3
"""Start stacks, load the deterministic demo workload (run id "demo"), and leave them up.

    uv run --project bench bench/demo.py [--stacks canardstack lgtm victoria]

Service names are svc-demo-0..4 and trace ids are fixed, so the Grafana dashboards under
bench/grafana work without any lookup. Do not run this while the benchmark suite is
running: both use the same ports and compose projects.
"""

from __future__ import annotations

import argparse
import subprocess
import time

from backends import BACKENDS
from run import compose, ingest, start_created, visibility, wait_ready
from workload import Workload


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stacks", nargs="+", choices=list(BACKENDS), default=list(BACKENDS))
    ap.add_argument("--services", type=int, default=5)
    ap.add_argument("--records", type=int, default=2000)
    args = ap.parse_args()
    for name in args.stacks:
        backend = BACKENDS[name]()
        print(f"=== {name}: wipe, start, load demo data", flush=True)
        compose(backend, "down", "-v")
        try:
            compose(backend, "up", "-d")
        except subprocess.CalledProcessError:
            pass  # dependents that raced their healthcheck are started below
        start_created(backend)
        wait_ready(backend)
        time.sleep(20)  # Loki/Tempo/Mimir hold ingesters unready for ~15 s after boot
        base_ns = (int(time.time()) - args.records - 120) * 10**9
        workload = Workload(1, args.services, args.records, base_ns, run="demo")
        accepted = ingest(backend, workload, 4)
        visibility(backend, workload, accepted, 120)
    print("stacks are up with the demo data; start Grafana with:")
    print("  cd bench/grafana && podman-compose -p bench-grafana up -d   # then open http://localhost:13030")


if __name__ == "__main__":
    main()
