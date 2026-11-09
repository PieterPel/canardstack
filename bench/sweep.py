#!/usr/bin/env python3
"""Run the shared workload at several sizes (and attribute cardinalities) against several stacks.

    uv run --project bench bench/sweep.py --stacks canardstack-pg lgtm --records 1000 10000 100000
    uv run --project bench bench/sweep.py --stacks canardstack-pg lgtm --records 10000 --users 0 1000 -- --rounds 2

Each (stack, size) is one `run.py --manage` run with every intent (`--intents all`), one client and
few repeats, since the heavy intents take seconds at large sizes. Results land in `bench/results/`
like any run; `scale_report.py` turns them into a table. Arguments after `--` go to `run.py`.

Timestamps are one second apart and start `records + 120` seconds ago, so 600,000 records per
service reach back a week, past what Loki accepts by default (`reject_old_samples_max_age`): keep
`--records` at or below 200,000 unless the stacks are configured for older data.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stacks", nargs="+", default=["canardstack-pg", "lgtm"])
    ap.add_argument("--records", type=int, nargs="+", default=[1000, 10000, 100000],
                    help="records per service, per signal")
    ap.add_argument("--users", type=int, nargs="+", default=[0],
                    help="log attribute cardinalities; 0 adds no attribute")
    ap.add_argument("--services", type=int, default=5)
    ap.add_argument("--repeats", type=int, default=5, help="runs per round")
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--clients", type=int, nargs="+", default=[1])
    ap.add_argument("extra", nargs="*", help="after --: more arguments for run.py")
    args = ap.parse_args()

    logs = HERE / "results" / "sweep-logs"
    logs.mkdir(parents=True, exist_ok=True)
    failed = []
    for users in args.users:
        for records in args.records:
            for stack in args.stacks:
                tag = f"{stack}-n{records}-u{users}"
                print(f"== {tag}", flush=True)
                cmd = [
                    sys.executable, str(HERE / "run.py"), "--stack", stack, "--manage",
                    "--services", str(args.services), "--records", str(records), "--users", str(users),
                    "--intents", "all", "--repeats", str(args.repeats), "--rounds", str(args.rounds),
                    "--clients", *map(str, args.clients), *args.extra,
                ]
                log = logs / f"{tag}-{time.strftime('%Y%m%dT%H%M%S')}.log"
                with log.open("w") as out:
                    code = subprocess.run(cmd, stdout=out, stderr=subprocess.STDOUT).returncode
                print(f"   exit {code}, log {log}", flush=True)
                if code:
                    failed.append(tag)
    if failed:
        print("failed runs:", ", ".join(failed))
        sys.exit(1)


if __name__ == "__main__":
    main()
