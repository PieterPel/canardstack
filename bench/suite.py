#!/usr/bin/env python3
"""Run every stack for several trials, rotating the stack order each trial.

    uv run --project bench bench/suite.py --trials 3 -- --services 5 --records 2000

Everything after `--` is passed to run.py. Each run starts from freshly wiped
volumes (`--manage`), so trials are independent; rotating the order keeps one
stack from always running right after the same neighbour.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from backends import BACKENDS

HERE = Path(__file__).parent


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--stacks", nargs="+", choices=list(BACKENDS), default=list(BACKENDS))
    args, passthrough = ap.parse_known_args()
    passthrough = [a for a in passthrough if a != "--"]
    failed = []
    for trial in range(args.trials):
        shift = trial % len(args.stacks)
        order = args.stacks[shift:] + args.stacks[:shift]
        for stack in order:
            print(f"=== trial {trial + 1}/{args.trials}: {stack}", flush=True)
            code = subprocess.run(
                [sys.executable, str(HERE / "run.py"), "--stack", stack, "--manage", *passthrough]
            ).returncode
            if code:
                failed.append((trial + 1, stack))
    if failed:
        raise SystemExit(f"failed runs: {failed}")


if __name__ == "__main__":
    main()
