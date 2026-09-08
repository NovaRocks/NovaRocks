#!/usr/bin/env python3
"""Run one development-feedback command and record elapsed time and peak RSS."""

from __future__ import annotations

import argparse
import json
import resource
import subprocess
import time
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a command is required")
    started = time.monotonic_ns()
    completed = subprocess.run(command, check=False)
    elapsed_ns = time.monotonic_ns() - started
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    peak_rss = int(usage.ru_maxrss)
    if __import__("sys").platform != "darwin":
        peak_rss *= 1024
    report = {
        "schema_version": 1,
        "command": command,
        "exit_code": completed.returncode,
        "elapsed_ns": elapsed_ns,
        "peak_process_tree_rss_bytes": peak_rss,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
