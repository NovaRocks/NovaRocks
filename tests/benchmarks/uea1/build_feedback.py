#!/usr/bin/env python3
"""Run one command and sample the RSS of its complete process group."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Callable


DEFAULT_SAMPLE_INTERVAL_MILLIS = 100


def _process_snapshot() -> tuple[list[tuple[int, int, int, int]], str | None]:
    """Return one same-tick pid/ppid/pgid/RSS snapshot."""

    try:
        completed = subprocess.run(
            ["ps", "-axo", "pid=,ppid=,pgid=,rss="],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        return [], f"ps unavailable: {error}"
    if completed.returncode != 0:
        reason = completed.stderr.strip() or f"ps exited {completed.returncode}"
        return [], reason
    rows: list[tuple[int, int, int, int]] = []
    malformed = 0
    for line in completed.stdout.splitlines():
        fields = line.split()
        if len(fields) != 4:
            malformed += 1
            continue
        try:
            pid, ppid, pgid, rss_kib = (int(field) for field in fields)
        except ValueError:
            malformed += 1
            continue
        rows.append((pid, ppid, pgid, rss_kib * 1024))
    if not rows:
        return [], "ps returned no usable process rows"
    if malformed:
        return rows, f"ps omitted {malformed} malformed rows"
    return rows, None


def _aggregate_process_tree(
    rows: list[tuple[int, int, int, int]], root_pid: int, process_group: int
) -> tuple[int, list[int]]:
    """Aggregate the isolated group and descendants visible in one snapshot."""

    by_parent: dict[int, list[int]] = {}
    rss_by_pid: dict[int, int] = {}
    members: set[int] = set()
    for pid, ppid, pgid, rss_bytes in rows:
        by_parent.setdefault(ppid, []).append(pid)
        rss_by_pid[pid] = rss_bytes
        if pgid == process_group:
            members.add(pid)

    # Group membership keeps children visible if the root exits and they are
    # reparented. Ancestry also finds descendants that changed process group.
    frontier = [root_pid, *members]
    while frontier:
        parent = frontier.pop()
        for child in by_parent.get(parent, []):
            if child not in members:
                members.add(child)
                frontier.append(child)
    if root_pid in rss_by_pid:
        members.add(root_pid)
    pids = sorted(pid for pid in members if pid in rss_by_pid)
    return sum(rss_by_pid[pid] for pid in pids), pids


def run_command(
    command: list[str],
    sample_interval_millis: int = DEFAULT_SAMPLE_INTERVAL_MILLIS,
    snapshot: Callable[[], tuple[list[tuple[int, int, int, int]], str | None]] = _process_snapshot,
) -> tuple[int, dict[str, object]]:
    if sample_interval_millis <= 0:
        raise ValueError("sample interval must be positive")

    started = time.monotonic_ns()
    process = subprocess.Popen(command, start_new_session=True)
    process_group = process.pid
    interval_ns = sample_interval_millis * 1_000_000
    next_sample_ns = started
    samples: list[dict[str, object]] = []
    unavailable_reasons: list[str] = []
    missed_deadlines = 0
    attempted_samples = 0

    while True:
        now = time.monotonic_ns()
        if now >= next_sample_ns:
            overdue_intervals = (now - next_sample_ns) // interval_ns
            missed_deadlines += int(overdue_intervals)
            next_sample_ns += (overdue_intervals + 1) * interval_ns
            attempted_samples += 1
            rows, warning = snapshot()
            rss_bytes, pids = _aggregate_process_tree(rows, process.pid, process_group)
            unavailable_reason: str | None = None
            if warning and not rows:
                unavailable_reason = warning
                unavailable_reasons.append(warning)
            elif not pids:
                unavailable_reason = "target process tree was absent from process snapshot"
                unavailable_reasons.append(unavailable_reason)
            samples.append(
                {
                    "elapsed_millis": (time.monotonic_ns() - started) // 1_000_000,
                    "rss_bytes": rss_bytes if unavailable_reason is None else None,
                    "process_count": len(pids),
                    "pids": pids,
                    "unavailable_reason": unavailable_reason,
                    "snapshot_warning": warning if rows else None,
                }
            )
        return_code = process.poll()
        if return_code is not None:
            break
        remaining_ns = max(0, next_sample_ns - time.monotonic_ns())
        time.sleep(min(remaining_ns / 1_000_000_000, sample_interval_millis / 1000, 0.01))

    elapsed_ns = time.monotonic_ns() - started
    valid_rss = [
        int(sample["rss_bytes"])
        for sample in samples
        if sample["rss_bytes"] is not None
    ]
    report: dict[str, object] = {
        "schema_version": 2,
        "command": command,
        "exit_code": return_code,
        "elapsed_ns": elapsed_ns,
        "process_group": process_group,
        "peak_process_tree_rss_bytes": max(valid_rss) if valid_rss else None,
        "sampling": {
            "interval_millis": sample_interval_millis,
            "attempted_samples": attempted_samples,
            "successful_samples": len(valid_rss),
            "unavailable_samples": attempted_samples - len(valid_rss),
            "missed_deadlines": missed_deadlines,
            "unavailable_reasons": sorted(set(unavailable_reasons)),
        },
        "rss_samples": samples,
    }
    return return_code, report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--sample-interval-millis",
        type=int,
        default=DEFAULT_SAMPLE_INTERVAL_MILLIS,
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a command is required")
    try:
        return_code, report = run_command(command, args.sample_interval_millis)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
