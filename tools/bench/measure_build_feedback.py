#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Measure reproducible NovaRocks build-feedback samples.

The caller supplies a dedicated checkout and target directory.  For an
incremental source-change case this tool warms the unmodified checkout,
applies the supplied patch, measures the configured command sequence, then
reverses the patch and warms the unmodified state before the next sample.
Raw command output, Cargo timings, environment identity, and exit status are
retained even when a command fails.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


CHANGE_CASES = {"private", "public", "generic"}
NO_PATCH_CASES = {"warm", "clean"}
TIME_RSS_PATTERNS = (
    re.compile(r"^\s*(\d+)\s+maximum resident set size\s*$"),
    re.compile(r"^\s*Maximum resident set size \(kbytes\):\s*(\d+)\s*$"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--case", choices=sorted(CHANGE_CASES | NO_PATCH_CASES), required=True)
    parser.add_argument("--patch", type=Path)
    parser.add_argument("--target-dir", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument(
        "--command",
        action="append",
        required=True,
        help="Shell command relative to the checkout; repeat to define the sequence",
    )
    parser.add_argument("--samples", type=int)
    return parser.parse_args()


def run_text(command: list[str], *, cwd: Path) -> str:
    return subprocess.run(
        command,
        cwd=cwd,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout.strip()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require_checkout(args: argparse.Namespace) -> tuple[Path, Path, int]:
    checkout = args.checkout.resolve()
    if not (checkout / ".git").exists():
        raise SystemExit(f"checkout is not a Git worktree: {checkout}")
    actual_revision = run_text(["git", "rev-parse", "HEAD"], cwd=checkout)
    expected_revision = run_text(["git", "rev-parse", args.revision], cwd=checkout)
    if actual_revision != expected_revision:
        raise SystemExit(
            f"checkout HEAD {actual_revision} does not match revision {expected_revision}"
        )
    dirty = run_text(["git", "status", "--porcelain", "--untracked-files=no"], cwd=checkout)
    if dirty:
        raise SystemExit("measurement checkout has tracked changes")

    if args.case in CHANGE_CASES and args.patch is None:
        raise SystemExit(f"--patch is required for {args.case}")
    if args.case in NO_PATCH_CASES and args.patch is not None:
        raise SystemExit(f"--patch is not allowed for {args.case}")

    samples = args.samples if args.samples is not None else (3 if args.case in NO_PATCH_CASES else 5)
    if samples < 1:
        raise SystemExit("--samples must be positive")
    if args.case == "clean" and args.target_dir.exists():
        raise SystemExit("clean measurement target must not exist before the run")

    patch = args.patch.resolve() if args.patch is not None else None
    if patch is not None:
        if not patch.is_file():
            raise SystemExit(f"patch does not exist: {patch}")
        subprocess.run(["git", "apply", "--check", str(patch)], cwd=checkout, check=True)
    return checkout, patch, samples


def tool_version(name: str, checkout: Path) -> str:
    return run_text([name, "--version"], cwd=checkout)


def parse_max_rss(stderr: str) -> tuple[int | None, str | None]:
    for line in stderr.splitlines():
        for pattern in TIME_RSS_PATTERNS:
            match = pattern.match(line)
            if match:
                unit = "bytes" if "maximum resident" in line else "KiB"
                return int(match.group(1)), unit
    return None, None


def timed_command(
    command: str,
    *,
    checkout: Path,
    target: Path,
    raw_dir: Path,
    label: str,
) -> dict[str, Any]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = raw_dir / f"{label}.stdout.log"
    stderr_path = raw_dir / f"{label}.stderr.log"
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(target)
    timer = ["/usr/bin/time", "-l"] if sys.platform == "darwin" else ["/usr/bin/time", "-v"]
    started = time.monotonic_ns()
    result = subprocess.run(
        timer + ["/bin/zsh", "-lc", command],
        cwd=checkout,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    elapsed_ns = time.monotonic_ns() - started
    stdout_path.write_text(result.stdout)
    stderr_path.write_text(result.stderr)
    max_rss, max_rss_unit = parse_max_rss(result.stderr)
    return {
        "command": command,
        "elapsed_ns": elapsed_ns,
        "exit_code": result.returncode,
        "max_rss": max_rss,
        "max_rss_unit": max_rss_unit,
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
    }


def run_sequence(
    commands: list[str],
    *,
    checkout: Path,
    target: Path,
    raw_dir: Path,
    sample: str,
) -> dict[str, Any]:
    records = []
    started = time.monotonic_ns()
    for index, command in enumerate(commands, 1):
        record = timed_command(
            command,
            checkout=checkout,
            target=target,
            raw_dir=raw_dir,
            label=f"{sample}-command-{index}",
        )
        records.append(record)
        if record["exit_code"] != 0:
            break
    return {
        "sample": sample,
        "elapsed_ns": time.monotonic_ns() - started,
        "exit_code": next((item["exit_code"] for item in records if item["exit_code"]), 0),
        "commands": records,
    }


def apply_patch(checkout: Path, patch: Path, *, reverse: bool = False) -> None:
    command = ["git", "apply"]
    if reverse:
        command.append("--reverse")
    command.append(str(patch))
    subprocess.run(command, cwd=checkout, check=True)


def copy_timings(checkout: Path, target: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for timings_root in (target / "cargo-timings", target / "debug" / ".fingerprint"):
        if timings_root.is_dir() and timings_root.name == "cargo-timings":
            for source in timings_root.iterdir():
                if source.is_file():
                    shutil.copy2(source, destination / source.name)
    # Cargo can place the timings directory under the checkout target when a
    # command overrides CARGO_TARGET_DIR. Record that condition explicitly.
    default_timings = checkout / "target" / "cargo-timings"
    if default_timings.is_dir() and default_timings.resolve() != (target / "cargo-timings").resolve():
        for source in default_timings.iterdir():
            if source.is_file():
                    shutil.copy2(source, destination / f"default-{source.name}")


def distribution(values: list[int]) -> dict[str, float | int]:
    if not values:
        return {}
    median = float(statistics.median(values))
    deviations = [abs(value - median) for value in values]
    mad = float(statistics.median(deviations))
    return {
        "samples": len(values),
        "median_ns": int(median),
        "mad_ns": int(mad),
        "noise_band": max(0.02, 3 * mad / median),
        "slowest_ns": max(values),
    }


def cargo_timing_summary(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    document = path.read_text()
    duration = re.search(r"^DURATION = ([0-9.]+);$", document, re.MULTILINE)
    units = re.search(
        r"const UNIT_DATA = (\[.*?\]);\nconst CONCURRENCY_DATA", document, re.DOTALL
    )
    if duration is None or units is None:
        return None
    unit_data = json.loads(units.group(1))
    longest = max(unit_data, key=lambda unit: float(unit.get("duration", 0)), default=None)
    return {
        "wall_seconds": float(duration.group(1)),
        "recompiled_units": len(unit_data),
        "longest_unit": (
            {
                "name": longest["name"],
                "target": longest.get("target", ""),
                "duration_seconds": longest["duration"],
            }
            if longest is not None
            else None
        ),
    }


def build_summary(
    records: list[dict[str, Any]],
    *,
    expected_samples: int,
    dirty: str,
    report_dir: Path,
) -> dict[str, Any]:
    successful = len(records) == expected_samples and all(
        item["exit_code"] == 0 for item in records
    )
    command_count = max((len(item["commands"]) for item in records), default=0)
    command_stats = []
    for index in range(command_count):
        items = [item["commands"][index] for item in records if len(item["commands"]) > index]
        elapsed = [int(item["elapsed_ns"]) for item in items if item["exit_code"] == 0]
        rss_bytes = []
        for item in items:
            rss = item.get("max_rss")
            if rss is None:
                continue
            rss_bytes.append(int(rss) * (1024 if item.get("max_rss_unit") == "KiB" else 1))
        stats = distribution(elapsed)
        stats.update(
            {
                "command_index": index + 1,
                "command": items[0]["command"] if items else None,
                "max_rss_bytes": max(rss_bytes, default=None),
            }
        )
        command_stats.append(stats)

    timing_stats = []
    for number in range(1, expected_samples + 1):
        timing = cargo_timing_summary(report_dir / "timings" / f"sample-{number}" / "cargo-timing.html")
        if timing is not None:
            timing_stats.append({"sample": number, **timing})

    return {
        "status": "passed" if successful and not dirty else "failed",
        "tracked_checkout_clean": not bool(dirty),
        "measured_samples": len(records),
        "successful_samples": sum(item["exit_code"] == 0 for item in records),
        "sequence": distribution([int(item["elapsed_ns"]) for item in records]),
        "commands": command_stats,
        "cargo_timings": timing_stats,
    }


def write_summary_markdown(summary: dict[str, Any], path: Path) -> None:
    lines = [
        "# Build feedback summary",
        "",
        f"Status: **{summary['status']}**",
        "",
        "| Command | Median (s) | MAD (s) | Noise band | Slowest (s) | Peak RSS (MiB) |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for item in summary["commands"]:
        lines.append(
            "| `{command}` | {median:.3f} | {mad:.3f} | {band:.2%} | {slowest:.3f} | {rss:.1f} |".format(
                command=item["command"],
                median=item["median_ns"] / 1e9,
                mad=item["mad_ns"] / 1e9,
                band=item["noise_band"],
                slowest=item["slowest_ns"] / 1e9,
                rss=(item["max_rss_bytes"] or 0) / (1024 * 1024),
            )
        )
    lines.extend(["", "## Cargo timings", ""])
    if summary["cargo_timings"]:
        lines.extend(
            [
                "| Sample | Cargo wall (s) | Recompiled units | Longest unit | Unit time (s) |",
                "|---:|---:|---:|---|---:|",
            ]
        )
        for item in summary["cargo_timings"]:
            longest = item["longest_unit"] or {}
            lines.append(
                f"| {item['sample']} | {item['wall_seconds']:.3f} | {item['recompiled_units']} | "
                f"{longest.get('name', '')}{longest.get('target', '')} | "
                f"{float(longest.get('duration_seconds', 0)):.3f} |"
            )
    else:
        lines.append("No Cargo timing report was produced.")
    path.write_text("\n".join(lines) + "\n")


def main() -> int:
    args = parse_args()
    checkout, patch, samples = require_checkout(args)
    report_dir = args.report_dir.resolve()
    report_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = report_dir / "raw"
    target_base = args.target_dir.resolve()
    lock = checkout / "Cargo.lock"
    manifest = {
        "schema_version": 1,
        "revision": run_text(["git", "rev-parse", "HEAD"], cwd=checkout),
        "case": args.case,
        "checkout": str(checkout),
        "target": str(target_base),
        "commands": args.command,
        "sample_count": samples,
        "rustc": tool_version("rustc", checkout),
        "cargo": tool_version("cargo", checkout),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "logical_cpus": os.cpu_count(),
        "cargo_lock_sha256": sha256(lock),
        "patch": str(patch) if patch is not None else None,
        "patch_sha256": sha256(patch) if patch is not None else None,
    }
    (report_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    patch_applied = False
    records: list[dict[str, Any]] = []
    try:
        if args.case != "clean":
            target_base.mkdir(parents=True, exist_ok=True)
            prewarm = run_sequence(
                args.command,
                checkout=checkout,
                target=target_base,
                raw_dir=raw_dir,
                sample="initial-prewarm",
            )
            if prewarm["exit_code"] != 0:
                records.append(prewarm)
                raise RuntimeError("initial prewarm failed")

        for number in range(1, samples + 1):
            target = target_base
            if args.case == "clean":
                target = target_base.parent / f"{target_base.name}-sample-{number}"
                if target.exists():
                    raise RuntimeError(f"clean sample target already exists: {target}")
                target.mkdir(parents=True)
            elif patch is not None:
                apply_patch(checkout, patch)
                patch_applied = True

            sample = run_sequence(
                args.command,
                checkout=checkout,
                target=target,
                raw_dir=raw_dir,
                sample=f"measured-{number}",
            )
            records.append(sample)
            copy_timings(checkout, target, report_dir / "timings" / f"sample-{number}")

            if patch_applied:
                apply_patch(checkout, patch, reverse=True)
                patch_applied = False
            if args.case not in NO_PATCH_CASES:
                prewarm = run_sequence(
                    args.command,
                    checkout=checkout,
                    target=target_base,
                    raw_dir=raw_dir,
                    sample=f"restore-prewarm-{number}",
                )
                if prewarm["exit_code"] != 0:
                    records.append(prewarm)
                    raise RuntimeError("restore prewarm failed")
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        (report_dir / "failure.txt").write_text(f"{type(error).__name__}: {error}\n")
    finally:
        if patch_applied and patch is not None:
            apply_patch(checkout, patch, reverse=True)
        (report_dir / "samples.json").write_text(json.dumps(records, indent=2) + "\n")

    dirty = run_text(["git", "status", "--porcelain", "--untracked-files=no"], cwd=checkout)
    summary = build_summary(
        records, expected_samples=samples, dirty=dirty, report_dir=report_dir
    )
    (report_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    write_summary_markdown(summary, report_dir / "SUMMARY.md")
    return 0 if summary["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
