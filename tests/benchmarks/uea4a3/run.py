#!/usr/bin/env python3
"""Run the frozen UEA-4A-3 scan-producer performance protocol, failing closed.

Each configuration group runs once per side, B0 first and the candidate right
after it, so a drift of the machine between groups affects both sides of a
comparison alike. Every run is one native 1FE+3BE cluster launched by the
candidate's system-test runner under the performance profile. The script only
launches and checks provenance; compare.py derives every statistic from the
raw receipts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

SCENARIO = "uea4/scan-producer-performance"
SCENARIO_DIR = "uea4-scan-producer-performance"
RECEIPT = "uea4a3-performance.json"
# B0 plus the three pre-existing fixes the candidate carries, cherry-picked
# in order (README).
B0_REVISION = "63237c9314827b9747b11860eb223fcee433b82d"
B0_BASE_REVISION = "94304f0154b6af25cc7d0378557ae2ab3ccd15ff"
SHARED_FIX_REVISIONS = (
    # PR #1080, #1081 and #1082 as first applied to 94304f0 (README).
    "cd5f684676242def4136544f4cbd87e46fb12a26",
    "73c2df298cfcaef773400a8fe9ec861a9c32cb46",
    "c214c723960686d26044e5a3c8fae3316fcd4ba0",
)
FROZEN_MANIFEST = Path(__file__).resolve().parent / "workload.json"
SIDES = ("b0", "candidate")
REQUIRED_ENVIRONMENT = (
    "NOVAROCKS_ICEBERG_REST_URI",
    "NOVAROCKS_ICEBERG_REST_WAREHOUSE",
    "AWS_S3_ENDPOINT",
    "AWS_S3_ACCESS_KEY_ID",
    "AWS_S3_SECRET_ACCESS_KEY",
)


class PreflightError(ValueError):
    pass


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def existing_file(path: Path, label: str) -> Path:
    if not path.is_file():
        raise PreflightError(f"{label} is missing: {path}")
    return path.resolve()


def load_manifest(path: Path) -> dict:
    document = json.loads(existing_file(path, "workload manifest").read_text())
    if document.get("schema_version") != 1 or document.get("scenario") != SCENARIO:
        raise PreflightError(f"{path} is not a {SCENARIO} manifest")
    names = {workload["name"] for workload in document["workloads"]}
    for group in document["config_groups"]:
        unknown = set(group["workloads"]) - names
        if unknown:
            raise PreflightError(f"group {group['name']} names unknown workloads {sorted(unknown)}")
    return document


def run_budget_seconds(manifest: dict, group: dict) -> int:
    """An upper bound on one run, from the manifest; only a runner deadline."""
    measure = manifest["measurement"]
    per_workload = (measure["warmup_seconds"]
                    + measure["repetitions"] * (measure["window_seconds"]
                                                + measure["query_timeout_seconds"]))
    budget = 3600 + per_workload * len(group["workloads"])
    if group["control"]:
        control = manifest["control"]
        budget += (control["window_seconds"]
                   + 2 * control["short_query_samples"]
                   * (control["short_query_interval_ms"] / 1000 + measure["query_timeout_seconds"])
                   + 2 * control["kill_samples"] * 60)
    return int(budget)


def git_revision(repository: Path) -> tuple[str, list[str]]:
    head = subprocess.run(["git", "-C", str(repository), "rev-parse", "HEAD"],
                          capture_output=True, text=True, check=True).stdout.strip()
    status = subprocess.run(["git", "-C", str(repository), "status", "--porcelain"],
                            capture_output=True, text=True, check=True).stdout.splitlines()
    return head, [line[3:] for line in status]


def runner_has_scenario(runner: Path) -> None:
    listing = subprocess.run([str(runner), "--list"], capture_output=True, text=True, check=False)
    if listing.returncode or SCENARIO not in listing.stdout.split():
        raise PreflightError(f"runner does not list {SCENARIO}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner", type=Path, required=True)
    parser.add_argument("--b0", type=Path, required=True, help="B0 server binary")
    parser.add_argument("--candidate", type=Path, required=True, help="candidate server binary")
    parser.add_argument("--paimon-fixture", type=Path, required=True,
                        help="READY Paimon fixture directory (manifest.json, base-server.toml)")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, default=FROZEN_MANIFEST)
    parser.add_argument("--smoke", action="store_true",
                        help="plumbing check with a non-frozen manifest; no gate is evaluated")
    args = parser.parse_args()
    try:
        manifest_path = existing_file(args.manifest, "workload manifest")
        if not args.smoke and manifest_path != FROZEN_MANIFEST:
            raise PreflightError("a formal run uses the frozen workload.json; pass --smoke otherwise")
        if args.smoke and manifest_path == FROZEN_MANIFEST:
            raise PreflightError("a smoke run must not use the frozen manifest")
        manifest = load_manifest(manifest_path)
        runner = existing_file(args.runner, "runner binary")
        binaries = {"b0": existing_file(args.b0, "B0 binary"),
                    "candidate": existing_file(args.candidate, "candidate binary")}
        if sha256(binaries["b0"]) == sha256(binaries["candidate"]):
            raise PreflightError("B0 and candidate are the same binary")
        fixture = args.paimon_fixture.resolve()
        fixture_manifest = existing_file(fixture / "manifest.json", "Paimon fixture manifest")
        existing_file(fixture / "READY", "Paimon fixture READY")
        base_config = existing_file(fixture / "base-server.toml", "Paimon fixture base config")
        missing = [name for name in REQUIRED_ENVIRONMENT if not os.environ.get(name)]
        if missing:
            raise PreflightError("source docker/iceberg-rest/runtime/current/env.sh first; missing "
                                 + ", ".join(missing))
        runner_has_scenario(runner)
        output = args.output.resolve()
        if output.exists() and any(output.iterdir()):
            raise PreflightError(f"output is not empty: {output}")
        repository = Path(__file__).resolve().parents[3]
        revision, dirty = git_revision(repository)
        # Only test harness and benchmark files may differ from the candidate
        # revision: the server binary is built from that revision alone.
        product_dirty = [path for path in dirty
                         if not path.startswith(("tests/", "reports/", "docker/iceberg-rest/runtime"))]
        if product_dirty and not args.smoke:
            raise PreflightError("the candidate tree has uncommitted product changes: "
                                 + ", ".join(product_dirty))
        provenance = {
            "smoke": args.smoke,
            "manifest_path": str(manifest_path),
            "manifest_sha256": sha256(manifest_path),
            "runner_sha256": sha256(runner),
            "binary_path_by_side": {side: str(path) for side, path in binaries.items()},
            "binary_sha256_by_side": {side: sha256(path) for side, path in binaries.items()},
            "b0_revision": B0_REVISION,
            "b0_base_revision": B0_BASE_REVISION,
            "shared_fix_revisions": list(SHARED_FIX_REVISIONS),
            "candidate_revision": revision,
            "candidate_dirty_paths": dirty,
            "paimon_fixture_manifest_sha256": sha256(fixture_manifest),
            "base_config_sha256": sha256(base_config),
            "order": [[group["name"], side] for group in manifest["config_groups"] for side in SIDES],
        }
        output.mkdir(parents=True, exist_ok=True)
        (output / "input-hashes.json").write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n")
        env = os.environ.copy()
        env["NOVAROCKS_UEA4A3_WORKLOAD_MANIFEST"] = str(manifest_path)
        env["NOVAROCKS_PAIMON_FIXTURE_MANIFEST"] = str(fixture_manifest)
        env["NO_PROXY"] = "127.0.0.1,localhost"
        for group in manifest["config_groups"]:
            for side in SIDES:
                name = f"{group['name']}/{side}"
                env["NOVAROCKS_UEA4A3_CONFIG_GROUP"] = group["name"]
                env["NOVAROCKS_UEA4A3_SIDE"] = side
                command = [str(runner), "--only", SCENARIO, "--binary", str(binaries[side]),
                           "--config", str(base_config),
                           "--artifact-root", str(output / group["name"] / side),
                           "--cluster-size", "3", "--launch-profile", "performance",
                           "--timeout-secs", str(run_budget_seconds(manifest, group))]
                print(f"UEA-4A-3: running {name}", flush=True)
                completed = subprocess.run(command, env=env, check=False)
                if completed.returncode:
                    raise PreflightError(f"{name} runner failed with exit {completed.returncode}")
                existing_file(output / group["name"] / side / SCENARIO_DIR / RECEIPT,
                              f"{name} performance receipt")
        from compare import compare  # The receipts exist; compare re-derives everything.
        result = compare(output, manifest_path)
        print(result["verdict_line"])
        return 0 if (args.smoke or result["passed"]) else 1
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"UEA-4A-3 benchmark preflight/run failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
