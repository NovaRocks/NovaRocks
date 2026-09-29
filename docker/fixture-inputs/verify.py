#!/usr/bin/env python3
"""Pure-local verifier for an already provisioned fixture input BOM."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from fixture_inputs import FixtureInputError, definition_sha256, fixture_store, fixture_store_lock, inspect_image, load_lock, read_json, require_relative, required_input_names, validate_artifact, verify_image


SCRIPT_DIR = Path(__file__).resolve().parent


def verify(store: Path, repo_root: Path, lock_path: Path, *, consumer: str = "all") -> dict[str, Any]:
    lock, lock_sha = load_lock(lock_path)
    images, artifacts, derived_images = required_input_names(lock, consumer)
    ready = store / "READY"
    bom_path = store / "bom.json"
    if not ready.is_file() or not bom_path.is_file():
        raise FixtureInputError("fixture input BOM is missing; run docker/fixture-inputs/provision.sh outside verify CI")
    with fixture_store_lock(store, exclusive=False):
        if not ready.is_file() or not bom_path.is_file():
            raise FixtureInputError("fixture input BOM is missing; run docker/fixture-inputs/provision.sh outside verify CI")
        if ready.read_text().strip() != f"sha256:{lock_sha}":
            raise FixtureInputError("fixture input READY lock digest mismatch")
        bom = read_json(bom_path)
        if bom.get("schema") != 1 or bom.get("lock_sha256") != lock_sha:
            raise FixtureInputError("fixture input BOM lock digest mismatch")
        artifact_dir = store / require_relative(str(bom.get("artifact_dir", "")))
        for name, item in lock["images"].items():
            if name not in images:
                continue
            receipt = bom.get("images", {}).get(name, {})
            expected_receipt = {
                "alias": item["alias"],
                "manifest_digest": item["manifest_digest"],
                "platform": item["platform"],
            }
            if not isinstance(receipt, dict) or any(
                receipt.get(field) != value for field, value in expected_receipt.items()
            ):
                raise FixtureInputError(f"fixture image BOM receipt mismatch: {name}")
            verify_image(inspect_image(item["alias"]), item)
        for name, item in lock["artifacts"].items():
            if name not in artifacts:
                continue
            actual = validate_artifact(artifact_dir / name, item)
            if bom.get("artifacts", {}).get(name) != actual:
                raise FixtureInputError(f"fixture artifact BOM receipt mismatch: {name}")
        for name, item in lock["derived_images"].items():
            if name not in derived_images:
                continue
            receipt = bom.get("derived_images", {}).get(name, {})
            expected_definition = definition_sha256(repo_root, item["definition_files"])
            expected_receipt = {
                "alias": item["alias"],
                "platform": item["platform"],
                "definition_sha256": expected_definition,
            }
            if not isinstance(receipt, dict) or any(
                receipt.get(field) != value for field, value in expected_receipt.items()
            ):
                raise FixtureInputError(f"fixture derived image definition mismatch: {name}")
            info = inspect_image(item["alias"])
            verify_image(info, item, derived=True)
            labels = ((info.get("Config") or {}).get("Labels") or {})
            if labels.get("novarocks.fixture.lock.sha256") != lock_sha or labels.get("novarocks.fixture.definition.sha256") != expected_definition:
                raise FixtureInputError(f"fixture derived image label mismatch: {name}")
            if receipt.get("image_id") != str(info.get("Id", "")):
                raise FixtureInputError(f"fixture derived image BOM receipt mismatch: {name}")
        return bom


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store")
    parser.add_argument("--repo-root", default=SCRIPT_DIR.parents[1])
    parser.add_argument("--lock", default=SCRIPT_DIR / "lock.json")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--consumer", choices=("all", "iceberg-rest"), default="all")
    args = parser.parse_args()
    try:
        bom = verify(fixture_store(args.store), Path(args.repo_root).resolve(), Path(args.lock).resolve(), consumer=args.consumer)
    except FixtureInputError as error:
        print(f"BLOCKED: fixture prerequisite: {error}")
        return 75
    if args.json:
        print(json.dumps(bom, sort_keys=True))
    else:
        print("fixture input BOM verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
