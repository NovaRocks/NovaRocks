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

"""Resolve and validate the immutable shared benchmark fixture contracts."""

import argparse
import hashlib
import json
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
import sys
import tomllib


RESOLVED_SCHEMA_VERSION = 1
ENSURE_RESULT_SCHEMA_VERSION = 1
ERROR_SCHEMA_VERSION = 1
VALID_ENSURE_STATES = {"ReadyValid"}
VALID_ERROR_CODES = {
    "ready_invalid",
    "writer_failed",
    "publication_conflict",
    "publication_failed",
}


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest_bytes(value):
    return hashlib.sha256(value).hexdigest()


def digest_file(path):
    return digest_bytes(path.read_bytes())


def normalized_scale(suite, value):
    try:
        decimal = Decimal(str(value).strip().lower().removesuffix("gb").removesuffix("g"))
    except InvalidOperation as exc:
        raise ValueError(f"invalid scale for {suite}: {value}") from exc
    if decimal <= 0:
        raise ValueError(f"scale for {suite} must be positive: {value}")
    text = format(decimal.normalize(), "f").rstrip("0").rstrip(".")
    if not text:
        text = "0"
    return f"{text}GB" if suite == "tpc-ds" else text


def load_model(config_path):
    with config_path.open("rb") as source:
        model = tomllib.load(source)
    if not isinstance(model.get("fixture"), dict):
        raise ValueError("benchmark_tools.toml must contain [fixture]")
    return model


def source_input_digests(workspace_root, producer_inputs):
    if not isinstance(producer_inputs, dict) or not producer_inputs:
        raise ValueError("fixture.producer_inputs must be a non-empty table")
    digests = []
    for label, relative_path in sorted(producer_inputs.items()):
        if not isinstance(relative_path, str) or relative_path.startswith("/"):
            raise ValueError(f"producer input {label} must be a relative path")
        candidate = (workspace_root / relative_path).resolve()
        try:
            candidate.relative_to(workspace_root.resolve())
        except ValueError as exc:
            raise ValueError(f"producer input {label} escapes the workspace") from exc
        if not candidate.is_file():
            raise ValueError(f"producer input {label} is missing: {relative_path}")
        digests.append({"label": label, "sha256": digest_file(candidate)})
    return digests


def producer_lock_projections(workspace_root, declarations):
    """Hash producer semantics, never transport aliases or unrelated fixtures."""
    if not isinstance(declarations, dict) or not declarations:
        raise ValueError("fixture.producer_lock_projections must be a non-empty table")
    result = []
    for label, declaration in sorted(declarations.items()):
        relative = Path(declaration["lock"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("producer lock must be a workspace relative path")
        lock = json.loads((workspace_root / relative).read_text())
        name = declaration["derived_image"]
        derived = lock["derived_images"][name]
        base_names = [derived["base"]] if "base" in derived else list(derived["bases"].values())
        bases = [{field: lock["images"][base][field] for field in
                  ("source", "platform", "manifest_digest")} for base in base_names]
        projection = {
            "derived_image": name, "dockerfile": derived["dockerfile"],
            "platform": derived["platform"],
            "bases": sorted(bases, key=canonical_json),
            "artifacts": [{"name": name, **{field: lock["artifacts"][name][field]
                          for field in ("url", "bytes", "sha1")}}
                          for name in sorted(derived["artifacts"])],
        }
        result.append({"label": label, "projection": projection})
    return result


def validate_producer(workspace_root, resolved, publication, actual_image, actual_container):
    """Check the bound producer before consulting or publishing a READY object."""
    manifest = json.loads((publication / "manifest.json").read_text())
    runtime = manifest["runtime"]
    if not manifest.get("ready") or Path(runtime["publication_dir"]).resolve() != publication.resolve():
        raise ValueError("ProducerIdentityMismatch: publication is not ready or has moved")
    catalog = runtime["catalog"]
    saved = catalog["images"]["spark"]
    receipt = runtime["producer_receipt"]
    bom = json.loads(Path(manifest["fixture_inputs"]["bom"]).read_text())
    model = load_model(workspace_root / "tests/sql/fixtures/benchmarks/benchmark_tools.toml")
    declarations = model["fixture"]["producer_lock_projections"]
    if resolved["producer_fingerprint"]["lock_projections"] != producer_lock_projections(workspace_root, declarations):
        raise ValueError("ProducerIdentityMismatch: resolver producer inputs changed")
    # The supply receipt binds a canonical lock and exact definition bytes.
    # Compare those receipts before accepting the semantic projection above.
    sys.path.insert(0, str(workspace_root / "docker/fixture-inputs"))
    from fixture_inputs import load_lock, definition_sha256
    declaration = declarations["spark"]
    lock, lock_hash = load_lock(workspace_root / declaration["lock"])
    derived = lock["derived_images"][declaration["derived_image"]]
    bom_receipt = bom["derived_images"][declaration["derived_image"]]
    expected = {"lock_sha256": lock_hash, **bom_receipt}
    if (receipt != expected or saved["receipt"] != bom_receipt
            or bom["lock_sha256"] != lock_hash
            or bom_receipt["definition_sha256"] != definition_sha256(workspace_root, derived["definition_files"])
            or bom_receipt["platform"] != derived["platform"]
            or actual_container != catalog["containers"]["spark"]
            or not actual_image
            or actual_image != saved["image_id"] or actual_image != bom_receipt["image_id"]):
        raise ValueError("ProducerIdentityMismatch: actual Spark, saved record and BOM disagree")


def normalized_layouts(layouts):
    if not isinstance(layouts, list):
        raise ValueError("fixture.table_layouts must be an array")
    required = {"suite", "table", "range_partitions", "sort_columns", "target_file_size_bytes"}
    normalized = []
    for layout in layouts:
        require_fields(layout, required, "fixture table layout")
        if layout["suite"] not in ("ssb", "tpc-h", "tpc-ds") or not isinstance(layout["table"], str):
            raise ValueError("fixture table layout has an unknown suite or table")
        if not isinstance(layout["range_partitions"], int) or layout["range_partitions"] < 1:
            raise ValueError("fixture table layout range_partitions must be positive")
        if not isinstance(layout["sort_columns"], list) or not all(isinstance(column, str) for column in layout["sort_columns"]):
            raise ValueError("fixture table layout sort_columns must be a string array")
        normalized.append({field: layout[field] for field in required})
    return sorted(normalized, key=lambda value: (value["suite"], value["table"]))


def resolve_fixture(model, workspace_root, suite, scale, shared_root=None):
    if suite not in ("ssb", "tpc-h", "tpc-ds"):
        raise ValueError(f"unsupported suite: {suite}")
    suite_model = model.get(suite)
    fixture = model["fixture"]
    loader = fixture.get("loader")
    runtime = fixture.get("spark_runtime")
    if not isinstance(suite_model, dict) or not isinstance(loader, dict) or not isinstance(runtime, dict):
        raise ValueError("fixture model is missing a suite, loader, or spark_runtime section")
    normalized = normalized_scale(suite, scale or suite_model.get("default_scale"))
    root = (shared_root or fixture.get("shared_root", "")).rstrip("/")
    if not root.startswith("s3://") or root == "s3://":
        raise ValueError("shared root must be a non-empty s3:// URI")
    tables = suite_model.get("raw_tables")
    if not isinstance(tables, list) or not tables or any(not isinstance(table, str) for table in tables):
        raise ValueError(f"{suite}.raw_tables must be a non-empty string array")
    encoding = {"ssb": "UTF-8", "tpc-h": "UTF-8", "tpc-ds": "ISO-8859-1"}[suite]
    contract = {
        "contract_schema_version": fixture.get("contract_schema_version"),
        "suite": suite,
        "normalized_scale": normalized,
        "generator": {
            "name": suite_model.get("name"),
            "version": suite_model.get("version"),
            "archive_sha256": suite_model.get("archive_sha256"),
            "build_command": suite_model.get("build_command"),
        },
        "database": {"ssb": "ssb", "tpc-h": "tpch", "tpc-ds": "tpcds"}[suite],
        "tables": sorted(tables),
        "schema_version": loader.get("schema_version"),
        "raw_text_encoding": encoding,
        "iceberg_format_version": loader.get("iceberg_format_version"),
        "parquet_write_properties": loader.get("parquet_write_properties"),
        "parquet_hadoop_properties": loader.get("parquet_hadoop_properties"),
        "statistics_contract": loader.get("statistics_contract"),
        "table_layouts": normalized_layouts(fixture.get("table_layouts")),
        "spark_runtime": runtime,
    }
    if any(value in (None, "") for value in contract.values()):
        raise ValueError("fixture contract has missing required fields")
    producer_inputs = source_input_digests(workspace_root, fixture.get("producer_inputs"))
    producer_fingerprint = {
        "schema_version": fixture.get("schema_version"),
        "source_inputs": producer_inputs,
        "lock_projections": producer_lock_projections(workspace_root, fixture.get("producer_lock_projections")),
        "spark_runtime": runtime,
    }
    contract["producer_fingerprint"] = producer_fingerprint
    contract_id = digest_bytes(canonical_json(contract).encode())[:24]
    dataset_root = f"{root}/{suite}/{normalized.lower()}/{contract_id}"
    dataset_key = {"suite": suite, "scale": normalized, "fixture_contract_id": contract_id}
    return {
        "schema_version": RESOLVED_SCHEMA_VERSION,
        "dataset_key": dataset_key,
        "contract": contract,
        "producer_fingerprint": producer_fingerprint,
        "fixture_contract_id": contract_id,
        "dataset_root": dataset_root,
        "ready_uri": f"{dataset_root}/{fixture['ready_filename']}",
        "manifest_filename": fixture["manifest_filename"],
        "staging_parent": f"{dataset_root}/staging",
    }


def require_fields(value, fields, schema_name):
    if not isinstance(value, dict):
        raise ValueError(f"{schema_name} must be a JSON object")
    missing = sorted(field for field in fields if field not in value)
    if missing:
        raise ValueError(f"{schema_name} is missing required fields: {', '.join(missing)}")


def validate_ensure_result(value, expected_key=None):
    require_fields(value, {"schema_version", "dataset_key", "state", "reused", "built", "exact_warehouse", "manifest_uri", "publication"}, "EnsureResult")
    if value["schema_version"] != ENSURE_RESULT_SCHEMA_VERSION:
        raise ValueError("EnsureResult has an unknown schema_version")
    if value["state"] not in VALID_ENSURE_STATES:
        raise ValueError("EnsureResult has an unknown or non-ready state")
    if not isinstance(value["reused"], bool) or not isinstance(value["built"], bool) or value["reused"] == value["built"]:
        raise ValueError("EnsureResult must set exactly one of reused or built")
    if not isinstance(value["exact_warehouse"], str) or not value["exact_warehouse"].startswith("s3://"):
        raise ValueError("EnsureResult exact_warehouse must be an s3:// URI")
    if not isinstance(value["manifest_uri"], str) or not value["manifest_uri"].startswith("s3://"):
        raise ValueError("EnsureResult manifest_uri must be an s3:// URI")
    require_fields(value["publication"], {"ready_uri", "etag", "identity"}, "EnsureResult publication")
    if expected_key is not None and value["dataset_key"] != expected_key:
        raise ValueError("EnsureResult dataset_key does not match the resolved dataset")
    return value


def validate_error(value, expected_key=None):
    require_fields(value, {"schema_version", "error", "dataset_key", "message"}, "FixtureError")
    if value["schema_version"] != ERROR_SCHEMA_VERSION:
        raise ValueError("FixtureError has an unknown schema_version")
    if value["error"] not in VALID_ERROR_CODES:
        raise ValueError("FixtureError has an unknown error code")
    if expected_key is not None and value["dataset_key"] != expected_key:
        raise ValueError("FixtureError dataset_key does not match the resolved dataset")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace-root", default=Path(__file__).resolve().parents[4], type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--suite")
    parser.add_argument("--scale")
    parser.add_argument("--shared-root")
    parser.add_argument("--validate-ensure-result", type=Path)
    parser.add_argument("--validate-error", type=Path)
    parser.add_argument("--validate-producer", type=Path)
    parser.add_argument("--resolved-dataset", type=Path)
    parser.add_argument("--actual-image")
    parser.add_argument("--actual-container")
    args = parser.parse_args()
    workspace_root = args.workspace_root.resolve()
    config_path = args.config or workspace_root / "tests/sql/fixtures/benchmarks/benchmark_tools.toml"
    if args.validate_producer:
        validate_producer(workspace_root, json.loads(args.resolved_dataset.read_text()),
                          args.validate_producer.resolve(), args.actual_image, args.actual_container)
        return
    if args.validate_ensure_result or args.validate_error:
        source = args.validate_ensure_result or args.validate_error
        value = json.loads(source.read_text(encoding="utf-8"))
        expected_key = None
        if args.suite:
            model = load_model(config_path)
            expected_key = resolve_fixture(model, workspace_root, args.suite, args.scale, args.shared_root)["dataset_key"]
        if args.validate_ensure_result:
            validate_ensure_result(value, expected_key)
        else:
            validate_error(value, expected_key)
        return
    if not args.suite:
        parser.error("--suite is required when resolving a fixture")
    resolved = resolve_fixture(load_model(config_path), workspace_root, args.suite, args.scale, args.shared_root)
    print(canonical_json(resolved))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        print(f"fixture contract error: {exc}", file=sys.stderr)
        raise SystemExit(2)
