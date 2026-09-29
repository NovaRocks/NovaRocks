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

"""Publish one immutable, task-private Spark/Iceberg REST planning fixture."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence


REPO = Path(__file__).resolve().parents[3]
WRITER_PATH = REPO / "docker" / "paimon-read" / "fixture.py"
WRITER_SPEC = importlib.util.spec_from_file_location("novarocks_fixture_support", WRITER_PATH)
if WRITER_SPEC is None or WRITER_SPEC.loader is None:
    raise RuntimeError("the shared fixture support is unavailable")
support = importlib.util.module_from_spec(WRITER_SPEC)
sys.modules[WRITER_SPEC.name] = support
WRITER_SPEC.loader.exec_module(support)

KIND = "uea4a4-iceberg-performance-v1"
TABLE = "planning_files"
ROWS_PER_INSERT = 32
DEFAULT_FILES = 24
MIN_FILES = 16
SCOPE_RE = re.compile(
    r"^fixtures/uea4a4/iceberg/[a-z0-9][a-z0-9-]{0,63}/"
    r"[a-z0-9][a-z0-9-]{0,47}-[0-9a-f]{12}$"
)
SNAPSHOT_RE = re.compile(r"NR_ORACLE\|(?P<json>\{[^\n}]+\})")


def definition_sha256(files_per_table: int) -> str:
    digest = hashlib.sha256()
    digest.update(Path(__file__).read_bytes())
    digest.update(f"\0{files_per_table}\0{ROWS_PER_INSERT}".encode())
    return digest.hexdigest()


def scope_for(run_id: str, env_id: str, definition: str) -> str:
    support.validate_run_id(run_id)
    env = support.slug(env_id, 64)
    run = support.slug(run_id, 48)
    suffix = support.sha256_bytes(
        f"{KIND}\0{env_id}\0{run_id}\0{definition}".encode()
    )[:12]
    prefix = f"fixtures/uea4a4/iceberg/{env}/{run}-{suffix}"
    if not SCOPE_RE.fullmatch(prefix):
        raise support.FixtureError("generated Iceberg fixture prefix is unsafe")
    return prefix


def validate_location(uri: str) -> str:
    base = "s3://warehouse/"
    suffix = "/table"
    if not uri.startswith(base) or not uri.endswith(suffix):
        raise support.FixtureError("Iceberg location is outside the task-private warehouse")
    prefix = uri[len(base):-len(suffix)]
    if not SCOPE_RE.fullmatch(prefix):
        raise support.FixtureError("Iceberg location has an invalid task-private prefix")
    if any(part in ("", ".", "..") for part in prefix.split("/")):
        raise support.FixtureError("Iceberg location has an unsafe path segment")
    return prefix


def database_for(prefix: str) -> str:
    return f"uea4a4_{prefix.rsplit('-', 1)[-1]}"


def render_sql(database: str, location: str, files_per_table: int) -> str:
    if not MIN_FILES <= files_per_table <= 128:
        raise support.FixtureError("files-per-table must be between 16 and 128")
    if not re.fullmatch(r"uea4a4_[0-9a-f]{12}", database):
        raise support.FixtureError("Iceberg namespace is not task-private")
    validate_location(location)
    target = f"ice_rest.{database}.{TABLE}"
    statements = [
        f"CREATE NAMESPACE IF NOT EXISTS ice_rest.{database}",
        f"CREATE TABLE {target} (id BIGINT, value BIGINT) USING iceberg "
        f"LOCATION '{location}' TBLPROPERTIES ("
        "'format-version'='2', 'write.format.default'='parquet', "
        "'write.distribution-mode'='none')",
    ]
    for ordinal in range(files_per_table):
        offset = ordinal * ROWS_PER_INSERT
        statements.append(
            f"INSERT INTO {target} SELECT CAST(id + {offset} AS BIGINT), "
            f"CAST({ordinal} AS BIGINT) FROM range(0, {ROWS_PER_INSERT})"
        )
    statements.append(
        "SELECT concat('NR_ORACLE|', to_json(named_struct("
        "'row_count', count(*), 'min_id', min(id), 'max_id', max(id), "
        "'id_sum', sum(id), 'value_sum', sum(value)))) "
        f"FROM {target}"
    )
    return ";\n".join(statements) + ";\n"


def install_verified_images(store: Path, runtime: Any) -> dict[str, Any]:
    support.run_command(
        [str(REPO / "docker" / "fixture-inputs" / "verify.sh"), "--store", str(store)]
    )
    bom = support.read_json(store / "bom.json")
    try:
        mc_alias = bom["images"]["minio-mc"]["alias"]
        spark_id = bom["derived_images"]["iceberg-spark"]["image_id"]
    except (KeyError, TypeError) as error:
        raise support.FixtureError("verified input BOM lacks Iceberg Spark or MinIO mc") from error
    if not isinstance(mc_alias, str) or not re.fullmatch(r"[a-z0-9][a-z0-9./:_-]+", mc_alias):
        raise support.FixtureError("verified MinIO mc alias is invalid")
    os.environ["MINIO_MC_IMAGE"] = mc_alias
    container = f"{runtime.compose_project}-spark-1"
    image = support.run_command(["docker", "inspect", container, "--format", "{{.Image}}"])
    if image.stdout.strip() != spark_id:
        raise support.FixtureError("running Spark container differs from the verified input BOM")
    return {"spark_image_id": spark_id, "mc_alias": mc_alias, "lock_sha256": bom["lock_sha256"]}


def inventory(runtime: Any, location: str, *, allow_empty: bool = False) -> list[dict[str, Any]]:
    prefix = validate_location(location)
    output = support.compose_mc(
        runtime,
        """
set -eu
/usr/bin/mc alias set minio http://minio:9000 "${MINIO_ROOT_USER:-admin}" "${MINIO_ROOT_PASSWORD:-admin123}" >/dev/null
/usr/bin/mc ls --recursive --json "$1"
""".strip(),
        f"minio/warehouse/{prefix}/table/",
    )
    objects: list[dict[str, Any]] = []
    for line in output.splitlines():
        if not line.strip().startswith("{"):
            continue
        record = json.loads(line)
        key = str(record.get("key", "")).lstrip("/")
        for leader in (f"warehouse/{prefix}/table/", f"{prefix}/table/"):
            if key.startswith(leader):
                key = key[len(leader):]
                break
        if not key or key.startswith("/") or any(part in ("", ".", "..") for part in key.split("/")):
            raise support.FixtureError("MinIO returned an unsafe Iceberg object key")
        objects.append({"key": key, "size": int(record["size"]), "etag": record.get("etag")})
    objects.sort(key=lambda item: item["key"])
    if len({item["key"] for item in objects}) != len(objects):
        raise support.FixtureError("MinIO returned duplicate Iceberg object keys")
    if not objects and not allow_empty:
        raise support.FixtureError("Spark wrote no Iceberg objects")
    return objects


def object_bytes(runtime: Any, location: str, relative_key: str) -> bytes:
    prefix = validate_location(location)
    if not relative_key.startswith("metadata/") or not relative_key.endswith(".metadata.json"):
        raise support.FixtureError("Iceberg metadata object path is invalid")
    return support.compose_mc_bytes(
        runtime,
        """
set -eu
/usr/bin/mc alias set minio http://minio:9000 "${MINIO_ROOT_USER:-admin}" "${MINIO_ROOT_PASSWORD:-admin123}" >/dev/null
/usr/bin/mc cat "$1"
""".strip(),
        f"minio/warehouse/{prefix}/table/{relative_key}",
    )


def current_metadata(runtime: Any, location: str, objects: list[dict[str, Any]]) -> dict[str, Any]:
    paths = sorted(
        item["key"] for item in objects
        if item["key"].startswith("metadata/") and item["key"].endswith(".metadata.json")
    )
    if not paths:
        raise support.FixtureError("Iceberg fixture has no metadata JSON")
    try:
        return json.loads(object_bytes(runtime, location, paths[-1]))
    except (ValueError, UnicodeError) as error:
        raise support.FixtureError("Iceberg current metadata JSON is invalid") from error


def spark_build(runtime: Any, env_file: Path, sql_path: Path) -> str:
    command_env = os.environ.copy()
    command_env["NOVA_ENV_REST_ENV_FILE"] = str(env_file)
    result = support.run_command(
        [str(REPO / "docker" / "iceberg-rest" / "spark-sql.sh"), str(sql_path)],
        env=command_env,
        redactions=(runtime.access_key, runtime.secret_key),
    )
    return support.sanitize_output(result.stdout, runtime)


def parse_oracle(output: str, files_per_table: int) -> dict[str, int]:
    matches = SNAPSHOT_RE.findall(output)
    if len(matches) != 1:
        raise support.FixtureError("Spark did not emit exactly one Iceberg relation oracle")
    values = json.loads(matches[0])
    rows = files_per_table * ROWS_PER_INSERT
    expected = {
        "row_count": rows,
        "min_id": 0,
        "max_id": rows - 1,
        "id_sum": rows * (rows - 1) // 2,
        "value_sum": ROWS_PER_INSERT * files_per_table * (files_per_table - 1) // 2,
    }
    if {key: int(values[key]) for key in expected} != expected:
        raise support.FixtureError("Spark Iceberg relation differs from the deterministic oracle")
    return expected


def remove_prefix(runtime: Any, location: str) -> None:
    prefix = validate_location(location)
    support.compose_mc(
        runtime,
        """
set -eu
/usr/bin/mc alias set minio http://minio:9000 "${MINIO_ROOT_USER:-admin}" "${MINIO_ROOT_PASSWORD:-admin123}" >/dev/null
/usr/bin/mc rm --recursive --force --quiet "$1"
""".strip(),
        f"minio/warehouse/{prefix}/table/",
    )
    if inventory(runtime, location, allow_empty=True):
        raise support.FixtureError("task-private Iceberg prefix remains after cleanup")


def verify_ready(output_dir: Path, runtime: Any) -> dict[str, Any]:
    manifest_path = output_dir / "manifest.json"
    ready_path = output_dir / "READY"
    if not manifest_path.is_file() or not ready_path.is_file():
        raise support.FixtureError("Iceberg performance fixture is not READY")
    if ready_path.read_text().strip() != f"sha256:{support.sha256_file(manifest_path)}":
        raise support.FixtureError("Iceberg fixture READY digest differs")
    manifest = support.read_json(manifest_path)
    if manifest.get("fixture_kind") != KIND or manifest.get("schema_version") != 1:
        raise support.FixtureError("Iceberg fixture manifest version differs")
    location = str(manifest.get("table_location", ""))
    validate_location(location)
    if set(manifest.get("artifacts", {})) != {"rendered.sql", "oracle.json", "objects.json", "writer.log"}:
        raise support.FixtureError("Iceberg fixture artifact set differs")
    for name, digest in manifest["artifacts"].items():
        if support.sha256_file(output_dir / name) != digest:
            raise support.FixtureError(f"Iceberg fixture artifact differs: {name}")
    if support.sha256_file(output_dir / "objects.json") != manifest.get("objects_sha256"):
        raise support.FixtureError("Iceberg fixture object inventory digest differs")
    expected = support.read_json(output_dir / "objects.json")
    actual = inventory(runtime, location)
    if expected != actual:
        raise support.FixtureError("Iceberg fixture remote object inventory differs")
    metadata = current_metadata(runtime, location, actual)
    if (metadata.get("current-snapshot-id"), metadata.get("current-schema-id")) != (
        manifest.get("snapshot_id"), manifest.get("schema_id")
    ):
        raise support.FixtureError("Iceberg fixture current snapshot or schema changed")
    support.assert_artifacts_secret_free(output_dir, runtime)
    return manifest


def prepare(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir).expanduser().resolve()
    if output_dir == Path("/"):
        raise support.FixtureError("output directory cannot be filesystem root")
    env_file = Path(args.env_file).expanduser().resolve()
    values = support.parse_env_file(env_file)
    runtime = support.load_runtime(env_file)
    definition = definition_sha256(args.files_per_table)
    prefix = scope_for(args.run_id, runtime.env_id, definition)
    location = f"s3://warehouse/{prefix}/table"
    database = database_for(prefix)
    rest_uri = support.require_value(values, "NOVAROCKS_ICEBERG_REST_URI")
    rest_warehouse = support.require_value(values, "NOVAROCKS_ICEBERG_REST_WAREHOUSE")
    sql = render_sql(database, location, args.files_per_table)
    if args.dry_run:
        print(json.dumps({"table_location": location, "sql_sha256": support.sha256_bytes(sql.encode()), "ready_published": False}, sort_keys=True))
        return 0
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / ".fixture.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        inputs = install_verified_images(Path(args.fixture_store).expanduser().resolve(), runtime)
        if (output_dir / "READY").exists():
            manifest = verify_ready(output_dir, runtime)
            if (manifest.get("run_id"), manifest.get("table_location"), manifest.get("definition_sha256")) != (
                args.run_id, location, definition
            ):
                raise support.FixtureError("existing Iceberg fixture belongs to another run or definition")
            print(output_dir / "manifest.json")
            return 0
        if any(path.name != ".fixture.lock" for path in output_dir.iterdir()):
            raise support.FixtureError("non-READY Iceberg output directory is not empty")
        if inventory(runtime, location, allow_empty=True):
            raise support.FixtureError("task-private Iceberg prefix is already occupied")
        sql_path = output_dir / "rendered.sql"
        support.atomic_write(sql_path, sql.encode())
        output = spark_build(runtime, env_file, sql_path)
        oracle = parse_oracle(output, args.files_per_table)
        objects = inventory(runtime, location)
        data_file_count = sum(item["key"].startswith("data/") and item["key"].endswith(".parquet") for item in objects)
        if data_file_count < MIN_FILES:
            raise support.FixtureError("Spark retained fewer than 16 Iceberg Parquet data files")
        metadata = current_metadata(runtime, location, objects)
        snapshot_id = metadata.get("current-snapshot-id")
        schema_id = metadata.get("current-schema-id")
        if not isinstance(snapshot_id, int) or not isinstance(schema_id, int) or snapshot_id < 0 or schema_id < 0:
            raise support.FixtureError("Iceberg metadata omitted a valid current snapshot or schema")
        support.atomic_write(output_dir / "writer.log", output.encode())
        support.write_json(output_dir / "oracle.json", oracle)
        support.write_json(output_dir / "objects.json", objects)
        manifest = {
            "schema_version": 1,
            "fixture_kind": KIND,
            "run_id": args.run_id,
            "definition_sha256": definition,
            "warehouse_uri": rest_warehouse,
            "table_location": location,
            "rest_uri": rest_uri,
            "s3_endpoint": runtime.minio_endpoint_host,
            "region": "us-east-1",
            "credential_name": runtime.credential_name,
            "credential_generation": runtime.credential_generation,
            "database": database,
            "table": TABLE,
            "snapshot_id": snapshot_id,
            "schema_id": schema_id,
            "data_file_count": data_file_count,
            "row_count": oracle["row_count"],
            "objects_sha256": support.sha256_file(output_dir / "objects.json"),
            "writer": inputs,
            "artifacts": {name: support.sha256_file(output_dir / name)
                          for name in ("rendered.sql", "oracle.json", "objects.json", "writer.log")},
        }
        support.write_json(output_dir / "manifest.json", manifest)
        support.assert_artifacts_secret_free(output_dir, runtime)
        support.atomic_write(output_dir / "READY", f"sha256:{support.sha256_file(output_dir / 'manifest.json')}\n".encode())
        verify_ready(output_dir, runtime)
        print(output_dir / "manifest.json")
    return 0


def verify(args: argparse.Namespace) -> int:
    env_file = Path(args.env_file).expanduser().resolve()
    runtime = support.load_runtime(env_file)
    install_verified_images(Path(args.fixture_store).expanduser().resolve(), runtime)
    output_dir = Path(args.output_dir).expanduser().resolve()
    verify_ready(output_dir, runtime)
    print(output_dir / "manifest.json")
    return 0


def cleanup(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir).expanduser().resolve()
    env_file = Path(args.env_file).expanduser().resolve()
    runtime = support.load_runtime(env_file)
    install_verified_images(Path(args.fixture_store).expanduser().resolve(), runtime)
    with (output_dir / ".fixture.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        manifest = verify_ready(output_dir, runtime)
        if manifest.get("run_id") != args.run_id:
            raise support.FixtureError("Iceberg cleanup run ID differs from READY fixture")
        prefix = scope_for(args.run_id, runtime.env_id, str(manifest["definition_sha256"]))
        location = str(manifest["table_location"])
        if location != f"s3://warehouse/{prefix}/table" or manifest.get("database") != database_for(prefix):
            raise support.FixtureError("Iceberg cleanup target differs from exact task scope")
        sql = f"DROP TABLE IF EXISTS ice_rest.{manifest['database']}.{TABLE};\nDROP NAMESPACE IF EXISTS ice_rest.{manifest['database']};\n"
        sql_path = output_dir / "cleanup.sql"
        support.atomic_write(sql_path, sql.encode())
        spark_build(runtime, env_file, sql_path)
        remove_prefix(runtime, location)
        (output_dir / "READY").unlink()
        support.write_json(output_dir / "CLEANED", {"run_id": args.run_id, "table_location": location})
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    default_env = REPO / "docker" / "iceberg-rest" / "runtime" / "current" / "env.sh"
    default_store = Path(os.environ.get("NOVA_FIXTURE_STORE", Path.home() / ".cache" / "novarocks" / "fixture-inputs"))
    for name, handler in (("prepare", prepare), ("verify", verify), ("cleanup", cleanup)):
        command = commands.add_parser(name)
        command.add_argument("--output-dir", required=True)
        command.add_argument("--env-file", default=str(default_env))
        command.add_argument("--fixture-store", default=str(default_store))
        command.set_defaults(handler=handler)
        if name != "verify":
            command.add_argument("--run-id", required=True)
        if name == "prepare":
            command.add_argument("--files-per-table", type=int, default=DEFAULT_FILES)
            command.add_argument("--dry-run", action="store_true")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except (support.FixtureError, OSError, KeyError, ValueError) as error:
        print(f"Iceberg performance fixture error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
