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

"""Publish a task-private, externally written Paimon planning fixture.

The provisioned Spark/Paimon writer is the only table author. This module
reuses its immutable writer receipt and Spark launch path, then records Spark
oracle summaries and the exact MinIO object inventory for B0 and candidate.
"""

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
sys.dont_write_bytecode = True
WRITER_SPEC = importlib.util.spec_from_file_location("novarocks_paimon_writer", WRITER_PATH)
if WRITER_SPEC is None or WRITER_SPEC.loader is None:
    raise RuntimeError("the provisioned Paimon writer is unavailable")
writer = importlib.util.module_from_spec(WRITER_SPEC)
sys.modules[WRITER_SPEC.name] = writer
WRITER_SPEC.loader.exec_module(writer)

KIND = "uea4a4-paimon-performance-v1"
DATABASE = "fixture"
APPEND_TABLE = "append_perf"
DEDUP_TABLE = "pk_perf"
ROWS_PER_COMMIT = 32
MIN_FILES = 16
DEFAULT_FILES = 24
SCOPE_RE = re.compile(
    r"^fixtures/uea4a4/paimon/[a-z0-9][a-z0-9-]{0,63}/"
    r"[a-z0-9][a-z0-9-]{0,47}-[0-9a-f]{12}$"
)


def definition_sha256(files_per_table: int) -> str:
    digest = hashlib.sha256()
    digest.update(Path(__file__).read_bytes())
    digest.update(f"\0{files_per_table}\0{ROWS_PER_COMMIT}".encode())
    return digest.hexdigest()


def scope_for(run_id: str, env_id: str, definition: str) -> str:
    writer.validate_run_id(run_id)
    env = writer.slug(env_id, 64)
    run = writer.slug(run_id, 48)
    suffix = writer.sha256_bytes(
        f"{KIND}\0{env_id}\0{run_id}\0{definition}".encode()
    )[:12]
    prefix = f"fixtures/uea4a4/paimon/{env}/{run}-{suffix}"
    if not SCOPE_RE.fullmatch(prefix):
        raise writer.FixtureError("generated performance prefix is unsafe")
    return prefix


def validate_warehouse(uri: str) -> str:
    prefix = uri.removeprefix("s3://novarocks/")
    if uri != f"s3://novarocks/{prefix}" or not SCOPE_RE.fullmatch(prefix):
        raise writer.FixtureError("warehouse is outside this task's MinIO prefix")
    if any(part in ("", ".", "..") for part in prefix.split("/")):
        raise writer.FixtureError("warehouse contains an unsafe path segment")
    return prefix


def render_sql(files_per_table: int) -> str:
    if not MIN_FILES <= files_per_table <= 128:
        raise writer.FixtureError("files-per-table must be between 16 and 128")
    statements = [
        f"CREATE NAMESPACE IF NOT EXISTS paimon.{DATABASE}",
        f"USE paimon.{DATABASE}",
        f"CREATE TABLE {APPEND_TABLE} (id BIGINT, value BIGINT) "
        "TBLPROPERTIES ('file.format'='parquet', 'file.compression'='zstd', "
        "'write-only'='true')",
        f"CREATE TABLE {DEDUP_TABLE} (id BIGINT, value BIGINT) "
        "TBLPROPERTIES ('primary-key'='id', 'merge-engine'='deduplicate', "
        "'bucket'='2', 'deletion-vectors.enabled'='false', "
        "'file.format'='parquet', 'write-only'='true')",
    ]
    for commit in range(files_per_table):
        offset = commit * ROWS_PER_COMMIT
        statements.append(
            f"INSERT INTO {APPEND_TABLE} SELECT CAST(id + {offset} AS BIGINT), "
            f"CAST({commit} AS BIGINT) FROM range(0, {ROWS_PER_COMMIT})"
        )
        statements.append(
            f"INSERT INTO {DEDUP_TABLE} SELECT CAST(id AS BIGINT), "
            f"CAST({commit} AS BIGINT) FROM range(0, {ROWS_PER_COMMIT})"
        )
    for table in (APPEND_TABLE, DEDUP_TABLE):
        statements.extend(
            [
                f"SELECT concat('NR_SNAPSHOT|build|{table}|', to_json(named_struct("
                "'snapshot_id', max(snapshot_id), 'schema_id', max(schema_id)))) "
                f"FROM `{table}$snapshots`",
                f"SELECT concat('NR_FILE|build|{table}|', to_json(named_struct("
                "'data_file_count', count(*), 'record_count', coalesce(sum(record_count), 0)))) "
                f"FROM `{table}$files`",
                f"SELECT concat('NR_ORACLE|build|{table}|', to_json(named_struct("
                "'row_count', count(*), 'min_id', min(id), 'max_id', max(id), "
                f"'id_sum', coalesce(sum(id), 0), 'value_sum', coalesce(sum(value), 0)))) "
                f"FROM {table}",
            ]
        )
    return ";\n".join(statements) + ";\n"


def spark_build(runtime: Any, versions: Mapping[str, str], image: str, uri: str, sql: str) -> str:
    environment = os.environ.copy()
    environment.update(
        {
            "AWS_ACCESS_KEY_ID": runtime.access_key,
            "AWS_SECRET_ACCESS_KEY": runtime.secret_key,
            "AWS_REGION": "us-east-1",
            "PAIMON_WAREHOUSE": uri,
            "PAIMON_S3_ENDPOINT": runtime.minio_endpoint_container,
        }
    )
    result = writer.run_command(
        writer.spark_command(runtime, versions, image),
        env=environment,
        stdin=sql,
        redactions=(runtime.access_key, runtime.secret_key),
    )
    return writer.sanitize_output(result.stdout, runtime)


def install_verified_mc_image(store: Path) -> None:
    """Resolve the exact local image even when prepare-only wrote a stub env."""
    writer.run_command(
        [str(REPO / "docker" / "fixture-inputs" / "verify.sh"), "--store", str(store)]
    )
    bom = writer.read_json(store / "bom.json")
    try:
        alias = bom["derived_images"]["minio-mc"]["image_id"]
    except (KeyError, TypeError) as error:
        raise writer.FixtureError("verified input BOM has no MinIO mc image") from error
    if not isinstance(alias, str) or not re.fullmatch(r"[a-z0-9][a-z0-9./:_-]+", alias):
        raise writer.FixtureError("verified input BOM has an invalid MinIO mc image ID")
    os.environ["MINIO_MC_IMAGE"] = alias


def summarize_oracle(output: str, files_per_table: int) -> dict[str, dict[str, int]]:
    markers = writer.parse_markers(output, "build")
    expected_tables = {APPEND_TABLE, DEDUP_TABLE}
    result: dict[str, dict[str, int]] = {}
    for marker in ("snapshot", "file", "oracle"):
        records = markers.get(marker, [])
        if len(records) != 2 or {record["table"] for record in records} != expected_tables:
            raise writer.FixtureError(f"Spark emitted incomplete {marker} oracle records")
    by_kind = {
        kind: {record["table"]: record["value"] for record in markers[kind]}
        for kind in ("snapshot", "file", "oracle")
    }
    for table in expected_tables:
        snapshot = by_kind["snapshot"][table]
        files = by_kind["file"][table]
        oracle = by_kind["oracle"][table]
        values = {
            "snapshot_id": int(snapshot["snapshot_id"]),
            "schema_id": int(snapshot["schema_id"]),
            "data_file_count": int(files["data_file_count"]),
            "row_count": int(oracle["row_count"]),
            "min_id": int(oracle["min_id"]),
            "max_id": int(oracle["max_id"]),
            "id_sum": int(oracle["id_sum"]),
            "value_sum": int(oracle["value_sum"]),
        }
        if values["snapshot_id"] < 0 or values["schema_id"] < 0:
            raise writer.FixtureError("Spark reported an invalid Paimon snapshot or schema")
        if values["data_file_count"] < MIN_FILES:
            raise writer.FixtureError("Spark did not retain at least 16 data files per table")
        if table == APPEND_TABLE:
            rows = files_per_table * ROWS_PER_COMMIT
            expected = (rows, 0, rows - 1, rows * (rows - 1) // 2,
                        ROWS_PER_COMMIT * files_per_table * (files_per_table - 1) // 2)
        else:
            expected = (ROWS_PER_COMMIT, 0, ROWS_PER_COMMIT - 1,
                        ROWS_PER_COMMIT * (ROWS_PER_COMMIT - 1) // 2,
                        ROWS_PER_COMMIT * (files_per_table - 1))
        actual = tuple(values[key] for key in
                       ("row_count", "min_id", "max_id", "id_sum", "value_sum"))
        if actual != expected:
            raise writer.FixtureError(f"Spark relation oracle differs for {table}")
        result[table] = values
    return result


def inventory(runtime: Any, uri: str, *, allow_empty: bool = False) -> list[dict[str, Any]]:
    prefix = validate_warehouse(uri)
    output = writer.compose_mc(
        runtime,
        """
set -eu
/usr/bin/mc alias set minio http://minio:9000 "${MINIO_ROOT_USER:-admin}" "${MINIO_ROOT_PASSWORD:-admin123}" >/dev/null
/usr/bin/mc ls --recursive --json "$1"
""".strip(),
        f"minio/novarocks/{prefix}/",
    )
    objects: list[dict[str, Any]] = []
    for line in output.splitlines():
        if not line.strip().startswith("{"):
            continue
        record = json.loads(line)
        if record.get("type") == "folder":
            continue
        key = str(record.get("key", "")).lstrip("/")
        if key.startswith(f"novarocks/{prefix}/"):
            key = key[len(f"novarocks/{prefix}/"):]
        elif key.startswith(f"{prefix}/"):
            key = key[len(f"{prefix}/"):]
        if not key or key.startswith("/") or any(part in ("", ".", "..") for part in key.split("/")):
            raise writer.FixtureError("MinIO returned an unsafe fixture object key")
        objects.append({"key": key, "size": int(record["size"]), "etag": record.get("etag")})
    objects.sort(key=lambda entry: entry["key"])
    if len({item["key"] for item in objects}) != len(objects):
        raise writer.FixtureError("MinIO returned duplicate fixture object keys")
    if not objects and not allow_empty:
        raise writer.FixtureError("Spark wrote no Paimon objects")
    return objects


def remove_prefix(runtime: Any, uri: str) -> None:
    prefix = validate_warehouse(uri)
    writer.compose_mc(
        runtime,
        """
set -eu
/usr/bin/mc alias set minio http://minio:9000 "${MINIO_ROOT_USER:-admin}" "${MINIO_ROOT_PASSWORD:-admin123}" >/dev/null
/usr/bin/mc rm --recursive --force --quiet "$1"
""".strip(),
        f"minio/novarocks/{prefix}/",
    )
    if inventory(runtime, uri, allow_empty=True):
        raise writer.FixtureError("task-private Paimon prefix remains after cleanup")


def verify_ready(output_dir: Path, runtime: Any) -> dict[str, Any]:
    manifest_path = output_dir / "manifest.json"
    ready_path = output_dir / "READY"
    if not manifest_path.is_file() or not ready_path.is_file():
        raise writer.FixtureError("Paimon performance fixture is not READY")
    if ready_path.read_text().strip() != f"sha256:{writer.sha256_file(manifest_path)}":
        raise writer.FixtureError("Paimon performance READY digest differs")
    manifest = writer.read_json(manifest_path)
    if manifest.get("fixture_kind") != KIND or manifest.get("schema_version") != 1:
        raise writer.FixtureError("Paimon performance manifest version differs")
    validate_warehouse(str(manifest.get("warehouse_uri", "")))
    for name, digest in manifest.get("artifacts", {}).items():
        if name not in ("rendered.sql", "oracle.json", "objects.json", "writer.log"):
            raise writer.FixtureError("manifest lists an unknown artifact")
        if writer.sha256_file(output_dir / name) != digest:
            raise writer.FixtureError(f"Paimon performance artifact differs: {name}")
    if set(manifest.get("artifacts", {})) != {"rendered.sql", "oracle.json", "objects.json", "writer.log"}:
        raise writer.FixtureError("Paimon performance manifest omits an artifact")
    if manifest.get("objects_sha256") != writer.sha256_file(output_dir / "objects.json"):
        raise writer.FixtureError("Paimon performance object digest differs")
    expected = writer.read_json(output_dir / "objects.json")
    if expected != inventory(runtime, str(manifest["warehouse_uri"])):
        raise writer.FixtureError("Paimon performance remote object inventory differs")
    writer.assert_artifacts_secret_free(output_dir, runtime)
    return manifest


def prepare(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir).expanduser().resolve()
    if output_dir == Path("/"):
        raise writer.FixtureError("output directory cannot be filesystem root")
    runtime = writer.load_runtime(Path(args.env_file).expanduser().resolve())
    versions = writer.load_versions()
    definition = definition_sha256(args.files_per_table)
    prefix = scope_for(args.run_id, runtime.env_id, definition)
    uri = f"s3://novarocks/{prefix}"
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / ".fixture.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        store = Path(args.fixture_store).expanduser().resolve()
        if not args.dry_run:
            install_verified_mc_image(store)
        if (output_dir / "READY").exists():
            manifest = verify_ready(output_dir, runtime)
            if (manifest.get("run_id"), manifest.get("warehouse_uri"), manifest.get("definition_sha256")) != (args.run_id, uri, definition):
                raise writer.FixtureError("existing fixture belongs to another run or definition")
            print(output_dir / "manifest.json")
            return 0
        if any(path.name != ".fixture.lock" for path in output_dir.iterdir()):
            raise writer.FixtureError("non-READY output directory is not empty")
        sql = render_sql(args.files_per_table)
        if args.dry_run:
            print(json.dumps({"warehouse_uri": uri, "sql_sha256": writer.sha256_bytes(sql.encode()), "ready_published": False}, sort_keys=True))
            return 0
        if inventory(runtime, uri, allow_empty=True):
            raise writer.FixtureError("task-private prefix is already occupied")
        receipt = writer.load_writer_bom(store / "bom.json", versions)
        try:
            output = spark_build(runtime, versions, receipt["alias"], uri, sql)
            oracle = summarize_oracle(output, args.files_per_table)
            objects = inventory(runtime, uri)
            for table in (APPEND_TABLE, DEDUP_TABLE):
                physical = sum(
                    f"{DATABASE}.db/{table}/" in item["key"] and item["key"].endswith(".parquet")
                    for item in objects
                )
                if physical < MIN_FILES:
                    raise writer.FixtureError(f"MinIO inventory has fewer than 16 Parquet files for {table}")
            writer.atomic_write(output_dir / "rendered.sql", sql.encode())
            writer.atomic_write(output_dir / "writer.log", output.encode())
            writer.write_json(output_dir / "oracle.json", oracle)
            writer.write_json(output_dir / "objects.json", objects)
            main = oracle[APPEND_TABLE]
            manifest = {
                "schema_version": 1,
                "fixture_kind": KIND,
                "run_id": args.run_id,
                "definition_sha256": definition,
                "warehouse_uri": uri,
                "s3_endpoint": runtime.minio_endpoint_host,
                "region": "us-east-1",
                "credential_name": runtime.credential_name,
                "credential_generation": runtime.credential_generation,
                "database": DATABASE,
                "table": APPEND_TABLE,
                "deduplicate_table": DEDUP_TABLE,
                "snapshot_id": main["snapshot_id"],
                "schema_id": main["schema_id"],
                "data_file_count": main["data_file_count"],
                "row_count": main["row_count"],
                "tables": oracle,
                "objects_sha256": writer.sha256_file(output_dir / "objects.json"),
                "writer": {
                    "provisioned_image_id": receipt["image_id"],
                    "provisioned_definition_sha256": receipt["definition_sha256"],
                    "input_lock_sha256": receipt["lock_sha256"],
                    "spark_manifest_digest": versions["SPARK_IMAGE_MANIFEST_DIGEST"],
                    "paimon_version": versions["PAIMON_VERSION"],
                },
                "artifacts": {
                    name: writer.sha256_file(output_dir / name)
                    for name in ("rendered.sql", "oracle.json", "objects.json", "writer.log")
                },
            }
            writer.write_json(output_dir / "manifest.json", manifest)
            writer.assert_artifacts_secret_free(output_dir, runtime)
            writer.atomic_write(output_dir / "READY", f"sha256:{writer.sha256_file(output_dir / 'manifest.json')}\n".encode())
            verify_ready(output_dir, runtime)
        except Exception:
            if not (output_dir / "READY").exists():
                remove_prefix(runtime, uri)
            raise
        print(output_dir / "manifest.json")
    return 0


def verify(args: argparse.Namespace) -> int:
    install_verified_mc_image(Path(args.fixture_store).expanduser().resolve())
    runtime = writer.load_runtime(Path(args.env_file).expanduser().resolve())
    verify_ready(Path(args.output_dir).expanduser().resolve(), runtime)
    print(Path(args.output_dir).expanduser().resolve() / "manifest.json")
    return 0


def cleanup(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir).expanduser().resolve()
    install_verified_mc_image(Path(args.fixture_store).expanduser().resolve())
    runtime = writer.load_runtime(Path(args.env_file).expanduser().resolve())
    with (output_dir / ".fixture.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        manifest = verify_ready(output_dir, runtime)
        if args.run_id != manifest.get("run_id"):
            raise writer.FixtureError("cleanup run ID differs from READY fixture")
        prefix = scope_for(args.run_id, runtime.env_id, str(manifest.get("definition_sha256")))
        uri = str(manifest["warehouse_uri"])
        if uri != f"s3://novarocks/{prefix}":
            raise writer.FixtureError("cleanup scope differs from READY fixture")
        if args.dry_run:
            print(uri)
            return 0
        remove_prefix(runtime, uri)
        (output_dir / "READY").unlink()
        writer.write_json(
            output_dir / "CLEANED",
            {
                "run_id": args.run_id,
                "warehouse_uri": uri,
                "manifest_sha256": writer.sha256_file(output_dir / "manifest.json"),
            },
        )
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
        if name == "cleanup":
            command.add_argument("--dry-run", action="store_true")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except (writer.FixtureError, OSError, KeyError, ValueError) as error:
        print(f"Paimon performance fixture error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
