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

"""Build and publish the PAI-1 external Paimon read fixture.

Spark and Apache Paimon are the data-writing oracle. This script only renders
the checked-in SQL, runs each monotonic fixture stage, validates the emitted
relations, and publishes a content-addressed manifest. It never implements or
reuses the NovaRocks Paimon reader.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import fcntl
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlparse


SCRIPT_DIR = Path(__file__).resolve().parent
STAGES = ("s1", "s2", "compacted", "schema")
MARKERS = ("ORACLE", "SNAPSHOT", "SCHEMA", "FILE", "MANIFEST")
FIXTURE_KIND = "novarocks-paimon-read-v1"
# Local-only alias for the pinned Spark base manifest. The tag embeds the
# digest, so the name can only ever stand for that one manifest.
LOCAL_BASE_ALIAS_REPOSITORY = "novarocks/paimon-read-base"
ALL_TABLES = (
    "append_none",
    "append_snappy",
    "append_zstd",
    "append_lz4",
    "append_partitioned",
    "empty_append",
    "pk_default",
    "pk_dynamic",
    "pk_sequence",
    "pk_all_deleted",
    "pk_composite",
    "pk_date_decimal",
    "pk_no_stats",
    "schema_evolution",
    "type_matrix",
    "unsupported_orc",
    "unsupported_avro_data",
    "unsupported_dv",
    "unsupported_aggregation",
    "unsupported_nested",
    "unsupported_timestamp_ltz",
    "unsupported_postpone",
    "unsupported_multi_sequence",
)
SNAPSHOT_ADVANCES = {
    "s2": (
        "append_none",
        "append_partitioned",
        "pk_default",
        "pk_dynamic",
        "pk_sequence",
        "pk_all_deleted",
        "pk_composite",
        "pk_date_decimal",
        "pk_no_stats",
    ),
    "compacted": (
        "pk_default",
        "pk_sequence",
        "pk_composite",
        "pk_date_decimal",
        "pk_no_stats",
    ),
    "schema": ("schema_evolution",),
}
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
SAFE_PREFIX_RE = re.compile(
    r"^fixtures/paimon-read/[a-z0-9][a-z0-9-]{0,63}/"
    r"[a-z0-9][a-z0-9-]{0,47}-[0-9a-f]{12}$"
)


class FixtureError(RuntimeError):
    """A deterministic fixture contract failure."""


@dataclasses.dataclass(frozen=True)
class Runtime:
    env_id: str
    compose_project: str
    compose_file: Path
    compose_env: Path
    minio_endpoint_host: str
    minio_endpoint_container: str
    docker_network: str
    access_key: str
    secret_key: str
    credential_name: str
    credential_generation: str
    fe_config: Path
    sql_config: Path


@dataclasses.dataclass(frozen=True)
class Scope:
    run_id: str
    run_slug: str
    bucket: str
    prefix: str
    warehouse_uri: str


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_write(path: Path, value: bytes, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.chmod(mode)
    temporary.replace(path)


def write_json(path: Path, value: Any) -> None:
    atomic_write(path, canonical_json(value))


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise FixtureError(f"cannot read fixture JSON {path}: {error}") from error


def parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for number, raw in enumerate(path.read_text().splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :]
        if "=" not in line:
            raise FixtureError(f"invalid environment line {path}:{number}")
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1].replace('\\"', '"').replace("\\\\", "\\")
        values[key] = value
    return values


def require_value(values: Mapping[str, str], name: str) -> str:
    value = values.get(name)
    if not value:
        raise FixtureError(f"required fixture setting is missing: {name}")
    return value


def load_runtime(env_file: Path) -> Runtime:
    values = parse_env_file(env_file)
    host_endpoint = require_value(values, "AWS_S3_ENDPOINT")
    parsed = urlparse(host_endpoint)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise FixtureError(f"invalid AWS_S3_ENDPOINT: {host_endpoint}")
    container_endpoint = values.get(
        "NOVAROCKS_PAIMON_SPARK_S3_ENDPOINT", "http://minio:9000"
    )
    if urlparse(container_endpoint).scheme not in ("http", "https"):
        raise FixtureError(f"invalid container S3 endpoint: {container_endpoint}")
    return Runtime(
        env_id=require_value(values, "NOVA_ENV_ID"),
        compose_project=require_value(values, "NOVA_ENV_COMPOSE_PROJECT"),
        compose_file=Path(require_value(values, "NOVA_ENV_COMPOSE_FILE")).resolve(),
        compose_env=Path(require_value(values, "NOVA_ENV_COMPOSE_ENV")).resolve(),
        minio_endpoint_host=host_endpoint,
        minio_endpoint_container=container_endpoint,
        docker_network=values.get(
            "NOVAROCKS_PAIMON_DOCKER_NETWORK",
            f"{require_value(values, 'NOVA_ENV_COMPOSE_PROJECT')}_iceberg_net",
        ),
        access_key=require_value(values, "AWS_S3_ACCESS_KEY_ID"),
        secret_key=require_value(values, "AWS_S3_SECRET_ACCESS_KEY"),
        credential_name=values.get("iceberg_object_store_credential_name", "iceberg-test-data"),
        credential_generation=values.get(
            "iceberg_object_store_credential_generation", "v1"
        ),
        fe_config=Path(require_value(values, "NOVAROCKS_FE_CONFIG")).resolve(),
        sql_config=Path(require_value(values, "NOVAROCKS_SQL_TEST_CONFIG")).resolve(),
    )


def validate_run_id(run_id: str) -> str:
    if not RUN_ID_RE.fullmatch(run_id):
        raise FixtureError(
            "run id must be 1-64 ASCII letters, digits, dot, underscore, or dash; "
            "the first character must be alphanumeric"
        )
    return run_id


def slug(value: str, limit: int) -> str:
    candidate = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    candidate = re.sub(r"-+", "-", candidate)[:limit].rstrip("-")
    if not candidate:
        raise FixtureError("value cannot be normalized into an object-store segment")
    return candidate


def make_scope(run_id: str, env_id: str, definition_sha256: str) -> Scope:
    validate_run_id(run_id)
    run_slug = slug(run_id, 48)
    env_slug = slug(env_id, 64)
    discriminator = sha256_bytes(
        f"{FIXTURE_KIND}\0{env_id}\0{run_id}\0{definition_sha256}".encode()
    )[:12]
    prefix = f"fixtures/paimon-read/{env_slug}/{run_slug}-{discriminator}"
    if not SAFE_PREFIX_RE.fullmatch(prefix):
        raise FixtureError(f"generated fixture prefix is outside the allowed shape: {prefix}")
    return Scope(run_id, run_slug, "novarocks", prefix, f"s3://novarocks/{prefix}")


def validate_warehouse_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    prefix = parsed.path.lstrip("/").rstrip("/")
    if (
        parsed.scheme != "s3"
        or parsed.netloc != "novarocks"
        or not SAFE_PREFIX_RE.fullmatch(prefix)
        or any(part in ("", ".", "..") for part in prefix.split("/"))
    ):
        raise FixtureError(f"refusing object-store access outside a PAI-1 run prefix: {uri}")
    return parsed.netloc, prefix


def definition_files() -> list[Path]:
    files = [
        SCRIPT_DIR / "Dockerfile",
        SCRIPT_DIR / "versions.env",
        SCRIPT_DIR / "requirements.txt",
        Path(__file__),
    ]
    files.extend(sorted((SCRIPT_DIR / "sql").glob("*.sql.in")))
    files.extend(sorted((SCRIPT_DIR / "expected").glob("*.json")))
    for path in files:
        if not path.is_file():
            raise FixtureError(f"fixture definition file is missing: {path}")
    return files


def fixture_definition_sha256() -> str:
    digest = hashlib.sha256()
    for path in definition_files():
        relative = path.relative_to(SCRIPT_DIR).as_posix().encode()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        content = path.read_bytes()
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def load_versions() -> dict[str, str]:
    versions = parse_env_file(SCRIPT_DIR / "versions.env")
    required = (
        "SPARK_VERSION",
        "SPARK_IMAGE_PLATFORM",
        "SPARK_IMAGE_REPOSITORY",
        "SPARK_IMAGE_MANIFEST_DIGEST",
        "PAIMON_VERSION",
        "PAIMON_SPARK_ARTIFACT",
        "PAIMON_SPARK_JAR_SHA1",
        "PAIMON_SPARK_JAR_SIZE",
        "PAIMON_S3_ARTIFACT",
        "PAIMON_S3_JAR_SHA1",
        "PAIMON_S3_JAR_SIZE",
        "MAVEN_REPOSITORY",
    )
    for name in required:
        if not versions.get(name):
            raise FixtureError(f"versions.env is missing {name}")
    for digest_name in ("SPARK_IMAGE_MANIFEST_DIGEST",):
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", versions[digest_name]):
            raise FixtureError(f"{digest_name} is not an immutable SHA-256 digest")
    for checksum_name in ("PAIMON_SPARK_JAR_SHA1", "PAIMON_S3_JAR_SHA1"):
        if not re.fullmatch(r"[0-9a-f]{40}", versions[checksum_name]):
            raise FixtureError(f"{checksum_name} is not a SHA-1 checksum")
    for size_name in ("PAIMON_SPARK_JAR_SIZE", "PAIMON_S3_JAR_SIZE"):
        if int(versions[size_name]) <= 0:
            raise FixtureError(f"{size_name} must be positive")
    # prepare overrides SPARK_IMAGE_REPOSITORY with the selected registry path;
    # keep the manifest's own value so the local lookup can try both names.
    versions["SPARK_IMAGE_DEFAULT_REPOSITORY"] = versions["SPARK_IMAGE_REPOSITORY"]
    return versions


def select_image_repository(versions: Mapping[str, str]) -> str:
    """Select only the registry path; the immutable manifest digest stays fixed."""
    repository = os.environ.get(
        "PAIMON_SPARK_IMAGE_REPOSITORY", versions["SPARK_IMAGE_REPOSITORY"]
    ).strip()
    if not repository or "@" in repository or any(char.isspace() for char in repository):
        raise FixtureError("Spark image repository override must be a non-empty registry path")
    return repository.rstrip("/")


def render_stage(stage: str) -> str:
    if stage not in STAGES:
        raise FixtureError(f"unknown fixture stage: {stage}")
    template = (SCRIPT_DIR / "sql" / f"{STAGES.index(stage):02d}_{stage}.sql.in").read_text()
    replacements = {"{{CATALOG}}": "paimon", "{{DATABASE}}": "fixture"}
    for source, target in replacements.items():
        template = template.replace(source, target)
    leftovers = re.findall(r"\{\{[^}]+\}\}", template)
    if leftovers:
        raise FixtureError(f"unresolved fixture SQL placeholders: {sorted(set(leftovers))}")
    return template.rstrip() + "\n\n" + metadata_sql(stage)


def metadata_sql(stage: str) -> str:
    """Render one complete metadata summary row per table and marker kind."""

    queries: list[str] = []
    definitions = (
        (
            "SNAPSHOT",
            "snapshots",
            "'summary', true, 'count', count(*), "
            "'max_snapshot_id', max(snapshot_id), 'max_schema_id', max(schema_id), "
            "'commit_kinds', sort_array(collect_set(commit_kind))",
        ),
        (
            "SCHEMA",
            "schemas",
            "'summary', true, 'count', count(*), 'max_schema_id', max(schema_id)",
        ),
        (
            "FILE",
            "files",
            "'summary', true, 'count', count(*), "
            "'record_count', coalesce(sum(record_count), 0), "
            "'formats', sort_array(collect_set(file_format)), "
            "'levels', sort_array(collect_set(level))",
        ),
        (
            "MANIFEST",
            "manifests",
            "'summary', true, 'count', count(*), "
            "'file_bytes', coalesce(sum(file_size), 0)",
        ),
    )
    for marker, suffix, fields in definitions:
        parts = [
            f"SELECT concat('NR_{marker}|{stage}|{table}|', "
            f"to_json(named_struct({fields}))) FROM `{table}${suffix}`"
            for table in ALL_TABLES
        ]
        queries.append("\nUNION ALL\n".join(parts) + ";")
    return "\n\n".join(queries) + "\n"


def parse_markers(output: str, stage: str) -> dict[str, list[dict[str, Any]]]:
    parsed: dict[str, list[dict[str, Any]]] = {marker.lower(): [] for marker in MARKERS}
    prefix = "NR_"
    for raw in output.splitlines():
        line = raw.strip()
        position = line.find(prefix)
        if position < 0:
            continue
        line = line[position:]
        parts = line.split("|", 3)
        if len(parts) != 4 or not parts[0].startswith(prefix):
            continue
        marker = parts[0][len(prefix) :]
        if marker not in MARKERS or parts[1] != stage:
            continue
        try:
            payload, payload_end = json.JSONDecoder().raw_decode(parts[3])
        except json.JSONDecodeError as error:
            raise FixtureError(f"invalid {marker} JSON emitted by Spark: {line}") from error
        trailing = parts[3][payload_end:].strip()
        if trailing and not trailing.startswith("Time taken:"):
            raise FixtureError(
                f"unexpected text after {marker} JSON emitted by Spark: {line}"
            )
        if not isinstance(payload, dict):
            raise FixtureError(f"{marker} payload must be a JSON object")
        parsed[marker.lower()].append({"table": parts[2], "value": payload})
    for records in parsed.values():
        records.sort(key=lambda entry: canonical_json(entry))
    return parsed


def load_expected(stage: str) -> list[dict[str, Any]]:
    expected = read_json(SCRIPT_DIR / "expected" / f"{stage}.json")
    if not isinstance(expected, list):
        raise FixtureError(f"expected/{stage}.json must contain a JSON array")
    expected.sort(key=lambda entry: canonical_json(entry))
    return expected


def verify_oracle(stage: str, markers: Mapping[str, list[dict[str, Any]]]) -> None:
    actual = list(markers["oracle"])
    expected = load_expected(stage)
    if actual != expected:
        raise FixtureError(
            f"Spark oracle mismatch for {stage}; expected {len(expected)} rows, "
            f"received {len(actual)} rows"
        )


def summary_by_table(
    markers: Mapping[str, list[dict[str, Any]]], marker: str
) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    for record in markers[marker]:
        value = record["value"]
        if value.get("summary") is not True:
            continue
        table = record["table"]
        if table in summaries:
            raise FixtureError(f"duplicate {marker} summary for table {table}")
        summaries[table] = value
    return summaries


def validate_stage_markers(
    output_dir: Path,
    stage: str,
    markers: Mapping[str, list[dict[str, Any]]],
) -> None:
    verify_oracle(stage, markers)
    for marker in ("snapshot", "schema", "file", "manifest"):
        if not markers[marker]:
            raise FixtureError(f"Spark emitted no {marker} markers for {stage}")
        summaries = summary_by_table(markers, marker)
        missing = sorted(set(ALL_TABLES) - set(summaries))
        extra = sorted(set(summaries) - set(ALL_TABLES))
        if missing or extra:
            raise FixtureError(
                f"incomplete {marker} summaries for {stage}; missing={missing}, extra={extra}"
            )
        for table, value in summaries.items():
            count = value.get("count")
            if not isinstance(count, int) or count < 0:
                raise FixtureError(
                    f"invalid {marker} count for {stage}/{table}: {count!r}"
                )

    position = stage_number(stage)
    if position == 0:
        return
    previous_stage = STAGES[position - 1]
    previous_path = output_dir / "stages" / f"{previous_stage}.json"
    if not previous_path.is_file():
        raise FixtureError(f"stage {stage} is missing predecessor evidence {previous_stage}")
    previous = read_json(previous_path)
    for table in SNAPSHOT_ADVANCES.get(stage, ()):
        before = summary_by_table(previous, "snapshot")[table].get("max_snapshot_id")
        after = summary_by_table(markers, "snapshot")[table].get("max_snapshot_id")
        if not isinstance(before, int) or not isinstance(after, int) or after <= before:
            raise FixtureError(
                f"snapshot did not advance for {stage}/{table}: {before!r} -> {after!r}"
            )
    if stage == "schema":
        before = summary_by_table(previous, "schema")["schema_evolution"].get(
            "max_schema_id"
        )
        after = summary_by_table(markers, "schema")["schema_evolution"].get(
            "max_schema_id"
        )
        if not isinstance(before, int) or not isinstance(after, int) or after <= before:
            raise FixtureError(
                f"schema id did not advance for schema_evolution: {before!r} -> {after!r}"
            )
    if stage == "compacted":
        snapshots = summary_by_table(markers, "snapshot")
        for table in SNAPSHOT_ADVANCES[stage]:
            kinds = snapshots[table].get("commit_kinds")
            normalized_kinds = (
                {str(kind).upper() for kind in kinds} if isinstance(kinds, list) else set()
            )
            if "COMPACT" not in normalized_kinds:
                raise FixtureError(f"compaction commit is absent for {table}")


def sanitize_output(output: str, runtime: Runtime) -> str:
    sanitized = output
    for secret in (runtime.access_key, runtime.secret_key):
        if secret:
            sanitized = sanitized.replace(secret, "<redacted>")
    return sanitized


def run_command(
    command: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    stdin: str | None = None,
    redactions: Sequence[str] = (),
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        check=False,
        input=stdin,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=dict(env) if env is not None else None,
    )
    if result.returncode != 0:
        output = result.stdout
        rendered_command = " ".join(command[:4])
        for secret in redactions:
            if secret:
                output = output.replace(secret, "<redacted>")
                rendered_command = rendered_command.replace(secret, "<redacted>")
        raise FixtureError(
            f"command failed with exit {result.returncode}: {rendered_command}\n"
            f"{output[-8000:]}"
        )
    return result


def image_tag(versions: Mapping[str, str], definition_sha256: str) -> str:
    return (
        f"novarocks/paimon-read:{versions['SPARK_VERSION']}-"
        f"{versions['PAIMON_VERSION']}-{definition_sha256[:12]}"
    )


def inspect_local_image(reference: str) -> dict[str, Any] | None:
    """Return `docker image inspect` output for a reference on this host.

    `docker image inspect` is a local-store lookup and never contacts a
    registry. None means this host has no such reference.
    """
    result = subprocess.run(
        ["docker", "image", "inspect", reference, "--format", "{{json .}}"],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return None
    try:
        payload = json.loads(result.stdout.strip())
    except json.JSONDecodeError as error:
        raise FixtureError(
            f"cannot parse docker image inspect output for {reference}"
        ) from error
    if not isinstance(payload, dict):
        raise FixtureError(f"unexpected docker image inspect output for {reference}")
    return payload


def image_platform(info: Mapping[str, Any]) -> str:
    platform = f"{info.get('Os')}/{info.get('Architecture')}"
    variant = info.get("Variant")
    if variant:
        platform = f"{platform}/{variant}"
    return platform


def local_base_alias(versions: Mapping[str, str]) -> str:
    """Local-only tag standing for the pinned Spark base manifest.

    The tag embeds the digest, so this name can only ever mean that one
    manifest. It is also the Dockerfile's SPARK_BASE default, which keeps a
    direct `docker build` working once the fixture has run on this host.
    """
    digest = versions["SPARK_IMAGE_MANIFEST_DIGEST"]
    return f"{LOCAL_BASE_ALIAS_REPOSITORY}:{digest.split(':', 1)[1][:12]}"


def local_base_candidates(versions: Mapping[str, str]) -> list[str]:
    """References that may already name the pinned Spark base on this host."""
    digest = versions["SPARK_IMAGE_MANIFEST_DIGEST"]
    default_repository = versions.get(
        "SPARK_IMAGE_DEFAULT_REPOSITORY", versions["SPARK_IMAGE_REPOSITORY"]
    )
    candidates = [
        f"{versions['SPARK_IMAGE_REPOSITORY']}@{digest}",
        f"{default_repository}@{digest}",
        # A containerd-backed image store keys images by manifest digest, so
        # this finds the pinned manifest under whatever name it carries locally.
        digest,
    ]
    unique: list[str] = []
    for candidate in candidates:
        if candidate not in unique:
            unique.append(candidate)
    return unique


def missing_base_image_error(
    versions: Mapping[str, str], candidates: Sequence[str]
) -> FixtureError:
    digest = versions["SPARK_IMAGE_MANIFEST_DIGEST"]
    platform = versions["SPARK_IMAGE_PLATFORM"]
    default_repository = versions.get(
        "SPARK_IMAGE_DEFAULT_REPOSITORY", versions["SPARK_IMAGE_REPOSITORY"]
    )
    tried = "\n".join(f"  {candidate}" for candidate in candidates)
    return FixtureError(
        "the pinned Spark base image is not in this host's image store\n"
        "preparing the fixture never pulls; import the pinned manifest once, "
        "then re-run:\n"
        f"  docker pull --platform {platform} {default_repository}@{digest}\n"
        "if the daemon cannot reach Docker Hub, pull the same digest through a "
        "reachable mirror and name it:\n"
        f"  docker pull --platform {platform} dockerproxy.net/apache/spark@{digest}\n"
        "  PAIMON_SPARK_IMAGE_REPOSITORY=dockerproxy.net/apache/spark ...\n"
        f"local references tried:\n{tried}"
    )


def resolve_local_base_image(versions: Mapping[str, str]) -> str:
    """Return a local alias tag for the pinned Linux/amd64 Spark manifest.

    Design: ADR-0141 (docs/adr/ADR-0141-fixture-images-never-pull.md)

    Preparing the fixture never pulls, so the manifest must already be on this
    host and a missing image is an error. BuildKit is then handed a tag rather
    than the digest, because it resolves a digest-pinned `FROM` against the
    registry even for an image that is already local with a matching
    RepoDigest; the digest identity is checked here instead.
    """
    digest = versions["SPARK_IMAGE_MANIFEST_DIGEST"]
    platform = versions["SPARK_IMAGE_PLATFORM"]
    candidates = local_base_candidates(versions)
    for candidate in candidates:
        info = inspect_local_image(candidate)
        if info is None:
            continue
        names = info.get("RepoDigests") or []
        # A containerd store reports the manifest digest as the image id; a
        # graphdriver store reports the config digest and carries the manifest
        # digest in RepoDigests. Either one proves the pinned identity.
        if info.get("Id") != digest and not any(
            isinstance(name, str) and name.endswith(f"@{digest}") for name in names
        ):
            raise FixtureError(
                f"local image {candidate} is not the pinned Spark manifest {digest}"
            )
        found = image_platform(info)
        if found != platform:
            raise FixtureError(
                f"local Spark base {candidate} is {found}, "
                f"but the fixture pins {platform}"
            )
        alias = local_base_alias(versions)
        run_command(["docker", "tag", candidate, alias])
        return alias
    raise missing_base_image_error(versions, candidates)


def build_image(versions: Mapping[str, str], definition_sha256: str) -> str:
    tag = image_tag(versions, definition_sha256)
    base = resolve_local_base_image(versions)
    arguments = [
        "docker",
        "build",
        "--platform",
        versions["SPARK_IMAGE_PLATFORM"],
        "--build-arg",
        f"SPARK_BASE={base}",
    ]
    for name in (
        "PAIMON_VERSION",
        "PAIMON_SPARK_ARTIFACT",
        "PAIMON_SPARK_JAR_SHA1",
        "PAIMON_SPARK_JAR_SIZE",
        "PAIMON_S3_ARTIFACT",
        "PAIMON_S3_JAR_SHA1",
        "PAIMON_S3_JAR_SIZE",
        "MAVEN_REPOSITORY",
    ):
        arguments.extend(("--build-arg", f"{name}={versions[name]}"))
    arguments.extend(("--label", f"novarocks.fixture.sha256={definition_sha256}", "-t", tag))
    arguments.extend(("-f", str(SCRIPT_DIR / "Dockerfile"), str(SCRIPT_DIR)))
    run_command(arguments)
    return tag


def spark_command(runtime: Runtime, versions: Mapping[str, str], tag: str) -> list[str]:
    return [
        "docker",
        "run",
        # Never pull: the tag was just built from this host's own image store.
        "--pull",
        "never",
        "--interactive",
        "--rm",
        "--platform",
        versions["SPARK_IMAGE_PLATFORM"],
        "--network",
        runtime.docker_network,
        "-e",
        "AWS_ACCESS_KEY_ID",
        "-e",
        "AWS_SECRET_ACCESS_KEY",
        "-e",
        "AWS_REGION=us-east-1",
        "-e",
        "PAIMON_WAREHOUSE",
        "-e",
        "PAIMON_S3_ENDPOINT",
        "--entrypoint",
        "/bin/bash",
        tag,
        "-lc",
        """
set -euo pipefail
sql_file=/tmp/paimon-fixture.sql
cat > "$sql_file"
/opt/spark/bin/spark-sql \
  --master local[2] \
  --conf spark.ui.enabled=false \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.jsonGenerator.ignoreNullFields=false \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse="$PAIMON_WAREHOUSE" \
  --conf spark.sql.catalog.paimon.s3.endpoint="$PAIMON_S3_ENDPOINT" \
  --conf spark.sql.catalog.paimon.s3.access-key="$AWS_ACCESS_KEY_ID" \
  --conf spark.sql.catalog.paimon.s3.secret-key="$AWS_SECRET_ACCESS_KEY" \
  --conf spark.sql.catalog.paimon.s3.path.style.access=true \
  -f "$sql_file"
""".strip(),
    ]


def run_spark_stage(
    runtime: Runtime,
    versions: Mapping[str, str],
    scope: Scope,
    tag: str,
    stage: str,
) -> str:
    environment = os.environ.copy()
    environment.update(
        {
            "AWS_ACCESS_KEY_ID": runtime.access_key,
            "AWS_SECRET_ACCESS_KEY": runtime.secret_key,
            "AWS_REGION": "us-east-1",
            "PAIMON_WAREHOUSE": scope.warehouse_uri,
            "PAIMON_S3_ENDPOINT": runtime.minio_endpoint_container,
        }
    )
    result = run_command(
        spark_command(runtime, versions, tag),
        env=environment,
        stdin=render_stage(stage),
        redactions=(runtime.access_key, runtime.secret_key),
    )
    return sanitize_output(result.stdout, runtime)


def compose_mc(runtime: Runtime, script: str, target: str) -> str:
    command = [
        "docker",
        "compose",
        "--env-file",
        str(runtime.compose_env),
        "-p",
        runtime.compose_project,
        "-f",
        str(runtime.compose_file),
        "run",
        "--pull",
        "never",
        "--rm",
        "--no-deps",
        "-T",
        "--entrypoint",
        "/bin/sh",
        "mc",
        "-c",
        script,
        "_",
        target,
    ]
    result = run_command(
        command, redactions=(runtime.access_key, runtime.secret_key)
    )
    return sanitize_output(result.stdout, runtime)


def compose_mc_bytes(runtime: Runtime, script: str, target: str) -> bytes:
    command = [
        "docker",
        "compose",
        "--env-file",
        str(runtime.compose_env),
        "-p",
        runtime.compose_project,
        "-f",
        str(runtime.compose_file),
        "run",
        "--pull",
        "never",
        "--rm",
        "--no-deps",
        "-T",
        "--entrypoint",
        "/bin/sh",
        "mc",
        "-c",
        script,
        "_",
        target,
    ]
    result = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        output = (result.stdout + result.stderr).decode(errors="replace")
        output = sanitize_output(output, runtime)
        raise FixtureError(
            f"command failed with exit {result.returncode}: {' '.join(command[:4])}\n"
            f"{output[-8000:]}"
        )
    return result.stdout


def cleanup_prefix(runtime: Runtime, warehouse_uri: str) -> None:
    bucket, prefix = validate_warehouse_uri(warehouse_uri)
    target = f"minio/{bucket}/{prefix}/"
    compose_mc(
        runtime,
        """
set -eu
/usr/bin/mc alias set minio http://minio:9000 "${MINIO_ROOT_USER:-admin}" "${MINIO_ROOT_PASSWORD:-admin123}" >/dev/null
/usr/bin/mc rm --recursive --force --quiet "$1"
""".strip(),
        target,
    )


def cleanup_and_assert_empty(runtime: Runtime, warehouse_uri: str) -> None:
    cleanup_prefix(runtime, warehouse_uri)
    remaining = collect_objects(runtime, warehouse_uri, allow_empty=True)
    if remaining:
        raise FixtureError(
            f"fixture cleanup left {len(remaining)} objects under the owned prefix"
        )


def collect_objects(
    runtime: Runtime, warehouse_uri: str, *, allow_empty: bool = False
) -> list[dict[str, Any]]:
    bucket, prefix = validate_warehouse_uri(warehouse_uri)
    target = f"minio/{bucket}/{prefix}/"
    output = compose_mc(
        runtime,
        """
set -eu
/usr/bin/mc alias set minio http://minio:9000 "${MINIO_ROOT_USER:-admin}" "${MINIO_ROOT_PASSWORD:-admin123}" >/dev/null
/usr/bin/mc ls --recursive --json "$1"
""".strip(),
        target,
    )
    objects: list[dict[str, Any]] = []
    prefix_with_slash = f"{prefix}/"
    for line in output.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise FixtureError(f"invalid mc inventory record: {line}") from error
        key = str(record.get("key", "")).lstrip("/")
        if key.startswith(f"{bucket}/"):
            key = key[len(bucket) + 1 :]
        if key.startswith(prefix_with_slash):
            relative_key = key[len(prefix_with_slash) :]
        elif key and not key.startswith("/") and ".." not in key.split("/"):
            # Some mc releases return a key relative to the exact listing
            # target. The command cannot list outside that target, and this
            # branch still rejects absolute paths and traversal segments.
            relative_key = key
        else:
            raise FixtureError(f"mc returned an object outside the fixture prefix: {key}")
        objects.append(
            {
                "key": relative_key,
                "size": int(record.get("size", 0)),
                "etag": record.get("etag"),
            }
        )
    objects.sort(key=lambda value: value["key"])
    if not objects and not allow_empty:
        raise FixtureError("external writer published no objects")
    return objects


def expected_objects(output_dir: Path) -> list[dict[str, Any]]:
    objects = read_json(output_dir / "objects.json")
    if not isinstance(objects, list) or not objects:
        raise FixtureError("objects.json must contain a non-empty object inventory")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in objects:
        if not isinstance(item, dict):
            raise FixtureError("objects.json entries must be JSON objects")
        normalized_item = {
            "key": item.get("key"),
            "size": item.get("size"),
            "etag": item.get("etag"),
        }
        key = normalized_item["key"]
        if (
            not isinstance(key, str)
            or not key
            or key.startswith("/")
            or ".." in key.split("/")
            or key in seen
            or not isinstance(normalized_item["size"], int)
            or normalized_item["size"] < 0
            or not isinstance(normalized_item["etag"], str)
            or not normalized_item["etag"]
        ):
            raise FixtureError(f"invalid or duplicate objects.json entry: {item!r}")
        seen.add(key)
        normalized.append(normalized_item)
    normalized.sort(key=lambda value: value["key"])
    return normalized


def verify_remote_inventory(
    output_dir: Path, runtime: Runtime, warehouse_uri: str
) -> list[dict[str, Any]]:
    expected = expected_objects(output_dir)
    actual = collect_objects(runtime, warehouse_uri)
    if actual != expected:
        raise FixtureError(
            "remote fixture inventory differs from objects.json; refusing reuse or cleanup"
        )
    return actual


def object_table(key: str) -> str | None:
    parts = key.split("/")
    for position, part in enumerate(parts[:-1]):
        if part == "fixture.db" and position + 1 < len(parts):
            table = parts[position + 1]
            return table if table in ALL_TABLES else None
    return None


def read_remote_object(
    runtime: Runtime, warehouse_uri: str, relative_key: str
) -> bytes:
    bucket, prefix = validate_warehouse_uri(warehouse_uri)
    if (
        not relative_key
        or relative_key.startswith("/")
        or ".." in relative_key.split("/")
    ):
        raise FixtureError(f"invalid fixture object key: {relative_key!r}")
    target = f"minio/{bucket}/{prefix}/{relative_key}"
    return compose_mc_bytes(
        runtime,
        """
set -eu
/usr/bin/mc alias set minio http://minio:9000 "${MINIO_ROOT_USER:-admin}" "${MINIO_ROOT_PASSWORD:-admin123}" >/dev/null
/usr/bin/mc cat "$1"
""".strip(),
        target,
    )


def inspect_object_formats(
    runtime: Runtime, warehouse_uri: str, objects: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    try:
        import fastavro  # type: ignore[import-not-found]
        import pyarrow.parquet as parquet  # type: ignore[import-not-found]
    except ImportError as error:
        raise FixtureError(
            "format inspection requires the pinned packages in requirements.txt"
        ) from error

    parquet_codecs: dict[str, set[str]] = {}
    manifest_codecs: dict[str, set[str]] = {}
    inspected: list[dict[str, str]] = []
    for item in objects:
        key = str(item["key"])
        table = object_table(key)
        if table is None:
            continue
        basename = key.rsplit("/", 1)[-1]
        is_parquet = basename.endswith(".parquet")
        is_manifest = "/manifest/" in f"/{key}" and basename.startswith("manifest-")
        if not is_parquet and not is_manifest:
            continue
        content = read_remote_object(runtime, warehouse_uri, key)
        if is_parquet:
            try:
                metadata = parquet.ParquetFile(io.BytesIO(content)).metadata
            except Exception as error:
                raise FixtureError(f"cannot inspect Parquet footer for {key}: {error}") from error
            codecs = {
                (
                    "LZ4"
                    if str(metadata.row_group(group).column(column).compression).upper()
                    == "LZ4_RAW"
                    else str(metadata.row_group(group).column(column).compression).upper()
                )
                for group in range(metadata.num_row_groups)
                for column in range(metadata.row_group(group).num_columns)
            }
            if not codecs:
                raise FixtureError(f"Parquet object has no column codec metadata: {key}")
            parquet_codecs.setdefault(table, set()).update(codecs)
            inspected.append({"key": key, "kind": "parquet-footer"})
        else:
            try:
                reader = fastavro.reader(io.BytesIO(content))
                codec = reader.metadata.get("avro.codec", "null")
            except Exception as error:
                raise FixtureError(f"cannot inspect Avro OCF header for {key}: {error}") from error
            if isinstance(codec, bytes):
                codec = codec.decode("ascii")
            manifest_codecs.setdefault(table, set()).add(str(codec).lower())
            inspected.append({"key": key, "kind": "avro-ocf-header"})

    expected_parquet = {
        "append_none": {"UNCOMPRESSED"},
        "append_snappy": {"SNAPPY"},
        "append_zstd": {"ZSTD"},
        "append_lz4": {"LZ4"},
    }
    expected_manifest = {
        "append_none": {"null"},
        "append_snappy": {"snappy"},
        "append_zstd": {"zstandard"},
    }
    for table, expected in expected_parquet.items():
        if parquet_codecs.get(table) != expected:
            raise FixtureError(
                f"Parquet footer codec mismatch for {table}: "
                f"expected {sorted(expected)}, found {sorted(parquet_codecs.get(table, set()))}"
            )
    for table, expected in expected_manifest.items():
        if manifest_codecs.get(table) != expected:
            raise FixtureError(
                f"Avro manifest codec mismatch for {table}: "
                f"expected {sorted(expected)}, found {sorted(manifest_codecs.get(table, set()))}"
            )
    if not inspected:
        raise FixtureError("no Parquet footer or Avro OCF header was inspected")
    return {
        "parquet": {table: sorted(values) for table, values in sorted(parquet_codecs.items())},
        "avro_manifest": {
            table: sorted(values) for table, values in sorted(manifest_codecs.items())
        },
        "inspected": inspected,
    }


def generated_paths(output_dir: Path) -> Iterable[Path]:
    for name in (
        "READY",
        "FAILED.json",
        "CLEANED",
        "manifest.json",
        "state.json",
        "objects.json",
        "formats.json",
        "catalog.sql",
        "base-server.toml",
        "sql-runner.toml",
        "dry-run.json",
        "rendered",
        "logs",
        "stages",
    ):
        yield output_dir / name


def remove_generated_paths(output_dir: Path) -> None:
    for path in generated_paths(output_dir):
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
        else:
            with contextlib.suppress(FileNotFoundError):
                path.unlink()


def publish_configs(output_dir: Path, runtime: Runtime, scope: Scope) -> None:
    if runtime.fe_config.is_file():
        base_config = redact_config_secrets(runtime.fe_config.read_text(), runtime)
        atomic_write(output_dir / "base-server.toml", base_config.encode())
    else:
        raise FixtureError(f"FE base config does not exist: {runtime.fe_config}")
    if runtime.sql_config.is_file():
        sql_config = redact_config_secrets(runtime.sql_config.read_text(), runtime).rstrip() + "\n"
    else:
        raise FixtureError(f"SQL runner config does not exist: {runtime.sql_config}")
    sql_config += (
        f'paimon_fixture_manifest = "{(output_dir / "manifest.json").resolve()}"\n'
        f'paimon_warehouse = "{scope.warehouse_uri}"\n'
        f'paimon_catalog_sql = "{(output_dir / "catalog.sql").resolve()}"\n'
        f'paimon_object_store_credential_name = "{runtime.credential_name}"\n'
        f'paimon_object_store_credential_generation = "{runtime.credential_generation}"\n'
    )
    atomic_write(output_dir / "sql-runner.toml", sql_config.encode())
    catalog_sql = f'''CREATE EXTERNAL CATALOG paimon_fixture
PROPERTIES (
  "type" = "paimon",
  "paimon.catalog.type" = "filesystem",
  "warehouse" = "{scope.warehouse_uri}",
  "aws.s3.endpoint" = "{runtime.minio_endpoint_host}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "{runtime.credential_name}",
  "credential.object-store-metadata.generation" = "{runtime.credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "{runtime.credential_name}",
  "credential.object-store-data.generation" = "{runtime.credential_generation}"
);
'''
    atomic_write(output_dir / "catalog.sql", catalog_sql.encode())


def redact_config_secrets(value: str, runtime: Runtime) -> str:
    replacements = (
        (runtime.secret_key, "${ENV:AWS_S3_SECRET_ACCESS_KEY}"),
        (runtime.access_key, "${ENV:AWS_S3_ACCESS_KEY_ID}"),
    )
    redacted = value
    for secret, reference in sorted(replacements, key=lambda item: len(item[0]), reverse=True):
        if secret:
            redacted = redacted.replace(secret, reference)
    return redacted


def assert_artifacts_secret_free(output_dir: Path, runtime: Runtime) -> None:
    secrets = tuple(
        secret.encode() for secret in (runtime.access_key, runtime.secret_key) if secret
    )
    for path in output_dir.rglob("*"):
        if not path.is_file() or path.name in {".fixture.lock", "FAILED.json"}:
            continue
        content = path.read_bytes()
        for secret in secrets:
            if secret in content:
                raise FixtureError(
                    f"generated artifact contains an object-store credential: {path}"
                )


def artifact_entry(path: Path, output_dir: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(output_dir).as_posix(),
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
    }


def verify_ready(output_dir: Path) -> dict[str, Any]:
    manifest_path = output_dir / "manifest.json"
    ready_path = output_dir / "READY"
    if not manifest_path.is_file() or not ready_path.is_file():
        raise FixtureError(f"fixture is not READY: {output_dir}")
    expected = f"sha256:{sha256_file(manifest_path)}"
    actual = ready_path.read_text().strip()
    if actual != expected:
        raise FixtureError(f"READY digest mismatch: expected {expected}, found {actual}")
    manifest = read_json(manifest_path)
    if manifest.get("fixture_kind") != FIXTURE_KIND:
        raise FixtureError("manifest fixture kind is not PAI-1")
    validate_warehouse_uri(str(manifest.get("warehouse_uri", "")))
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise FixtureError("manifest must contain a non-empty artifact inventory")
    required_artifacts = {"state.json", "objects.json", "formats.json"}
    artifact_names: set[str] = set()
    for artifact in artifacts:
        if not isinstance(artifact, dict) or not isinstance(artifact.get("path"), str):
            raise FixtureError("manifest artifact entry is invalid")
        relative = Path(artifact["path"])
        if relative.is_absolute() or ".." in relative.parts or relative.as_posix() in artifact_names:
            raise FixtureError(f"manifest artifact path is unsafe or duplicate: {relative}")
        artifact_names.add(relative.as_posix())
        path = output_dir / relative
        if (
            not path.is_file()
            or sha256_file(path) != artifact.get("sha256")
            or path.stat().st_size != artifact.get("bytes")
        ):
            raise FixtureError(f"fixture artifact digest mismatch: {path}")
    missing = required_artifacts - artifact_names
    if missing:
        raise FixtureError(f"manifest is missing required artifacts: {sorted(missing)}")
    if manifest.get("secret_scan") != "passed":
        raise FixtureError("manifest does not record a successful secret scan")
    return manifest


def state_matches(
    manifest: Mapping[str, Any], scope: Scope, definition_sha256: str
) -> None:
    expected = {
        "fixture_kind": FIXTURE_KIND,
        "run_id": scope.run_id,
        "warehouse_uri": scope.warehouse_uri,
        "fixture_definition_sha256": definition_sha256,
    }
    for name, value in expected.items():
        if manifest.get(name) != value:
            raise FixtureError(
                f"existing fixture {name} mismatch: expected {value!r}, "
                f"found {manifest.get(name)!r}"
            )


def stage_number(stage: str) -> int:
    try:
        return STAGES.index(stage)
    except ValueError as error:
        raise FixtureError(f"unknown fixture stage: {stage}") from error


def write_manifest(
    output_dir: Path,
    scope: Scope,
    versions: Mapping[str, str],
    definition_sha256: str,
    last_stage: str,
    objects: list[dict[str, Any]],
) -> dict[str, Any]:
    artifacts: list[dict[str, Any]] = []
    for path in sorted((output_dir / "stages").glob("*.json")):
        artifacts.append(artifact_entry(path, output_dir))
    for path in sorted((output_dir / "logs").glob("*.log")):
        artifacts.append(artifact_entry(path, output_dir))
    for path in sorted((output_dir / "rendered").glob("*.sql")):
        artifacts.append(artifact_entry(path, output_dir))
    for name in (
        "state.json",
        "objects.json",
        "formats.json",
        "catalog.sql",
        "base-server.toml",
        "sql-runner.toml",
    ):
        artifacts.append(artifact_entry(output_dir / name, output_dir))
    manifest = {
        "fixture_kind": FIXTURE_KIND,
        "run_id": scope.run_id,
        "warehouse_uri": scope.warehouse_uri,
        "bucket": scope.bucket,
        "prefix": scope.prefix,
        "last_stage": last_stage,
        "fixture_definition_sha256": definition_sha256,
        "writer": {
            "spark_version": versions["SPARK_VERSION"],
            "spark_image_repository": versions["SPARK_IMAGE_REPOSITORY"],
            "spark_image_platform": versions["SPARK_IMAGE_PLATFORM"],
            "spark_image_index_digest": versions.get("SPARK_IMAGE_INDEX_DIGEST"),
            "spark_image_manifest_digest": versions["SPARK_IMAGE_MANIFEST_DIGEST"],
            "paimon_version": versions["PAIMON_VERSION"],
            "paimon_spark_jar_sha1": versions["PAIMON_SPARK_JAR_SHA1"],
            "paimon_spark_jar_size": int(versions["PAIMON_SPARK_JAR_SIZE"]),
            "paimon_s3_jar_sha1": versions["PAIMON_S3_JAR_SHA1"],
            "paimon_s3_jar_size": int(versions["PAIMON_S3_JAR_SIZE"]),
        },
        "catalog": {
            "type": "filesystem",
            "database": "fixture",
            "endpoint": None,
            "region": "us-east-1",
            "path_style_access": True,
            "credential_bindings": {
                "object_store_metadata": {
                    "name": None,
                    "generation": None,
                    "consumer_role": "frontend",
                },
                "object_store_data": {
                    "name": None,
                    "generation": None,
                    "consumer_role": "backend",
                },
            },
        },
        "object_count": len(objects),
        "object_bytes": sum(item["size"] for item in objects),
        "objects_sha256": sha256_file(output_dir / "objects.json"),
        "formats_sha256": sha256_file(output_dir / "formats.json"),
        "secret_scan": "passed",
        "artifacts": artifacts,
    }
    # Endpoint and credential binding names are useful non-secret inputs, but
    # are filled by prepare after the generic manifest body is built.
    return manifest


def execute_prepare(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir).expanduser().resolve()
    if output_dir == Path("/"):
        raise FixtureError("output directory cannot be the filesystem root")
    output_dir.mkdir(parents=True, exist_ok=True)
    lock_path = output_dir / ".fixture.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        versions = load_versions()
        versions["SPARK_IMAGE_REPOSITORY"] = select_image_repository(versions)
        definition_sha256 = fixture_definition_sha256()
        env_values = parse_env_file(Path(args.env_file).resolve())
        env_id = require_value(env_values, "NOVA_ENV_ID")
        scope = make_scope(args.run_id, env_id, definition_sha256)
        target_stage = args.stop_after

        if args.dry_run:
            rendered_dir = output_dir / "rendered"
            rendered_dir.mkdir(exist_ok=True)
            for stage in STAGES[: stage_number(target_stage) + 1]:
                atomic_write(rendered_dir / f"{stage}.sql", render_stage(stage).encode())
            dry_run = {
                "fixture_kind": FIXTURE_KIND,
                "run_id": scope.run_id,
                "warehouse_uri": scope.warehouse_uri,
                "target_stage": target_stage,
                "fixture_definition_sha256": definition_sha256,
                "writer": {
                    "spark_version": versions["SPARK_VERSION"],
                    "spark_image_platform": versions["SPARK_IMAGE_PLATFORM"],
                    "spark_image_manifest_digest": versions[
                        "SPARK_IMAGE_MANIFEST_DIGEST"
                    ],
                    "paimon_version": versions["PAIMON_VERSION"],
                },
                "ready_published": False,
            }
            write_json(output_dir / "dry-run.json", dry_run)
            print(json.dumps(dry_run, sort_keys=True))
            return 0

        runtime = load_runtime(Path(args.env_file).resolve())
        current_stage_number = -1
        if (output_dir / "READY").is_file():
            existing = verify_ready(output_dir)
            state_matches(existing, scope, definition_sha256)
            verify_remote_inventory(
                output_dir, runtime, str(existing["warehouse_uri"])
            )
            current_stage_number = stage_number(existing["last_stage"])
            if current_stage_number >= stage_number(target_stage):
                print(output_dir / "manifest.json")
                return 0
        elif (output_dir / "manifest.json").exists() or (output_dir / "state.json").exists():
            partial = read_json(
                output_dir / (
                    "manifest.json" if (output_dir / "manifest.json").exists() else "state.json"
                )
            )
            state_matches(partial, scope, definition_sha256)
            cleanup_and_assert_empty(runtime, scope.warehouse_uri)
            remove_generated_paths(output_dir)
            current_stage_number = -1
        elif (output_dir / "dry-run.json").exists():
            dry_run = read_json(output_dir / "dry-run.json")
            state_matches(dry_run, scope, definition_sha256)
            remove_generated_paths(output_dir)
        elif any(path.exists() for path in generated_paths(output_dir)):
            raise FixtureError(
                "output directory contains unrecognized fixture artifacts; clean it explicitly"
            )

        with contextlib.suppress(FileNotFoundError):
            (output_dir / "READY").unlink()
        with contextlib.suppress(FileNotFoundError):
            (output_dir / "FAILED.json").unlink()

        rendered_dir = output_dir / "rendered"
        rendered_dir.mkdir(exist_ok=True)
        for stage in STAGES[: stage_number(target_stage) + 1]:
            atomic_write(rendered_dir / f"{stage}.sql", render_stage(stage).encode())

        write_json(
            output_dir / "state.json",
            {
                "fixture_kind": FIXTURE_KIND,
                "run_id": scope.run_id,
                "warehouse_uri": scope.warehouse_uri,
                "fixture_definition_sha256": definition_sha256,
                "last_stage": (
                    STAGES[current_stage_number] if current_stage_number >= 0 else None
                ),
            },
        )
        try:
            tag = build_image(versions, definition_sha256)
            for stage in STAGES[current_stage_number + 1 : stage_number(target_stage) + 1]:
                output = run_spark_stage(runtime, versions, scope, tag, stage)
                atomic_write(output_dir / "logs" / f"{stage}.log", output.encode())
                markers = parse_markers(output, stage)
                validate_stage_markers(output_dir, stage, markers)
                write_json(output_dir / "stages" / f"{stage}.json", markers)
                write_json(
                    output_dir / "state.json",
                    {
                        "fixture_kind": FIXTURE_KIND,
                        "run_id": scope.run_id,
                        "warehouse_uri": scope.warehouse_uri,
                        "fixture_definition_sha256": definition_sha256,
                        "last_stage": stage,
                    },
                )
            objects = collect_objects(runtime, scope.warehouse_uri)
            write_json(output_dir / "objects.json", objects)
            formats = inspect_object_formats(runtime, scope.warehouse_uri, objects)
            write_json(output_dir / "formats.json", formats)
            publish_configs(output_dir, runtime, scope)
            assert_artifacts_secret_free(output_dir, runtime)
            manifest = write_manifest(
                output_dir,
                scope,
                versions,
                definition_sha256,
                target_stage,
                objects,
            )
            manifest["catalog"]["endpoint"] = runtime.minio_endpoint_host
            for binding in manifest["catalog"]["credential_bindings"].values():
                binding["name"] = runtime.credential_name
                binding["generation"] = runtime.credential_generation
            write_json(output_dir / "manifest.json", manifest)
            assert_artifacts_secret_free(output_dir, runtime)
            atomic_write(
                output_dir / "READY",
                f"sha256:{sha256_file(output_dir / 'manifest.json')}\n".encode(),
            )
            verify_ready(output_dir)
        except Exception as error:
            write_json(
                output_dir / "FAILED.json",
                {
                    "fixture_kind": FIXTURE_KIND,
                    "run_id": scope.run_id,
                    "warehouse_uri": scope.warehouse_uri,
                    "target_stage": target_stage,
                    "error": sanitize_output(str(error), runtime),
                },
            )
            raise
        print(output_dir / "manifest.json")
        return 0


def execute_cleanup(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir).expanduser().resolve()
    lock_path = output_dir / ".fixture.lock"
    if not output_dir.is_dir():
        raise FixtureError(f"fixture output directory does not exist: {output_dir}")
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        manifest = verify_ready(output_dir)
        if args.run_id and manifest.get("run_id") != args.run_id:
            raise FixtureError(
                f"run id mismatch: expected {args.run_id!r}, found {manifest.get('run_id')!r}"
            )
        warehouse_uri = str(manifest["warehouse_uri"])
        validate_warehouse_uri(warehouse_uri)
        if args.dry_run:
            print(warehouse_uri)
            return 0
        runtime = load_runtime(Path(args.env_file).resolve())
        verify_remote_inventory(output_dir, runtime, warehouse_uri)
        cleanup_and_assert_empty(runtime, warehouse_uri)
        with contextlib.suppress(FileNotFoundError):
            (output_dir / "READY").unlink()
        write_json(
            output_dir / "CLEANED",
            {
                "fixture_kind": FIXTURE_KIND,
                "run_id": manifest["run_id"],
                "warehouse_uri": warehouse_uri,
                "manifest_sha256": sha256_file(output_dir / "manifest.json"),
            },
        )
        print(warehouse_uri)
        return 0


def execute_verify(args: argparse.Namespace) -> int:
    manifest = verify_ready(Path(args.output_dir).expanduser().resolve())
    print(json.dumps(manifest, sort_keys=True))
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)

    prepare = commands.add_parser("prepare", help="write or advance a fixture")
    prepare.add_argument("--run-id", required=True)
    prepare.add_argument("--output-dir", required=True)
    prepare.add_argument("--env-file", required=True)
    prepare.add_argument("--stop-after", choices=STAGES, default="schema")
    prepare.add_argument("--dry-run", action="store_true")
    prepare.set_defaults(handler=execute_prepare)

    cleanup = commands.add_parser("cleanup", help="remove one manifest-owned prefix")
    cleanup.add_argument("--output-dir", required=True)
    cleanup.add_argument("--env-file", required=True)
    cleanup.add_argument("--run-id")
    cleanup.add_argument("--dry-run", action="store_true")
    cleanup.set_defaults(handler=execute_cleanup)

    verify = commands.add_parser("verify", help="verify READY and all artifact hashes")
    verify.add_argument("--output-dir", required=True)
    verify.set_defaults(handler=execute_verify)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except FixtureError as error:
        print(f"paimon fixture error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
