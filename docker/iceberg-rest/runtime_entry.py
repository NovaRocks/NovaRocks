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

"""Pure fixture entry rendering and worktree-side requests to the runtime owner."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import socket
import subprocess
import sys
import time
from typing import Any
from urllib.parse import urlsplit

import fixture_runtime as runtime
import machine_runtime as machine
import entry_publication as publication
import oss_ops

HERE = Path(__file__).resolve().parent
FILES = runtime.ENTRY_FILES
LOCAL_RANGES = {"mysql": 9030, "fe_grpc": 9080, "be_grpc": 9280, "be_control_grpc": 9480, "fe_http": 8240, "be_http": 8440}
MANAGED = """NOVA_ENV_CONFIG_FILE NOVA_ENV_ID NOVA_ENV_SHARED_DOCKER NOVA_ENV_COMPOSE_PROJECT
NOVA_ENV_RUNTIME_DIR NOVA_ENV_CURRENT_DIR NOVA_ENV_REST_ENV_FILE NOVA_ENV_MANIFEST NOVA_ENV_README
NOVA_ENV_COMPOSE_FILE NOVA_ENV_COMPOSE_ENV NOVA_ENV_MINIO_PORT NOVA_ENV_MINIO_CONSOLE_PORT
NOVA_ENV_REST_PORT NOVA_ENV_SPARK_UI_PORT NOVA_ENV_MYSQL_PORT NOVA_ENV_FE_GRPC_PORT
NOVA_ENV_REST_MV_PORT NOVA_ENV_REST_MV_SERVER_WAREHOUSE_URI NOVAROCKS_SPARK_REST_MV_URI
NOVAROCKS_ICEBERG_REST_MV_URI NOVAROCKS_ICEBERG_REST_MV_WAREHOUSE
NOVA_ENV_FE_HTTP_PORT NOVA_ENV_BE_GRPC_PORT NOVA_ENV_BE_CONTROL_GRPC_PORT NOVA_ENV_BE_HTTP_PORT NOVAROCKS_NATIVE_SHARED_SECRET
AWS_S3_ENDPOINT AWS_S3_ACCESS_KEY_ID AWS_S3_SECRET_ACCESS_KEY MINIO_ROOT_USER MINIO_ROOT_PASSWORD
AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_SECURITY_TOKEN
AWS_REQUEST_CHECKSUM_CALCULATION AWS_RESPONSE_CHECKSUM_VALIDATION
iceberg_object_store_credential_name iceberg_object_store_credential_generation CATALOG_WAREHOUSE_URI
NOVAROCKS_ICEBERG_TEST_WAREHOUSE NOVAROCKS_ICEBERG_REST_URI NOVA_ENV_REST_SERVER_WAREHOUSE_URI
NOVA_ENV_REST_WAREHOUSE_URI NOVAROCKS_ICEBERG_REST_WAREHOUSE NOVA_ENV_SHARED_BENCHMARK_ROOT
NOVA_ENV_BENCHMARK_BUILD_TIMEOUT_SECONDS NOVAROCKS_FE_CONFIG NOVAROCKS_BE_CONFIG
NOVAROCKS_STATE_STORE_PATH NOVAROCKS_SQL_TEST_CONFIG NOVAROCKS_ICE_REST_CATALOG_SQL
NOVAROCKS_SPARK_IMAGE NOVA_ENV_SPARK_VERSION NOVA_ENV_ICEBERG_VERSION NOVAROCKS_SPARK_UI
NOVAROCKS_SPARK_REST_URI NOVAROCKS_SPARK_S3_ENDPOINT NOVAROCKS_SPARK_DEFAULTS
NOVAROCKS_SPARK_V3_SMOKE_SQL NOVAROCKS_SPARK_SQL NOVA_FIXTURE_INPUT_BOM NOVA_FIXTURE_INPUT_LOCK_SHA256
NOVA_ENV_OBJECT_STORE_RUNTIME NOVA_ENV_CATALOG_RUNTIME NOVA_ENV_OBJECT_STORE_CONTAINER
NOVA_ENV_SHARED_COMPOSE_PROJECT NOVA_ENV_SHARED_REST_WAREHOUSE_URI NOVA_ENV_PUBLICATION_HOOK_CONTROL_URI
NOVA_FIXTURE_MACHINE_ID NOVA_FIXTURE_MACHINE_DAEMON NOVA_FIXTURE_MACHINE_VERSION NOVA_FIXTURE_MACHINE_GENERATION
NOVA_FIXTURE_MACHINE_ROOT NOVAROCKS_PAIMON_DOCKER_NETWORK NOVA_FIXTURE_SCRATCH_TTL_SECONDS
NOVA_FIXTURE_MAX_RUN_SECONDS NOVA_FIXTURE_CLEANUP_GRACE_SECONDS""".split()
MANAGED += "NOVA_ENV_OBJECT_STORE_CONFIG NOVA_ENV_OBJECT_STORE_KIND NOVA_OSS_ROLE_NAME NOVA_OSS_OPS_PYTHON iceberg_object_store_credential_mode iceberg_object_store_metadata_credential_name iceberg_object_store_data_credential_name oss_region oss_path_style".split()


def cloud_generation(config):
    return "v1-" + hashlib.sha256(json.dumps(config, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


def cloud_base(config):
    return f"s3://{config['bucket']}/{config['root_prefix']}"


def text_file(directory: Path, name: str, text: str) -> None:
    path = directory / name
    path.write_text(text)
    path.chmod(0o600)


def json_string(value: Any) -> str:
    return json.dumps(str(value), ensure_ascii=False)


def model_hash(repo: Path) -> str:
    result = hashlib.sha256()
    for relative in sorted(("docker/iceberg-rest/templates/catalog.yml", "docker/iceberg-rest/templates/object-store.yml", "docker/iceberg-rest/spark/Dockerfile")):
        name, content = relative.encode(), (repo / relative).read_bytes()
        result.update(len(name).to_bytes(8, "little")); result.update(name)
        result.update(len(content).to_bytes(8, "little")); result.update(content)
    return result.hexdigest()


def storage_root(uri: str) -> str:
    """Canonical directory spelling without changing the physical S3 key."""
    if not isinstance(uri, str) or any(ord(char) <= 32 or char.isspace() for char in uri):
        raise runtime.RuntimeFailure("InvalidConfiguration", "storage root must be an S3 directory")
    parsed = urlsplit(uri)
    if (parsed.scheme != "s3" or not parsed.netloc or parsed.query or parsed.fragment
            or any(char in uri for char in "%*?#") or "@" in parsed.netloc or ":" in parsed.netloc):
        raise runtime.RuntimeFailure("InvalidConfiguration", "storage root must be an unambiguous s3://bucket/prefix")
    key = parsed.path.removeprefix("/")
    if key.endswith("/"):
        key = key[:-1]
    if not key or any(part in ("", ".", "..") for part in key.split("/")):
        raise runtime.RuntimeFailure("InvalidConfiguration", "storage root contains an ambiguous path component")
    return f"s3://{parsed.netloc}/{key}/"


def root_contains(root: str, child: str) -> bool:
    outer, inner = urlsplit(storage_root(root)), urlsplit(storage_root(child))
    left, right = outer.path.strip("/").split("/"), inner.path.strip("/").split("/")
    return outer.netloc == inner.netloc and right[:len(left)] == left


def storage_domains(config: dict, record: dict, *, shared: bool) -> dict:
    """Validate only known roots on the same object store, never common ancestors."""
    owner = record if shared else record.get("external_minio")
    if not owner:
        return {}
    independent = {service: storage_root(uri) for service, uri in owner["server_warehouses"].items()}
    independent["benchmark"] = storage_root(owner["config"]["benchmark_root"])
    owned_children = {}
    if shared:
        env_id = config["env_id"]
        cloud = owner["config"].get("object_store")
        independent.update(worktree=storage_root(cloud_base(cloud) + f"worktrees/{env_id}" if cloud else f"s3://novarocks/{env_id}"),
                           private_catalog=storage_root(cloud_base(cloud) + f"catalogs/{env_id}" if cloud else f"s3://warehouse/{env_id}"))
        for name, uri in config["warehouses"].items():
            base = independent["private_catalog" if name in ("rest_client", "rest_mv_client") else "worktree"]
            if not root_contains(base, uri):
                raise runtime.RuntimeFailure("InvalidConfiguration", "configured warehouse leaves its exact worktree storage root")
    else:
        # A private REST stack using shared MinIO owns children of the original
        # run root; those are intentional containment, not independent domains.
        run_root = storage_root(record["run_context"]["identity"]["scope_root"])
        independent["run_root"] = run_root
        for name, uri in config["warehouses"].items():
            canonical = storage_root(uri)
            if not root_contains(run_root, canonical):
                raise runtime.RuntimeFailure("InvalidConfiguration", "private warehouse leaves its original run storage root")
            owned_children[name] = canonical
    items = list(independent.items())
    for index, (name, root) in enumerate(items):
        for other, candidate in items[index + 1:]:
            if root_contains(root, candidate) or root_contains(candidate, root):
                raise runtime.RuntimeFailure("InvalidConfiguration", f"independent storage domains overlap: {name}, {other}")
    return {"independent": independent, "owned_children": owned_children}


def render_entry(context: dict[str, Any], staging_dir: Path) -> list[str]:
    """Write text only. Paths name the final immutable publication."""
    staging = Path(staging_dir)
    staging.mkdir(parents=True, exist_ok=True)
    config = context["config"]
    publication = Path(context["publication_dir"])
    stable = Path(context["stable_runtime_dir"])
    env_id = context.get("env_id", context["worktree"])
    ready = bool(context["ready"])
    shared = config.get("shared_docker", True)
    repo = Path(config["repo_root"])
    manifest = {
        "ready": ready, "workspace_root": config["workspace_root"], "config_file": config["config_file"],
        "env_id": env_id, "shared_docker": shared, "runtime_dir": str(stable),
        "current_dir": config["current_link"] if config["update_current"] else str(stable),
        "benchmark_fixture": config["benchmark"], "fixture_inputs": config["fixture_inputs"],
        "runtime": {"profile": context.get("profile", "stock"), "control_uri": context.get("control_uri"),
                    "template_model_sha256": context.get("template_model_sha256"),
                    "publication_dir": str(publication), "entry_root": str(stable),
                    "producer_receipt": context.get("producer_receipt")},
    }
    exports = {
        "NOVAROCKS_WORKSPACE_ROOT": config["workspace_root"], "NOVA_ENV_CONFIG_FILE": config["config_file"],
        "NOVA_ENV_ID": env_id, "NOVA_ENV_SHARED_DOCKER": str(shared).lower(),
        "NOVA_ENV_READY": str(ready).lower(), "NOVA_ENV_RUNTIME_DIR": str(stable),
        "NOVA_ENV_CURRENT_DIR": manifest["current_dir"], "NOVA_ENV_REST_ENV_FILE": str(publication / "env.sh"),
        "NOVA_ENV_MANIFEST": str(publication / "manifest.json"), "NOVA_ENV_README": str(publication / "README.md"),
        "NOVA_ENV_SHARED_BENCHMARK_ROOT": config["benchmark"]["shared_root"],
        "NOVA_ENV_BENCHMARK_BUILD_TIMEOUT_SECONDS": config["benchmark"]["build_timeout_seconds"],
    }
    for name in FILES:
        text_file(staging, name, "# Fixture is not ready; run docker/iceberg-rest/up.sh.\n")
    if ready:
        record = context["machine"] if shared else context["private_record"]
        external = record.get("external_minio")
        machine_record = record if shared else external
        endpoint = machine.endpoints(record) if shared else runtime.endpoints(record)
        cloud = record["config"].get("object_store") if shared else None
        if cloud and cloud != config.get("object_store"):
            raise runtime.RuntimeFailure("MachineVersionMismatch", "worktree object store differs from machine configuration")
        credentials = None if cloud else (record["credentials"] if shared else record["config"]["credentials"])
        server_warehouses = {service: storage_root(uri) for service, uri in
            (record["server_warehouses"] if shared else {service: runtime.server_warehouse(record, service) for service in runtime.rest_services(record)}).items()}
        domains = storage_domains(config, record, shared=shared)
        ak, sk = (None, None) if cloud else (credentials["access_key"], credentials["secret_key"])
        store_endpoint = endpoint["object_store_endpoint"] if cloud else endpoint["minio_endpoint"]
        container_endpoint = store_endpoint if cloud else endpoint["container_minio_endpoint"]
        region, path_style = (cloud["region"], "false") if cloud else ("us-east-1", "true")
        mode = "node-local" if cloud else "static"
        generation = cloud_generation(cloud) if cloud else "v1"
        metadata_name, data_name = ("iceberg-oss-metadata", "iceberg-oss-data") if cloud else ("iceberg-test-data", "iceberg-test-data")
        warehouse = config["warehouses"]
        ports, trust = config["local_ports"], config["native_trust"]
        rest = warehouse["rest_client"]
        versions = config["versions"]
        compose = {"compose_project": record["project"], "compose_file": record["compose_file"], "compose_env": record["compose_env"]}
        manifest.update(compose)
        manifest["runtime"]["machine" if shared else "catalog"] = copy.deepcopy(record)
        if shared:
            manifest["runtime"]["producer_receipt"] = record["producer_receipt"]
        elif external:
            manifest["runtime"]["machine"] = copy.deepcopy(external)
        if cloud:
            manifest["object_store"] = {"config": cloud, "endpoint": store_endpoint,
                "config_file": record["object_store_config_file"], "ops_python": record["oss_ops_python"]}
        else:
            manifest["minio"] = {"endpoint": endpoint["minio_endpoint"], "console": endpoint["minio_console"],
                              "access_key_id": ak, "secret_access_key": sk, "volume": machine_record["config"]["minio_volume"] if machine_record else record["volumes"][0]}
        if machine_record:
            manifest["storage_owner"] = copy.deepcopy(record["storage_owner"]) if cloud else {"daemon_id": machine_record["daemon_id"],
                                         "minio_volume": machine_record["config"]["minio_volume"]}
            manifest["storage_domains"] = domains
        manifest["iceberg_rest"] = {"uri": endpoint["rest_uri"], "warehouse": rest, "server_default_warehouse": server_warehouses["rest"]}
        if "rest-mv" in record["images"]:
            manifest["iceberg_rest_mv"] = {"uri": endpoint["rest_mv_uri"], "warehouse": warehouse["rest_mv_client"],
                                           "server_default_warehouse": server_warehouses["rest-mv"]}
        manifest["spark"] = {"image": record["images"]["spark"]["tag"], "spark_version": versions["spark"], "iceberg_version": versions["iceberg"],
            "ui": endpoint["spark_ui"], "container_rest_uri": endpoint["container_rest_uri"], "object_store_endpoint": container_endpoint,
            "defaults_file": str(publication / "spark-defaults.conf"), "v3_smoke_sql": str(publication / "spark-iceberg-v3-smoke.sql"), "helper": str(repo / "docker/iceberg-rest/spark-sql.sh")}
        manifest["novarocks"] = {**{name + "_port": value for name, value in ports.items()},
            "native_trust_deployment_id": trust["deployment_id"], "fe_config": str(publication / "fe.toml"),
            "be_config": str(publication / "be.toml"), "state_store_path": str(stable / "frontend-state.sqlite"),
            "sql_test_config": str(publication / "sql-test.toml"), "ice_rest_catalog_sql": str(publication / "ice-rest-catalog.sql"),
            "iceberg_catalog_warehouse": warehouse["catalog"], "iceberg_test_warehouse": warehouse["test"]}
        exports.update({
            "NOVA_ENV_COMPOSE_PROJECT": record["project"], "NOVA_ENV_COMPOSE_FILE": record["compose_file"], "NOVA_ENV_COMPOSE_ENV": record["compose_env"],
            "NOVA_ENV_REST_PORT": record["ports"]["rest"], "NOVA_ENV_SPARK_UI_PORT": record["ports"]["spark"],
            "NOVA_ENV_OBJECT_STORE_RUNTIME": (machine_record or record)["id"], "NOVA_ENV_CATALOG_RUNTIME": record["id"],
            "NOVA_ENV_OBJECT_STORE_CONTAINER": (machine_record or record).get("containers", {}).get("minio", ""),
            "NOVAROCKS_NATIVE_SHARED_SECRET": trust["shared_secret"],
            "AWS_S3_ENDPOINT": store_endpoint,
            "iceberg_object_store_credential_name": data_name, "iceberg_object_store_credential_generation": generation,
            "iceberg_object_store_metadata_credential_name": metadata_name, "iceberg_object_store_data_credential_name": data_name,
            "iceberg_object_store_credential_mode": mode, "oss_region": region, "oss_path_style": path_style,
            "CATALOG_WAREHOUSE_URI": warehouse["catalog"], "NOVAROCKS_ICEBERG_TEST_WAREHOUSE": warehouse["test"],
            "NOVAROCKS_ICEBERG_REST_URI": endpoint["rest_uri"], "NOVA_ENV_REST_SERVER_WAREHOUSE_URI": server_warehouses["rest"],
            "NOVA_ENV_REST_WAREHOUSE_URI": rest, "NOVAROCKS_ICEBERG_REST_WAREHOUSE": rest,
            "NOVAROCKS_FE_CONFIG": str(publication / "fe.toml"), "NOVAROCKS_BE_CONFIG": str(publication / "be.toml"),
            "NOVAROCKS_STATE_STORE_PATH": str(stable / "frontend-state.sqlite"), "NOVAROCKS_SQL_TEST_CONFIG": str(publication / "sql-test.toml"),
            "NOVAROCKS_ICE_REST_CATALOG_SQL": str(publication / "ice-rest-catalog.sql"), "NOVAROCKS_SPARK_IMAGE": record["images"]["spark"]["tag"],
            "NOVA_ENV_SPARK_VERSION": versions["spark"], "NOVA_ENV_ICEBERG_VERSION": versions["iceberg"],
            "NOVAROCKS_SPARK_UI": endpoint["spark_ui"], "NOVAROCKS_SPARK_REST_URI": endpoint["container_rest_uri"],
            "NOVAROCKS_SPARK_S3_ENDPOINT": container_endpoint, "NOVAROCKS_SPARK_DEFAULTS": str(publication / "spark-defaults.conf"),
            "NOVAROCKS_SPARK_V3_SMOKE_SQL": str(publication / "spark-iceberg-v3-smoke.sql"), "NOVAROCKS_SPARK_SQL": manifest["spark"]["helper"],
            "NOVA_FIXTURE_INPUT_BOM": config["fixture_inputs"]["bom"], "NOVA_FIXTURE_INPUT_LOCK_SHA256": config["fixture_inputs"]["lock_sha256"],
        })
        if cloud:
            exports.update(NOVA_ENV_OBJECT_STORE_KIND="aliyun-oss", NOVA_ENV_OBJECT_STORE_CONFIG=record["object_store_config_file"],
                NOVA_OSS_ROLE_NAME=cloud["role_name"], NOVA_OSS_OPS_PYTHON=record["oss_ops_python"],
                AWS_REQUEST_CHECKSUM_CALCULATION="WHEN_REQUIRED", AWS_RESPONSE_CHECKSUM_VALIDATION="WHEN_REQUIRED")
        else:
            exports.update(NOVA_ENV_MINIO_PORT=record["ports"]["minio"], NOVA_ENV_MINIO_CONSOLE_PORT=record["ports"]["minio_console"],
                AWS_S3_ACCESS_KEY_ID=ak, AWS_S3_SECRET_ACCESS_KEY=sk, MINIO_ROOT_USER=ak, MINIO_ROOT_PASSWORD=sk)
            manifest["spark"]["container_minio_endpoint"] = container_endpoint
        exports.update({"NOVA_ENV_" + name.upper() + "_PORT": value for name, value in ports.items()})
        if machine_record:
            exports.update(NOVA_FIXTURE_MACHINE_ID=machine_record["id"], NOVA_FIXTURE_MACHINE_DAEMON=machine_record["daemon_id"],
                NOVA_FIXTURE_MACHINE_VERSION=machine_record["version"], NOVA_FIXTURE_MACHINE_GENERATION=machine_record["generation"],
                NOVA_FIXTURE_MACHINE_ROOT=machine_record["control_root"],
                NOVAROCKS_PAIMON_DOCKER_NETWORK=machine_record["network"],
                NOVA_FIXTURE_SCRATCH_TTL_SECONDS=machine_record["policy"]["scratch_ttl_seconds"],
                NOVA_FIXTURE_MAX_RUN_SECONDS=machine_record["policy"]["max_run_seconds"],
                NOVA_FIXTURE_CLEANUP_GRACE_SECONDS=machine_record["policy"]["cleanup_grace_seconds"])
        if "rest-mv" in record["images"]:
            exports.update({
                "NOVA_ENV_REST_MV_PORT": record["ports"]["rest_mv"],
                "NOVA_ENV_REST_MV_SERVER_WAREHOUSE_URI": manifest["iceberg_rest_mv"]["server_default_warehouse"],
                "NOVAROCKS_ICEBERG_REST_MV_URI": endpoint["rest_mv_uri"],
                "NOVAROCKS_ICEBERG_REST_MV_WAREHOUSE": warehouse["rest_mv_client"],
                "NOVAROCKS_SPARK_REST_MV_URI": endpoint["container_rest_mv_uri"],
            })
        if context.get("control_uri"):
            exports["NOVA_ENV_PUBLICATION_HOOK_CONTROL_URI"] = context["control_uri"]
        render_role_configs(staging, stable, env_id, ports, trust, object_store=cloud)
        values = {"url": "http://127.0.0.1:8030", "oss_endpoint": store_endpoint,
                  "iceberg_catalog_type": "hadoop", "iceberg_catalog_warehouse": warehouse["catalog"], "iceberg_test_warehouse": warehouse["test"],
                  "iceberg_rest_uri": endpoint["rest_uri"], "iceberg_rest_warehouse": rest, "iceberg_object_store_credential_name": "iceberg-test-data",
                  "iceberg_object_store_credential_generation": generation, "benchmark_shared_root": config["benchmark"]["shared_root"], "fixture_env_file": str(publication / "env.sh"),
                  "fixture_object_store_kind": "aliyun-oss" if cloud else "minio", "oss_region": region, "oss_path_style": path_style,
                  "iceberg_object_store_credential_mode": mode, "iceberg_object_store_metadata_credential_name": metadata_name,
                  "iceberg_object_store_data_credential_name": data_name}
        values["iceberg_object_store_credential_name"] = data_name
        if not cloud:
            values.update(oss_ak=ak, oss_sk=sk)
        if "rest-mv" in record["images"]:
            values.update(iceberg_rest_mv_uri=endpoint["rest_mv_uri"], iceberg_rest_mv_warehouse=warehouse["rest_mv_client"])
        text_file(staging, "sql-test.toml", '[cluster]\nhost = "127.0.0.1"\nport = ' + json_string(ports["mysql"]) + '\nuser = "root"\npassword = ""\n\n[env]\n' + ''.join(f'{key} = {json_string(value)}\n' for key, value in sorted(values.items())))
        sql_values = {"type": "iceberg", "iceberg.catalog.type": "rest", "uri": endpoint["rest_uri"], "warehouse": rest,
                      "aws.s3.endpoint": store_endpoint,
                      "aws.s3.region": region, "aws.s3.enable_path_style_access": path_style}
        for purpose, role in (("metadata", "frontend"), ("data", "backend")):
            prefix = f"credential.object-store-{purpose}."
            sql_values.update({prefix + "consumer-role": role, prefix + "mode": mode,
                               prefix + "name": metadata_name if purpose == "metadata" else data_name, prefix + "generation": generation})
        sql_quote = lambda value: "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"
        text_file(staging, "ice-rest-catalog.sql", "CREATE EXTERNAL CATALOG ice_rest PROPERTIES (\n" + ',\n'.join(f'  {sql_quote(k)} = {sql_quote(v)}' for k, v in sorted(sql_values.items())) + "\n);\n")
        spark = {"spark.master": "local[*]", "spark.app.name": "NovaRocksIcebergSpark", "spark.ui.bindAddress": "0.0.0.0", "spark.driver.bindAddress": "0.0.0.0",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions", "spark.sql.catalog.ice_rest": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.ice_rest.type": "rest", "spark.sql.catalog.ice_rest.uri": endpoint["container_rest_uri"], "spark.sql.catalog.ice_rest.warehouse": rest,
            "spark.sql.catalog.ice_rest.io-impl": "org.apache.iceberg.aws.s3.S3FileIO", "spark.sql.catalog.ice_rest.s3.endpoint": container_endpoint,
            "spark.sql.catalog.ice_rest.s3.path-style-access": "true", "spark.sql.catalog.ice_rest.s3.access-key-id": ak, "spark.sql.catalog.ice_rest.s3.secret-access-key": sk,
            "spark.sql.catalog.ice_rest.s3.region": "us-east-1", "spark.sql.defaultCatalog": "ice_rest", "spark.hadoop.fs.s3a.endpoint": container_endpoint,
            "spark.hadoop.fs.s3a.access.key": ak, "spark.hadoop.fs.s3a.secret.key": sk, "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false", "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"}
        if cloud:
            for key in list(spark):
                if key.endswith((".access-key-id", ".secret-access-key", ".access.key", ".secret.key")):
                    del spark[key]
            spark.update({
                "spark.sql.catalog.ice_rest.s3.endpoint": container_endpoint,
                "spark.sql.catalog.ice_rest.s3.path-style-access": path_style,
                "spark.sql.catalog.ice_rest.s3.region": region,
                "spark.sql.catalog.ice_rest.client.region": region,
                "spark.sql.catalog.ice_rest.client.credentials-provider": "org.novarocks.fixture.auth.AliyunEcsCredentialsProvider",
                "spark.sql.catalog.ice_rest.client.credentials-provider.role-name": cloud["role_name"],
                "spark.hadoop.fs.s3a.endpoint": container_endpoint,
                "spark.hadoop.fs.s3a.endpoint.region": region,
                "spark.hadoop.fs.s3a.path.style.access": path_style,
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.novarocks.fixture.auth.AliyunEcsV1CredentialsProvider",
            })
        if "rest-mv" in record["images"]:
            spark.update({key.replace("spark.sql.catalog.ice_rest", "spark.sql.catalog.ice_rest_mv", 1): value
                          for key, value in spark.items() if key.startswith("spark.sql.catalog.ice_rest")})
            spark.update({"spark.sql.catalog.ice_rest_mv.uri": endpoint["container_rest_mv_uri"],
                          "spark.sql.catalog.ice_rest_mv.warehouse": warehouse["rest_mv_client"]})
        text_file(staging, "spark-defaults.conf", ''.join(f'{key} {value}\n' for key, value in sorted(spark.items())))
        smoke_sql = "CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_v3;\n\nDROP TABLE IF EXISTS ice_rest.nr_v3.spark_v3_smoke;\n\nCREATE TABLE ice_rest.nr_v3.spark_v3_smoke (\n  id BIGINT,\n  data STRING,\n  category STRING,\n  ts TIMESTAMP\n) USING iceberg\nTBLPROPERTIES (\n  'format-version' = '3',\n  'write.row-lineage' = 'true',\n  'write.format.default' = 'parquet'\n);\n\nINSERT INTO ice_rest.nr_v3.spark_v3_smoke VALUES\n  (1, 'spark-v3-a', 'alpha', TIMESTAMP '2026-05-07 00:00:00'),\n  (2, 'spark-v3-b', 'beta', TIMESTAMP '2026-05-07 00:01:00'),\n  (3, 'spark-v3-c', 'alpha', TIMESTAMP '2026-05-07 00:02:00');\n\nSELECT * FROM ice_rest.nr_v3.spark_v3_smoke ORDER BY id;\n"
        if machine_record:
            # Run identity belongs to the caller, never to this stable publication.
            smoke_sql = smoke_sql.replace("CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_v3;", "CREATE NAMESPACE ice_rest.__NOVA_SMOKE_NAMESPACE__;")
            smoke_sql = smoke_sql.replace("DROP TABLE IF EXISTS ice_rest.nr_v3.spark_v3_smoke;\n\n", "")
            smoke_sql = smoke_sql.replace("ice_rest.nr_v3", "ice_rest.__NOVA_SMOKE_NAMESPACE__")
            smoke_sql = "-- novarocks-generated-shared-spark-v3-smoke-v1\n" + smoke_sql
        text_file(staging, "spark-iceberg-v3-smoke.sql", smoke_sql)
    text_file(staging, "env.sh", '# Generated fixture publication.\nunset ' + ' '.join(MANAGED) + '\n' + ''.join(f'export {key}={shlex.quote(str(value))}\n' for key, value in sorted(exports.items())))
    text_file(staging, "manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + '\n')
    text_file(staging, "README.md", f'# NovaRocks fixture {env_id}\n\nReady: {str(ready).lower()}\n\nPublication: `{publication}`\n\nStable runtime data: `{stable}`\n\n' + (f'REST: {manifest["iceberg_rest"]["uri"]}\n\nObject store: {store_endpoint}\n' if ready else 'Run `docker/iceberg-rest/up.sh` to bind this worktree.\n') +
              ('\nShared writers use run names, location checks, the original budget, and catalog-first cleanup. Node-local cloud credentials do not grant per-run IAM isolation or TTL deletion rights. Run identity and deadlines belong to the caller.\n' if ready and machine_record and cloud else '\nShared REST and Spark use shared storage credentials. Run IAM policies constrain only writers using the run credentials; shared writers rely on run names, location checks, the original budget, and catalog-first cleanup. Run identity and deadlines belong to the caller and are not stored in this publication.\n' if ready and machine_record else ''))
    return list(FILES)


def render_role_configs(staging: Path, stable: Path, env_id: str, ports: dict, trust: dict, *, object_store=None) -> None:
    for role in ("fe", "be"):
        control = f'control_grpc_port = {ports["be_control_grpc"]}\n' if role == "be" else ""
        value = f'''[native_trust]
deployment_id = {json_string(trust["deployment_id"])}
shared_secret = "${{ENV:NOVAROCKS_NATIVE_SHARED_SECRET}}"

[server]
host = "127.0.0.1"
http_port = {ports[role + "_http"]}
grpc_port = {ports[role + "_grpc"]}
{control}
[cluster]
role = "{role}"
'''
        if role == "be":
            value += f'advertise_host = "127.0.0.1"\nfrontend_endpoint = "127.0.0.1:{ports["fe_grpc"]}"\n'
        else:
            value += '\n[catalog_source]\nmode = "dynamic-state-store"\n'
        value += '\n[runtime]\nexchange_wait_ms = 300000\n'
        if role == "fe":
            value += f'''connector_split_initial_dynamic_filter_wait_cap_ms = 1000
query_control_task_update_rpc_timeout_ms = 5000
query_control_task_update_retry_error_duration_ms = 30000
query_control_task_update_retry_initial_backoff_ms = 100
query_control_task_update_retry_max_backoff_ms = 1000

[state_store]
provider = "sqlite"
path = {json_string(stable / "frontend-state.sqlite")}
cluster_id = {json_string(env_id)}

[standalone_server]
mysql_port = {ports["mysql"]}
user = "root"
mv_refresh_scheduler_enabled = true
mv_refresh_scheduler_interval_ms = 200
mv_refresh_scheduler_max_concurrent = 1
mv_refresh_scheduler_failure_backoff_ms = 500
mv_refresh_scheduler_max_failure_backoff_ms = 2000
'''
        if role == "fe":
            value += '''
[runtime.frontend_constant_policy]
# Frozen compiled-package constant policy (ADR-0185); no deployment override.
max_rows = 4096
max_array_nodes = 4096
max_logical_elements = 65536
max_retained_buffer_bytes = 8388608
max_type_depth = 64
max_type_nodes = 4096
max_dictionary_depth = 8
max_metadata_bytes = 65536
max_library_validation_work = 33554432
max_library_validation_bytes = 33554432
'''
        purpose = "metadata" if role == "fe" else "data"
        if object_store:
            value += f'''\n[[connector.credentials]]
purpose = "object-store-{purpose}"
name = "iceberg-oss-{purpose}"
generation = {json_string(cloud_generation(object_store))}
kind = "aliyun-ecs-ram-role"
role_name = {json_string(object_store["role_name"])}
scope_prefixes = [{json_string(cloud_base(object_store))}]
'''
        else:
            value += f'''\n[[connector.credentials]]
purpose = "object-store-{purpose}"
name = "iceberg-test-data"
generation = "v1"
kind = "s3"
access_key_id = "${{ENV:AWS_S3_ACCESS_KEY_ID}}"
access_key_secret = "${{ENV:AWS_S3_SECRET_ACCESS_KEY}}"
'''
        text_file(staging, role + ".toml", value)


def sections(text: str) -> dict[str, str]:
    """Split the small, checked-in Compose templates without a YAML dependency."""
    result: dict[str, str] = {}
    section = "header"
    for line in text.splitlines(keepends=True):
        match = re.match(r'^(x-owner-labels|services|volumes|networks):', line)
        if match:
            section = match[1]
        result[section] = result.get(section, "") + line
    return result


def blocks(section: str) -> dict[str, str]:
    result: dict[str, str] = {}
    name = ""
    for line in section.splitlines(keepends=True)[1:]:
        match = re.match(r'^  ([a-zA-Z0-9_-]+):', line)
        if match:
            name = match[1]
        if not name:
            raise runtime.RuntimeFailure("RuntimeDefinitionMissing", "invalid template section")
        result[name] = result.get(name, "") + line
    return result


def render_isolated_stack(bom: dict, project: str, ports: dict, out: Path, config: dict,
                          *, profile: str = "stock", hook_image: str | None = None) -> dict:
    """Merge the owner's exact two templates; do not inspect or start Docker."""
    if not project.startswith("nr-isolated-rest-"):
        raise runtime.RuntimeFailure("InvalidIdentity", "isolated project must use nr-isolated-rest- prefix")
    if profile not in {"stock", "publication-hook"}:
        raise runtime.RuntimeFailure("RuntimeDefinitionMissing", "unknown isolated profile")
    out = Path(out).resolve(); out.mkdir(parents=True, exist_ok=True)
    templates = {kind: sections((HERE / "templates" / name).read_text()) for kind, name in (("os", "object-store.yml"), ("cat", "catalog.yml"))}
    services = blocks(templates["os"]["services"])
    external = config.get("shared_minio")
    for name, definition in blocks(templates["cat"]["services"]).items():
        if name in services and services[name] != definition:
            raise runtime.RuntimeFailure("RuntimeDefinitionMissing", "conflicting template service")
        services[name] = definition
    if templates["os"]["networks"] != templates["cat"]["networks"]:
        raise runtime.RuntimeFailure("RuntimeDefinitionMissing", "conflicting template network")
    if profile == "publication-hook":
        if not hook_image or not 0 < int(ports.get("control", 0)) < 65536:
            raise runtime.RuntimeFailure("RuntimeDefinitionMissing", "publication hook needs image and control port")
        services["rest"] = services["rest"].replace("org.apache.iceberg.aws.s3.S3FileIO", "org.apache.iceberg.rest.fixture.TracingFileIO\n      UEA7_DELEGATE_FILE_IO: s3")
        services["rest"] = services["rest"].replace("ports: ['${NOVA_ENV_REST_PORT}:8181']", "ports: ['${NOVA_ENV_REST_PORT}:8181', '127.0.0.1:${NOVA_ENV_PUBLICATION_HOOK_CONTROL_PORT}:8182']")
    if external:
        for name in ("minio", "mc-init"):
            services.pop(name)
        from private_deadline import guard_service
        for name in ("rest", "rest-mv", "spark"):
            services[name] = guard_service(services[name], config["writer_commands"][name], config["run_context"])
    text = templates["os"]["header"] + templates["os"]["x-owner-labels"] + "services:\n" + ''.join(services.values())
    text += "volumes:\n" + ("" if external else ''.join(blocks(templates["os"]["volumes"]).values())) + ''.join(blocks(templates["cat"]["volumes"]).values()) + templates["os"]["networks"]
    images = {"minio": bom["derived_images"]["minio"]["image_id"], "mc": bom["derived_images"]["minio-mc"]["image_id"],
              "rest": hook_image if profile == "publication-hook" else bom["images"]["iceberg-rest"]["alias"],
              "rest-mv": bom["derived_images"]["rest-mv"]["image_id"],
              "spark": bom["derived_images"]["iceberg-spark"]["image_id"]}
    images["mc-init"] = images["mc"]
    if external:
        images.pop("minio")
        images.pop("mc-init")
        images = {name: config["writer_images"][name] for name in images}
    rest_services = runtime.rest_services({"images": images})
    for key in ("minio", "minio_console", "rest", "rest_mv", "spark"):
        if not 0 < int(ports[key]) < 65536:
            raise runtime.RuntimeFailure("PortUnavailable", key)
    key = runtime.digest({"project": project, "templates": text, "images": images, "profile": profile})
    credentials = config["credentials"]
    values = {"NOVA_FIXTURE_OWNER": project, "NOVA_FIXTURE_KEY": key, "NOVA_FIXTURE_KIND": "isolated", "NOVA_FIXTURE_PROJECT": project,
              "MINIO_ROOT_USER": credentials["access_key"], "MINIO_ROOT_PASSWORD": credentials["secret_key"],
              "NOVA_ENV_REST_SERVER_WAREHOUSE_URI": config["warehouses"]["rest_client"],
              "NOVA_ENV_REST_MV_SERVER_WAREHOUSE_URI": config["warehouses"]["rest_mv_client"]}
    values.update({service.upper().replace('-', '_') + '_IMAGE': image for service, image in images.items()})
    values.update({'NOVA_ENV_' + name.upper() + '_PORT': value for name, value in ports.items() if name != 'control'})
    if profile == "publication-hook":
        values["NOVA_ENV_PUBLICATION_HOOK_CONTROL_PORT"] = ports["control"]
    text_file(out, "compose.yml", text)
    text_file(out, "compose.env", ''.join(f"{name}='{str(value).replace(chr(39), chr(92) + chr(39))}'\n" for name, value in sorted(values.items())))
    record = {"id": project, "key": key, "kind": "isolated", "namespace": project, "project": project,
        "network": project + "_iceberg_net", "ports": ports, "compose_file": str(out / "compose.yml"), "compose_env": str(out / "compose.env"),
        "images": {name: {"tag": image, "image_id": image} for name, image in images.items()}, "config": {"credentials": credentials},
        "volumes": [project + "_minio-data"] + [project + "_" + service + "-catalog" for service in rest_services],
        "required_services": ["minio", "mc-init", *rest_services, "spark"],
        "service_ports": {"minio": {"9000/tcp": ports["minio"], "9001/tcp": ports["minio_console"]}, "spark": {"4040/tcp": ports["spark"]},
                          **{service: {"8181/tcp": ports[service.replace('-', '_')]} for service in rest_services}},
        "health_urls": [f'http://127.0.0.1:{ports["minio"]}/minio/health/live'] +
                       [f'http://127.0.0.1:{ports[service.replace("-", "_")]}/v1/config' for service in rest_services],
        "server_warehouse": config["warehouses"]["rest_client"]}
    if external:
        record["external_minio"] = copy.deepcopy(external)
        record["run_context"] = copy.deepcopy(config["run_context"])
        record["volumes"] = [project + "_" + service + "-catalog" for service in rest_services]
        record["required_services"] = [*rest_services, "spark"]
        record["service_ports"].pop("minio")
    if profile == 'publication-hook':
        record['service_ports']['rest']['8182/tcp'] = ports['control']
    context = {"ready": True, "worktree": config["env_id"], "env_id": config["env_id"],
        "config": config, "publication_dir": str(out), "stable_runtime_dir": str(out), "entry_root": str(out),
        "private_record": record,
        "producer_receipt": {"lock_sha256": bom["lock_sha256"], **bom["derived_images"]["iceberg-spark"]},
        "profile": profile, "control_uri": f'http://127.0.0.1:{ports["control"]}' if profile == "publication-hook" else None,
        "template_model_sha256": model_hash(Path(config["repo_root"]))}
    return {"record": record, "context": context, "compose_file": record["compose_file"], "compose_env": record["compose_env"]}


def load_settings(path: Path) -> dict[str, str]:
    allowed = {"NOVA_ENV_SHARED_DOCKER", "NOVA_ENV_COMPOSE_PROJECT", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "NOVA_ENV_SPARK_VERSION", "NOVA_ENV_ICEBERG_VERSION", "NOVA_ENV_SHARED_BENCHMARK_ROOT", "NOVA_ENV_BENCHMARK_BUILD_TIMEOUT_SECONDS", "NOVA_ENV_RUNTIME_PORT_START", "NOVA_ENV_RUNTIME_PORT_END"}
    allowed.add("NOVA_ENV_OBJECT_STORE_CONFIG")
    for name in LOCAL_RANGES:
        allowed.update({"NOVA_ENV_" + name.upper() + "_PORT_START", "NOVA_ENV_" + name.upper() + "_PORT_RANGE"})
    values = {}
    for line in path.read_text().splitlines():
        if not line.strip() or line.lstrip().startswith('#'):
            continue
        key, separator, value = line.partition('=')
        key = key.strip()
        if not separator or key not in allowed:
            raise runtime.RuntimeFailure("InvalidConfiguration", f"unsupported setting {key}")
        parsed = shlex.split(value, comments=True)
        if len(parsed) != 1:
            raise runtime.RuntimeFailure("InvalidConfiguration", key)
        values[key] = parsed[0]
    return values


def environment_identity(workspace: Path) -> str:
    slug = re.sub('[^a-z0-9]+', '-', workspace.name.lower()).strip('-')[:24] or 'novarocks'
    return slug + '-' + hashlib.sha1(str(workspace).encode()).hexdigest()[:8]


def port_available(port: int) -> bool:
    with socket.socket() as probe:
        try:
            probe.bind(('127.0.0.1', port))
            return True
        except OSError:
            return False


def choose_ports(names: dict[str, int], offset: int, ranges: dict[str, int] | None = None, *, reserved: set[int] | None = None) -> dict[str, int]:
    result = {}
    reserved = reserved or set()
    for name, base in names.items():
        count = (ranges or {}).get(name, 199) + 1
        for index in range(count):
            port = base + (offset + index) % count
            if port not in result.values() and port not in reserved and port_available(port):
                result[name] = port
                break
        else:
            raise runtime.RuntimeFailure('PortUnavailable', name)
    return result


def request_config(workspace: Path, settings: dict, config_file: Path, entry: Path, *, shared: bool, update_current: bool) -> dict:
    previous = previous_publication(entry) if shared else None
    old_config = previous.get('config', {}) if previous else {}
    env_id = entry.name
    offset = int(hashlib.sha1(str(workspace).encode()).hexdigest()[:8], 16)
    starts = {name: int(settings.get('NOVA_ENV_' + name.upper() + '_PORT_START', base)) for name, base in LOCAL_RANGES.items()}
    ranges = {name: int(settings.get('NOVA_ENV_' + name.upper() + '_PORT_RANGE', '199')) for name in LOCAL_RANGES}
    ports = dict(old_config.get('local_ports') or {})
    missing = {name: base for name, base in starts.items() if name not in ports}
    if missing:
        ports.update(choose_ports(missing, offset, ranges, reserved=set(ports.values())))
    cloud_path = os.environ.get('NOVA_ENV_OBJECT_STORE_CONFIG', settings.get('NOVA_ENV_OBJECT_STORE_CONFIG'))
    cloud = old_config.get('object_store')
    if cloud_path:
        if not Path(cloud_path).is_absolute():
            raise runtime.RuntimeFailure('InvalidConfiguration', 'object store configuration path must be absolute')
        try:
            cloud = oss_ops.validate_config(runtime.read_json(Path(cloud_path)))
        except (OSError, ValueError, oss_ops.OpsError) as error:
            raise runtime.RuntimeFailure('InvalidConfiguration', 'invalid object store configuration') from error
    if cloud and not shared:
        raise runtime.RuntimeFailure('UnsupportedFixtureProvider', 'private MinIO fixture profiles do not support OSS')
    benchmark_root = storage_root(settings.get('NOVA_ENV_SHARED_BENCHMARK_ROOT', cloud_base(cloud) + 'benchmarks' if cloud else 's3://novarocks/shared/benchmarks'))
    if cloud:
        machine.machine_config({'object_store': cloud, 'benchmark_root': benchmark_root})
    timeout = int(settings.get('NOVA_ENV_BENCHMARK_BUILD_TIMEOUT_SECONDS', '3600'))
    if timeout < 1:
        raise runtime.RuntimeFailure('InvalidConfiguration', 'benchmark timeout must be positive')
    store = Path(os.environ.get('NOVA_FIXTURE_STORE', str(Path(os.environ.get('XDG_CACHE_HOME', str(Path.home() / '.cache'))) / 'novarocks/fixture-inputs'))).resolve()
    result = {'workspace_root': str(workspace), 'repo_root': str(HERE.parent.parent), 'config_file': str(config_file),
            'env_id': env_id, 'shared_docker': shared, 'current_link': str(entry.parent / 'current'), 'update_current': update_current,
            'credentials': None if cloud else {'access_key': settings['MINIO_ROOT_USER'], 'secret_key': settings['MINIO_ROOT_PASSWORD']}, 'buckets': ['warehouse', 'novarocks'],
            'local_ports': ports, 'native_trust': old_config.get('native_trust') or {'deployment_id': env_id, 'shared_secret': 'local-native-trust-fixture-' + hashlib.sha256(str(workspace).encode()).hexdigest()},
            'benchmark': {'shared_root': benchmark_root, 'build_timeout_seconds': timeout},
            'versions': {'spark': settings.get('NOVA_ENV_SPARK_VERSION', '3.5.5-java17'), 'iceberg': settings.get('NOVA_ENV_ICEBERG_VERSION', '1.11.0')},
            'fixture_inputs': old_config.get('fixture_inputs') or {'bom': str(store / 'bom.json'), 'lock_sha256': None, 'verified': False},
            'warehouses': {'catalog': f's3://novarocks/{env_id}/iceberg-catalog', 'test': f's3://novarocks/{env_id}/novarocks-sql-test-iceberg-extra',
                           'rest_client': f's3://warehouse/{env_id}/rest', 'rest_mv_client': f's3://warehouse/{env_id}/rest-mv'}}
    if cloud:
        result.pop('credentials')
        result['object_store'] = cloud
        result['buckets'] = [cloud['bucket']]
        base = cloud_base(cloud)
        result['warehouses'] = {'catalog': base + f'worktrees/{env_id}/iceberg-catalog',
            'test': base + f'worktrees/{env_id}/novarocks-sql-test-iceberg-extra',
            'rest_client': base + f'catalogs/{env_id}/rest', 'rest_mv_client': base + f'catalogs/{env_id}/rest-mv'}
    return result


def verify_inputs(config: dict) -> dict:
    # Consume the verifier's locked snapshot, not a later read of mutable bom.json.
    # The closed consumer and offline verification remain owned by ADR-0155.
    consumer = 'iceberg-rest-oss' if config.get('object_store') else 'iceberg-rest'
    bom = machine.verified_bom(Path(config['repo_root']), Path(config['fixture_inputs']['bom']).parent, consumer=consumer)
    config['fixture_inputs'].update(lock_sha256=bom['lock_sha256'], platform=bom['platform'], verified=True, consumer=consumer)
    return bom


def isolated_start(config: dict, project: str, entry: Path, profile: str, hook_image: str | None, bom: dict) -> dict:
    if os.environ.get("NOVA_ENV_PRIVATE_SHARED_MINIO") == "true":
        from scratch import ScratchIdentity, RunBudget
        inherited = json.loads(os.environ.get("NOVA_FIXTURE_RUN_CONTEXT", "null"))
        if not isinstance(inherited, dict):
            raise runtime.RuntimeFailure("RunBudgetRejected", "shared MinIO requires the original run context")
        binding = {name: os.environ.get("NOVA_FIXTURE_MACHINE_" + name) for name in ("ROOT", "DAEMON", "GENERATION")}
        if not all(binding.values()):
            raise runtime.RuntimeFailure("RunBudgetRejected", "shared MinIO requires its original machine binding")
        # The frozen daemon identity permits a saved-state read without Docker.
        # Admit before image/daemon/health probes, not after waiting for them.
        owner = machine.MachineOwner(binding["ROOT"], daemon=binding["DAEMON"])
        saved = owner.read_saved_state()
        if not saved:
            raise runtime.RuntimeFailure("MachineUnavailable", "shared MinIO has no saved authority")
        identity = ScratchIdentity.from_dict(inherited["identity"])
        budget = RunBudget.admit(saved, identity, inherited=inherited)
        deadline = budget.deadline - budget.cleanup_grace_seconds
        previous_deadline = getattr(owner.backend, "operation_deadline", None)
        owner.backend.operation_deadline = min(previous_deadline, deadline) if previous_deadline is not None else deadline
        try:
            definition = owner.required_definition(owner.load_config(), bom)
            with owner.ready_snapshot(definition, deadline=deadline,
                                      expected_generation=binding["GENERATION"]) as snapshot:
                budget.check(work=True)
                config = copy.deepcopy(config)
                config["shared_minio"] = snapshot
                config["credentials"] = snapshot["credentials"]
                config["run_context"] = budget.as_dict()
                config["warehouses"] = {"catalog": identity.scope_root + "/catalog", "test": identity.scope_root + "/test",
                    "rest_client": identity.scope_root + "/rest", "rest_mv_client": identity.scope_root + "/rest-mv"}
                return start_private_stack(config, project, entry, profile, hook_image, bom, budget=budget)
        finally:
            owner.backend.operation_deadline = previous_deadline
    return start_private_stack(config, project, entry, profile, hook_image, bom)


def start_private_stack(config, project, entry, profile, hook_image, bom, *, budget=None):
    offset = int(hashlib.sha1(config['env_id'].encode()).hexdigest()[:8], 16)
    ports = choose_ports({'minio': 19000, 'minio_console': 20000, 'rest': 21000, 'spark': 22000, 'rest_mv': 24000}, offset)
    if config.get("shared_minio"):
        for name in ("minio", "minio_console"):
            ports[name] = config["shared_minio"]["ports"][name]
    if profile == 'publication-hook':
        ports['control'] = int(os.environ.get('NOVA_ENV_PUBLICATION_HOOK_CONTROL_PORT', '0'))
    backend = runtime.Docker(timeout=min(120, budget.remaining_seconds(work=True)) if budget else 120)
    if budget:
        backend.operation_deadline = budget.deadline - budget.cleanup_grace_seconds
    if config.get("shared_minio"):
        # Inspect exact locally supplied images before creating any writer.
        # The default image entrypoint is data, never a guessed Java command.
        references = {"rest": hook_image if profile == "publication-hook" else bom["images"]["iceberg-rest"]["alias"],
            "rest-mv": bom["derived_images"]["rest-mv"]["image_id"],
            "spark": bom["derived_images"]["iceberg-spark"]["image_id"], "mc": bom["derived_images"]["minio-mc"]["image_id"]}
        config["writer_images"], config["writer_commands"] = {}, {}
        for service, reference in references.items():
            budget.check(work=True)
            backend.timeout = min(120, budget.remaining_seconds(work=True))
            info = backend.inspect("image", reference)
            config["writer_images"][service] = info["Id"]
            if service == "spark":
                config["writer_commands"][service] = ["/bin/bash", "-lc", "trap 'exit 0' TERM INT; sleep infinity & wait"]
            elif service != "mc":
                image_config = info.get("Config") or {}
                config["writer_commands"][service] = (image_config.get("Entrypoint") or []) + (image_config.get("Cmd") or [])
    result = render_isolated_stack(bom, project, ports, entry, config, profile=profile, hook_image=hook_image)
    record, context = result['record'], result['context']
    # Resolve exact image facts before activation; no image pull or build occurs here.
    for image in record['images'].values():
        image['image_id'] = backend.image_id(image['tag'])
    # A generated manifest also identifies partial startup for harness teardown.
    render_entry(context, entry)
    if config.get("shared_minio"):
        backend.compose(record, ['create'])
        record["object_store_container"] = backend.attach(config["shared_minio"], record)
        render_entry(context, entry)
    backend.compose(record, ['up', '-d'])
    deadline = time.monotonic() + min(120, budget.remaining_seconds(work=True) if budget else 120)
    while not backend.healthy(record) or (context['control_uri'] and not backend.http_ready(context['control_uri'] + '/health')):
        if budget:
            budget.check(work=True)
        if time.monotonic() > deadline:
            raise runtime.RuntimeFailure('ReadinessTimeout', project)
        time.sleep(0.1)
    record['containers'] = {name: backend.container(record, name)['Id'] for name in record['required_services']}
    # The mc client remains a completed container for identity checks and run --rm.
    mc = backend.container(record, 'mc')
    if not mc or mc.get('State', {}).get('Status') != 'exited' or mc.get('State', {}).get('ExitCode') != 0:
        raise runtime.RuntimeFailure('ReadinessTimeout', 'isolated mc did not complete successfully')
    record['containers']['mc'] = mc['Id']
    render_entry(context, entry)
    return {'published_dir': str(entry), 'private_record': record, 'producer_receipt': context['producer_receipt']}


def isolated_down(entry: Path, project: str, *, volumes: bool, purge: bool) -> dict:
    if not project.startswith('nr-isolated-rest-'):
        raise runtime.RuntimeFailure('InvalidIdentity', 'refusing a non-isolated Docker project')
    if volumes and (os.environ.get('NOVA_ENV_ALLOW_VOLUME_DELETE') != 'true' or os.environ.get('NOVA_ENV_EXPECTED_COMPOSE_PROJECT') != project or os.environ.get('NOVA_ENV_EXPECTED_MINIO_VOLUME') != project + '_minio-data'):
        raise runtime.RuntimeFailure('OwnerMismatch', 'isolated volume deletion needs exact project and volume confirmation')
    manifest_path = entry / 'manifest.json'
    if manifest_path.exists():
        manifest = runtime.read_json(manifest_path)
        if manifest['shared_docker'] or manifest['env_id'] != entry.name:
            raise runtime.RuntimeFailure('OwnerMismatch', 'isolated manifest belongs to another project')
        if manifest['ready']:
            if manifest['compose_project'] != project:
                raise runtime.RuntimeFailure('OwnerMismatch', 'isolated manifest belongs to another project')
            record = manifest['runtime']['catalog']
            backend = runtime.Docker(timeout=120)
            if record.get("external_minio"):
                from scratch import ScratchIdentity, RunBudget
                context = record["run_context"]
                budget = RunBudget.admit(record["external_minio"], ScratchIdentity.from_dict(context["identity"]),
                                         inherited=context, cleanup=True)
                backend.operation_deadline = budget.deadline
            backend.validate_resources(record)
            if record.get("external_minio"):
                identity = record.get("object_store_container") or record["external_minio"]["containers"]["minio"]
                info = backend.inspect("container", identity, absent_ok=True)
                if info:
                    expected = record["external_minio"]
                    if info.get("Image") != expected["images"]["minio"]["image_id"] or any(
                        info.get("Config", {}).get("Labels", {}).get(k) != v for k, v in machine.MachineDocker.labels(expected).items()
                    ):
                        raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "shared MinIO teardown identity changed")
                    network = backend.own_network(record)
                    if network and identity in network.get("Containers", {}):
                        backend.command(["network", "disconnect", record["network"], identity])
            backend.compose(record, ['down'] + (['-v'] if volumes else []))
            if record.get("external_minio"):
                if backend.command(["ps", "-a", "--filter", f"label=com.docker.compose.project={project}", "-q"]):
                    raise runtime.RuntimeFailure("ResourcesStillPresent", "private project containers remain")
                if backend.inspect("network", record["network"], absent_ok=True):
                    raise runtime.RuntimeFailure("ResourcesStillPresent", "private project network remains")
                if volumes:
                    for name in record["volumes"]:
                        if backend.inspect("volume", name, absent_ok=True):
                            raise runtime.RuntimeFailure("ResourcesStillPresent", "private project volume remains")
    if purge and entry.exists():
        shutil.rmtree(entry)
    return {'project': project, 'removed_entry': purge}


def previous_publication(entry):
    """Old owner publications are deliberately not read or treated as authority."""
    pointer = Path(entry) / 'published'
    if not pointer.is_symlink():
        return publication.read(Path(entry))
    target = pointer.resolve(strict=False)
    if target.parent != (Path(entry) / 'publications').resolve():
        raise runtime.RuntimeFailure('RuntimeIdentityMismatch', 'invalid publication pointer')
    if not (target / 'entry.json').exists():
        return None
    return publication.read(Path(entry))


def attach_entry(owner, env_id, entry, config, bom):
    entry = Path(entry).resolve()
    with runtime.file_lock(entry / '.entry.lock'):
        try:
            started = time.monotonic()
            deadline = started + owner.timeout
            inherited = None
            raw_context = os.environ.get('NOVA_FIXTURE_RUN_CONTEXT')
            if raw_context is not None:
                if len(raw_context) > 16384:
                    raise runtime.RuntimeFailure('RunBudgetRejected', 'inherited context exceeds its input bound')
                try:
                    inherited = json.loads(raw_context)
                except ValueError as error:
                    raise runtime.RuntimeFailure('RunBudgetRejected', 'invalid inherited run context JSON') from error
            if inherited is not None:
                from scratch import RunBudget, ScratchIdentity
                if not isinstance(inherited, dict) or 'identity' not in inherited:
                    raise runtime.RuntimeFailure('RunBudgetRejected', 'attach requires the original run context')
                saved = owner.read_saved_state()
                if not saved or saved['phase'] != 'ready':
                    raise runtime.RuntimeFailure('MachineUnavailable', 'shared fixture has no ready saved authority')
                identity = ScratchIdentity.from_dict(inherited['identity'])
                budget = RunBudget.admit(saved, identity, inherited=inherited,
                                         wall_clock=time.time, monotonic=time.monotonic)
                deadline = min(deadline, budget.deadline - budget.cleanup_grace_seconds)
            previous = getattr(owner.backend, 'operation_deadline', None)
            owner.backend.operation_deadline = min(previous, deadline) if previous is not None else deadline
            try:
                definition = owner.required_definition(owner.load_config(), bom)
                # Lock contention stays short; full validation retains the original work deadline.
                with owner.ready_snapshot(definition, deadline=deadline,
                                          lock_deadline=time.monotonic() + 2) as snapshot:
                    if inherited is not None:
                        RunBudget.admit(snapshot, identity, inherited=inherited,
                                        wall_clock=time.time, monotonic=time.monotonic).check(work=True)
                    if config['benchmark']['shared_root'].rstrip('/') != snapshot['config']['benchmark_root']:
                        raise runtime.RuntimeFailure('MachineVersionMismatch', 'worktree benchmark root differs from machine configuration')
                    return publication.publish(entry, {'worktree': env_id, 'ready': True,
                        'machine': snapshot, 'config': config, 'producer_receipt': snapshot['producer_receipt']}, render_entry)
            finally:
                owner.backend.operation_deadline = previous
        except runtime.RuntimeFailure:
            # Failed identity/health observation also invalidates discovery.
            # Pinned immutable publications stay readable for existing owners.
            unavailable = copy.deepcopy(config)
            unavailable['fixture_inputs']['verified'] = False
            publication.publish(entry, {'worktree': env_id, 'ready': False,
                'machine': None, 'config': unavailable, 'producer_receipt': None}, render_entry)
            raise


def prepare_entry(env_id, entry, config):
    # Offline preparation never labels saved facts as live readiness.
    config = copy.deepcopy(config)
    config['fixture_inputs']['verified'] = False
    with runtime.file_lock(Path(entry) / '.entry.lock'):
        return publication.publish(Path(entry), {'worktree': env_id, 'ready': False,
            'machine': None, 'config': config, 'producer_receipt': None}, render_entry)


def release_entry(env_id, entry, *, purge=False):
    with runtime.file_lock(Path(entry) / '.entry.lock'):
        previous = previous_publication(entry)
        if not previous:
            return {'ready': False, 'published_dir': None}
        if previous['worktree'] != env_id:
            raise runtime.RuntimeFailure('RuntimeIdentityMismatch', 'entry belongs to another worktree')
        result = publication.publish(Path(entry), {'worktree': env_id, 'ready': False,
            'machine': None, 'config': previous['config'], 'producer_receipt': None}, render_entry)
        # Another run may still consume any immutable input. Without consumer
        # leases, releasing this entry does not authorize deleting those files.
        if purge:
            result['retained_publications'] = True
        return result


def worktree_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['up', 'down', 'status'])
    parser.add_argument('--prepare-only', '--no-docker', action='store_true')
    parser.add_argument('--profile', choices=['stock', 'publication-hook'], default='stock')
    parser.add_argument('--hook-image')
    parser.add_argument('--runtime-only', action='store_true')
    parser.add_argument('--docker', action='store_true')
    parser.add_argument('--purge', action='store_true')
    parser.add_argument('--volumes', action='store_true')
    runtime.install_signal_handlers()
    args = parser.parse_args(argv)
    try:
        workspace = Path(os.environ.get('NOVAROCKS_WORKSPACE_ROOT', HERE.parent.parent)).resolve()
        config_file = Path(os.environ.get('NOVA_ENV_CONFIG_FILE', HERE / 'shared.env')).resolve()
        if args.operation == 'down' and not config_file.exists() and os.environ.get('NOVA_ENV_SHARED_DOCKER') == 'false':
            # Teardown retains exact caller ownership even after its temporary workspace vanished.
            settings = {'NOVA_ENV_SHARED_DOCKER': 'false', 'NOVA_ENV_COMPOSE_PROJECT': os.environ['NOVA_ENV_COMPOSE_PROJECT']}
        else:
            settings = load_settings(config_file)
        shared_value = settings.get('NOVA_ENV_SHARED_DOCKER', 'true')
        if shared_value not in ('true', 'false'):
            raise runtime.RuntimeFailure('InvalidConfiguration', 'NOVA_ENV_SHARED_DOCKER')
        shared = shared_value == 'true'
        update_value = os.environ.get('NOVA_ENV_UPDATE_CURRENT', 'true')
        if update_value not in ('true', 'false'):
            raise runtime.RuntimeFailure('InvalidConfiguration', 'NOVA_ENV_UPDATE_CURRENT')
        if not shared and update_value != 'false':
            raise runtime.RuntimeFailure('InvalidConfiguration', 'isolated requests must set NOVA_ENV_UPDATE_CURRENT=false')
        env_id = environment_identity(workspace)
        if not shared and args.operation != 'up' and os.environ.get('NOVA_ENV_ID'):
            env_id = runtime.safe_component(os.environ['NOVA_ENV_ID'])
            if env_id in ('.', '..'):
                raise runtime.RuntimeFailure('InvalidIdentity', env_id)
        # A shared publication and its W lock belong to the consumer worktree,
        # regardless of which checkout supplies this request's declarations.
        fixture_root = workspace / 'docker' / 'iceberg-rest' if shared else HERE
        entry = fixture_root / 'runtime' / env_id
        if args.operation == 'status':
            result = previous_publication(entry) if shared else (runtime.read_json(entry / 'manifest.json') if (entry / 'manifest.json').exists() else None)
        elif args.operation == 'down':
            if shared:
                if args.docker or args.volumes:
                    raise runtime.RuntimeFailure('ExplicitRuntimeManagementRequired', 'shared services are maintained only by fixture-runtime.sh')
                result = release_entry(env_id, entry, purge=args.purge)
            else:
                project = settings['NOVA_ENV_COMPOSE_PROJECT']
                result = isolated_down(entry, project, volumes=args.volumes or args.purge, purge=args.purge)
        else:
            if not workspace.is_dir():
                raise runtime.RuntimeFailure('InvalidConfiguration', 'workspace directory does not exist')
            config = request_config(workspace, settings, config_file, entry, shared=shared, update_current=update_value == 'true')
            if args.prepare_only:
                if shared:
                    result = prepare_entry(env_id, entry, config)
                else:
                    entry.mkdir(parents=True, exist_ok=True)
                    render_entry({'ready': False, 'worktree': env_id, 'config': config, 'publication_dir': str(entry), 'stable_runtime_dir': str(entry)}, entry)
                    result = {'ready': False, 'published_dir': str(entry)}
            else:
                config['fixture_inputs'] = {'bom': str(Path(os.environ.get('NOVA_FIXTURE_STORE', str(Path(os.environ.get('XDG_CACHE_HOME', str(Path.home() / '.cache'))) / 'novarocks/fixture-inputs'))).resolve() / 'bom.json'), 'lock_sha256': None, 'verified': False}
                try:
                    bom = verify_inputs(config)
                except runtime.RuntimeFailure:
                    if shared:
                        prepare_entry(env_id, entry, config)
                    raise
                if shared:
                    if args.profile != 'stock' or args.hook_image:
                        raise runtime.RuntimeFailure('InvalidConfiguration', 'hook profile requires an isolated request')
                    owner = machine.MachineOwner(os.environ.get("NOVA_FIXTURE_MACHINE_ROOT"))
                    result = attach_entry(owner, env_id, entry, config, bom)
                else:
                    result = isolated_start(config, settings['NOVA_ENV_COMPOSE_PROJECT'], entry, args.profile, args.hook_image, bom)
        if result is None:
            output = None
        else:
            output = {key: result[key] for key in ('ready', 'published_dir', 'project', 'removed_entry') if key in result}
            if result.get('machine'):
                output['machine'] = {key: result['machine'][key] for key in ('id', 'daemon_id', 'version', 'generation', 'phase')}
        print(json.dumps(output, sort_keys=True))
        return 0
    except runtime.RuntimeFailure as error:
        print(str(error), file=sys.stderr)
        return 75 if error.code in machine.BLOCKED else 1
    except (KeyError, OSError, ValueError) as error:
        print(f'InvalidConfiguration: {error}', file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'compose':
        os.execvpe('docker', ['docker', 'compose', *sys.argv[2:]], runtime.controlled_environment())
    raise SystemExit(worktree_main())
