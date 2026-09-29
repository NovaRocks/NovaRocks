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

import fixture_runtime as runtime

HERE = Path(__file__).resolve().parent
FILES = runtime.ENTRY_FILES
LOCAL_RANGES = {"mysql": 9030, "fe_grpc": 9080, "be_grpc": 9280, "fe_http": 8240, "be_http": 8440}
MANAGED = """NOVA_ENV_CONFIG_FILE NOVA_ENV_ID NOVA_ENV_SHARED_DOCKER NOVA_ENV_COMPOSE_PROJECT
NOVA_ENV_RUNTIME_DIR NOVA_ENV_CURRENT_DIR NOVA_ENV_REST_ENV_FILE NOVA_ENV_MANIFEST NOVA_ENV_README
NOVA_ENV_COMPOSE_FILE NOVA_ENV_COMPOSE_ENV NOVA_ENV_MINIO_PORT NOVA_ENV_MINIO_CONSOLE_PORT
NOVA_ENV_REST_PORT NOVA_ENV_SPARK_UI_PORT NOVA_ENV_MYSQL_PORT NOVA_ENV_FE_GRPC_PORT
NOVA_ENV_FE_HTTP_PORT NOVA_ENV_BE_GRPC_PORT NOVA_ENV_BE_HTTP_PORT NOVAROCKS_NATIVE_SHARED_SECRET
AWS_S3_ENDPOINT AWS_S3_ACCESS_KEY_ID AWS_S3_SECRET_ACCESS_KEY MINIO_ROOT_USER MINIO_ROOT_PASSWORD
iceberg_object_store_credential_name iceberg_object_store_credential_generation CATALOG_WAREHOUSE_URI
NOVAROCKS_ICEBERG_TEST_WAREHOUSE NOVAROCKS_ICEBERG_REST_URI NOVA_ENV_REST_SERVER_WAREHOUSE_URI
NOVA_ENV_REST_WAREHOUSE_URI NOVAROCKS_ICEBERG_REST_WAREHOUSE NOVA_ENV_SHARED_BENCHMARK_ROOT
NOVA_ENV_BENCHMARK_BUILD_TIMEOUT_SECONDS NOVAROCKS_FE_CONFIG NOVAROCKS_BE_CONFIG
NOVAROCKS_STATE_STORE_PATH NOVAROCKS_SQL_TEST_CONFIG NOVAROCKS_ICE_REST_CATALOG_SQL
NOVAROCKS_SPARK_IMAGE NOVA_ENV_SPARK_VERSION NOVA_ENV_ICEBERG_VERSION NOVAROCKS_SPARK_UI
NOVAROCKS_SPARK_REST_URI NOVAROCKS_SPARK_S3_ENDPOINT NOVAROCKS_SPARK_DEFAULTS
NOVAROCKS_SPARK_V3_SMOKE_SQL NOVAROCKS_SPARK_SQL NOVA_FIXTURE_INPUT_BOM NOVA_FIXTURE_INPUT_LOCK_SHA256
NOVA_ENV_OBJECT_STORE_RUNTIME NOVA_ENV_CATALOG_RUNTIME NOVA_ENV_OBJECT_STORE_CONTAINER
NOVA_ENV_SHARED_COMPOSE_PROJECT NOVA_ENV_SHARED_REST_WAREHOUSE_URI NOVA_ENV_PUBLICATION_HOOK_CONTROL_URI""".split()


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


def render_entry(context: dict[str, Any], staging_dir: Path) -> list[str]:
    """Write text only. Paths in text name the final publication, never staging."""
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
                    "owner_locator": context.get("owner_locator"), "producer_receipt": context.get("producer_receipt")},
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
        os_record, cat = context["records"]["object_store"], context["records"]["catalog"]
        endpoint = context["endpoints"]
        credentials = os_record["config"]["credentials"]
        ak, sk = credentials["access_key"], credentials["secret_key"]
        warehouse = config["warehouses"]
        ports, trust = config["local_ports"], config["native_trust"]
        rest = warehouse["rest_client"]
        versions = config["versions"]
        compose = {"compose_project": cat["project"], "compose_file": cat["compose_file"], "compose_env": cat["compose_env"]}
        manifest.update(compose)
        manifest["runtime"].update(object_store=copy.deepcopy(os_record), catalog=copy.deepcopy(cat))
        manifest["minio"] = {"endpoint": endpoint["minio_endpoint"], "console": endpoint["minio_console"],
                              "access_key_id": ak, "secret_access_key": sk, "volume": os_record["volumes"][0]}
        manifest["iceberg_rest"] = {"uri": endpoint["rest_uri"], "warehouse": rest, "server_default_warehouse": cat["server_warehouse"]}
        manifest["spark"] = {"image": cat["images"]["spark"]["tag"], "spark_version": versions["spark"], "iceberg_version": versions["iceberg"],
            "ui": endpoint["spark_ui"], "container_rest_uri": endpoint["container_rest_uri"], "container_minio_endpoint": endpoint["container_minio_endpoint"],
            "defaults_file": str(publication / "spark-defaults.conf"), "v3_smoke_sql": str(publication / "spark-iceberg-v3-smoke.sql"), "helper": str(repo / "docker/iceberg-rest/spark-sql.sh")}
        manifest["novarocks"] = {**{name + "_port": value for name, value in ports.items()},
            "native_trust_deployment_id": trust["deployment_id"], "fe_config": str(publication / "fe.toml"),
            "be_config": str(publication / "be.toml"), "state_store_path": str(stable / "frontend-state.sqlite"),
            "sql_test_config": str(publication / "sql-test.toml"), "ice_rest_catalog_sql": str(publication / "ice-rest-catalog.sql"),
            "iceberg_catalog_warehouse": warehouse["catalog"], "iceberg_test_warehouse": warehouse["test"]}
        exports.update({
            "NOVA_ENV_COMPOSE_PROJECT": cat["project"], "NOVA_ENV_COMPOSE_FILE": cat["compose_file"], "NOVA_ENV_COMPOSE_ENV": cat["compose_env"],
            "NOVA_ENV_MINIO_PORT": os_record["ports"]["minio"], "NOVA_ENV_MINIO_CONSOLE_PORT": os_record["ports"]["minio_console"],
            "NOVA_ENV_REST_PORT": cat["ports"]["rest"], "NOVA_ENV_SPARK_UI_PORT": cat["ports"]["spark"],
            "NOVA_ENV_OBJECT_STORE_RUNTIME": os_record["id"], "NOVA_ENV_CATALOG_RUNTIME": cat["id"],
            "NOVA_ENV_OBJECT_STORE_CONTAINER": os_record.get("containers", {}).get("minio", ""),
            "NOVAROCKS_NATIVE_SHARED_SECRET": trust["shared_secret"],
            "AWS_S3_ENDPOINT": endpoint["minio_endpoint"], "AWS_S3_ACCESS_KEY_ID": ak, "AWS_S3_SECRET_ACCESS_KEY": sk,
            "MINIO_ROOT_USER": ak, "MINIO_ROOT_PASSWORD": sk,
            "iceberg_object_store_credential_name": "iceberg-test-data", "iceberg_object_store_credential_generation": "v1",
            "CATALOG_WAREHOUSE_URI": warehouse["catalog"], "NOVAROCKS_ICEBERG_TEST_WAREHOUSE": warehouse["test"],
            "NOVAROCKS_ICEBERG_REST_URI": endpoint["rest_uri"], "NOVA_ENV_REST_SERVER_WAREHOUSE_URI": cat["server_warehouse"],
            "NOVA_ENV_REST_WAREHOUSE_URI": rest, "NOVAROCKS_ICEBERG_REST_WAREHOUSE": rest,
            "NOVAROCKS_FE_CONFIG": str(publication / "fe.toml"), "NOVAROCKS_BE_CONFIG": str(publication / "be.toml"),
            "NOVAROCKS_STATE_STORE_PATH": str(stable / "frontend-state.sqlite"), "NOVAROCKS_SQL_TEST_CONFIG": str(publication / "sql-test.toml"),
            "NOVAROCKS_ICE_REST_CATALOG_SQL": str(publication / "ice-rest-catalog.sql"), "NOVAROCKS_SPARK_IMAGE": cat["images"]["spark"]["tag"],
            "NOVA_ENV_SPARK_VERSION": versions["spark"], "NOVA_ENV_ICEBERG_VERSION": versions["iceberg"],
            "NOVAROCKS_SPARK_UI": endpoint["spark_ui"], "NOVAROCKS_SPARK_REST_URI": endpoint["container_rest_uri"],
            "NOVAROCKS_SPARK_S3_ENDPOINT": endpoint["container_minio_endpoint"], "NOVAROCKS_SPARK_DEFAULTS": str(publication / "spark-defaults.conf"),
            "NOVAROCKS_SPARK_V3_SMOKE_SQL": str(publication / "spark-iceberg-v3-smoke.sql"), "NOVAROCKS_SPARK_SQL": manifest["spark"]["helper"],
            "NOVA_FIXTURE_INPUT_BOM": config["fixture_inputs"]["bom"], "NOVA_FIXTURE_INPUT_LOCK_SHA256": config["fixture_inputs"]["lock_sha256"],
        })
        exports.update({"NOVA_ENV_" + name.upper() + "_PORT": value for name, value in ports.items()})
        if context.get("control_uri"):
            exports["NOVA_ENV_PUBLICATION_HOOK_CONTROL_URI"] = context["control_uri"]
        render_role_configs(staging, stable, env_id, ports, trust)
        values = {"url": "http://127.0.0.1:8030", "oss_ak": ak, "oss_sk": sk, "oss_endpoint": endpoint["minio_endpoint"],
                  "iceberg_catalog_type": "hadoop", "iceberg_catalog_warehouse": warehouse["catalog"], "iceberg_test_warehouse": warehouse["test"],
                  "iceberg_rest_uri": endpoint["rest_uri"], "iceberg_rest_warehouse": rest, "iceberg_object_store_credential_name": "iceberg-test-data",
                  "iceberg_object_store_credential_generation": "v1", "benchmark_shared_root": config["benchmark"]["shared_root"], "fixture_env_file": str(publication / "env.sh")}
        text_file(staging, "sql-test.toml", '[cluster]\nhost = "127.0.0.1"\nport = ' + json_string(ports["mysql"]) + '\nuser = "root"\npassword = ""\n\n[env]\n' + ''.join(f'{key} = {json_string(value)}\n' for key, value in values.items()))
        sql_values = {"type": "iceberg", "iceberg.catalog.type": "rest", "uri": endpoint["rest_uri"], "warehouse": rest,
                      "aws.s3.endpoint": endpoint["minio_endpoint"],
                      "aws.s3.region": "us-east-1", "aws.s3.enable_path_style_access": "true"}
        for purpose, role in (("metadata", "frontend"), ("data", "backend")):
            prefix = f"credential.object-store-{purpose}."
            sql_values.update({prefix + "consumer-role": role, prefix + "mode": "static",
                               prefix + "name": "iceberg-test-data", prefix + "generation": "v1"})
        sql_quote = lambda value: "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"
        text_file(staging, "ice-rest-catalog.sql", "CREATE EXTERNAL CATALOG ice_rest PROPERTIES (\n" + ',\n'.join(f'  {sql_quote(k)} = {sql_quote(v)}' for k, v in sql_values.items()) + "\n);\n")
        spark = {"spark.master": "local[*]", "spark.app.name": "NovaRocksIcebergSpark", "spark.ui.bindAddress": "0.0.0.0", "spark.driver.bindAddress": "0.0.0.0",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions", "spark.sql.catalog.ice_rest": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.ice_rest.type": "rest", "spark.sql.catalog.ice_rest.uri": endpoint["container_rest_uri"], "spark.sql.catalog.ice_rest.warehouse": rest,
            "spark.sql.catalog.ice_rest.io-impl": "org.apache.iceberg.aws.s3.S3FileIO", "spark.sql.catalog.ice_rest.s3.endpoint": endpoint["container_minio_endpoint"],
            "spark.sql.catalog.ice_rest.s3.path-style-access": "true", "spark.sql.catalog.ice_rest.s3.access-key-id": ak, "spark.sql.catalog.ice_rest.s3.secret-access-key": sk,
            "spark.sql.catalog.ice_rest.s3.region": "us-east-1", "spark.sql.defaultCatalog": "ice_rest", "spark.hadoop.fs.s3a.endpoint": endpoint["container_minio_endpoint"],
            "spark.hadoop.fs.s3a.access.key": ak, "spark.hadoop.fs.s3a.secret.key": sk, "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false", "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"}
        text_file(staging, "spark-defaults.conf", ''.join(f'{key} {value}\n' for key, value in spark.items()))
        text_file(staging, "spark-iceberg-v3-smoke.sql", "CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_v3;\n\nDROP TABLE IF EXISTS ice_rest.nr_v3.spark_v3_smoke;\n\nCREATE TABLE ice_rest.nr_v3.spark_v3_smoke (\n  id BIGINT,\n  data STRING,\n  category STRING,\n  ts TIMESTAMP\n) USING iceberg\nTBLPROPERTIES (\n  'format-version' = '3',\n  'write.row-lineage' = 'true',\n  'write.format.default' = 'parquet'\n);\n\nINSERT INTO ice_rest.nr_v3.spark_v3_smoke VALUES\n  (1, 'spark-v3-a', 'alpha', TIMESTAMP '2026-05-07 00:00:00'),\n  (2, 'spark-v3-b', 'beta', TIMESTAMP '2026-05-07 00:01:00'),\n  (3, 'spark-v3-c', 'alpha', TIMESTAMP '2026-05-07 00:02:00');\n\nSELECT * FROM ice_rest.nr_v3.spark_v3_smoke ORDER BY id;\n")
    text_file(staging, "env.sh", '# Generated fixture publication.\nunset ' + ' '.join(MANAGED) + '\n' + ''.join(f'export {key}={shlex.quote(str(value))}\n' for key, value in exports.items()))
    text_file(staging, "manifest.json", json.dumps(manifest, indent=2) + '\n')
    text_file(staging, "README.md", f'# NovaRocks fixture {env_id}\n\nReady: {str(ready).lower()}\n\nPublication: `{publication}`\n\nStable runtime data: `{stable}`\n\n' + (f'REST: {manifest["iceberg_rest"]["uri"]}\n\nObject store: {manifest["minio"]["endpoint"]}\n' if ready else 'Run `docker/iceberg-rest/up.sh` to bind this worktree.\n'))
    return list(FILES)


def render_role_configs(staging: Path, stable: Path, env_id: str, ports: dict, trust: dict) -> None:
    for role in ("fe", "be"):
        value = f'''[native_trust]
deployment_id = {json_string(trust["deployment_id"])}
shared_secret = "${{ENV:NOVAROCKS_NATIVE_SHARED_SECRET}}"

[server]
host = "127.0.0.1"
http_port = {ports[role + "_http"]}
grpc_port = {ports[role + "_grpc"]}

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
        purpose = "metadata" if role == "fe" else "data"
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
    text = templates["os"]["header"] + templates["os"]["x-owner-labels"] + "services:\n" + ''.join(services.values())
    text += "volumes:\n" + ''.join(blocks(templates["os"]["volumes"]).values()) + ''.join(blocks(templates["cat"]["volumes"]).values()) + templates["os"]["networks"]
    images = {"minio": bom["images"]["minio"]["alias"], "mc": bom["images"]["minio-mc"]["alias"],
              "rest": hook_image if profile == "publication-hook" else bom["images"]["iceberg-rest"]["alias"],
              "spark": bom["derived_images"]["iceberg-spark"]["image_id"]}
    images["mc-init"] = images["mc"]
    for key in ("minio", "minio_console", "rest", "spark"):
        if not 0 < int(ports[key]) < 65536:
            raise runtime.RuntimeFailure("PortUnavailable", key)
    key = runtime.digest({"project": project, "templates": text, "images": images, "profile": profile})
    credentials = config["credentials"]
    values = {"NOVA_FIXTURE_OWNER": project, "NOVA_FIXTURE_KEY": key, "NOVA_FIXTURE_KIND": "isolated", "NOVA_FIXTURE_PROJECT": project,
              "MINIO_ROOT_USER": credentials["access_key"], "MINIO_ROOT_PASSWORD": credentials["secret_key"],
              "NOVA_ENV_REST_SERVER_WAREHOUSE_URI": config["warehouses"]["rest_client"]}
    values.update({service.upper().replace('-', '_') + '_IMAGE': image for service, image in images.items()})
    values.update({'NOVA_ENV_' + name.upper() + '_PORT': value for name, value in ports.items() if name != 'control'})
    if profile == "publication-hook":
        values["NOVA_ENV_PUBLICATION_HOOK_CONTROL_PORT"] = ports["control"]
    text_file(out, "compose.yml", text)
    text_file(out, "compose.env", ''.join(f"{name}='{str(value).replace(chr(39), chr(92) + chr(39))}'\n" for name, value in sorted(values.items())))
    record = {"id": project, "key": key, "kind": "isolated", "namespace": project, "project": project,
        "network": project + "_iceberg_net", "ports": ports, "compose_file": str(out / "compose.yml"), "compose_env": str(out / "compose.env"),
        "images": {name: {"tag": image, "image_id": image} for name, image in images.items()}, "config": {"credentials": credentials},
        "volumes": [project + "_minio-data", project + "_rest-catalog"], "required_services": ["minio", "mc-init", "rest", "spark"],
        "service_ports": {"minio": {"9000/tcp": ports["minio"], "9001/tcp": ports["minio_console"]}, "rest": {"8181/tcp": ports["rest"]}, "spark": {"4040/tcp": ports["spark"]}},
        "health_urls": [f'http://127.0.0.1:{ports["minio"]}/minio/health/live', f'http://127.0.0.1:{ports["rest"]}/v1/config'],
        "server_warehouse": config["warehouses"]["rest_client"]}
    if profile == 'publication-hook':
        record['service_ports']['rest']['8182/tcp'] = ports['control']
    context = {"ready": True, "binding": None, "owner_locator": None, "worktree": config["env_id"], "env_id": config["env_id"],
        "config": config, "publication_dir": str(out), "stable_runtime_dir": str(out), "entry_root": str(out),
        "records": {"object_store": record, "catalog": record}, "endpoints": runtime.RuntimeOwner.endpoints({"object_store": record, "catalog": record}),
        "producer_receipt": {"lock_sha256": bom["lock_sha256"], **bom["derived_images"]["iceberg-spark"]},
        "profile": profile, "control_uri": f'http://127.0.0.1:{ports["control"]}' if profile == "publication-hook" else None,
        "template_model_sha256": model_hash(Path(config["repo_root"]))}
    return {"record": record, "context": context, "compose_file": record["compose_file"], "compose_env": record["compose_env"]}


def load_settings(path: Path) -> dict[str, str]:
    allowed = {"NOVA_ENV_SHARED_DOCKER", "NOVA_ENV_COMPOSE_PROJECT", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "NOVA_ENV_SPARK_VERSION", "NOVA_ENV_ICEBERG_VERSION", "NOVA_ENV_SHARED_BENCHMARK_ROOT", "NOVA_ENV_BENCHMARK_BUILD_TIMEOUT_SECONDS", "NOVA_ENV_RUNTIME_PORT_START", "NOVA_ENV_RUNTIME_PORT_END"}
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


def choose_ports(names: dict[str, int], offset: int, ranges: dict[str, int] | None = None) -> dict[str, int]:
    result = {}
    for name, base in names.items():
        count = (ranges or {}).get(name, 199) + 1
        for index in range(count):
            port = base + (offset + index) % count
            if port not in result.values() and port_available(port):
                result[name] = port
                break
        else:
            raise runtime.RuntimeFailure('PortUnavailable', name)
    return result


def request_config(workspace: Path, settings: dict, config_file: Path, entry: Path, *, shared: bool, update_current: bool) -> dict:
    previous = runtime.current_publication(entry) if shared else None
    old_config = previous.get('config', {}) if previous else {}
    env_id = entry.name
    offset = int(hashlib.sha1(str(workspace).encode()).hexdigest()[:8], 16)
    starts = {name: int(settings.get('NOVA_ENV_' + name.upper() + '_PORT_START', base)) for name, base in LOCAL_RANGES.items()}
    ranges = {name: int(settings.get('NOVA_ENV_' + name.upper() + '_PORT_RANGE', '199')) for name in LOCAL_RANGES}
    ports = old_config.get('local_ports') or choose_ports(starts, offset, ranges)
    benchmark_root = settings.get('NOVA_ENV_SHARED_BENCHMARK_ROOT', 's3://novarocks/shared/benchmarks')
    if not re.fullmatch(r's3://[^/\s]+/[^\s]+', benchmark_root):
        raise runtime.RuntimeFailure('InvalidConfiguration', 'benchmark root requires s3://bucket/prefix')
    timeout = int(settings.get('NOVA_ENV_BENCHMARK_BUILD_TIMEOUT_SECONDS', '3600'))
    if timeout < 1:
        raise runtime.RuntimeFailure('InvalidConfiguration', 'benchmark timeout must be positive')
    store = Path(os.environ.get('NOVA_FIXTURE_STORE', str(Path(os.environ.get('XDG_CACHE_HOME', str(Path.home() / '.cache'))) / 'novarocks/fixture-inputs'))).resolve()
    return {'workspace_root': str(workspace), 'repo_root': str(HERE.parent.parent), 'config_file': str(config_file),
            'env_id': env_id, 'shared_docker': shared, 'current_link': str(entry.parent / 'current'), 'update_current': update_current,
            'credentials': {'access_key': settings['MINIO_ROOT_USER'], 'secret_key': settings['MINIO_ROOT_PASSWORD']}, 'buckets': ['warehouse', 'novarocks'],
            'local_ports': ports, 'native_trust': old_config.get('native_trust') or {'deployment_id': env_id, 'shared_secret': 'local-native-trust-fixture-' + hashlib.sha256(str(workspace).encode()).hexdigest()},
            'benchmark': {'shared_root': benchmark_root, 'build_timeout_seconds': timeout},
            'versions': {'spark': settings.get('NOVA_ENV_SPARK_VERSION', '3.5.5-java17'), 'iceberg': settings.get('NOVA_ENV_ICEBERG_VERSION', '1.11.0')},
            'fixture_inputs': old_config.get('fixture_inputs') or {'bom': str(store / 'bom.json'), 'lock_sha256': None, 'verified': False},
            'warehouses': {'catalog': f's3://novarocks/{env_id}/iceberg-catalog', 'test': f's3://novarocks/{env_id}/novarocks-sql-test-iceberg-extra', 'rest_client': f's3://warehouse/{env_id}/rest'}}


def verify_inputs(config: dict) -> dict:
    command = [str(HERE.parent / 'fixture-inputs/verify.sh'), '--store', str(Path(config['fixture_inputs']['bom']).parent), '--repo-root', config['repo_root'], '--consumer', 'iceberg-rest']
    outcome = subprocess.run(command, env=runtime.controlled_environment(), stdout=sys.stderr, check=False)
    if outcome.returncode:
        raise runtime.RuntimeFailure('FixturePrerequisiteMissing', 'run fixture input provision explicitly before up')
    bom = runtime.read_json(Path(config['fixture_inputs']['bom']))
    config['fixture_inputs'].update(lock_sha256=bom['lock_sha256'], verified=True, consumer='iceberg-rest')
    return bom


def isolated_start(config: dict, project: str, entry: Path, profile: str, hook_image: str | None, bom: dict) -> dict:
    offset = int(hashlib.sha1(config['env_id'].encode()).hexdigest()[:8], 16)
    ports = choose_ports({'minio': 19000, 'minio_console': 20000, 'rest': 21000, 'spark': 22000}, offset)
    if profile == 'publication-hook':
        ports['control'] = int(os.environ.get('NOVA_ENV_PUBLICATION_HOOK_CONTROL_PORT', '0'))
    result = render_isolated_stack(bom, project, ports, entry, config, profile=profile, hook_image=hook_image)
    record, context = result['record'], result['context']
    backend = runtime.Docker(timeout=120)
    # Resolve exact image facts before activation; no image pull or build occurs here.
    for image in record['images'].values():
        image['image_id'] = backend.image_id(image['tag'])
    # A generated manifest also identifies partial startup for harness teardown.
    render_entry(context, entry)
    backend.compose(record, ['up', '-d'])
    deadline = time.monotonic() + 120
    while not backend.healthy(record) or (context['control_uri'] and not backend.http_ready(context['control_uri'] + '/health')):
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
    return {'published_dir': str(entry), 'records': context['records'], 'binding': None, 'producer_receipt': context['producer_receipt']}


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
            backend.validate_resources(record)
            backend.compose(record, ['down'] + (['-v'] if volumes else []))
    if purge and entry.exists():
        shutil.rmtree(entry)
    return {'project': project, 'removed_entry': purge}


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
        owner = runtime.RuntimeOwner(port_start=int(os.environ.get('NOVA_ENV_RUNTIME_PORT_START', settings.get('NOVA_ENV_RUNTIME_PORT_START', '28000'))),
                                     port_end=int(os.environ.get('NOVA_ENV_RUNTIME_PORT_END', settings.get('NOVA_ENV_RUNTIME_PORT_END', '28999'))))
        if args.operation == 'status':
            result = runtime.current_publication(entry) if shared else (runtime.read_json(entry / 'manifest.json') if (entry / 'manifest.json').exists() else None)
        elif args.operation == 'down':
            if shared:
                if args.docker or args.volumes:
                    raise runtime.RuntimeFailure('ExplicitRuntimeManagementRequired', 'use fixture-runtime.sh stop/delete <exact-runtime-id>')
                result = owner.unbind(env_id, entry, purge=args.purge)
            else:
                project = settings['NOVA_ENV_COMPOSE_PROJECT']
                result = isolated_down(entry, project, volumes=args.volumes or args.purge, purge=args.purge)
        else:
            if not workspace.is_dir():
                raise runtime.RuntimeFailure('InvalidConfiguration', 'workspace directory does not exist')
            config = request_config(workspace, settings, config_file, entry, shared=shared, update_current=update_value == 'true')
            if args.prepare_only:
                if shared:
                    result = owner.prepare_entry(env_id, entry, config)
                else:
                    entry.mkdir(parents=True, exist_ok=True)
                    render_entry({'ready': False, 'worktree': env_id, 'config': config, 'records': {}, 'publication_dir': str(entry), 'stable_runtime_dir': str(entry)}, entry)
                    result = {'ready': False, 'published_dir': str(entry)}
            else:
                config['fixture_inputs'] = {'bom': str(Path(os.environ.get('NOVA_FIXTURE_STORE', str(Path(os.environ.get('XDG_CACHE_HOME', str(Path.home() / '.cache'))) / 'novarocks/fixture-inputs'))).resolve() / 'bom.json'), 'lock_sha256': None, 'verified': False}
                bom = verify_inputs(config)
                if shared:
                    if args.profile != 'stock' or args.hook_image:
                        raise runtime.RuntimeFailure('InvalidConfiguration', 'hook profile requires an isolated request')
                    if os.environ.get('NOVA_FIXTURE_CATALOG_TEMPLATE'):
                        owner.templates = {'os': HERE / 'templates/object-store.yml', 'cat': Path(os.environ['NOVA_FIXTURE_CATALOG_TEMPLATE']).resolve()}
                    result = owner.bind(env_id, entry, config, bom)
                else:
                    result = isolated_start(config, settings['NOVA_ENV_COMPOSE_PROJECT'], entry, args.profile, args.hook_image, bom)
        print(json.dumps(result, sort_keys=True))
        return 0
    except runtime.RuntimeFailure as error:
        print(str(error), file=sys.stderr)
        return 75 if error.code == 'FixturePrerequisiteMissing' else 1
    except (KeyError, OSError, ValueError) as error:
        print(f'InvalidConfiguration: {error}', file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'compose':
        os.execvpe('docker', ['docker', 'compose', *sys.argv[2:]], runtime.controlled_environment())
    raise SystemExit(worktree_main())
