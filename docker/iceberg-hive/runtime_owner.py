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

"""Versioned local fixture ownership, durable references and atomic publication."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import socket
import sys
import time
import uuid
from xml.sax.saxutils import escape

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "iceberg-rest"))
import fixture_runtime as runtime


class HiveOwner:
    """HMS owns its project; the fixture owner owns only the catalog attachment."""

    def __init__(self, owner):
        self.owner = owner
        self.backend = owner.backend

    def directory(self, catalog):
        return self.owner.base / "hms" / runtime.safe_component(catalog)

    def read(self, catalog):
        path = self.directory(catalog) / "manifest.json"
        if not path.exists():
            return None
        record = runtime.read_json(path)
        if (record.get("catalog_id") != catalog or record.get("owner_locator") != self.owner.locator
                or record.get("project") != f"nr-hms-{self.owner.namespace}-{catalog}"):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "HMS record owner differs")
        return record

    def save(self, record):
        runtime.atomic_json(self.directory(record["catalog_id"]) / "manifest.json", record)

    def prepare(self, manifest, image):
        catalog = manifest["runtime"]["catalog"]
        identity = catalog["id"]
        directory = self.directory(identity)
        existing = self.read(identity)
        if existing:
            return existing
        # This lock protects HMS reservations across catalog generations. It is
        # never held while acquiring a fixture catalog/object-store lock.
        with self.owner.lock("ports"):
            used = {runtime.read_json(path)["ports"]["hms"] for path in
                    (self.owner.base / "hms").glob("*/manifest.json")}
            used.update(port for item in self.owner.records() for port in item["ports"].values())
            port = None
            for candidate in range(self.owner.port_start, self.owner.port_end + 1):
                if candidate in used:
                    continue
                with socket.socket() as probe:
                    try:
                        probe.bind(("127.0.0.1", candidate))
                    except OSError:
                        continue
                port = candidate
                break
            if port is None:
                raise runtime.RuntimeFailure("PortUnavailable", "HMS port range exhausted")
            project = f"nr-hms-{self.owner.namespace}-{identity}"
            warehouse = catalog["server_warehouse"].rstrip("/") + "/hms"
            record = {
                "schema": 1, "kind": "hms", "key": identity, "catalog_id": identity,
                "namespace": self.owner.namespace, "owner_locator": self.owner.locator,
                "project": project, "network": project + "_default",
                "rest_network": catalog["network"], "ports": {"hms": port},
                "compose_file": str(directory / "compose.yml"), "compose_env": str(directory / "compose.env"),
                "volumes": [project + "_metastore"], "state": "prepared", "container_id": None,
                "images": {"hms": {"image_id": None, "tag": image}},
                "service_ports": {"hms": {"9083/tcp": port}},
                "hms": {"uri": f"thrift://127.0.0.1:{port}", "port": port, "warehouse": warehouse,
                        "catalog_sql": str(directory / "ice-hms-catalog.sql"),
                        "spark_defaults": str(directory / "spark-hms-defaults.conf"), "image": image},
                "minio": manifest["minio"],
            }
            directory.mkdir(parents=True, exist_ok=True)
            self.render(record)
            self.save(record)
        return record

    @staticmethod
    def write_definition(path, content, *, pin_image=False):
        if path.exists():
            if path.read_bytes() == content:
                return
            if not pin_image:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "saved HMS definition changed: " + path.name)
        runtime.atomic_bytes(path, content)

    def render(self, record, *, pin_image=False):
        directory = self.directory(record["catalog_id"])
        credentials = record["minio"]
        user, secret = credentials["access_key_id"], credentials["secret_access_key"]
        if any("\n" in value or "\r" in value for value in (user, secret)):
            raise runtime.RuntimeFailure("InvalidConfig", "multiline HMS credentials")
        env = {
            "HMS_IMAGE": record["images"]["hms"]["image_id"] or record["hms"]["image"],
            "NOVA_ENV_HMS_PORT": record["ports"]["hms"], "NOVA_HMS_PROJECT": record["project"],
            "NOVA_FIXTURE_OWNER": record["namespace"], "NOVA_FIXTURE_KEY": record["key"],
            "NOVA_HMS_CONFIG": str(directory / "core-site.xml"),
            "NOVA_HMS_WAREHOUSE": record["hms"]["warehouse"].replace("s3://", "s3a://", 1),
        }
        self.write_definition(directory / "compose.yml", (HERE / "compose.yml").read_bytes())
        self.write_definition(directory / "compose.env", "".join(f"{key}={json.dumps(str(value))}\n" for key, value in env.items()).encode(), pin_image=pin_image)
        core = (HERE / "core-site.xml").read_text().replace("<value>admin</value>", f"<value>{escape(user)}</value>").replace("<value>admin123</value>", f"<value>{escape(secret)}</value>")
        self.write_definition(directory / "core-site.xml", core.encode())
        # This read-only bind mount must be readable by the image's hive UID.
        (directory / "core-site.xml").chmod(0o644)
        props = {"type": "iceberg", "iceberg.catalog.type": "hive",
                 "iceberg.catalog.hive.metastore.uris": record["hms"]["uri"],
                 "iceberg.catalog.warehouse": record["hms"]["warehouse"],
                 "aws.s3.endpoint": credentials["endpoint"],
                 "aws.s3.region": "us-east-1", "aws.s3.enable_path_style_access": "true"}
        for purpose, role in (("metadata", "frontend"), ("data", "backend")):
            prefix = f"credential.object-store-{purpose}."
            props.update({prefix + "consumer-role": role, prefix + "mode": "static",
                          prefix + "name": "iceberg-test-data", prefix + "generation": "v1"})
        sql = "CREATE EXTERNAL CATALOG ice_hms\nPROPERTIES (\n" + ",\n".join(f"  {json.dumps(key)} = {json.dumps(value)}" for key, value in props.items()) + "\n);\n"
        self.write_definition(directory / "ice-hms-catalog.sql", sql.encode())
        props = {"": "org.apache.iceberg.spark.SparkCatalog", ".type": "hive", ".uri": "thrift://hms:9083",
                 ".warehouse": record["hms"]["warehouse"], ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                 ".s3.endpoint": "http://minio:9000", ".s3.path-style-access": "true",
                 ".s3.access-key-id": user, ".s3.secret-access-key": secret, ".s3.region": "us-east-1"}
        self.write_definition(directory / "spark-hms-defaults.conf", "".join(f"spark.sql.catalog.hms_catalog{key} {value}\n" for key, value in props.items()).encode())
        exports = {"NOVA_ENV_HIVE_RUNTIME_DIR": str(directory), "NOVA_ENV_HIVE_MANIFEST": str(directory / "manifest.json"),
                   "NOVA_ENV_HIVE_CATALOG_ID": record["catalog_id"], "NOVA_ENV_HIVE_COMPOSE_PROJECT": record["project"],
                   "NOVA_ENV_HIVE_COMPOSE_FILE": record["compose_file"], "NOVA_ENV_HIVE_COMPOSE_ENV": record["compose_env"],
                   "NOVA_ENV_HMS_PORT": record["ports"]["hms"], "NOVA_ENV_REST_NETWORK": record["rest_network"],
                   "NOVA_ENV_SHARED_HMS_WAREHOUSE_URI": record["hms"]["warehouse"],
                   "NOVAROCKS_ICEBERG_HMS_URI": record["hms"]["uri"], "NOVAROCKS_ICEBERG_HMS_WAREHOUSE": record["hms"]["warehouse"],
                   "NOVAROCKS_ICE_HMS_CATALOG_SQL": record["hms"]["catalog_sql"],
                   "NOVAROCKS_SPARK_HMS_DEFAULTS": record["hms"]["spark_defaults"],
                   "NOVAROCKS_SPARK_EXTRA_DEFAULTS": record["hms"]["spark_defaults"]}
        self.write_definition(directory / "env.sh", "".join(f"export {key}={shlex.quote(str(value))}\n" for key, value in exports.items()).encode())

    def up(self, manifest, image, prepare_only=False):
        identity = manifest["runtime"]["catalog"]["id"]
        with self.owner.lock("hms-" + identity):
            record = self.prepare(manifest, image)
            if record["state"] == "deleting":
                raise runtime.RuntimeFailure("RuntimeDeleting", identity)
            if prepare_only:
                return record
            self.owner.assert_live_daemon()
            image_id = self.backend.image_id(record["hms"]["image"])
            saved = record["images"]["hms"]["image_id"]
            if saved and image_id != saved:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "HMS image alias changed")
            record["images"]["hms"]["image_id"] = image_id
            record["state"] = "starting"
            record.pop("failure", None)
            # Only the first activation may replace the prepared image alias
            # with an exact image ID, before Docker has created any container.
            self.render(record, pin_image=saved is None)
            self.save(record)
            self.backend.container(record, "hms")
            self.backend.validate_resources(record)
            self.backend.compose(record, ["up", "-d", "--no-build"])
            container = self.backend.container(record, "hms")
            if not container:
                raise runtime.RuntimeFailure("RuntimeStateUnreadable", "HMS container missing")
            record["container_id"] = container["Id"]
            self.save(record)
            self.owner.consumer(identity, container["Id"], alias="hms")
            try:
                self.wait_ready(record)
            except runtime.RuntimeFailure as error:
                record.update(state="failed", failure={"code": error.code, "message": str(error)})
                self.save(record)
                raise
            record["state"] = "ready"
            self.save(record)
            return record

    def wait_ready(self, record, timeout=60):
        # Docker Desktop's published port proxy can accept connections before
        # Derby initialization finishes. Probe the listener in the exact saved
        # container instead, with each Docker operation bounded by the deadline.
        deadline = time.monotonic() + timeout
        saved_timeout = self.backend.timeout
        identity = record["container_id"]
        try:
            while time.monotonic() < deadline:
                self.backend.timeout = min(saved_timeout, 5, max(0.001, deadline - time.monotonic()))
                container = self.backend.inspect("container", identity, absent_ok=True)
                if (not container or container.get("Id") != identity
                        or not container.get("State", {}).get("Running")):
                    raise runtime.RuntimeFailure("RuntimeNotReady", "HMS container exited before readiness")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self.backend.timeout = min(saved_timeout, 2, remaining)
                try:
                    self.backend.command(["exec", identity, "/bin/bash", "-c",
                                          "exec 3<>/dev/tcp/127.0.0.1/9083"])
                    return
                except runtime.RuntimeFailure as error:
                    if error.code not in ("DockerOperationFailed", "DockerOperationTimeout"):
                        raise
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    time.sleep(min(1, remaining))
        finally:
            self.backend.timeout = saved_timeout
        raise runtime.RuntimeFailure("RuntimeNotReady", "HMS listener did not become ready before the deadline")

    def down(self, identity, volumes=False):
        with self.owner.lock("hms-" + identity):
            record = self.read(identity)
            if not record:
                return {"catalog_id": identity, "state": "absent"}
            self.owner.assert_live_daemon()
            # A retry must finish a saved destructive request, never restart an
            # instance whose Derby volume may already have been removed.
            deleting = volumes or record["state"] == "deleting"
            if deleting:
                record["state"] = "deleting"
                self.save(record)
            self.backend.validate_resources(record)
            container = self.backend.container(record, "hms") if record["images"]["hms"]["image_id"] else None
            if container:
                self.owner.consumer(identity, container["Id"], disconnect=True)
            elif record["container_id"]:
                self.owner.consumer(identity, record["container_id"], disconnect=True)
            if deleting:
                # HMS records use catalog_id as their identity. The common
                # resource verifier needs the same identity in its projection.
                self.backend.delete_resources({**record, "id": identity})
                directory = self.directory(identity)
                retirement_root = self.owner.base / "retired-hms"
                retirement_root.mkdir(parents=True, exist_ok=True)
                retired = retirement_root / f"{identity}-{uuid.uuid4().hex}"
                # The allocator reads HMS manifests under ports. Retire the
                # whole saved definition atomically before releasing its port.
                with self.owner.lock("ports"):
                    os.rename(directory, retired)
                    runtime.fsync_directory(directory.parent)
                    runtime.fsync_directory(retirement_root)
                shutil.rmtree(retired)
                runtime.fsync_directory(retirement_root)
                return {"catalog_id": identity, "state": "absent"}
            self.backend.compose(record, ["down"])
            record.update(state="stopped", container_id=None)
            self.save(record)
            return record


def main():
    parser = argparse.ArgumentParser(description="Catalog-scoped Hive Metastore owner")
    parser.add_argument("command", choices=("up", "down", "status"))
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--catalog-id")
    parser.add_argument("--root", type=Path)
    parser.add_argument("--daemon")
    parser.add_argument("--port-start", type=int, default=int(os.environ.get("NOVA_ENV_RUNTIME_PORT_START", "28000")))
    parser.add_argument("--port-end", type=int, default=int(os.environ.get("NOVA_ENV_RUNTIME_PORT_END", "28999")))
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--volumes", "-v", action="store_true")
    parser.add_argument("--purge", action="store_true")
    parser.add_argument("--docker", action="store_true")
    args = parser.parse_args()
    runtime.install_signal_handlers()
    path = args.env_file or Path(os.environ.get("NOVA_ENV_REST_ENV_FILE", HERE.parent / "iceberg-rest/runtime/current/env.sh"))
    manifest = None
    if args.command == "up" or not (args.catalog_id and args.root and args.daemon):
        publication = path.expanduser().resolve(strict=True).parent
        manifest = runtime.read_json(publication / "manifest.json")
        if not manifest.get("ready") or not manifest["runtime"].get("owner_locator"):
            raise runtime.RuntimeFailure("RuntimeNotReady", "bind a shared REST fixture first")
        locator = manifest["runtime"]["owner_locator"]
    else:
        locator = {"control_root": str(args.root.resolve()), "daemon_id": args.daemon}
    owner = runtime.RuntimeOwner(args.root or locator["control_root"], daemon=args.daemon or locator["daemon_id"],
                                 port_start=args.port_start, port_end=args.port_end)
    if owner.locator != locator:
        raise runtime.RuntimeFailure("OwnerMismatch", "HMS request differs from publication owner")
    hive = HiveOwner(owner)
    identity = args.catalog_id or manifest["runtime"]["catalog"]["id"]
    if args.command == "up":
        if identity != manifest["runtime"]["catalog"]["id"]:
            raise runtime.RuntimeFailure("OwnerMismatch", "up catalog differs from publication")
        result = hive.up(manifest, os.environ.get("HMS_IMAGE", "novarocks/hive-metastore:4.0.0"), args.prepare_only)
    elif args.command == "down":
        result = hive.down(identity, args.volumes or args.purge)
    else:
        result = hive.read(identity) or {"catalog_id": identity, "state": "absent"}
    # Credential material stays in the private manifest and generated config.
    print(json.dumps({key: value for key, value in result.items() if key != "minio"}, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(130)
    except (runtime.RuntimeFailure, OSError, ValueError, KeyError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
