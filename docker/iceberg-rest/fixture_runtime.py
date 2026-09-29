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
from contextlib import contextmanager
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
from typing import Any, Callable
import urllib.request
import uuid


SCRIPT_DIR = Path(__file__).resolve().parent
PROTOCOL = 1
ENTRY_FILES = (
    "env.sh", "manifest.json", "README.md", "fe.toml", "be.toml",
    "sql-test.toml", "ice-rest-catalog.sql", "spark-defaults.conf",
    "spark-iceberg-v3-smoke.sql",
)
ENVIRONMENT_KEYS = {
    "PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "SSL_CERT_FILE",
    "DOCKER_CONTEXT", "DOCKER_HOST", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH",
    "DOCKER_CONFIG", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME",
    "NOVA_FIXTURE_STORE", "NOVA_FIXTURE_RUNTIME_DIR",
}


class RuntimeFailure(RuntimeError):
    def __init__(self, code: str, detail: str):
        self.code = code
        super().__init__(f"{code}: {detail}")


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def controlled_environment(source: dict[str, str] | None = None) -> dict[str, str]:
    source = os.environ if source is None else source
    return {key: value for key, value in source.items() if key in ENVIRONMENT_KEYS}


def install_signal_handlers() -> None:
    def cancel(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, cancel)


def fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_json(path: Path, value: Any) -> None:
    atomic_bytes(path, canonical(value) + b"\n")


def atomic_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}")
    try:
        with temporary.open("xb") as out:
            os.chmod(temporary, 0o600)
            out.write(content)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        raise RuntimeFailure("RuntimeStateUnreadable", str(path)) from error
    if not isinstance(value, dict):
        raise RuntimeFailure("RuntimeStateUnreadable", str(path))
    return value


@contextmanager
def file_lock(path: Path, *, shared: bool = False):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        fcntl.flock(handle, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def safe_component(value: str) -> str:
    if not re.fullmatch(r"[a-zA-Z0-9_.:-]+", value):
        raise RuntimeFailure("InvalidIdentity", value)
    return value


def current_publication(entry: Path) -> dict[str, Any] | None:
    entry = entry.resolve()
    pointer = entry / "published"
    if not pointer.is_symlink():
        if pointer.exists():
            raise RuntimeFailure("RuntimeStateUnreadable", str(pointer))
        publications = entry / "publications"
        if (publications.exists() and any(publications.iterdir())) or any(
            (entry / name).is_symlink() or (entry / name).exists() for name in ENTRY_FILES
        ):
            raise RuntimeFailure("RuntimeStateUnreadable", "initialized entry lost its publication pointer")
        return None
    directory = pointer.resolve(strict=False)
    if directory.parent != (entry / "publications").resolve():
        raise RuntimeFailure("RuntimeStateUnreadable", str(pointer))
    value = read_json(directory / "binding.json")
    if value.get("schema") != PROTOCOL or value.get("entry_root") != str(entry):
        raise RuntimeFailure("RuntimeIdentityMismatch", str(pointer))
    value["published_dir"] = str(directory)
    return value


def production_renderer(context: dict[str, Any], staging: Path):
    # The renderer owns text formats; it cannot perform the ownership commit.
    from runtime_entry import render_entry
    return render_entry(context, staging)


class Docker:
    """Bounded Docker operations; all Compose inputs are owner-saved definitions."""

    def __init__(self, timeout: float = 90, environment: dict[str, str] | None = None):
        self.timeout = timeout
        self.environment = controlled_environment(environment)

    def command(self, args: list[str], *, absent_ok: bool = False) -> str:
        try:
            process = subprocess.Popen(
                ["docker", *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, env=self.environment, start_new_session=True,
            )
        except OSError as error:
            raise RuntimeFailure("DockerUnavailable", "cannot start Docker CLI") from error
        try:
            stdout, stderr = process.communicate(timeout=self.timeout)
        except BaseException as error:
            # Cancellation must settle our child before releasing ownership locks.
            try:
                os.killpg(process.pid, signal.SIGTERM)
                process.communicate(timeout=2)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.communicate()
            if isinstance(error, subprocess.TimeoutExpired):
                raise RuntimeFailure("DockerOperationTimeout", args[0]) from error
            raise
        if process.returncode:
            missing_object = re.search(r"no such (object|container|image|volume|network)\b", stderr.lower())
            daemon_absence = "error response from daemon" in stderr.lower() and (
                "not found" in stderr.lower() or "is not connected" in stderr.lower()
            )
            if absent_ok and (missing_object or daemon_absence):
                return ""
            # Do not print commands containing fixture credentials or cleanup URLs.
            code = "PortUnavailable" if any(term in stderr.lower() for term in (
                "port is already allocated", "address already in use",
            )) else "DockerOperationFailed"
            raise RuntimeFailure(code, f"{args[0]} exited with {process.returncode}")
        return stdout.strip()

    def daemon_id(self) -> str:
        endpoint = self.environment.get("DOCKER_HOST", "")
        if not endpoint:
            context = self.environment.get("DOCKER_CONTEXT") or self.command(["context", "show"])
            raw = self.command(["context", "inspect", context, "--format", "{{json .Endpoints.docker.Host}}"])
            try:
                endpoint = json.loads(raw)
            except ValueError as error:
                raise RuntimeFailure("UnsupportedDaemon", "invalid Docker context endpoint") from error
        if endpoint and not endpoint.startswith(("unix://", "tcp://127.0.0.1:", "tcp://localhost:")):
            raise RuntimeFailure("UnsupportedDaemon", "shared fixtures require a local daemon")
        identity = self.command(["info", "--format", "{{.ID}}"])
        if not identity:
            raise RuntimeFailure("DockerUnavailable", "daemon has no identity")
        return safe_component(identity)

    def inspect(self, kind: str, identity: str, *, absent_ok: bool = False) -> dict[str, Any] | None:
        output = self.command([kind, "inspect", identity, "--format", "{{json .}}"], absent_ok=absent_ok)
        if not output:
            return None
        try:
            value = json.loads(output)
        except ValueError as error:
            raise RuntimeFailure("RuntimeIdentityMismatch", "invalid Docker inspect result") from error
        if not isinstance(value, dict):
            raise RuntimeFailure("RuntimeIdentityMismatch", "invalid Docker inspect result")
        return value

    def image_id(self, reference: str) -> str:
        value = self.inspect("image", reference)
        if not value or not value.get("Id"):
            raise RuntimeFailure("RuntimeIdentityMismatch", "missing provisioned image")
        return value["Id"]

    def compose(self, record: dict[str, Any], args: list[str]) -> str:
        return self.command([
            "compose", "--env-file", record["compose_env"], "-p", record["project"],
            "-f", record["compose_file"], *args,
        ])

    def container(self, record: dict[str, Any], service: str) -> dict[str, Any] | None:
        ids = self.command([
            "ps", "-a", "--filter", f"label=com.docker.compose.project={record['project']}",
            "--filter", f"label=com.docker.compose.service={service}", "--format", "{{.ID}}",
        ]).splitlines()
        if not ids:
            return None
        if len(ids) != 1:
            raise RuntimeFailure("RuntimeIdentityMismatch", f"multiple {service} containers")
        info = self.inspect("container", ids[0])
        labels = (info.get("Config") or {}).get("Labels") or {}
        if info.get("Image") != record["images"][service]["image_id"] or any(
            labels.get(key) != value for key, value in self.labels(record).items()
        ):
            raise RuntimeFailure("RuntimeIdentityMismatch", f"{service} does not match saved definition")
        port = record["service_ports"].get(service)
        if port:
            bindings = (info.get("HostConfig") or {}).get("PortBindings") or {}
            for container_port, host_port in port.items():
                if not any(item.get("HostPort") == str(host_port) for item in bindings.get(container_port, [])):
                    raise RuntimeFailure("RuntimeIdentityMismatch", f"{service} port mismatch")
        return info

    @staticmethod
    def labels(record: dict[str, Any]) -> dict[str, str]:
        return {
            "novarocks.fixture.owner": record["namespace"],
            "novarocks.fixture.key": record["key"],
            "novarocks.fixture.kind": record["kind"],
        }

    @staticmethod
    def http_ready(url: str) -> bool:
        try:
            # Host-local health checks must not go through inherited proxy settings.
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(url, timeout=2) as response:
                return response.status == 200
        except (OSError, ValueError):
            return False

    @staticmethod
    def service_aliases(service: str) -> list[str]:
        return ["minio", "warehouse.minio", "novarocks.minio"] if service == "minio" else [service]

    def own_network(self, record: dict[str, Any]) -> dict[str, Any] | None:
        network = self.inspect("network", record["network"], absent_ok=True)
        if network and any((network.get("Labels") or {}).get(key) != value
                           for key, value in self.labels(record).items()):
            raise RuntimeFailure("RuntimeIdentityMismatch", "foreign service network")
        return network

    def service_connected(self, record: dict[str, Any], service: str,
                          container: dict[str, Any], network: dict[str, Any]) -> bool:
        endpoint = ((container.get("NetworkSettings") or {}).get("Networks") or {}).get(record["network"])
        return bool(endpoint and endpoint.get("NetworkID") == network.get("Id")
                    and container["Id"] in (network.get("Containers") or {})
                    and set(self.service_aliases(service)).issubset(endpoint.get("Aliases") or []))

    def repair_service_networks(self, record: dict[str, Any]) -> None:
        network = self.own_network(record)
        if not network:
            raise RuntimeFailure("RuntimeStateUnreadable", "saved service network is missing")
        for service in record["required_services"]:
            container = self.container(record, service)
            # Completed initialization containers do not retain live endpoints.
            if not container or not (container.get("State") or {}).get("Running"):
                continue
            if self.service_connected(record, service, container, network):
                continue
            identity = container["Id"]
            endpoints = (container.get("NetworkSettings") or {}).get("Networks") or {}
            if record["network"] in endpoints or identity in (network.get("Containers") or {}):
                self.command(["network", "disconnect", record["network"], identity], absent_ok=True)
            aliases = [argument for alias in self.service_aliases(service) for argument in ("--alias", alias)]
            self.command(["network", "connect", *aliases, record["network"], identity])
            network = self.own_network(record)
            if not network:
                raise RuntimeFailure("RuntimeStateUnreadable", "saved service network disappeared")

    def healthy(self, record: dict[str, Any]) -> bool:
        network = self.own_network(record)
        if not network:
            return False
        for service in record["required_services"]:
            info = self.container(record, service)
            if not info:
                return False
            state = info.get("State") or {}
            if service == "mc-init":
                if state.get("Status") != "exited" or state.get("ExitCode") != 0:
                    return False
            elif not state.get("Running") or not self.service_connected(record, service, info, network):
                return False
        for url in record["health_urls"]:
            if not self.http_ready(url):
                return False
        if record["kind"] == "cat":
            network = self.inspect("network", record["network"], absent_ok=True)
            if not network or record.get("object_store_container") not in (network.get("Containers") or {}):
                return False
        return True

    def tag(self, record: dict[str, Any]) -> None:
        for image in record["images"].values():
            existing = self.inspect("image", image["tag"], absent_ok=True)
            if existing and existing.get("Id") != image["image_id"]:
                raise RuntimeFailure("RuntimeIdentityMismatch", "instance tag has different content")
            if not existing:
                self.command(["tag", image["image_id"], image["tag"]])

    def attach(self, os_record: dict[str, Any], cat: dict[str, Any]) -> str:
        container = self.container(os_record, "minio")
        if not container:
            raise RuntimeFailure("DockerUnavailable", "object store has no container")
        identity = container["Id"]
        network = self.inspect("network", cat["network"])
        if identity not in (network.get("Containers") or {}):
            self.command(["network", "connect", "--alias", "minio", "--alias", "warehouse.minio",
                          "--alias", "novarocks.minio", cat["network"], identity])
        return identity

    def ensure(self, record: dict[str, Any], parent: dict[str, Any] | None = None) -> None:
        # Refuse to replace foreign containers even when saved state is starting.
        for service in record["images"]:
            self.container(record, service)
        self.validate_resources(record)
        self.tag(record)
        if parent:
            self.compose(record, ["create", "rest", "spark", "mc"])
            record["object_store_container"] = self.attach(parent, record)
            self.compose(record, ["up", "-d", "rest", "spark", "mc"])
        else:
            self.compose(record, ["up", "-d", "minio", "mc-init"])
        deadline = time.monotonic() + self.timeout
        while True:
            # A failed Docker start can lose only the service's own endpoint
            # while retaining attachments to other catalog networks. Compose
            # up does not always restore that endpoint on the existing container.
            self.repair_service_networks(record)
            if self.healthy(record):
                break
            if time.monotonic() >= deadline:
                raise RuntimeFailure("ReadinessTimeout", record["id"])
            time.sleep(0.1)
        record["containers"] = {
            service: self.container(record, service)["Id"] for service in record["required_services"]
        }

    def reconnect(self, os_record: dict[str, Any], catalogs: list[dict[str, Any]]) -> None:
        for cat in catalogs:
            self.validate_resources(cat)
            if self.inspect("network", cat["network"], absent_ok=True):
                cat["object_store_container"] = self.attach(os_record, cat)

    def external_connections(self, record: dict[str, Any], parent: dict[str, Any]) -> list[str]:
        network = self.inspect("network", record["network"], absent_ok=True)
        if not network:
            return []
        allowed = set()
        for service in record["images"]:
            info = self.container(record, service)
            if info:
                allowed.add(info["Id"])
        minio = self.container(parent, "minio")
        if minio:
            allowed.add(minio["Id"])
        return sorted(set((network.get("Containers") or {}).keys()) - allowed)

    def purge(self, record: dict[str, Any], prefixes: list[str]) -> None:
        minio = self.container(record, "minio")
        if not minio or not (minio.get("State") or {}).get("Running"):
            raise RuntimeFailure("DockerUnavailable", "object store is stopped")
        urls = []
        for prefix in prefixes:
            if not prefix.startswith("s3://") or prefix.count("/") < 3:
                raise RuntimeFailure("InvalidCleanupTarget", prefix)
            urls.append(shlex.quote("store/" + prefix.removeprefix("s3://").rstrip("/") + "/"))
        script = 'mc alias set store http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && '
        script += " && ".join(f"mc rm --recursive --force {url}" for url in urls)
        self.compose(record, ["run", "--rm", "--no-deps", "mc", "-c", script])

    def stop(self, record: dict[str, Any]) -> None:
        self.validate_resources(record)
        self.compose(record, ["stop"])

    def validate_resources(self, record: dict[str, Any]) -> None:
        for service in record["images"]:
            self.container(record, service)
        for kind, identities in (("network", [record["network"]]), ("volume", record["volumes"])):
            for identity in identities:
                info = self.inspect(kind, identity, absent_ok=True)
                if info and any((info.get("Labels") or {}).get(key) != value
                                for key, value in self.labels(record).items()):
                    raise RuntimeFailure("RuntimeIdentityMismatch", f"foreign {kind}")

    def disconnect(self, record: dict[str, Any], identity: str) -> None:
        network = self.inspect("network", record["network"], absent_ok=True)
        if network and identity in (network.get("Containers") or {}):
            self.command(["network", "disconnect", record["network"], identity], absent_ok=True)

    def connect(self, record: dict[str, Any], identity: str, alias: str) -> None:
        info = self.inspect("container", identity)
        if not info or info.get("Id") != identity:
            raise RuntimeFailure("RuntimeIdentityMismatch", "consumer requires a full container ID")
        network = self.inspect("network", record["network"])
        if identity not in (network.get("Containers") or {}):
            self.command(["network", "connect", "--alias", alias, record["network"], identity])

    def delete_resources(self, record: dict[str, Any]) -> None:
        self.validate_resources(record)
        self.compose(record, ["down", "-v"])
        if self.command(["ps", "-a", "--filter", f"label=com.docker.compose.project={record['project']}", "-q"]):
            raise RuntimeFailure("ResourcesStillPresent", record["id"])
        if self.inspect("network", record["network"], absent_ok=True):
            raise RuntimeFailure("ResourcesStillPresent", record["network"])
        for volume in record["volumes"]:
            if self.inspect("volume", volume, absent_ok=True):
                raise RuntimeFailure("ResourcesStillPresent", volume)

    def untag(self, record: dict[str, Any]) -> None:
        for image in record["images"].values():
            if self.inspect("image", image["tag"], absent_ok=True):
                if self.image_id(image["tag"]) != image["image_id"]:
                    raise RuntimeFailure("RuntimeIdentityMismatch", "changed instance tag")
                self.command(["image", "rm", image["tag"]])


class RuntimeOwner:
    def __init__(self, root: Path | str | None = None, *, daemon: str | None = None,
                 backend: Any = None, renderer: Callable = production_renderer,
                 port_start: int = 28000, port_end: int = 28999,
                 hook: Callable | None = None, templates: dict[str, Path] | None = None):
        default = Path(os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local/state"))) / "novarocks/fixture-runtime"
        self.root = Path(root or os.environ.get("NOVA_FIXTURE_RUNTIME_DIR", default)).resolve()
        self._daemon = daemon
        self.backend = backend if backend is not None else Docker()
        self.renderer = renderer
        if not 0 < port_start <= port_end < 65536:
            raise RuntimeFailure("PortUnavailable", "invalid runtime port range")
        self.port_start, self.port_end = port_start, port_end
        self.hook = hook or self.test_hook
        self.templates = templates or {kind: SCRIPT_DIR / "templates" / name for kind, name in (
            ("os", "object-store.yml"), ("cat", "catalog.yml"),
        )}

    @property
    def daemon(self) -> str:
        if self._daemon is None:
            self._daemon = self.backend.daemon_id()
        return safe_component(self._daemon)

    @property
    def base(self) -> Path:
        return self.root / self.daemon

    @property
    def locator(self) -> dict[str, str]:
        return {"control_root": str(self.root), "daemon_id": self.daemon}

    @property
    def namespace(self) -> str:
        return hashlib.sha256((str(self.root) + "\0" + self.daemon).encode()).hexdigest()[:6]

    def lock(self, identity: str, *, shared: bool = False):
        return file_lock(self.base / "locks" / f"{safe_component(identity)}.lock", shared=shared)

    def for_locator(self, locator: dict[str, str]) -> RuntimeOwner:
        return RuntimeOwner(locator["control_root"], daemon=locator["daemon_id"],
                            backend=self.backend, renderer=self.renderer, hook=self.hook)

    def assert_live_daemon(self) -> None:
        if self.backend.daemon_id() != self.daemon:
            raise RuntimeFailure("OwnerMismatch", "Docker daemon differs from requested owner")

    def record_path(self, identity: str) -> Path:
        return self.base / "runtimes" / safe_component(identity) / "record.json"

    def record(self, identity: str) -> dict[str, Any] | None:
        path = self.record_path(identity)
        if not path.exists():
            return None
        try:
            value = read_json(path)
        except RuntimeFailure as error:
            if isinstance(error.__cause__, FileNotFoundError):
                return None
            raise
        if value.get("schema") != PROTOCOL or value.get("id") != identity or value.get("owner_locator") != self.locator:
            raise RuntimeFailure("RuntimeIdentityMismatch", identity)
        if value.get("state") not in {"starting", "ready", "deleting"}:
            raise RuntimeFailure("RuntimeStateUnreadable", identity)
        return value

    def records(self) -> list[dict[str, Any]]:
        records = (self.record(path.parent.name) for path in sorted((self.base / "runtimes").glob("*/record.json")))
        return [record for record in records if record is not None]

    def save_record(self, record: dict[str, Any]) -> None:
        record["updated_at"] = time.time()
        history = record.setdefault("history", [])
        if not history or history[-1]["state"] != record["state"]:
            history.append({"state": record["state"], "at": record["updated_at"]})
        atomic_json(self.record_path(record["id"]), record)

    def test_hook(self, name: str, **identities: Any) -> None:
        directory = os.environ.get("NOVA_FIXTURE_RUNTIME_TEST_HOOKS")
        if not directory:
            return
        root = Path(directory)
        root.mkdir(parents=True, exist_ok=True)
        atomic_json(root / f"{name}.reached", {"name": name, "pid": os.getpid(), **identities})
        if (root / f"{name}.exit").exists():
            os._exit(97)
        while (root / f"{name}.pause").exists() and not (root / f"{name}.release").exists():
            time.sleep(0.01)

    def entry_index(self, entry: Path, worktree: str) -> None:
        path = self.base / "worktrees" / f"{hashlib.sha256(str(entry).encode()).hexdigest()}.json"
        atomic_json(path, {"entry_root": str(entry), "worktree": worktree})

    def publications(self) -> list[dict[str, Any]]:
        publications = []
        for path in sorted((self.base / "worktrees").glob("*.json")):
            index = read_json(path)
            publication = current_publication(Path(index["entry_root"]))
            if not publication or publication.get("worktree") != index["worktree"]:
                raise RuntimeFailure("RuntimeStateUnreadable", str(path))
            publications.append(publication)
        return publications

    def bindings(self, identity: str) -> list[dict[str, Any]]:
        return [item for item in self.publications() if item.get("owner_locator") == self.locator
                and item.get("binding") and identity in item["binding"].values()]

    def object_references(self, identity: str) -> list[Any]:
        references = [item["id"] for item in self.records() if item.get("object_store") == identity]
        for item in self.publications():
            references.extend(location for location in item["data_locations"]
                              if location["owner_locator"] == self.locator and location["object_store_id"] == identity)
        return references

    # Design: ADR-0165 (docs/adr/ADR-0165-versioned-fixture-runtime-ownership.md)
    def publish(self, entry: Path, metadata: dict[str, Any]) -> dict[str, Any]:
        config = metadata["config"]
        current = None
        if config.get("update_current", False):
            current = Path(config["current_link"])
            if not current.is_absolute() or current.parent.resolve() != entry.parent or (
                current.exists() and not current.is_symlink()
            ):
                raise RuntimeFailure("RuntimeIdentityMismatch", "invalid current locator")
        publication_id = uuid.uuid4().hex
        directory = entry / "publications" / publication_id
        staging = entry / "publications" / f".prepare-{publication_id}"
        staging.mkdir(parents=True)
        os.chmod(staging, 0o700)
        metadata = copy.deepcopy(metadata)
        metadata.pop("published_dir", None)
        metadata.update(schema=PROTOCOL, entry_root=str(entry))
        context = {**metadata, "env_id": metadata["worktree"], "publication_dir": str(directory),
                   "stable_runtime_dir": str(entry), "entry_root": str(entry),
                   "endpoints": self.endpoints(metadata.get("records", {}))}
        try:
            atomic_json(staging / "binding.json", metadata)
            self.renderer(context, staging)
            for name in ENTRY_FILES:
                path = staging / name
                if not path.is_file() or path.is_symlink():
                    raise RuntimeFailure("IncompletePublication", name)
            for path in staging.iterdir():
                if path.is_file():
                    with path.open("rb") as handle:
                        os.fsync(handle.fileno())
            fsync_directory(staging)
            self.hook("publication.prepared", worktree=metadata["worktree"])
            os.rename(staging, directory)
            fsync_directory(directory.parent)
            # These stable locators never change their target on subsequent binds.
            for name in ENTRY_FILES:
                link = entry / name
                target = Path("published") / name
                if link.is_symlink() and os.readlink(link) == str(target):
                    continue
                temporary = entry / f".entry-{publication_id}-{name}"
                temporary.symlink_to(target)
                os.replace(temporary, link)
            fsync_directory(entry)
            self.hook("publication.before_swap", worktree=metadata["worktree"])
            temporary = entry / f".published-{publication_id}"
            temporary.symlink_to(Path("publications") / publication_id)
            os.replace(temporary, entry / "published")
            fsync_directory(entry)
            if current is not None:
                temporary = current.with_name(f".current-{publication_id}")
                temporary.symlink_to(entry.name)
                os.replace(temporary, current)
                fsync_directory(current.parent)
            self.hook("publication.after_swap", worktree=metadata["worktree"])
            return {**metadata, "published_dir": str(directory)}
        finally:
            if staging.exists():
                shutil.rmtree(staging)

    @staticmethod
    def endpoints(records: dict[str, Any]) -> dict[str, Any]:
        if not records:
            return {}
        os_record, cat = records["object_store"], records["catalog"]
        return {
            "minio_endpoint": f"http://127.0.0.1:{os_record['ports']['minio']}",
            "minio_console": f"http://127.0.0.1:{os_record['ports']['minio_console']}",
            "rest_uri": f"http://127.0.0.1:{cat['ports']['rest']}",
            "spark_ui": f"http://127.0.0.1:{cat['ports']['spark']}",
            "container_minio_endpoint": "http://minio:9000", "container_rest_uri": "http://rest:8181",
        }

    def initial(self, entry: Path, worktree: str, config: dict[str, Any]) -> dict[str, Any]:
        current = current_publication(entry)
        if current:
            if current["worktree"] != worktree:
                raise RuntimeFailure("RuntimeIdentityMismatch", "entry belongs to another worktree")
            return current
        return self.publish(entry, {"worktree": worktree, "owner_locator": None, "ready": False,
                                   "binding": None, "data_locations": [], "records": {},
                                   "config": config, "producer_receipt": None})

    def allocate(self, names: list[str]) -> dict[str, int]:
        used = {port for record in self.records() for port in record["ports"].values()}
        for path in (self.base / "hms").glob("*/manifest.json"):
            used.update(read_json(path)["ports"].values())
        assigned = {}
        for name in names:
            for port in range(self.port_start, self.port_end + 1):
                if port in used:
                    continue
                with socket.socket() as probe:
                    try:
                        probe.bind(("127.0.0.1", port))
                    except OSError:
                        continue
                assigned[name] = port
                used.add(port)
                break
            else:
                raise RuntimeFailure("PortUnavailable", "runtime port range exhausted")
        return assigned

    def image_facts(self, bom: dict[str, Any]) -> dict[str, Any]:
        images = {}
        for service, logical in (("minio", "minio"), ("mc", "minio-mc"), ("rest", "iceberg-rest")):
            receipt = bom.get("images", {}).get(logical)
            if not receipt or not receipt.get("alias"):
                raise RuntimeFailure("FixturePrerequisiteMissing", logical)
            images[service] = {"image_id": self.backend.image_id(receipt["alias"]), "receipt": receipt}
        spark = bom.get("derived_images", {}).get("iceberg-spark")
        if not spark or not spark.get("image_id"):
            raise RuntimeFailure("FixturePrerequisiteMissing", "iceberg-spark")
        # Inspect the exact ID, never a mutable derived alias.
        if self.backend.image_id(spark["image_id"]) != spark["image_id"]:
            raise RuntimeFailure("RuntimeIdentityMismatch", "Spark image receipt mismatch")
        images["spark"] = {"image_id": spark["image_id"], "receipt": spark}
        return images

    def new_definition(self, kind: str, images: dict[str, Any], config: dict[str, Any],
                       object_store: str | None = None) -> dict[str, Any]:
        try:
            credentials = config["credentials"]
            if sorted(config["buckets"]) != ["novarocks", "warehouse"]:
                raise RuntimeFailure("RuntimeDefinitionMissing", "unsupported bucket set")
            stable = {"credentials": {"access_key": credentials["access_key"], "secret_key": credentials["secret_key"]},
                      "buckets": config["buckets"], "path_style": True}
            template = self.templates[kind].read_text()
        except (KeyError, OSError) as error:
            raise RuntimeFailure("RuntimeDefinitionMissing", str(error)) from error
        selected = ("minio", "mc") if kind == "os" else ("rest", "spark", "mc")
        key = digest({"protocol": PROTOCOL, "kind": kind,
                      "images": {name: images[name]["image_id"] for name in selected},
                      "model": hashlib.sha256(template.encode()).hexdigest(),
                      "config": stable, "object_store": object_store})
        identity = f"{kind}-{key[:12]}"
        return {"id": identity, "key": key, "kind": kind, "template": template,
                "images": {name: copy.deepcopy(images[name]) for name in selected},
                "config": stable, "object_store": object_store}

    def materialize(self, definition: dict[str, Any]) -> dict[str, Any]:
        existing = self.record(definition["id"])
        if existing:
            if existing["key"] != definition["key"]:
                raise RuntimeFailure("RuntimeIdentityMismatch", "short runtime ID collision")
            return existing
        identity, kind = definition["id"], definition["kind"]
        with self.lock("ports"):
            ports = self.allocate(["minio", "minio_console"] if kind == "os" else ["rest", "spark"])
            project = f"nr-fx-{self.namespace}-{identity}"
            directory = self.record_path(identity).parent
            record = {"schema": PROTOCOL, "protocol": PROTOCOL, "id": identity,
                      "key": definition["key"], "kind": kind, "state": "starting",
                      "namespace": self.namespace, "owner_locator": self.locator,
                      "project": project, "network": project + "_iceberg_net", "ports": ports,
                      "images": copy.deepcopy(definition["images"]), "config": definition["config"],
                      "object_store": definition["object_store"],
                      "compose_file": str(directory / "compose.yml"),
                      "compose_env": str(directory / "compose.env"), "created_at": time.time(),
                      "template": definition["template"]}
            if kind == "os":
                record["images"]["mc-init"] = copy.deepcopy(record["images"]["mc"])
            for service, image in record["images"].items():
                image["tag"] = f"novarocks/fixture-runtime:{self.namespace}-{identity}-{service}"
            record["required_services"] = ["minio", "mc-init"] if kind == "os" else ["rest", "spark"]
            record["volumes"] = [project + ("_minio-data" if kind == "os" else "_rest-catalog")]
            record["service_ports"] = ({"minio": {"9000/tcp": ports["minio"], "9001/tcp": ports["minio_console"]}}
                                       if kind == "os" else {"rest": {"8181/tcp": ports["rest"]}, "spark": {"4040/tcp": ports["spark"]}})
            record["health_urls"] = ([f"http://127.0.0.1:{ports['minio']}/minio/health/live"]
                                     if kind == "os" else [f"http://127.0.0.1:{ports['rest']}/v1/config"])
            record["server_warehouse"] = f"s3://warehouse/{identity}/rest" if kind == "cat" else None
            self.save_record(record)
        self.hook("bind.ports_reserved", runtime=identity)
        return record

    def write_definition(self, record: dict[str, Any]) -> None:
        values = {"NOVA_FIXTURE_OWNER": record["namespace"], "NOVA_FIXTURE_KEY": record["key"],
                  "NOVA_FIXTURE_KIND": record["kind"], "NOVA_FIXTURE_PROJECT": record["project"],
                  "MINIO_ROOT_USER": record["config"]["credentials"]["access_key"],
                  "MINIO_ROOT_PASSWORD": record["config"]["credentials"]["secret_key"]}
        for service, image in record["images"].items():
            values[service.upper().replace("-", "_") + "_IMAGE"] = image["tag"]
        values.update({"NOVA_ENV_" + name.upper() + "_PORT": port for name, port in record["ports"].items()})
        if record["server_warehouse"]:
            values["NOVA_ENV_REST_SERVER_WAREHOUSE_URI"] = record["server_warehouse"]
        directory = self.record_path(record["id"]).parent
        definition = directory / "compose.yml"
        env = directory / "compose.env"
        env_text = "".join(f"{key}='{str(value).replace(chr(39), chr(92)+chr(39))}'\n" for key, value in sorted(values.items()))
        for path, content in ((definition, record["template"]), (env, env_text)):
            if path.exists() and path.read_text() != content:
                raise RuntimeFailure("RuntimeIdentityMismatch", "saved definition changed")
            if not path.exists():
                atomic_bytes(path, content.encode())
        fsync_directory(directory)

    def ensure(self, record: dict[str, Any], parent: dict[str, Any] | None = None,
               *, finalize: bool = True) -> dict[str, Any]:
        if record["state"] == "deleting":
            raise RuntimeFailure("RuntimeDeleting", record["id"])
        self.write_definition(record)
        if record["state"] != "ready" or not self.backend.healthy(record):
            record["state"] = "starting"
            self.save_record(record)
            self.backend.ensure(record, parent)
            if finalize:
                record["state"] = "ready"
                self.save_record(record)
        return record

    # Design: ADR-0165 (docs/adr/ADR-0165-versioned-fixture-runtime-ownership.md)
    def bind(self, worktree: str, entry: Path | str, config: dict[str, Any], bom: dict[str, Any]) -> dict[str, Any]:
        self.assert_live_daemon()
        entry = Path(entry).resolve()
        safe_component(worktree)
        with file_lock(entry / ".owner.lock"):
            previous = self.initial(entry, worktree, config)
            if previous["binding"] and previous["owner_locator"] != self.locator:
                raise RuntimeFailure("OwnerMismatch", "unbind before switching owner")
            self.entry_index(entry, worktree)
            images = self.image_facts(bom)
            os_definition = self.new_definition("os", images, config)
            os_id = os_definition["id"]
            # Release shared ownership before repair; revalidate after reacquisition.
            for _ in range(3):
                with self.lock(os_id, shared=True):
                    os_record = self.record(os_id)
                    if os_record and os_record["state"] == "deleting":
                        raise RuntimeFailure("RuntimeDeleting", os_id)
                    if os_record and os_record["state"] == "ready" and self.backend.healthy(os_record):
                        self.hook("bind.os_shared_ready", runtime=os_id, worktree=worktree)
                        cat_definition = self.new_definition("cat", images, config, os_id)
                        with self.lock(cat_definition["id"]):
                            cat = self.ensure(self.materialize(cat_definition), os_record)
                            location = {"owner_locator": self.locator, "object_store_id": os_id,
                                        "private_prefixes": [f"s3://warehouse/{worktree}/", f"s3://novarocks/{worktree}/"]}
                            locations = copy.deepcopy(previous["data_locations"])
                            if location not in locations:
                                locations.append(location)
                            receipt = {"lock_sha256": bom.get("lock_sha256"), **images["spark"]["receipt"]}
                            return self.publish(entry, {"worktree": worktree, "owner_locator": self.locator,
                                "binding": {"catalog": cat["id"], "object_store": os_id}, "ready": True,
                                "records": {"object_store": os_record, "catalog": cat}, "config": config,
                                "data_locations": locations, "producer_receipt": receipt})
                with self.lock(os_id):
                    os_record = self.ensure(self.materialize(os_definition), finalize=False)
                    catalogs = [item for item in self.records() if item.get("object_store") == os_id]
                    self.hook("object_store.before_reconnect", runtime=os_id)
                    self.backend.reconnect(os_record, catalogs)
                    for cat in catalogs:
                        self.save_record(cat)
                    os_record["state"] = "ready"
                    self.save_record(os_record)
            raise RuntimeFailure("ReadinessTimeout", "object store repair handoff did not converge")

    def unbound(self, metadata: dict[str, Any], *, config: dict[str, Any] | None = None) -> dict[str, Any]:
        result = copy.deepcopy(metadata)
        result.update(binding=None, ready=False, records={}, producer_receipt=None)
        if config is not None:
            result["config"] = config
        return result

    def unbind(self, worktree: str, entry: Path | str, *, purge: bool = False) -> dict[str, Any]:
        entry = Path(entry).resolve()
        with file_lock(entry / ".owner.lock"):
            previous = current_publication(entry)
            if not previous or previous["worktree"] != worktree:
                raise RuntimeFailure("RuntimeStateUnreadable", "unknown worktree entry")
            if purge:
                for location in previous["data_locations"]:
                    owner = self.for_locator(location["owner_locator"])
                    if self.backend.daemon_id() != owner.daemon:
                        raise RuntimeFailure("OwnerMismatch", "cleanup daemon differs from saved location")
                    with owner.lock(location["object_store_id"], shared=True):
                        record = owner.record(location["object_store_id"])
                        if not record or record["state"] == "deleting":
                            raise RuntimeFailure("RuntimeDeleting", location["object_store_id"])
                        owner.backend.purge(record, location["private_prefixes"])
                self.hook("unbind.purged", worktree=worktree)
            metadata = self.unbound(previous)
            if purge:
                metadata["data_locations"] = []
            result = self.publish(entry, metadata)
            self.hook("unbind.before_old_output_cleanup", worktree=worktree)
            for directory in (entry / "publications").iterdir():
                if directory.is_dir() and directory != Path(result["published_dir"]):
                    shutil.rmtree(directory)
            fsync_directory(entry / "publications")
            return result

    def prepare_entry(self, worktree: str, entry: Path | str, config: dict[str, Any]) -> dict[str, Any]:
        entry = Path(entry).resolve()
        with file_lock(entry / ".owner.lock"):
            previous = current_publication(entry)
            if not previous:
                return self.initial(entry, worktree, config)
            if previous["worktree"] != worktree:
                raise RuntimeFailure("RuntimeIdentityMismatch", "wrong worktree")
            if previous["binding"]:
                owner = self.for_locator(previous["owner_locator"])
                binding = previous["binding"]
                with owner.lock(binding["object_store"], shared=True), owner.lock(binding["catalog"]):
                    records = {"object_store": owner.record(binding["object_store"]), "catalog": owner.record(binding["catalog"])}
                    if all(record and record["state"] == "ready" for record in records.values()):
                        previous.update(records=records, config=config)
                        return self.publish(entry, previous)
                    return self.publish(entry, self.unbound(previous, config=config))
            return self.publish(entry, self.unbound(previous, config=config))

    # Design: ADR-0165 (docs/adr/ADR-0165-versioned-fixture-runtime-ownership.md)
    def delete_catalog(self, identity: str, *, force: bool = False) -> None:
        self.assert_live_daemon()
        initial = self.record(identity)
        if not initial:
            return
        os_id = initial["object_store"]
        with self.lock(os_id, shared=True), self.lock(identity):
            record = self.record(identity)
            if not record:
                return
            parent = self.record(os_id)
            if not parent:
                raise RuntimeFailure("RuntimeStateUnreadable", os_id)
            self.write_definition(record)
            if record["state"] != "deleting":
                if self.backend.external_connections(record, parent):
                    raise RuntimeFailure("ExternalAttachmentsPresent", identity)
                if not force and self.bindings(identity):
                    raise RuntimeFailure("BindingsPresent", identity)
                record.update(state="deleting", deletion_id=uuid.uuid4().hex)
                self.save_record(record)
            token = record["deletion_id"]
        self.hook("delete.marked", runtime=identity, deletion_id=token)
        for publication in sorted(self.bindings(identity), key=lambda item: item["entry_root"]):
            entry = Path(publication["entry_root"])
            self.hook("delete.before_worktree_lock", runtime=identity, worktree=publication["worktree"])
            with file_lock(entry / ".owner.lock"), self.lock(os_id, shared=True), self.lock(identity):
                record = self.record(identity)
                if not record or record.get("deletion_id") != token or record["state"] != "deleting":
                    return
                current = current_publication(entry)
                if current and current["owner_locator"] == self.locator and current["binding"] and current["binding"]["catalog"] == identity:
                    self.publish(entry, self.unbound(current))
                    self.hook("delete.after_conditional_unbind", runtime=identity, worktree=current["worktree"])
        with self.lock(os_id, shared=True), self.lock(identity):
            record = self.record(identity)
            if not record or record.get("deletion_id") != token or record["state"] != "deleting":
                return
            parent = self.record(os_id)
            if self.bindings(identity):
                raise RuntimeFailure("BindingsPresent", identity)
            if not parent:
                raise RuntimeFailure("RuntimeStateUnreadable", os_id)
            if self.backend.external_connections(record, parent):
                raise RuntimeFailure("ExternalAttachmentsPresent", identity)
            self.backend.stop(record)
            self.hook("delete.stopped", runtime=identity)
            self.backend.purge(parent, [record["server_warehouse"]])
            self.hook("delete.purged", runtime=identity)
            container = self.backend.container(parent, "minio")
            if container:
                self.backend.disconnect(record, container["Id"])
            self.hook("delete.disconnected", runtime=identity)
            self.backend.delete_resources(record)
            self.hook("delete.resources_removed", runtime=identity)
            self.backend.untag(record)
            self.remove_record(record)

    def remove_record(self, record: dict[str, Any]) -> None:
        directory = self.record_path(record["id"]).parent
        # Resource deletion and tag removal are already reconciled. The record is
        # retired atomically with its definitions so a new same-key instance
        # cannot encounter half-removed files from the completed deletion.
        retirement_root = self.base / "retired"
        retirement_root.mkdir(parents=True, exist_ok=True)
        retired = retirement_root / f"{record['id']}-{uuid.uuid4().hex}"
        os.rename(directory, retired)
        fsync_directory(directory.parent)
        fsync_directory(retirement_root)
        self.hook("delete.record_retired", runtime=record["id"])
        shutil.rmtree(retired)
        fsync_directory(retirement_root)

    def manage(self, identity: str, operation: str, *, force: bool = False) -> None:
        self.assert_live_daemon()
        record = self.record(identity)
        if not record:
            return
        if record["kind"] == "cat" and operation == "delete":
            return self.delete_catalog(identity, force=force)
        if record["kind"] == "os":
            with self.lock(identity):
                record = self.record(identity)
                if not record:
                    return
                references = self.object_references(identity)
                if references and (operation == "delete" or not force):
                    raise RuntimeFailure("BindingsPresent", "object store has catalog or data references")
                if operation == "stop":
                    self.write_definition(record)
                    self.backend.stop(record)
                else:
                    if record["state"] != "deleting":
                        record.update(state="deleting", deletion_id=uuid.uuid4().hex)
                        self.save_record(record)
                    self.write_definition(record)
                    self.backend.delete_resources(record)
                    self.backend.untag(record)
                    self.remove_record(record)
        else:
            with self.lock(record["object_store"], shared=True), self.lock(identity):
                record = self.record(identity)
                if not record:
                    return
                if record["state"] == "deleting":
                    raise RuntimeFailure("RuntimeDeleting", identity)
                if self.bindings(identity) and not force:
                    raise RuntimeFailure("BindingsPresent", identity)
                self.write_definition(record)
                self.backend.stop(record)

    def consumer(self, identity: str, container: str, *, alias: str = "hms", disconnect: bool = False) -> None:
        self.assert_live_daemon()
        initial = self.record(identity)
        if not initial or initial["kind"] != "cat":
            if disconnect:
                return
            raise RuntimeFailure("RuntimeStateUnreadable", identity)
        with self.lock(initial["object_store"], shared=True), self.lock(identity):
            record = self.record(identity)
            if not record:
                if disconnect:
                    return
                raise RuntimeFailure("RuntimeStateUnreadable", identity)
            if disconnect:
                self.backend.disconnect(record, container)
            else:
                if record["state"] != "ready":
                    raise RuntimeFailure("RuntimeDeleting", identity)
                self.hook("consumer.before_connect", runtime=identity)
                self.backend.connect(record, container, alias)


def render_isolated(bom: dict[str, Any], project: str, ports: dict[str, int], out: Path,
                    config: dict[str, Any],
                    *, profile: str = "stock", hook_image: str | None = None) -> dict[str, Any]:
    """Render one complete private stack; the harness owns activation and cleanup."""
    from runtime_entry import render_isolated_stack
    return render_isolated_stack(bom, project, ports, out, config,
                                 profile=profile, hook_image=hook_image)


def main() -> int:
    install_signal_handlers()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root")
    parser.add_argument("--daemon")
    parser.add_argument("--port-start", type=int, default=28000)
    parser.add_argument("--port-end", type=int, default=28999)
    parser.add_argument("--timeout", type=float, default=90)
    commands = parser.add_subparsers(dest="operation", required=True)
    for name in ("bind", "prepare-entry", "unbind", "show-binding"):
        sub = commands.add_parser(name)
        sub.add_argument("--worktree", required=True)
        sub.add_argument("--entry", required=True, type=Path)
        sub.add_argument("--json", action="store_true")
        if name in ("bind", "prepare-entry"):
            sub.add_argument("--config-json", required=True, type=Path)
        if name == "bind":
            sub.add_argument("--bom", required=True, type=Path)
        if name == "unbind":
            sub.add_argument("--purge", action="store_true")
    for name in ("stop", "delete", "status"):
        sub = commands.add_parser(name)
        sub.add_argument("identity")
        sub.add_argument("--force", action="store_true")
    commands.add_parser("list")
    for name in ("connect-consumer", "disconnect-consumer"):
        sub = commands.add_parser(name)
        sub.add_argument("identity")
        sub.add_argument("--container", required=True)
        sub.add_argument("--alias", default="hms")
    sub = commands.add_parser("render-isolated")
    sub.add_argument("--bom", required=True, type=Path)
    sub.add_argument("--project", required=True)
    sub.add_argument("--ports", required=True)
    sub.add_argument("--out", required=True, type=Path)
    sub.add_argument("--config-json", required=True, type=Path)
    sub.add_argument("--profile", choices=("stock", "publication-hook"), default="stock")
    sub.add_argument("--hook-image")
    args = parser.parse_args()
    try:
        owner = RuntimeOwner(args.root, daemon=args.daemon, backend=Docker(args.timeout),
                             port_start=args.port_start, port_end=args.port_end)
        if args.operation in {"bind", "prepare-entry"}:
            config = read_json(args.config_json)
            if args.operation == "bind":
                result = owner.bind(args.worktree, args.entry, config, read_json(args.bom))
            else:
                result = owner.prepare_entry(args.worktree, args.entry, config)
        elif args.operation == "unbind":
            result = owner.unbind(args.worktree, args.entry, purge=args.purge)
        elif args.operation == "show-binding":
            result = current_publication(args.entry.resolve())
            if result and result["worktree"] != args.worktree:
                raise RuntimeFailure("RuntimeIdentityMismatch", "wrong worktree")
        elif args.operation in {"stop", "delete"}:
            owner.manage(args.identity, args.operation, force=args.force)
            result = {"operation": args.operation, "id": args.identity}
        elif args.operation in {"connect-consumer", "disconnect-consumer"}:
            owner.consumer(args.identity, args.container, alias=args.alias,
                           disconnect=args.operation == "disconnect-consumer")
            result = {"operation": args.operation, "id": args.identity}
        elif args.operation == "render-isolated":
            result = render_isolated(read_json(args.bom), args.project, json.loads(args.ports), args.out,
                                     read_json(args.config_json),
                                     profile=args.profile, hook_image=args.hook_image)
        else:
            result = owner.records() if args.operation == "list" else owner.record(args.identity)
        print(json.dumps(result, sort_keys=True))
        return 0
    except RuntimeFailure as error:
        print(str(error), file=sys.stderr)
        return 75 if error.code == "FixturePrerequisiteMissing" else 1
    except (KeyboardInterrupt, SystemExit):
        return 130


if __name__ == "__main__":
    # The renderer imports this owner module; share exception and contract types
    # when this file is the CLI entrypoint rather than loading it twice.
    sys.modules.setdefault("fixture_runtime", sys.modules[__name__])
    raise SystemExit(main())
