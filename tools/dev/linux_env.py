#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Explicit Linux development preparation and observation, never a test runner.

Functional tests use a command adapter to exercise installation decisions. Native
installation and cold-machine evidence remain separate author obligations.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import platform
import pwd
import re
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import types
import uuid


DOCKER_PACKAGES = ("docker.io", "docker-compose-v2", "docker-buildx")


class EnvironmentFailure(RuntimeError):
    """A preparation stage or an unmet prerequisite failed."""


class EnvironmentCancelled(BaseException):
    """Terminal cancellation must never be mistaken for a missing tool."""

    def __init__(self, signum: int, detail: str):
        self.signum = signum
        super().__init__(detail)


@dataclass(frozen=True)
class Account:
    name: str
    uid: int
    gid: int
    home: Path
    shell: str = "/bin/bash"

    @classmethod
    def lookup(cls, name: str) -> Account:
        try:
            entry = pwd.getpwnam(name)
        except KeyError as error:
            raise EnvironmentFailure(f"installation account does not exist: {name}") from error
        home = Path(entry.pw_dir)
        if not home.is_absolute() or not home.is_dir():
            raise EnvironmentFailure(f"installation account has no existing absolute home: {name}")
        return cls(entry.pw_name, entry.pw_uid, entry.pw_gid, home, entry.pw_shell)


class Commands:
    def run(self, command: list[str], *, environment: dict[str, str] | None = None,
            timeout: int | None = None, check: bool = True) -> subprocess.CompletedProcess:
        print("+ " + shlex.join(command), file=sys.stderr, flush=True)
        try:
            process = subprocess.Popen(command, env=environment, text=True,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       start_new_session=True)
        except OSError as error:
            raise EnvironmentFailure(f"cannot start {command[0]}: {error}") from error
        cancelled = []
        timed_out = False
        previous = {}

        def forward(signum, frame):
            if not cancelled:
                cancelled.append(signum)
                print(f"cancellation requested; waiting for {command[0]} owner to exit", file=sys.stderr, flush=True)
                try:
                    os.killpg(process.pid, signum)
                except ProcessLookupError:
                    pass

        try:
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous[signum] = signal.signal(signum, forward)
            try:
                stdout, stderr = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                forward(signal.SIGTERM, None)
                stdout, stderr = process.communicate()
        finally:
            for signum, handler in previous.items():
                signal.signal(signum, handler)
        result = subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
        if stdout:
            print(stdout, file=sys.stderr, end="" if stdout.endswith("\n") else "\n")
        if stderr:
            print(stderr, file=sys.stderr, end="" if stderr.endswith("\n") else "\n")
        print(f"exit={result.returncode}", file=sys.stderr, flush=True)
        if timed_out:
            raise EnvironmentFailure(f"observation timed out after {timeout}s; {command[0]} exited {result.returncode}")
        if cancelled:
            raise EnvironmentCancelled(cancelled[0], f"cancelled by signal {cancelled[0]}; {command[0]} exited {result.returncode}")
        if check and result.returncode:
            raise EnvironmentFailure(f"command failed ({result.returncode}): {shlex.join(command)}")
        return result


def read_object(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        raise EnvironmentFailure(f"cannot read configuration: {path}") from error
    if not isinstance(value, dict):
        raise EnvironmentFailure(f"configuration is not an object: {path}")
    return value


def available_cpus() -> int:
    if hasattr(os, "process_cpu_count"):
        count = os.process_cpu_count()
    elif hasattr(os, "sched_getaffinity"):
        count = len(os.sched_getaffinity(0))
    else:
        count = os.cpu_count()
    if not count or count < 1:
        raise EnvironmentFailure("cannot observe available CPU count")
    return count


def atomic_text(path: Path, text: str, *, account: Account | None = None,
                mode: int | None = None) -> bool:
    """Publish only changed text and preserve unrelated file content upstream."""
    if path.is_file() and path.read_text() == text:
        return False
    if mode is None:
        mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else 0o644
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}")
    try:
        with temporary.open("x") as output:
            output.write(text)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temporary, mode)
        if account is not None and os.geteuid() == 0:
            os.chown(temporary, account.uid, account.gid)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return True


class LinuxEnvironment:
    def __init__(self, repo: Path, inputs: dict, profile: dict, account: Account,
                 *, commands: Commands | None = None, system_root: Path = Path("/"),
                 object_store_config: Path | None = None):
        self.repo, self.inputs, self.profile, self.account = repo, inputs, profile, account
        self.commands = commands or Commands()
        self.system_root = system_root
        if inputs.get("schema") != 1 or profile.get("schema") != 1:
            raise EnvironmentFailure("unsupported Linux environment input/profile schema")
        self.timeout = profile.get("download_timeout_seconds", 600)
        if not isinstance(self.timeout, int) or isinstance(self.timeout, bool) or self.timeout <= 0:
            raise EnvironmentFailure("download_timeout_seconds must be a positive integer")
        try:
            self.toolchain = tomllib.loads((repo / "rust-toolchain.toml").read_text())["toolchain"]
        except (OSError, ValueError, KeyError) as error:
            raise EnvironmentFailure("cannot read repository Rust toolchain") from error
        self.locale = profile.get("locale", "zh_CN.UTF-8")
        if not re.fullmatch(r"[a-z]{2}_[A-Z]{2}\.UTF-8", self.locale):
            raise EnvironmentFailure("profile locale must name an explicit UTF-8 locale")
        for name in ("rustup", "nextest"):
            item = inputs.get(name, {})
            if not isinstance(item.get("version"), str) or not item["version"]:
                raise EnvironmentFailure(f"missing pinned {name} version")
            if not re.fullmatch(r"[0-9a-f]{64}", item.get("sha256", "")):
                raise EnvironmentFailure(f"invalid {name} SHA-256")
            if not isinstance(item.get("url"), str) or not item["url"].startswith("https://"):
                raise EnvironmentFailure(f"{name} URL must use HTTPS")
        if not re.fullmatch(r"[0-9a-f]{64}", inputs["nextest"].get("executable_sha256", "")):
            raise EnvironmentFailure("invalid nextest executable SHA-256")
        packages = inputs.get("packages")
        if not isinstance(packages, list) or not packages or any(
                not isinstance(item, str) or not re.fullmatch(r"[a-z0-9][a-z0-9+.-]*", item)
                for item in packages):
            raise EnvironmentFailure("packages must be explicit Ubuntu package names")
        mirrors = profile.get("docker_registry_mirrors", [])
        if not isinstance(mirrors, list) or any(
                not isinstance(item, str) or not item.startswith("https://") for item in mirrors):
            raise EnvironmentFailure("docker_registry_mirrors must contain HTTPS URLs")
        prefixes = profile.get("fixture_artifact_url_prefixes", {})
        if not isinstance(prefixes, dict) or any(not isinstance(original, str) or not isinstance(transport, str)
                for original, transport in prefixes.items()):
            raise EnvironmentFailure("fixture_artifact_url_prefixes must map original URLs to transport URLs")
        self.object_store = None
        kind = profile.get("object_store_kind", "minio")
        if kind == "aliyun-oss":
            if object_store_config is None or not object_store_config.is_absolute():
                raise EnvironmentFailure("aliyun-oss requires an absolute --object-store-config")
            self.object_store = self.read_object_store_config(object_store_config)
        elif kind != "minio":
            raise EnvironmentFailure("unsupported profile object_store_kind")
        elif object_store_config is not None:
            raise EnvironmentFailure("--object-store-config requires an aliyun-oss profile")
        self.environment = self.account_environment()

    def path(self, name: str) -> Path:
        return self.system_root / name.lstrip("/")

    @property
    def cache(self) -> Path:
        return self.account.home / ".cache/novarocks/linux-env"

    @property
    def fixture_store(self) -> Path:
        return self.account.home / ".cache/novarocks/fixture-inputs"

    @property
    def env_path(self) -> Path:
        return self.account.home / ".config/novarocks/linux-env.sh"

    @property
    def object_store_path(self) -> Path:
        return self.cache / "object-store.json"

    @property
    def fixture_machine_config_path(self) -> Path:
        return self.cache / "fixture-machine.json"

    @property
    def fixture_consumer(self) -> str:
        return "iceberg-rest-oss" if self.object_store is not None else "iceberg-rest"

    def read_object_store_config(self, path: Path) -> dict:
        # Execute the one validation author without creating a bytecode cache.
        # The module uses only the standard library until an operation starts.
        source = self.repo / "docker/iceberg-rest/oss_ops.py"
        module = types.ModuleType("novarocks_installer_oss_configuration")
        module.__file__ = str(source)
        try:
            exec(compile(source.read_bytes(), str(source), "exec"), module.__dict__)
            return module.validate_config(read_object(path))
        except OSError as error:
            raise EnvironmentFailure("cannot load OSS configuration validation") from error
        except module.OpsError as error:
            raise EnvironmentFailure(f"invalid OSS configuration: {error.code}") from error

    def object_store_configuration(self, *, prepare: bool) -> None:
        if self.object_store is None:
            return
        copies = ((self.object_store_path, self.object_store),
                  (self.fixture_machine_config_path, {"object_store": self.object_store}))
        if prepare:
            self.owned_directory(self.cache)
            for path, value in copies:
                atomic_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n",
                            account=self.account, mode=0o600)
        else:
            for path, value in copies:
                if not path.is_file() or read_object(path) != value:
                    raise EnvironmentFailure(f"prepared OSS configuration is missing or differs from input: {path}")

    def machine_config_arguments(self) -> list[str]:
        return ["--config", str(self.fixture_machine_config_path)] if self.object_store is not None else []

    def account_environment(self) -> dict[str, str]:
        # Never inherit root's HOME/XDG/Cargo stores or Docker context for another
        # account. A caller can explicitly configure sources in the profile.
        env = {
            "HOME": str(self.account.home), "USER": self.account.name,
            "LOGNAME": self.account.name,
            "PATH": str(self.account.home / ".cargo/bin") + ":/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "CARGO_HOME": str(self.account.home / ".cargo"),
            "RUSTUP_HOME": str(self.account.home / ".rustup"),
            "RUSTUP_TOOLCHAIN": self.toolchain["channel"],
            "NOVA_FIXTURE_STORE": str(self.fixture_store),
            "DOCKER_CONTEXT": "default",
            "PYTHONDONTWRITEBYTECODE": "1",
            "LANG": self.locale, "LC_ALL": self.locale,
        }
        if self.object_store is not None:
            env["NOVA_ENV_OBJECT_STORE_CONFIG"] = str(self.object_store_path)
        for key, field in (("RUSTUP_DIST_SERVER", "rustup_dist_server"),
                           ("RUSTUP_UPDATE_ROOT", "rustup_update_root"),
                           ("CARGO_REGISTRIES_CRATES_IO_INDEX", "cargo_registry_index")):
            value = self.profile.get(field)
            if value:
                if not isinstance(value, str) or not (value.startswith("https://") or value.startswith("sparse+https://")):
                    raise EnvironmentFailure(f"invalid source configuration: {field}")
                env[key] = value
        jobs = os.environ.get("CARGO_BUILD_JOBS")
        policy = self.profile.get("cargo_build_jobs", "available")
        if jobs is None:
            jobs = str(available_cpus()) if policy == "available" else str(policy)
        if not re.fullmatch(r"-?[1-9][0-9]*", jobs):
            raise EnvironmentFailure("CARGO_BUILD_JOBS must be a nonzero Cargo integer")
        env["CARGO_BUILD_JOBS"] = jobs
        return env

    def run(self, args: list[str], *, account: bool = False, check: bool = True,
            timeout: int | None = None) -> subprocess.CompletedProcess:
        env = self.environment if account else None
        if not account and args[0] == "docker":
            # System daemon maintenance must observe this node even when the
            # invoking root shell selected another Docker endpoint.
            env = dict(os.environ)
            env.pop("DOCKER_HOST", None)
            env["DOCKER_CONTEXT"] = "default"
        if account and os.geteuid() != self.account.uid:
            if os.geteuid() != 0:
                raise EnvironmentFailure("observing another account requires root")
            args = ["runuser", "-u", self.account.name, "--", "env", "-i",
                    *[f"{key}={value}" for key, value in self.environment.items()], *args]
            env = None
        return self.commands.run(args, environment=env, timeout=timeout, check=check)

    def preflight(self, *, prepare: bool) -> dict:
        try:
            os_release = dict(line.split("=", 1) for line in self.path("/etc/os-release").read_text().splitlines()
                              if "=" in line and not line.startswith("#"))
            os_release = {key: value.strip('"') for key, value in os_release.items()}
        except OSError as error:
            raise EnvironmentFailure("cannot read operating system identity") from error
        support = self.profile.get("support", {})
        machine = platform.machine()
        observed = "linux/amd64" if sys.platform.startswith("linux") and machine == "x86_64" else f"{sys.platform}/{machine}"
        if (os_release.get("ID"), os_release.get("VERSION_ID"), observed) != (
                support.get("os_id"), support.get("version_id"), support.get("platform")):
            raise EnvironmentFailure(f"unsupported host: {os_release.get('ID')} {os_release.get('VERSION_ID')} {observed}")
        if observed != "linux/amd64" or support.get("version_id") != "26.04" or support.get("os_id") != "ubuntu":
            raise EnvironmentFailure("this installer supports only native Ubuntu 26.04 amd64")
        if prepare and os.geteuid() != 0:
            raise EnvironmentFailure("prepare requires root and an explicit --user")
        if prepare:
            self.login_profile()
        if not prepare and os.geteuid() not in (0, self.account.uid):
            raise EnvironmentFailure("check/env must run as the selected account or root")
        fs = self.run(["findmnt", "-n", "-o", "FSTYPE", "-T", str(self.account.home)]).stdout.strip()
        if fs not in ("ext2", "ext3", "ext4", "xfs", "btrfs", "tmpfs"):
            raise EnvironmentFailure(f"account state requires a supported native local filesystem; found {fs}")
        source_fs = self.run(["findmnt", "-n", "-o", "FSTYPE", "-T", str(self.repo)]).stdout.strip()
        if source_fs not in ("ext2", "ext3", "ext4", "xfs", "btrfs", "tmpfs"):
            raise EnvironmentFailure(f"worktree fixture state requires a supported native local filesystem; found {source_fs}")
        usage = shutil.disk_usage(self.account.home)
        return {"os": os_release, "platform": observed, "available_cpus": available_cpus(),
                "account": self.account.name, "home": str(self.account.home),
                "filesystem": fs, "source_filesystem": source_fs, "disk_free_bytes": usage.free, "disk_total_bytes": usage.total}

    def package_installed(self, package: str) -> bool:
        result = self.run(["dpkg-query", "-W", "-f=${Status}", package], check=False)
        return result.returncode == 0 and result.stdout.strip() == "install ok installed"

    def docker_capability(self, package: str) -> bool:
        command = {"docker.io": ["docker", "--version"],
                   "docker-compose-v2": ["docker", "compose", "version", "--short"],
                   "docker-buildx": ["docker", "buildx", "version"]}.get(package)
        if command is None:
            return False
        try:
            return self.run(command, check=False).returncode == 0
        except EnvironmentFailure:
            return False

    def ensure_packages(self) -> None:
        missing = [package for package in self.inputs["packages"] if not self.package_installed(package)]
        missing += [package for package in DOCKER_PACKAGES if not self.docker_capability(package)
                    and not self.package_installed(package)]
        if missing:
            self.run(["apt-get", "update"])
            self.run(["env", "DEBIAN_FRONTEND=noninteractive", "apt-get", "install", "-y", *missing])
        self.run(["dpkg-query", "-W", "-f=${Package}=${Version}\\n", *[
            name for name in self.inputs["packages"] if name not in ("docker.io", "docker-compose-v2", "docker-buildx")]])

    def ensure_docker(self) -> None:
        config_path = self.path("/etc/docker/daemon.json")
        old = read_object(config_path) if config_path.exists() else {}
        desired = dict(old)
        desired["registry-mirrors"] = self.profile.get("docker_registry_mirrors", [])
        active = self.run(["systemctl", "is-active", "--quiet", "docker"], check=False).returncode == 0
        changed = desired != old
        if changed:
            if active and self.run(["docker", "ps", "-q"]).stdout.strip():
                raise EnvironmentFailure("Docker configuration change requires a maintenance window: active containers exist")
            # Validate a sibling temporary configuration before replacing daemon.json.
            with tempfile.TemporaryDirectory(prefix="novarocks-docker-") as directory:
                candidate = Path(directory) / "daemon.json"
                candidate.write_text(json.dumps(desired, sort_keys=True, indent=2) + "\n")
                self.run(["dockerd", "--validate", "--config-file", str(candidate)])
            if config_path.exists():
                backup = config_path.with_name(f"daemon.json.before-novarocks-{uuid.uuid4().hex}")
                shutil.copy2(config_path, backup)
            atomic_text(config_path, json.dumps(desired, sort_keys=True, indent=2) + "\n")
        if changed and active:
            self.run(["systemctl", "restart", "docker"])
        if not active:
            self.run(["systemctl", "enable", "--now", "docker"])
        if self.account.uid != 0:
            groups = self.run(["id", "-nG", self.account.name]).stdout.split()
            if "docker" not in groups:
                self.run(["usermod", "-a", "-G", "docker", self.account.name])
        self.observe_docker()

    def observe_docker(self) -> dict:
        daemon = self.run(["docker", "info", "--format", "{{.OSType}}/{{.Architecture}}"], account=True).stdout.strip()
        if daemon not in ("linux/amd64", "linux/x86_64"):
            raise EnvironmentFailure(f"Docker daemon must be native linux/amd64; found {daemon}")
        mirrors = json.loads(self.run(["docker", "info", "--format", "{{json .RegistryConfig.Mirrors}}"], account=True).stdout)
        normalize = lambda values: [value.rstrip("/") for value in (values or [])]
        if normalize(mirrors) != normalize(self.profile.get("docker_registry_mirrors", [])):
            raise EnvironmentFailure("effective Docker registry mirrors differ from profile; maintenance may be required")
        return {"daemon_platform": daemon, "registry_mirrors": mirrors,
                "version": self.run(["docker", "version", "--format", "{{.Server.Version}}"], account=True).stdout.strip(),
                "compose": self.run(["docker", "compose", "version", "--short"], account=True).stdout.strip(),
                "buildx": self.run(["docker", "buildx", "version"], account=True).stdout.strip()}

    def ensure_locale(self) -> None:
        if self.locale_available():
            return
        path = self.path("/etc/locale.gen")
        text = path.read_text() if path.exists() else ""
        entry = self.locale + " UTF-8"
        pattern = re.compile(r"^\s*#?\s*" + re.escape(entry) + r"\s*$", re.MULTILINE)
        updated = pattern.sub(entry, text) if pattern.search(text) else text.rstrip("\n") + "\n" + entry + "\n"
        atomic_text(path, updated)
        self.run(["locale-gen", self.locale])
        if not self.locale_available():
            raise EnvironmentFailure(f"locale generation did not provide {self.locale}")

    def locale_available(self) -> bool:
        locales = self.run(["locale", "-a"]).stdout.splitlines()
        normalize = lambda value: value.lower().replace("-", "")
        return normalize(self.locale) in {normalize(value) for value in locales}

    def owned_directory(self, path: Path) -> None:
        missing = []
        current = path
        while not current.exists():
            missing.append(current)
            current = current.parent
        path.mkdir(parents=True, exist_ok=True)
        if os.geteuid() == 0:
            for entry in missing:
                os.chown(entry, self.account.uid, self.account.gid)

    def download(self, name: str) -> Path:
        item = self.inputs[name]
        self.owned_directory(self.cache)
        target = self.cache / f"{name}-{item['version']}-{item['sha256']}"
        if target.is_file():
            with target.open("rb") as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() == item["sha256"]:
                    return target
        temporary = target.with_name(target.name + ".partial-" + uuid.uuid4().hex)
        url = self.profile.get("tool_download_urls", {}).get(name, item["url"])
        if not isinstance(url, str) or not url.startswith("https://"):
            raise EnvironmentFailure(f"{name} transport URL must use HTTPS")
        self.run(["curl", "--fail", "--location", "--show-error", "--connect-timeout", "20",
                  "--max-time", str(self.timeout), "--output", str(temporary), url], account=True)
        with temporary.open("rb") as stream:
            actual = hashlib.file_digest(stream, "sha256").hexdigest()
        if actual != item["sha256"]:
            raise EnvironmentFailure(f"{name} download SHA-256 mismatch: {temporary}")
        os.replace(temporary, target)
        return target

    def tool_version(self, tool: str, expected: str) -> bool:
        try:
            result = self.run([tool, "--version"], account=True, check=False)
        except EnvironmentFailure:
            return False
        lines = result.stdout.strip().splitlines()
        return result.returncode == 0 and bool(lines) and bool(re.match(
            r"^" + re.escape(tool) + r" " + re.escape(expected) + r"(?:\s|$)", lines[0]))

    def ensure_rust(self) -> None:
        if not self.tool_version("rustup", self.inputs["rustup"]["version"]):
            source = self.download("rustup")
            # Rustup dispatches its installer/proxy role from argv[0]. Preserve
            # the upstream rustup-init basename while versioning its parent.
            item = self.inputs["rustup"]
            directory = self.cache / f"rustup-{item['version']}-{item['sha256']}.executable"
            self.owned_directory(directory)
            installer = directory / "rustup-init"
            temporary = directory / (".rustup-init." + uuid.uuid4().hex)
            try:
                shutil.copyfile(source, temporary)
                with temporary.open("rb") as candidate:
                    if hashlib.file_digest(candidate, "sha256").hexdigest() != item["sha256"]:
                        raise EnvironmentFailure("rustup installer publication SHA-256 mismatch")
                os.chmod(temporary, 0o755)
                if os.geteuid() == 0:
                    os.chown(temporary, self.account.uid, self.account.gid)
                os.replace(temporary, installer)
            finally:
                temporary.unlink(missing_ok=True)
            self.run([str(installer), "-y", "--no-modify-path", "--profile", "minimal", "--default-toolchain", "none"], account=True)
        channel = self.toolchain["channel"]
        installed = self.run(["rustup", "toolchain", "list"], account=True).stdout.splitlines()
        if not any(line.split()[0].startswith(channel + "-") or line.split()[0] == channel for line in installed if line.split()):
            args = ["rustup", "toolchain", "install", channel, "--profile", self.toolchain.get("profile", "minimal")]
            for component in self.toolchain.get("components", []):
                args += ["--component", component]
            self.run(args, account=True)
        else:
            present = self.run(["rustup", "component", "list", "--installed", "--toolchain", channel], account=True).stdout.splitlines()
            for component in self.toolchain.get("components", []):
                if not any(value == component or value.startswith(component + "-") for value in present):
                    self.run(["rustup", "component", "add", "--toolchain", channel, component], account=True)
        # Pin the selected toolchain in each command; do not change an account's
        # unrelated global default or assume the caller's working directory.
        self.observe_rust()

    def observe_rust(self) -> dict:
        channel = self.toolchain["channel"]
        rustc = self.run(["rustup", "run", channel, "rustc", "--version"], account=True).stdout.strip()
        if not re.match(r"^rustc " + re.escape(channel) + r"(?:\s|$)", rustc):
            raise EnvironmentFailure(f"Rust version does not match repository toolchain {channel}")
        verbose = self.run(["rustup", "run", channel, "rustc", "-vV"], account=True).stdout
        if "host: x86_64-unknown-linux-gnu" not in verbose.splitlines():
            raise EnvironmentFailure("Rust toolchain is not native x86_64-unknown-linux-gnu")
        installed = self.run(["rustup", "component", "list", "--installed", "--toolchain", channel], account=True).stdout.splitlines()
        for component in self.toolchain.get("components", []):
            if not any(value == component or value.startswith(component + "-") for value in installed):
                raise EnvironmentFailure(f"Rust component missing: {component}")
        return {"rustc": rustc, "cargo": self.run(["rustup", "run", channel, "cargo", "--version"], account=True).stdout.strip()}

    def nextest_valid(self) -> bool:
        path = self.account.home / ".cargo/bin/cargo-nextest"
        if not path.is_file():
            return False
        with path.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != self.inputs["nextest"]["executable_sha256"]:
                return False
        return self.tool_version("cargo-nextest", self.inputs["nextest"]["version"])

    def ensure_nextest(self) -> None:
        version = self.inputs["nextest"]["version"]
        if not self.nextest_valid():
            archive = self.download("nextest")
            destination = self.account.home / ".cargo/bin/cargo-nextest"
            self.owned_directory(destination.parent)
            with tarfile.open(archive, "r:gz") as source:
                members = [member for member in source.getmembers()
                           if Path(member.name).name == "cargo-nextest" and member.isfile()]
                if len(members) != 1:
                    raise EnvironmentFailure("nextest archive must contain one regular cargo-nextest executable")
                stream = source.extractfile(members[0])
                if stream is None:
                    raise EnvironmentFailure("nextest executable could not be read")
                temporary = destination.with_name(".cargo-nextest." + uuid.uuid4().hex)
                try:
                    with temporary.open("xb") as output:
                        shutil.copyfileobj(stream, output)
                    with temporary.open("rb") as candidate:
                        if hashlib.file_digest(candidate, "sha256").hexdigest() != self.inputs["nextest"]["executable_sha256"]:
                            raise EnvironmentFailure("nextest archive executable SHA-256 mismatch")
                    os.chmod(temporary, 0o755)
                    if os.geteuid() == 0:
                        os.chown(temporary, self.account.uid, self.account.gid)
                    os.replace(temporary, destination)
                finally:
                    temporary.unlink(missing_ok=True)
        if not self.nextest_valid():
            raise EnvironmentFailure(f"nextest installation did not provide verified {version}")

    def environment_text(self) -> str:
        lines = ["# Managed by NovaRocks tools/dev/linux-env.sh; source this file for non-login shells."]
        for key, value in self.environment.items():
            if key in ("USER", "LOGNAME", "HOME", "PATH", "CARGO_BUILD_JOBS", "PYTHONDONTWRITEBYTECODE"):
                continue
            lines.append(f"export {key}={shlex.quote(value)}")
        lines.append(f'export PATH={shlex.quote(str(self.account.home / ".cargo/bin"))}:"$PATH"')
        # The machine's available count is recomputed by env on each invocation.
        # An explicitly supplied user value remains the authority.
        default_jobs = str(available_cpus()) if self.profile.get("cargo_build_jobs", "available") == "available" else str(self.profile["cargo_build_jobs"])
        lines.append(': "${CARGO_BUILD_JOBS:=' + default_jobs + '}"')
        lines.append("export CARGO_BUILD_JOBS")
        return "\n".join(lines) + "\n"

    def login_profile(self) -> Path:
        shell = Path(self.account.shell).name
        if shell == "bash":
            for name in (".bash_profile", ".bash_login"):
                path = self.account.home / name
                if path.exists():
                    return path
            return self.account.home / ".profile"
        if shell in ("sh", "dash"):
            return self.account.home / ".profile"
        if shell == "zsh":
            return self.account.home / ".zprofile"
        raise EnvironmentFailure(f"unsupported login shell {self.account.shell}; use a supported bash/sh/zsh account and source the managed POSIX environment explicitly")

    def ensure_account_environment(self) -> None:
        self.owned_directory(self.env_path.parent)
        atomic_text(self.env_path, self.environment_text(), account=self.account)
        profile_path = self.login_profile()
        old = profile_path.read_text() if profile_path.exists() else ""
        begin, end = "# BEGIN NOVAROCKS LINUX ENV", "# END NOVAROCKS LINUX ENV"
        block = f'{begin}\n. {shlex.quote(str(self.env_path))}\n{end}'
        pattern = re.compile(r"(?m)^" + re.escape(begin) + r"\n.*?^" + re.escape(end) + r"$", re.DOTALL)
        if (begin in old or end in old) and not pattern.search(old):
            raise EnvironmentFailure("malformed managed environment block in account login profile")
        desired = pattern.sub(lambda match: block, old) if pattern.search(old) else old.rstrip("\n") + "\n\n" + block + "\n"
        atomic_text(profile_path, desired, account=self.account)

    def observe_native(self) -> dict:
        tools = {}
        for executable in ("gcc", "g++", "clang", "cmake", "pkg-config", "protoc"):
            output = self.run([executable, "--version"], account=True, timeout=60).stdout.strip()
            if not output:
                raise EnvironmentFailure(f"native tool has no version output: {executable}")
            tools[executable] = output.splitlines()[0]
        tools["openssl"] = self.run(["pkg-config", "--modversion", "openssl"], account=True, timeout=60).stdout.strip()
        libraries = self.run(["ldconfig", "-p"], account=True, timeout=60).stdout
        paths = [line.rsplit("=>", 1)[-1].strip() for line in libraries.splitlines()
                 if re.search(r"\blibclang(?:-[0-9]+)?\.so", line) and "=>" in line]
        if not paths:
            raise EnvironmentFailure("libclang is absent from the native dynamic loader cache")
        self.run(["python3", "-c", "import ctypes,sys; ctypes.CDLL(sys.argv[1])", paths[0]], account=True, timeout=60)
        tools["libclang"] = paths[0]
        return tools

    def observe_fixture_machine(self, store: str) -> dict:
        # Original owner verifies exact source/BOM version, resources and health.
        # Its check entry is an observation, not admission, repair or a lease.
        result = self.run([str(self.repo / "docker/iceberg-rest/fixture-runtime.sh"),
                           "check", "--store", store, *self.machine_config_arguments()], account=True)
        return json.loads(result.stdout)

    def fixture(self, *, prepare: bool) -> None:
        self.object_store_configuration(prepare=prepare)
        store = str(self.fixture_store)
        if prepare:
            args = [str(self.repo / "docker/fixture-inputs/provision.sh"), "--consumer", self.fixture_consumer,
                    "--platform", "linux/amd64", "--store", store,
                    "--docker-pull-timeout-seconds", str(self.timeout)]
            for logical, transport in sorted(self.profile.get("fixture_image_sources", {}).items()):
                args += ["--image-source", f"{logical}={transport}"]
            for original, transport in sorted(self.profile.get("fixture_artifact_url_prefixes", {}).items()):
                args += ["--artifact-url-prefix", f"{original}={transport}"]
            self.run(args, account=True)
        self.run([str(self.repo / "docker/fixture-inputs/verify.sh"), "--consumer", self.fixture_consumer,
                  "--platform", "linux/amd64", "--store", store], account=True)
        if prepare:
            self.run([str(self.repo / "docker/iceberg-rest/fixture-runtime.sh"), "start", "--store", store,
                      *self.machine_config_arguments()], account=True)
        else:
            snapshot = self.observe_fixture_machine(store)
            if not isinstance(snapshot, dict) or snapshot.get("phase") != "ready":
                raise EnvironmentFailure("fixture machine is absent, under maintenance or unhealthy")
        if prepare:
            self.run([str(self.repo / "docker/iceberg-rest/up.sh")], account=True)
        else:
            result = self.run([str(self.repo / "docker/iceberg-rest/status.sh")], account=True)
            binding = json.loads(result.stdout)
            if not isinstance(binding, dict) or binding.get("ready") is not True:
                raise EnvironmentFailure("worktree fixture publication is not bound and ready")
            if binding.get("machine", {}).get("generation") != snapshot.get("generation") or binding.get("machine", {}).get("version") != snapshot.get("version"):
                raise EnvironmentFailure("worktree publication differs from current fixture machine; attach explicitly")

    def prepare(self) -> dict:
        facts = self.preflight(prepare=True)
        self.ensure_packages()
        self.ensure_locale()
        self.ensure_docker()
        self.ensure_rust()
        self.ensure_nextest()
        self.ensure_account_environment()
        self.observe_native()
        self.fixture(prepare=True)
        return {"action": "prepare", "host": facts, "checks": self.check()}

    def check(self) -> dict:
        facts = self.preflight(prepare=False)
        missing = [package for package in self.inputs["packages"] if package not in (
            "docker.io", "docker-compose-v2", "docker-buildx") and not self.package_installed(package)]
        if missing:
            raise EnvironmentFailure("missing native packages: " + ", ".join(missing))
        if not self.locale_available():
            raise EnvironmentFailure(f"missing locale: {self.locale}")
        native = self.observe_native()
        daemon = self.observe_docker()
        config = read_object(self.path("/etc/docker/daemon.json"))
        if config.get("registry-mirrors", []) != self.profile.get("docker_registry_mirrors", []):
            raise EnvironmentFailure("Docker mirror configuration does not match profile")
        if not self.tool_version("rustup", self.inputs["rustup"]["version"]):
            raise EnvironmentFailure("pinned rustup is missing")
        rust = self.observe_rust()
        if not self.nextest_valid():
            raise EnvironmentFailure("pinned nextest is missing or its executable hash differs")
        if not self.env_path.is_file() or self.env_path.read_text() != self.environment_text():
            raise EnvironmentFailure("account environment is missing or differs from profile")
        profile_text = self.login_profile().read_text()
        expected_block = f"# BEGIN NOVAROCKS LINUX ENV\n. {shlex.quote(str(self.env_path))}\n# END NOVAROCKS LINUX ENV"
        if expected_block not in profile_text:
            raise EnvironmentFailure("account login profile does not source its managed environment")
        self.fixture(prepare=False)
        return {"action": "check", "host": facts, "native": native, "docker": daemon, "rust": rust,
                "nextest": self.inputs["nextest"]["version"], "environment": str(self.env_path)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "check", "env"))
    parser.add_argument("--user", required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--fixture", choices=("iceberg-rest",), required=True)
    parser.add_argument("--object-store-config", type=Path)
    args = parser.parse_args(argv)
    try:
        repo = Path(__file__).resolve().parents[2]
        installer = LinuxEnvironment(repo, read_object(repo / "tools/dev/linux-env.inputs.json"),
                                     read_object(args.profile), Account.lookup(args.user),
                                     object_store_config=args.object_store_config)
        if args.action == "env":
            installer.preflight(prepare=False)
            print(installer.environment_text(), end="")
        else:
            result = installer.prepare() if args.action == "prepare" else installer.check()
            print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except EnvironmentCancelled as error:
        print(f"LinuxEnvironmentCancelled: {error}", file=sys.stderr)
        return 128 + error.signum
    except (EnvironmentFailure, OSError, ValueError, KeyError) as error:
        print(f"LinuxEnvironmentFailed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
