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

"""Installer behavior tests, maintained with the Linux development entry.

The command adapter supplies independent OS/tool observations and installation
side effects. These low-cost tests run on installer changes; real package,
daemon and cold-machine obligations are verified separately by the author.
"""
import hashlib
import importlib.util
import io
import json
import os
import signal
import shutil
import time
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

MODULE = Path(__file__).resolve().parents[1] / "linux_env.py"
spec = importlib.util.spec_from_file_location("linux_env", MODULE)
linux = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = linux
spec.loader.exec_module(linux)


class FakeCommands:
    def __init__(self, inputs):
        self.inputs = inputs
        self.calls = []
        self.packages = set()
        self.tools = {}
        self.components = set()
        self.toolchain = False
        self.active = False
        self.containers = ""
        self.locale = False
        self.bad_download = False
        self.fail_install = False
        self.download_failure = False
        self.daemon_platform = "linux/x86_64"
        self.daemon_mirrors = ["https://mirror.test/"]
        self.required_version = "v1"
        self.machine = {"phase": "ready", "observed_healthy": True, "generation": "g1", "version": "v1"}
        self.publication = {"ready": True, "machine": {"generation": "g1", "version": "v1"}}
        self.payloads = {"rustup": b"rustup-executable"}
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            member = tarfile.TarInfo("cargo-nextest")
            member.size = len(b"nextest-executable")
            archive.addfile(member, io.BytesIO(b"nextest-executable"))
        self.payloads["nextest"] = buffer.getvalue()
        inputs["nextest"]["executable_sha256"] = hashlib.sha256(b"nextest-executable").hexdigest()
        for name, data in self.payloads.items():
            inputs[name]["sha256"] = hashlib.sha256(data).hexdigest()

    def run(self, args, *, environment=None, timeout=600, check=True):
        self.calls.append((list(args), environment))
        if args[0] == "runuser":
            split = args.index("-i")
            args = args[split + 1:]
            environment = {}
            while args and "=" in args[0]:
                key, value = args.pop(0).split("=", 1)
                environment[key] = value
        command = args[0]
        code, output = 0, ""
        if command == "findmnt":
            output = "ext4\n"
        elif command == "dpkg-query":
            if args[2] == "-f=${Status}":
                code = 0 if args[-1] in self.packages else 1
                output = "install ok installed" if code == 0 else ""
            else:
                output = "\n".join(f"{name}=1.0" for name in args[3:])
        elif command == "apt-get":
            pass
        elif command == "env" and args[1] == "DEBIAN_FRONTEND=noninteractive":
            if self.fail_install:
                self.fail_install = False
                code = 100
            else:
                self.packages.update(args[5:])
                self.tools.update({"docker": "29.1.3", "compose": "2.40.3", "buildx": "0.30.1"})
        elif command == "systemctl":
            if args[1] == "is-active":
                code = 0 if self.active else 3
            else:
                self.active = True
        elif command == "dockerd":
            json.loads(Path(args[-1]).read_text())
        elif command == "docker":
            if args[1:3] == ["ps", "-q"]:
                selected = environment if environment is not None else os.environ
                local = selected.get("DOCKER_CONTEXT", "default") == "default" and not selected.get("DOCKER_HOST")
                output = self.containers if local else ""
            elif args[1] == "info":
                output = json.dumps(self.daemon_mirrors) if "RegistryConfig.Mirrors" in args[-1] else self.daemon_platform
            elif args[1] == "version":
                output = self.tools.get("docker", "")
                code = 0 if output and self.active else 1
            elif args[1] == "--version":
                output = self.tools.get("docker", "")
                code = 0 if output else 1
            else:
                key = args[1]
                output = self.tools.get(key, "")
                code = 0 if output else 1
        elif command in ("gcc", "g++", "clang", "cmake", "protoc"):
            output = command + " 1.0\n"
        elif command == "pkg-config":
            output = "3.0.0\n"
        elif command == "ldconfig":
            output = "libclang-21.so (libc6,x86-64) => /lib/libclang-21.so\n"
        elif command == "python3":
            if "ctypes.CDLL" in args[2]:
                pass
            else:
                if self.machine["version"] != self.required_version:
                    raise linux.EnvironmentFailure("fixture machine version differs from current source/BOM")
                if not self.machine["observed_healthy"]:
                    raise linux.EnvironmentFailure("fixture machine is unhealthy")
                output = json.dumps(self.machine)
        elif command == "id":
            output = "users docker\n"
        elif command == "usermod":
            pass
        elif command == "locale":
            output = "C\nC.utf8\n" + ("zh_CN.utf8\n" if self.locale else "")
        elif command == "locale-gen":
            self.locale = True
        elif command == "curl":
            if self.download_failure:
                raise linux.EnvironmentFailure("download timed out")
            name = "nextest" if "nextest" in args[-1] else "rustup"
            Path(args[args.index("--output") + 1]).write_bytes(b"corrupt" if self.bad_download else self.payloads[name])
        elif Path(command).name == "rustup-init":
            self.tools["rustup"] = "1.28.2"
        elif command == "rustup":
            if args[1] == "--version":
                version = self.tools.get("rustup")
                code = 0 if version else 1
                output = "rustup " + version if version else ""
            elif args[1:3] == ["toolchain", "list"]:
                output = "1.98.1-x86_64-unknown-linux-gnu\n" if self.toolchain else ""
            elif args[1:3] == ["toolchain", "install"]:
                self.toolchain = True
                self.components.update({"rustfmt", "clippy"})
            elif args[1:3] == ["component", "list"]:
                output = "\n".join(sorted(self.components))
            elif args[1:3] == ["component", "add"]:
                self.components.add(args[-1])
            elif args[1] == "run":
                if not self.toolchain:
                    code = 1
                elif args[-1] == "-vV":
                    output = "rustc 1.98.1\nhost: x86_64-unknown-linux-gnu\n"
                else:
                    output = args[3] + " 1.98.1 (test)\n"
        elif command == "cargo-nextest":
            # The real executable is installed by the Python filesystem owner.
            path = Path(environment["HOME"]) / ".cargo/bin/cargo-nextest"
            if path.exists():
                output = "cargo-nextest 0.9.148 (test)\n"
            else:
                code = 1
        elif command.endswith("fixture-runtime.sh"):
            if args[1] == "check":
                if self.machine["version"] != self.required_version:
                    raise linux.EnvironmentFailure("fixture machine version differs from current source/BOM")
                if not self.machine["observed_healthy"]:
                    raise linux.EnvironmentFailure("fixture machine is unhealthy")
            output = json.dumps(self.machine)
        elif command.endswith("status.sh"):
            output = json.dumps(self.publication)
        elif command.endswith(("provision.sh", "verify.sh", "up.sh")):
            output = "{}"
        else:
            raise AssertionError(f"unexpected command: {args}")
        result = subprocess.CompletedProcess(args, code, output, "")
        if code and check:
            raise linux.EnvironmentFailure(f"command failed ({code}): {args}")
        return result


class EnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        (self.repo / "rust-toolchain.toml").write_text('[toolchain]\nchannel="1.98.1"\nprofile="minimal"\ncomponents=["rustfmt","clippy"]\n')
        self.home = self.root / "target-account"
        self.home.mkdir()
        self.system = self.root / "system"
        (self.system / "etc").mkdir(parents=True)
        (self.system / "etc/os-release").write_text('ID=ubuntu\nVERSION_ID="26.04"\n')
        self.inputs = {"schema": 1, "packages": ["build-essential", "libssl-dev"],
                       "rustup": {"version": "1.28.2", "url": "https://official.test/rustup", "sha256": "0" * 64},
                       "nextest": {"version": "0.9.148", "url": "https://official.test/nextest", "sha256": "0" * 64}}
        self.profile = {"schema": 1, "support": {"os_id": "ubuntu", "version_id": "26.04", "platform": "linux/amd64"},
                        "docker_registry_mirrors": ["https://mirror.test"], "rustup_dist_server": "https://rust.test",
                        "cargo_build_jobs": "available", "locale": "zh_CN.UTF-8", "download_timeout_seconds": 30}
        self.commands = FakeCommands(self.inputs)
        self.account = linux.Account("test", 0, 0, self.home)
        for target, value in (("platform.machine", "x86_64"), ("sys.platform", "linux"), ("os.geteuid", 0),
                              ("available_cpus", 6)):
            patcher = patch.object(linux, target, return_value=value) if "." not in target else patch("linux_env." + target, return_value=value)
            if target == "sys.platform":
                patcher = patch("linux_env.sys.platform", value)
            patcher.start()
            self.addCleanup(patcher.stop)
        chown = patch("linux_env.os.chown")
        self.chown = chown.start()
        self.addCleanup(chown.stop)
        clean_env = patch.dict(os.environ, {}, clear=True)
        clean_env.start()
        self.addCleanup(clean_env.stop)
        self.installer = linux.LinuxEnvironment(self.repo, self.inputs, self.profile, self.account,
                                               commands=self.commands, system_root=self.system)

    def prepare(self):
        return self.installer.prepare()

    def commands_matching(self, name):
        return [args for args, _ in self.commands.calls if args[0] == name]

    def test_fixture_artifact_transports_are_forwarded_only_during_prepare(self):
        self.profile["fixture_artifact_url_prefixes"] = {
            "https://upstream.invalid/maven2/": "https://mirror.invalid/public/",
            "https://files.invalid/packages/": "https://mirror.invalid/packages/"}
        self.installer.fixture(prepare=True)
        provision = [args for args, _ in self.commands.calls if args[0].endswith("/provision.sh")]
        self.assertEqual(1, len(provision))
        for original, transport in self.profile["fixture_artifact_url_prefixes"].items():
            index = provision[0].index(f"{original}={transport}")
            self.assertEqual("--artifact-url-prefix", provision[0][index - 1])
        self.commands.calls.clear()
        self.installer.fixture(prepare=False)
        self.assertFalse(any("--artifact-url-prefix" in args for args, _ in self.commands.calls))
        self.assertFalse(any(args[0].endswith("/provision.sh") for args, _ in self.commands.calls))

    def test_artifact_transport_profile_requires_string_mapping(self):
        for value in ([], {"https://upstream.invalid/": None}, {3: "https://mirror.invalid/"}):
            with self.subTest(value=value), self.assertRaises(linux.EnvironmentFailure):
                linux.LinuxEnvironment(self.repo, self.inputs, {**self.profile, "fixture_artifact_url_prefixes": value},
                    self.account, commands=self.commands, system_root=self.system)

    def cloud_installer(self, *, config=None):
        author = self.repo / "docker/iceberg-rest/oss_ops.py"
        if not author.exists():
            author.parent.mkdir(parents=True)
            shutil.copyfile(MODULE.parents[2] / "docker/iceberg-rest/oss_ops.py", author)
        config_path = self.root / "object-store-input.json"
        if config is None:
            config = {"kind": "aliyun-oss", "region": "cn-beijing", "bucket": "fixture-bucket",
                      "role_name": "fixture-role", "root_prefix": "development"}
        config_path.write_text(json.dumps(config))
        installer = linux.LinuxEnvironment(
            self.repo, self.inputs, dict(self.profile, object_store_kind="aliyun-oss"), self.account,
            commands=self.commands, system_root=self.system, object_store_config=config_path)
        return installer, config_path

    def test_cloud_profile_requires_an_explicit_absolute_config_before_commands(self):
        profile = dict(self.profile, object_store_kind="aliyun-oss")
        for config_path in (None, Path("relative.json")):
            with self.assertRaisesRegex(linux.EnvironmentFailure, "absolute --object-store-config"):
                linux.LinuxEnvironment(self.repo, self.inputs, profile, self.account,
                                       commands=self.commands, system_root=self.system,
                                       object_store_config=config_path)
        self.assertEqual(self.commands.calls, [])

    def test_cloud_configuration_uses_closed_shared_validation_without_fallback(self):
        config = {"kind": "aliyun-oss", "region": "cn-beijing", "bucket": "fixture-bucket",
                  "role_name": "fixture-role", "root_prefix": "development", "access_key": "do-not-use"}
        with self.assertRaisesRegex(linux.EnvironmentFailure, "invalid OSS configuration") as failure:
            self.cloud_installer(config=config)
        self.assertNotIn("do-not-use", str(failure.exception))
        self.assertEqual(self.commands.calls, [])
        self.assertFalse(self.installer.cache.exists())

    def test_minio_profile_does_not_ignore_an_object_store_argument(self):
        with self.assertRaisesRegex(linux.EnvironmentFailure, "requires an aliyun-oss profile"):
            linux.LinuxEnvironment(self.repo, self.inputs, self.profile, self.account,
                                   commands=self.commands, system_root=self.system,
                                   object_store_config=self.root / "unread.json")
        self.assertEqual(self.commands.calls, [])

    def test_cloud_prepare_selects_oss_closure_and_forwards_machine_config(self):
        installer, _ = self.cloud_installer()
        installer.prepare()
        expected = {"kind": "aliyun-oss", "region": "cn-beijing", "bucket": "fixture-bucket",
                    "role_name": "fixture-role", "root_prefix": "development/"}
        self.assertEqual(json.loads(installer.object_store_path.read_text()), expected)
        self.assertEqual(json.loads(installer.fixture_machine_config_path.read_text()), {"object_store": expected})
        for args, environment in self.commands.calls:
            if args[0].endswith(("provision.sh", "verify.sh")):
                self.assertEqual(args[args.index("--consumer") + 1], "iceberg-rest-oss")
            if args[0].endswith("fixture-runtime.sh"):
                self.assertEqual(args[args.index("--config") + 1], str(installer.fixture_machine_config_path))
            if args[0].endswith(("provision.sh", "verify.sh", "fixture-runtime.sh", "up.sh", "status.sh")):
                self.assertEqual(environment["NOVA_ENV_OBJECT_STORE_CONFIG"], str(installer.object_store_path))
        self.assertIn("NOVA_ENV_OBJECT_STORE_CONFIG", installer.environment_text())

    def test_cloud_repeat_is_idempotent_and_check_does_not_write_or_install(self):
        installer, _ = self.cloud_installer()
        installer.prepare()
        paths = (installer.object_store_path, installer.fixture_machine_config_path, installer.env_path)
        before = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in paths}
        installer.prepare()
        self.assertEqual(before, {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in paths})
        self.commands.calls.clear()
        tree = {str(path): path.read_bytes() for path in self.root.rglob("*") if path.is_file()}
        installer.check()
        self.assertEqual(tree, {str(path): path.read_bytes() for path in self.root.rglob("*") if path.is_file()})
        for args, _ in self.commands.calls:
            self.assertFalse(args[0].endswith(("provision.sh", "up.sh")))
            self.assertNotIn(args[0], ("curl", "apt-get", "locale-gen", "systemctl", "dockerd"))
            self.assertNotIn("install", args)

    def test_cloud_check_missing_or_changed_copy_fails_without_repair(self):
        installer, _ = self.cloud_installer()
        before = sorted(str(path) for path in self.root.rglob("*"))
        with self.assertRaisesRegex(linux.EnvironmentFailure, "prepared OSS configuration"):
            installer.fixture(prepare=False)
        self.assertEqual(before, sorted(str(path) for path in self.root.rglob("*")))
        self.assertEqual(self.commands.calls, [])
        installer.fixture(prepare=True)
        changed = json.loads(installer.object_store_path.read_text())
        changed["role_name"] = "other-role"
        installer.object_store_path.write_text(json.dumps(changed))
        self.commands.calls.clear()
        with self.assertRaisesRegex(linux.EnvironmentFailure, "differs from input"):
            installer.fixture(prepare=False)
        self.assertEqual(json.loads(installer.object_store_path.read_text()), changed)
        self.assertEqual(self.commands.calls, [])

    def test_cloud_check_compares_existing_copies_with_new_input(self):
        installer, config_path = self.cloud_installer()
        installer.fixture(prepare=True)
        original = installer.object_store_path.read_bytes()
        config = json.loads(config_path.read_text())
        config["bucket"] = "different-bucket"
        changed, _ = self.cloud_installer(config=config)
        self.commands.calls.clear()
        with self.assertRaisesRegex(linux.EnvironmentFailure, "differs from input"):
            changed.fixture(prepare=False)
        self.assertEqual(installer.object_store_path.read_bytes(), original)
        self.assertEqual(self.commands.calls, [])

    def test_generic_profile_keeps_local_minio_consumer(self):
        self.installer.fixture(prepare=True)
        for args, environment in self.commands.calls:
            if args[0].endswith(("provision.sh", "verify.sh")):
                self.assertEqual(args[args.index("--consumer") + 1], "iceberg-rest")
                self.assertNotIn("NOVA_ENV_OBJECT_STORE_CONFIG", environment)
            if args[0].endswith("fixture-runtime.sh"):
                self.assertNotIn("--config", args)

    def test_cli_forwards_explicit_object_store_config_without_writing_on_env(self):
        installer, config_path = self.cloud_installer()
        profile_path = self.root / "cloud-profile.json"
        profile_path.write_text(json.dumps(dict(self.profile, object_store_kind="aliyun-oss")))
        before = {str(path): path.read_bytes() for path in self.root.rglob("*") if path.is_file()}
        with patch.object(linux, "LinuxEnvironment", return_value=installer) as factory, \
                patch.object(linux.Account, "lookup", return_value=self.account), \
                patch("linux_env.sys.stdout", new_callable=io.StringIO) as output:
            result = linux.main(["env", "--user", "test", "--profile", str(profile_path),
                                 "--fixture", "iceberg-rest", "--object-store-config", str(config_path)])
        self.assertEqual(result, 0)
        self.assertEqual(factory.call_args.kwargs["object_store_config"], config_path)
        self.assertIn(str(installer.object_store_path), output.getvalue())
        self.assertEqual(before, {str(path): path.read_bytes() for path in self.root.rglob("*") if path.is_file()})

    def test_empty_check_is_read_only_and_does_not_download(self):
        before = sorted(str(p.relative_to(self.root)) for p in self.root.rglob("*"))
        with self.assertRaisesRegex(linux.EnvironmentFailure, "missing native packages"):
            self.installer.check()
        self.assertEqual(before, sorted(str(p.relative_to(self.root)) for p in self.root.rglob("*")))
        self.assertFalse(self.commands_matching("curl"))
        self.assertFalse(self.commands_matching("apt-get"))
        self.assertFalse(self.commands_matching("systemctl"))

    def test_prepare_and_identical_repeat_keep_configuration_and_services(self):
        self.prepare()
        config = self.system / "etc/docker/daemon.json"
        paths = [config, self.home / ".profile", self.installer.env_path]
        mtimes = {p: p.stat().st_mtime_ns for p in paths}
        self.commands.calls.clear()
        self.prepare()
        self.assertEqual(mtimes, {p: p.stat().st_mtime_ns for p in paths})
        self.assertFalse(self.commands_matching("apt-get"))
        self.assertFalse(self.commands_matching("curl"))
        self.assertFalse(any(args[1] in ("restart", "enable") for args in self.commands_matching("systemctl")))
        self.assertEqual(self.installer.environment["CARGO_BUILD_JOBS"], "6")
        self.assertEqual(self.installer.environment["RUSTUP_TOOLCHAIN"], "1.98.1")
        self.assertEqual(self.installer.check()["action"], "check")

    def test_fixture_transfers_use_the_configured_download_deadline(self):
        self.profile["download_timeout_seconds"] = 317
        self.installer.timeout = 317
        self.installer.fixture(prepare=True)
        calls = [args for args, _ in self.commands.calls if args[0].endswith("provision.sh")]
        self.assertEqual(len(calls), 1)
        position = calls[0].index("--docker-pull-timeout-seconds")
        self.assertEqual(calls[0][position + 1], "317")

    def test_partial_install_failure_can_be_prepared_again(self):
        self.commands.fail_install = True
        with self.assertRaisesRegex(linux.EnvironmentFailure, "command failed"):
            self.prepare()
        self.assertFalse(self.installer.env_path.exists())
        self.prepare()
        self.assertEqual(self.installer.check()["action"], "check")

    def test_rustup_uses_verified_installer_with_upstream_dispatch_basename(self):
        self.installer.ensure_rust()
        installers = [args[0] for args, _ in self.commands.calls if Path(args[0]).name == "rustup-init"]
        self.assertEqual(len(installers), 1)
        path = Path(installers[0])
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), self.inputs["rustup"]["sha256"])
        self.assertIn(self.inputs["rustup"]["sha256"], str(path.parent))
        self.assertTrue(path.stat().st_mode & 0o111)
        self.assertEqual(self.commands.tools["rustup"], "1.28.2")

    def test_download_checksum_failure_does_not_install_and_retry_rechecks(self):
        self.commands.bad_download = True
        with self.assertRaisesRegex(linux.EnvironmentFailure, "SHA-256 mismatch"):
            self.installer.ensure_rust()
        self.assertNotIn("rustup", self.commands.tools)
        self.assertTrue(list(self.installer.cache.glob("*.partial-*")))
        self.commands.bad_download = False
        self.installer.ensure_rust()
        self.assertEqual(self.commands.tools["rustup"], "1.28.2")

    def test_download_deadline_failure_is_not_a_success_marker(self):
        self.commands.download_failure = True
        with self.assertRaisesRegex(linux.EnvironmentFailure, "timed out"):
            self.installer.ensure_rust()
        self.assertNotIn("rustup", self.commands.tools)
        self.assertFalse(self.installer.env_path.exists())

    def test_transport_override_keeps_content_hash(self):
        self.profile["tool_download_urls"] = {"rustup": "https://alternate.test/rustup"}
        path = self.installer.download("rustup")
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), self.inputs["rustup"]["sha256"])
        self.assertEqual(self.commands_matching("curl")[-1][-1], "https://alternate.test/rustup")

    def test_existing_daemon_configuration_is_merged_and_backed_up(self):
        path = self.system / "etc/docker/daemon.json"
        path.parent.mkdir()
        path.write_text('{"log-driver":"local","features":{"buildkit":true}}\n')
        self.installer.ensure_packages()
        self.installer.ensure_docker()
        self.assertEqual(json.loads(path.read_text())["features"], {"buildkit": True})
        self.assertEqual(json.loads(path.read_text())["log-driver"], "local")
        backups = list(path.parent.glob("daemon.json.before-novarocks-*"))
        self.assertEqual(len(backups), 1)
        self.assertNotIn("registry-mirrors", json.loads(backups[0].read_text()))

    def test_changed_daemon_configuration_refuses_active_containers_before_write(self):
        path = self.system / "etc/docker/daemon.json"
        path.parent.mkdir()
        old = '{"log-driver":"local"}\n'
        path.write_text(old)
        self.commands.active = True
        self.commands.containers = "existing-container\n"
        with self.assertRaisesRegex(linux.EnvironmentFailure, "maintenance window"):
            self.installer.ensure_docker()
        self.assertEqual(path.read_text(), old)
        self.assertFalse(self.commands_matching("dockerd"))
        self.assertEqual(self.commands_matching("systemctl"), [["systemctl", "is-active", "--quiet", "docker"]])

    def test_daemon_maintenance_observes_local_containers_despite_remote_caller_context(self):
        path = self.system / "etc/docker/daemon.json"
        path.parent.mkdir()
        original = '{"log-driver":"local"}\n'
        path.write_text(original)
        self.commands.active = True
        self.commands.containers = "local-container\n"
        with patch.dict(os.environ, {"DOCKER_HOST": "tcp://remote.test:2376", "DOCKER_CONTEXT": "remote"}):
            with self.assertRaisesRegex(linux.EnvironmentFailure, "maintenance window"):
                self.installer.ensure_docker()
        self.assertEqual(path.read_text(), original)
        self.assertFalse(self.commands_matching("dockerd"))

    def test_healthy_external_engine_is_not_replaced_by_distribution_package(self):
        self.commands.packages.update(self.inputs["packages"])
        self.commands.tools.update({"docker": "29.1.3", "compose": "2.40.3", "buildx": "0.30.1"})
        self.installer.ensure_packages()
        self.assertFalse(self.commands_matching("apt-get"))
        self.assertNotIn("docker.io", self.commands.packages)

    def test_account_environment_does_not_inherit_root_home_or_cargo_config(self):
        cargo_config = self.home / ".cargo/config.toml"
        cargo_config.parent.mkdir()
        cargo_config.write_text('[net]\ngit-fetch-with-cli=true\n')
        with patch.dict(os.environ, {"HOME": "/root", "XDG_CACHE_HOME": "/root/cache", "CARGO_HOME": "/root/cargo", "CARGO_BUILD_JOBS": "3"}):
            installer = linux.LinuxEnvironment(self.repo, self.inputs, self.profile,
                linux.Account("different-user", 1234, 1234, self.home), commands=self.commands, system_root=self.system)
            installer.ensure_account_environment()
            installer.run(["locale", "-a"], account=True)
        args, env = self.commands.calls[-1]
        self.assertEqual(args[:4], ["runuser", "-u", "different-user", "--"])
        self.assertIn("HOME=" + str(self.home), args)
        self.assertNotIn("XDG_CACHE_HOME=/root/cache", args)
        self.assertIn("CARGO_BUILD_JOBS=3", args)
        self.assertEqual(cargo_config.read_text(), '[net]\ngit-fetch-with-cli=true\n')
        self.assertIn('CARGO_BUILD_JOBS:=6', installer.env_path.read_text())
        self.assertTrue(self.chown.called)

    def test_environment_preserves_profile_and_explicit_job_override(self):
        path = self.home / ".profile"
        path.write_text('export MY_TOOL=keep\n')
        self.installer.ensure_account_environment()
        self.assertTrue(path.read_text().startswith('export MY_TOOL=keep\n'))
        result = subprocess.run(["/bin/sh", "-c", '. "$1"; printf "%s" "$CARGO_BUILD_JOBS"', "sh", str(self.installer.env_path)],
                                env={"PATH": "/bin:/usr/bin", "CARGO_BUILD_JOBS": "9"}, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "9")

    def test_existing_toolchain_restores_missing_component_without_reinstall(self):
        self.commands.tools["rustup"] = "1.28.2"
        self.commands.toolchain = True
        self.commands.components = {"rustfmt"}
        self.installer.ensure_rust()
        self.assertIn("clippy", self.commands.components)
        self.assertFalse(any(args[1:3] == ["toolchain", "install"] for args in self.commands_matching("rustup")))
        self.assertFalse(self.commands_matching("curl"))

    def test_check_is_offline_and_does_not_restart_or_republish(self):
        self.prepare()
        self.commands.calls.clear()
        self.installer.check()
        for args, _ in self.commands.calls:
            self.assertNotIn(args[0], ("curl", "apt-get", "locale-gen", "systemctl", "dockerd"))
            self.assertFalse(args[0].endswith(("provision.sh", "up.sh")))
            self.assertNotIn("install", args)
            self.assertNotIn("restart", args)

    def test_check_refuses_unhealthy_machine_or_stale_publication(self):
        self.prepare()
        self.commands.machine["observed_healthy"] = False
        with self.assertRaisesRegex(linux.EnvironmentFailure, "unhealthy"):
            self.installer.check()
        self.commands.machine["observed_healthy"] = True
        self.commands.publication["machine"]["generation"] = "stale"
        with self.assertRaisesRegex(linux.EnvironmentFailure, "differs from current"):
            self.installer.check()

    def test_nextest_same_version_with_wrong_bytes_is_replaced_from_verified_archive(self):
        destination = self.home / ".cargo/bin/cargo-nextest"
        destination.parent.mkdir(parents=True)
        destination.write_bytes(b"wrong-content-with-same-version")
        self.installer.ensure_nextest()
        self.assertEqual(destination.read_bytes(), b"nextest-executable")
        self.assertEqual(len(self.commands_matching("curl")), 1)
        self.commands.calls.clear()
        self.installer.ensure_nextest()
        self.assertFalse(self.commands_matching("curl"))

    def test_archive_executable_hash_mismatch_preserves_existing_binary(self):
        destination = self.home / ".cargo/bin/cargo-nextest"
        destination.parent.mkdir(parents=True)
        destination.write_bytes(b"previous-executable")
        self.inputs["nextest"]["executable_sha256"] = "f" * 64
        with self.assertRaisesRegex(linux.EnvironmentFailure, "archive executable SHA-256"):
            self.installer.ensure_nextest()
        self.assertEqual(destination.read_bytes(), b"previous-executable")

    def test_check_detects_missing_login_source_without_repairing_it(self):
        self.prepare()
        profile = self.home / ".profile"
        profile.write_text("# unrelated profile\n")
        with self.assertRaisesRegex(linux.EnvironmentFailure, "login profile"):
            self.installer.check()
        self.assertEqual(profile.read_text(), "# unrelated profile\n")

    def test_account_profile_keeps_original_private_permissions(self):
        path = self.home / ".profile"
        path.write_text("# private profile\n")
        path.chmod(0o600)
        self.installer.ensure_account_environment()
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_bash_login_precedence_and_zsh_profile_are_wired(self):
        bash_profile = self.home / ".bash_profile"
        bash_profile.write_text("# selected bash login\n")
        bash_login = self.home / ".bash_login"
        bash_login.write_text("# ignored by bash\n")
        self.installer.ensure_account_environment()
        self.assertIn("# BEGIN NOVAROCKS LINUX ENV", bash_profile.read_text())
        self.assertEqual(bash_login.read_text(), "# ignored by bash\n")
        self.assertFalse((self.home / ".profile").exists())
        zsh = linux.LinuxEnvironment(self.repo, self.inputs, self.profile,
            linux.Account("test", 0, 0, self.home, "/usr/bin/zsh"), commands=self.commands, system_root=self.system)
        zsh.ensure_account_environment()
        self.assertIn("# BEGIN NOVAROCKS LINUX ENV", (self.home / ".zprofile").read_text())

    def test_unsupported_login_shell_requires_explicit_action(self):
        installer = linux.LinuxEnvironment(self.repo, self.inputs, self.profile,
            linux.Account("test", 0, 0, self.home, "/usr/bin/fish"), commands=self.commands, system_root=self.system)
        with self.assertRaisesRegex(linux.EnvironmentFailure, "unsupported login shell"):
            installer.ensure_account_environment()

    def test_check_current_source_rejects_stale_healthy_machine_and_matching_publication(self):
        self.prepare()
        self.commands.required_version = "new-source-version"
        with self.assertRaisesRegex(linux.EnvironmentFailure, "differs from current source/BOM"):
            self.installer.check()

    def test_cancelled_version_observation_cannot_trigger_installation(self):
        original = self.commands.run

        def cancel_version(args, **kwargs):
            if args[:2] == ["rustup", "--version"]:
                self.commands.calls.append((list(args), kwargs.get("environment")))
                raise linux.EnvironmentCancelled(signal.SIGTERM, "cancelled version observation")
            return original(args, **kwargs)

        with patch.object(self.commands, "run", side_effect=cancel_version):
            with self.assertRaises(linux.EnvironmentCancelled):
                self.prepare()
        self.assertEqual(self.commands.calls[-1][0], ["rustup", "--version"])
        self.assertFalse(self.commands_matching("curl"))
        self.assertFalse(self.installer.env_path.exists())
        self.assertFalse(any(args[0].endswith("provision.sh") for args, _ in self.commands.calls))

    def test_cancelled_docker_probe_is_terminal_and_not_missing_capability(self):
        with patch.object(self.commands, "run", side_effect=linux.EnvironmentCancelled(signal.SIGINT, "cancelled Docker observation")):
            with self.assertRaises(linux.EnvironmentCancelled):
                self.installer.docker_capability("docker.io")
        self.assertFalse(self.commands_matching("apt-get"))

    def test_check_refuses_remote_or_emulated_daemon(self):
        self.prepare()
        self.commands.daemon_platform = "linux/aarch64"
        with self.assertRaisesRegex(linux.EnvironmentFailure, "must be native"):
            self.installer.check()


class CommandLifecycleTests(unittest.TestCase):
    def test_term_is_forwarded_and_original_child_exit_is_observed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ready, closed = root / "ready", root / "closed"
            child = """import os,signal,sys,time
from pathlib import Path
ready,closed=map(Path,sys.argv[1:])
def stop(signum,frame):
    closed.write_text('original child closed')
    raise SystemExit(0)
signal.signal(signal.SIGTERM,stop)
ready.write_text(str(os.getpid()))
while True: time.sleep(1)
"""
            driver = """import importlib.util,sys
spec=importlib.util.spec_from_file_location('linux_env',sys.argv[1])
module=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=module
spec.loader.exec_module(module)
module.Commands().run([sys.executable,'-c',sys.argv[2],sys.argv[3],sys.argv[4]])
"""
            process = subprocess.Popen([sys.executable, "-c", driver, str(MODULE), child, str(ready), str(closed)],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                deadline = time.monotonic() + 10
                while not ready.exists() and time.monotonic() < deadline and process.poll() is None:
                    time.sleep(0.01)
                self.assertTrue(ready.exists(), "original child failed to start")
                child_pid = int(ready.read_text())
                process.send_signal(signal.SIGTERM)
                stdout, stderr = process.communicate(timeout=10)
                self.assertNotEqual(process.returncode, 0)
                self.assertTrue(closed.exists())
                self.assertIn("exited 0", stderr)
                with self.assertRaises(ProcessLookupError):
                    os.kill(child_pid, 0)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()
                if ready.exists():
                    try:
                        os.killpg(int(ready.read_text()), signal.SIGTERM)
                    except ProcessLookupError:
                        pass


class ShellSeedTests(unittest.TestCase):
    def test_python_missing_check_and_env_never_run_package_manager(self):
        script = MODULE.with_name("linux-env.sh")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "dirname").symlink_to("/usr/bin/dirname")
            sentinel = root / "apt-called"
            apt = root / "apt-get"
            apt.write_text(f'#!/bin/sh\n: > "{sentinel}"\nexit 99\n')
            apt.chmod(0o755)
            for action in ("check", "env"):
                result = subprocess.run(["/bin/sh", str(script), action, "--user", "test", "--profile", "profile", "--fixture", "iceberg-rest"],
                                        env={"PATH": str(root)}, capture_output=True, text=True)
                self.assertEqual(result.returncode, 2)
                self.assertIn("Python 3 is missing", result.stderr)
                self.assertFalse(sentinel.exists())


if __name__ == "__main__":
    unittest.main()
