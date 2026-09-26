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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise capability mutations using real Cargo metadata, without compiling."""

import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path


CHECKER = Path(__file__).resolve().parents[1] / "check-local-program-dependency-boundary.py"
spec = importlib.util.spec_from_file_location("local_program_boundary", CHECKER)
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class BoundaryTests(unittest.TestCase):
    def fixture(self, root_dependency="", types_dependency="", extra="", build_script=False):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        (root / "Cargo.toml").write_text(
            '[workspace]\nresolver = "2"\nmembers = ["program", "types", "runtime"]\n')
        for directory, name, dependencies in (
                ("program", "novarocks-local-program", root_dependency),
                ("types", "novarocks-types", types_dependency),
                ("runtime", "tokio", "")):
            package = root / directory
            (package / "src").mkdir(parents=True)
            (package / "src/lib.rs").write_text("")
            (package / "Cargo.toml").write_text(
                f'[package]\nname = "{name}"\nversion = "0.1.0"\nedition = "2024"\n'
                + "[dependencies]\n" + dependencies
                + (extra if directory == "program" else ""))
        if build_script:
            (root / "program/build.rs").write_text("fn main() {}\n")
        subprocess.run(["cargo", "generate-lockfile", "--offline", "--manifest-path",
                        str(root / "Cargo.toml")], check=True, capture_output=True)
        return subprocess.run(["python3", str(CHECKER), "--manifest-path",
                               str(root / "Cargo.toml")], capture_output=True, text=True)

    def assert_rejected(self, result, diagnostic):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(diagnostic, result.stderr)

    def test_pure_contract_and_unrelated_workspace_runtime_are_allowed(self):
        result = self.fixture('novarocks-types = { path = "../types" }\n')
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_transitive_runtime_is_rejected(self):
        result = self.fixture('novarocks-types = { path = "../types" }\n',
                              'tokio = { path = "../runtime" }\n')
        self.assert_rejected(result, "runtime/wire/provider/storage capability: tokio")

    def test_optional_renamed_runtime_is_not_hidden(self):
        result = self.fixture('executor = { package = "tokio", path = "../runtime", optional = true }\n')
        self.assert_rejected(result, "runtime/wire/provider/storage capability: tokio")

    def test_inactive_target_runtime_is_not_hidden(self):
        result = self.fixture(extra='[target.\'cfg(target_os = "none")\'.dependencies]\n'
                              'tokio = { path = "../runtime" }\n')
        self.assert_rejected(result, "runtime/wire/provider/storage capability: tokio")

    def test_build_runtime_is_rejected(self):
        result = self.fixture(extra='[build-dependencies]\ntokio = { path = "../runtime" }\n')
        self.assert_rejected(result, "runtime/wire/provider/storage capability: tokio")

    def test_dev_runtime_is_rejected(self):
        result = self.fixture(extra='[dev-dependencies]\ntokio = { path = "../runtime" }\n')
        self.assert_rejected(result, "runtime/wire/provider/storage capability: tokio")

    def test_transitive_dev_runtime_is_rejected(self):
        result = self.fixture(types_dependency='tokio = { path = "../runtime" }\n',
                              extra='[dev-dependencies]\nnovarocks-types = { path = "../types" }\n')
        self.assert_rejected(result, "runtime/wire/provider/storage capability: tokio")

    def test_pure_owner_cannot_execute_build_script(self):
        self.assert_rejected(self.fixture(build_script=True), "executes a custom build script")

    @staticmethod
    def external(name, source=guard.REGISTRY_SOURCE):
        return {"id": f"{source}#{name}@1.0.0", "name": name, "source": source,
                "dependencies": [], "features": {}, "targets": []}

    def test_arrow_backing_is_allowed_without_exact_closure_snapshot(self):
        for name in ("arrow-array", "arrow-buffer", "arrow-data", "half", "num-traits"):
            self.assertEqual(guard.verify_package(self.external(name), set()), [])

    def test_same_named_pure_contract_replacement_is_rejected(self):
        package = self.external("novarocks-types")
        self.assertIn("not the workspace-owned pure package", " ".join(
            guard.verify_package(package, set())))

    def test_provider_and_wire_capabilities_are_rejected(self):
        for name in ("novarocks-connector-iceberg", "novarocks-spi",
                     "novarocks-proto-models", "novarocks-worker", "tonic", "object_store"):
            self.assertTrue(guard.verify_package(self.external(name), set()), name)


if __name__ == "__main__":
    unittest.main()
