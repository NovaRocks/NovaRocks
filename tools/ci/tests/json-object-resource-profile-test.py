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

"""Mutation contracts for resolver/ABI rules, without Cargo release constants."""
import copy
import importlib.util
from pathlib import Path
import tempfile
import unittest

path = Path(__file__).resolve().parents[1] / "check-json-object-resource-profile.py"
spec = importlib.util.spec_from_file_location("resource_profile", path)
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


def identities(root, version="9.8.7"):
    packages, records = [], []
    for name in guard.FEATURES:
        manifest = root / "registry/src/index" / f"{name}-{version}" / "Cargo.toml"
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text("# Synthetic unpacked Cargo authority for identity mutations.\n")
        packages.append({"name": name, "version": version, "source": guard.SOURCE,
                         "id": f"{guard.SOURCE}#{name}@{version}",
                         "manifest_path": str(manifest)})
        records.append({"name": name, "version": version, "source": guard.SOURCE,
                        "checksum": "a" * 64})
    return {"packages": packages}, {"package": records}


def tree():
    return "\n".join(f"{name} v9.8.7|" + ("std" if name in {"serde_json", "serde_core", "hex", "chrono"} else "")
                     for name in guard.FEATURES)


class ResourceProfileTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def test_identity_rules_derive_releases_from_cargo_authority(self):
        metadata, lock = identities(self.root)
        found = guard.verify_identities(metadata, lock)
        self.assertEqual(set(found), set(guard.FEATURES))
        metadata, lock = identities(self.root, "8.7.6")
        for record in lock["package"]:
            record["checksum"] = "b" * 64
        self.assertEqual(set(guard.verify_identities(metadata, lock).values()), {"8.7.6"})

    def test_foreign_duplicate_or_inconsistent_identities_refuse(self):
        metadata, lock = identities(self.root)
        for mutate in [
            lambda m, l: m["packages"].append(copy.deepcopy(m["packages"][0])),
            lambda m, l: l["package"].append(copy.deepcopy(l["package"][0])),
            lambda m, l: m["packages"][0].update(source=None),
            lambda m, l: m["packages"][0].update(version="0.0.1"),
            lambda m, l: m["packages"][0].update(manifest_path="/fixture/substitute/Cargo.toml"),
            lambda m, l: l["package"][0].update(checksum=None),
            lambda m, l: m["packages"][0].update(checksum="b" * 64),
        ]:
            m, l = copy.deepcopy(metadata), copy.deepcopy(lock)
            mutate(m, l)
            with self.assertRaises(guard.ProfileError):
                guard.verify_identities(m, l)

    def test_actual_registry_identity_refuses_path_vendor_missing_and_symlink_sources(self):
        metadata, lock = identities(self.root)
        for index, package in enumerate(metadata["packages"]):
            name, version = package["name"], package["version"]
            vendor = self.root / "vendor" / f"{name}-{version}" / "Cargo.toml"
            vendor.parent.mkdir(parents=True)
            vendor.write_text("# A real substituted source is still not the registry author.\n")
            missing = self.root / "missing/registry/src/index" / f"{name}-{version}" / "Cargo.toml"
            link = self.root / "link/registry/src/index" / f"{name}-{version}" / "Cargo.toml"
            link.parent.mkdir(parents=True)
            link.symlink_to(vendor)
            mutations = {
                "path-id": {"id": f"path+file://{vendor.parent}#{name}@{version}"},
                "git-id": {"id": f"git+https://example.invalid/substitute#{name}@{version}"},
                "missing-id": {"id": None},
                "vendor-source": {"manifest_path": str(vendor)},
                "missing-manifest": {"manifest_path": str(missing)},
                "symlink-to-vendor": {"manifest_path": str(link)},
            }
            for mutation, replacement in mutations.items():
                with self.subTest(package=name, mutation=mutation):
                    changed = copy.deepcopy(metadata)
                    changed["packages"][index].update(replacement)
                    with self.assertRaises(guard.ProfileError):
                        guard.verify_identities(changed, lock)

    def test_features_keep_distinct_contexts_and_allow_shipping_raw_value(self):
        authorized = guard.verify_identities(*identities(self.root))
        source = tree() + "\nserde_json v9.8.7|default,raw_value,std (*)"
        contexts = guard.verify_tree(source, authorized, "fixture")
        self.assertEqual(contexts["serde_json"], [("default", "raw_value", "std"), ("std",)])

    def test_feature_changes_do_not_pass_by_union_or_absence(self):
        authorized = guard.verify_identities(*identities(self.root))
        for name, feature in [
            ("serde_json", "preserve_order"), ("serde_json", "arbitrary_precision"),
            ("serde_json", "float_roundtrip"), ("serde_json", "unbounded_depth"),
            ("arrow-buffer", "pool"), ("arrow-data", "force_validate"),
        ]:
            with self.subTest(name=name, feature=feature), self.assertRaises(guard.ProfileError):
                guard.verify_tree(tree() + f"\n{name} v9.8.7|{feature},std", authorized, "fixture")
        for source in [tree().replace("serde_json v9.8.7|std", "serde_json v9.8.7|raw_value"),
                       tree().replace("serde_json v9.8.7|std", ""),
                       tree().replace("serde_json v9.8.7|std", "serde_json v8.7.6|std")]:
            with self.assertRaises(guard.ProfileError):
                guard.verify_tree(source, authorized, "fixture")

    def test_toolchain_and_targets_use_actual_authority(self):
        config = {"toolchain": {"channel": "9.8.7"}}
        actual = "release: 9.8.7\ncommit-hash: " + "a" * 40 + "\nhost: aarch64-apple-darwin\n"
        guard.verify_toolchain(config, actual)
        for source in [actual.replace("9.8.7", "9.8.6"), actual.replace("a" * 40, "short"),
                       actual.replace("aarch64-apple-darwin", "x86_64-pc-windows-msvc")]:
            with self.assertRaises(guard.ProfileError):
                guard.verify_toolchain(config, source)
        with self.assertRaises(guard.ProfileError):
            guard.verify_toolchain({"toolchain": {"channel": "stable"}}, actual)
        for target, arch in guard.TARGETS.items():
            cfg = f'target_pointer_width="64"\ntarget_endian="little"\ntarget_arch="{arch}"'
            guard.verify_target(target, cfg)
            for source in [cfg.replace('"64"', '"32"'), cfg.replace('"little"', '"big"'),
                           cfg.replace(f'"{arch}"', '"riscv64"')]:
                with self.assertRaises(guard.ProfileError):
                    guard.verify_target(target, source)

if __name__ == "__main__":
    unittest.main()
