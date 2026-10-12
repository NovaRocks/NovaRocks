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

"""Check the resolver and ABI premises of JSONObject's resource geometry.

Cargo owns release/source/checksum identity (ADR-0168); this guard contains no
release or registry checksum allow-list. The resource proof still requires
source review on upgrades: feature and identity agreement is not a proof of
private layout or allocation behavior. No owner count or migration token is
checked here. Test and shipping resolver contexts remain separate.
"""
import argparse
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path

SOURCE = "registry+https://github.com/rust-lang/crates.io-index"
TARGETS = {
    "aarch64-apple-darwin": "aarch64",
    "x86_64-unknown-linux-gnu": "x86_64",
    "aarch64-unknown-linux-gnu": "aarch64",
}
FEATURES = {
    "arrow-array": {"chrono-tz", "ffi"},
    "arrow-buffer": set(),
    "arrow-data": {"ffi"},
    "arrow-schema": {"bitflags", "canonical_extension_types", "ffi", "serde"},
    "serde_json": {"default", "raw_value", "std"},
    "serde_core": {"alloc", "default", "rc", "result", "std"},
    "hex": {"alloc", "default", "std"},
    "chrono": {"alloc", "clock", "default", "iana-time-zone", "js-sys", "now",
               "oldtime", "serde", "std", "wasm-bindgen", "wasmbind", "winapi", "windows-link"},
}
CONTEXTS = (
    ("functions-test", "novarocks-functions", "normal,build,dev", ()),
    ("execution-test", "novarocks-execution", "normal,build,dev", ()),
    ("server-default", "novarocks-server", "normal,build", ()),
    ("server-candidate", "novarocks-server", "normal,build", ("physical-wire-v2-candidate",)),
)

class ProfileError(ValueError):
    pass


def command(argv, root):
    completed = subprocess.run(argv, cwd=root, text=True, capture_output=True, check=False)
    if completed.returncode:
        raise ProfileError(f"{argv!r} failed ({completed.returncode}): {completed.stderr.strip()}")
    return completed.stdout


def verify_toolchain(config, verbose):
    channel = config.get("toolchain", {}).get("channel")
    if not isinstance(channel, str) or not re.fullmatch(r"\d+\.\d+\.\d+", channel):
        raise ProfileError("resource source review requires an exact stable root toolchain channel")
    fields = dict(line.split(": ", 1) for line in verbose.splitlines() if ": " in line)
    if fields.get("release") != channel:
        raise ProfileError("actual rustc release differs from the root toolchain authority")
    if not re.fullmatch(r"[0-9a-f]{40}", fields.get("commit-hash", "")):
        raise ProfileError("actual rustc must expose its complete source commit")
    if fields.get("host") not in TARGETS:
        raise ProfileError(f"unreviewed host ABI: {fields.get('host')!r}")
    return fields


def verify_target(target, cfg):
    facts = set(cfg.splitlines())
    for fact in ['target_pointer_width="64"', 'target_endian="little"',
                 f'target_arch="{TARGETS[target]}"']:
        if fact not in facts:
            raise ProfileError(f"{target} lacks the reviewed ABI premise {fact}")


def verify_identities(metadata, lock):
    identities = {}
    for name in FEATURES:
        packages = [p for p in metadata["packages"] if p["name"] == name]
        records = [p for p in lock["package"] if p["name"] == name]
        if len(packages) != 1 or len(records) != 1:
            raise ProfileError(f"{name} must have one Cargo-authorized identity")
        package, record = packages[0], records[0]
        if package.get("source") != SOURCE or record.get("source") != SOURCE:
            raise ProfileError(f"{name} must use the authorized registry source")
        if package["version"] != record["version"]:
            raise ProfileError(f"{name} metadata and lock disagree on identity")
        checksum = record.get("checksum")
        if not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum):
            raise ProfileError(f"{name} lacks a Cargo lock checksum")
        if package.get("checksum") is not None and package["checksum"] != checksum:
            raise ProfileError(f"{name} metadata and lock disagree on checksum")
        manifest = Path(package["manifest_path"]).resolve()
        if (package.get("id") != f"{SOURCE}#{name}@{package['version']}"
                or manifest.name != "Cargo.toml"
                or manifest.parent.name != f"{name}-{package['version']}"
                or len(manifest.parents) < 4
                or manifest.parents[2].name != "src"
                or manifest.parents[3].name != "registry"
                or not manifest.is_file()):
            raise ProfileError(f"{name} is not the actual unpacked registry identity")
        identities[name] = package["version"]
    return identities


def verify_tree(tree, identities, label):
    contexts = {name: set() for name in FEATURES}
    for line in tree.splitlines():
        match = re.fullmatch(r"([^ ]+) v([^ ]+)(?: [^|]*)?\|([^|]*)", line)
        if match is None:
            raise ProfileError(f"{label}: invalid Cargo feature record: {line!r}")
        name, version, raw = match.groups()
        if name not in FEATURES:
            continue
        if version != identities[name]:
            raise ProfileError(f"{label}: {name} differs from its Cargo-authorized identity")
        raw = raw.removesuffix(" (*)").strip()
        features = frozenset(filter(None, raw.split(",")))
        if features - FEATURES[name]:
            raise ProfileError(f"{label}: unreviewed {name} features {sorted(features - FEATURES[name])}")
        if name in {"serde_json", "serde_core", "chrono", "hex"} and "std" not in features:
            raise ProfileError(f"{label}: {name} must retain its reviewed std author")
        contexts[name].add(tuple(sorted(features)))
    for name, found in contexts.items():
        if not found:
            raise ProfileError(f"{label}: missing actual {name} resolver context")
    return {name: sorted(found) for name, found in contexts.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--cargo", default="cargo")
    parser.add_argument("--rustc", default="rustc")
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args()
    root = args.repo_root.resolve()
    with (root / "rust-toolchain.toml").open("rb") as stream:
        toolchain = tomllib.load(stream)
    with (root / "Cargo.lock").open("rb") as stream:
        lock = tomllib.load(stream)
    actual = verify_toolchain(toolchain, command([args.rustc, "-Vv"], root))
    metadata = json.loads(command([args.cargo, "metadata", "--format-version", "1",
                                   "--offline", "--locked"], root))
    identities = verify_identities(metadata, lock)
    records = []
    if args.evidence_dir:
        args.evidence_dir.mkdir(parents=True, exist_ok=False)
    for target in TARGETS:
        cfg = command([args.rustc, "--print", "cfg", "--target", target], root)
        verify_target(target, cfg)
        for label, package, edges, features in CONTEXTS:
            # Preserve the original host resolver semantics; explicit host
            # --target would introduce separate host/target build contexts.
            argv = [args.cargo, "tree", "--offline", "--locked", "-p", package,
                    "--edges", edges, "--prefix", "none", "--format", "{p}|{f}"]
            if target != actual["host"]:
                argv += ["--target", target]
            if features:
                argv += ["--features", ",".join(features)]
            tree = command(argv, root)
            name = f"{label}-{target}"
            contexts = verify_tree(tree, identities, name)
            records.append({"profile": name, "command": argv, "contexts": contexts})
            if args.evidence_dir:
                (args.evidence_dir / f"{name}.tree").write_text(tree)
        if args.evidence_dir:
            (args.evidence_dir / f"{target}.cfg").write_text(cfg)
    receipt = {"scope": "source identities, features and ABI premises; source upgrades still require resource proof review",
               "rustc": actual, "identities": identities, "profiles": records}
    if args.evidence_dir:
        (args.evidence_dir / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(f"JSONObject resource profiles passed ({len(records)} separate resolver contexts)")

if __name__ == "__main__":
    try:
        main()
    except (ProfileError, OSError, KeyError, json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
        print(f"JSONObject resource profile violation: {error}", file=sys.stderr)
        raise SystemExit(1)
