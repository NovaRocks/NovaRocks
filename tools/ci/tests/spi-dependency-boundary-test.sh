#!/usr/bin/env bash
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

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHECKER="$REPO_ROOT/tools/ci/check-spi-dependency-boundary.py"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

write_dependency_package() {
  local fixture_root="$1"
  local package_name="$2"
  local package_root="$fixture_root/deps/$package_name"

  mkdir -p "$package_root/src"
  cat >"$package_root/Cargo.toml" <<EOF
[package]
name = "$package_name"
version = "0.1.0"
edition = "2024"
EOF
  : >"$package_root/src/lib.rs"
}

write_fixture() {
  local fixture_root="$1"

  mkdir -p "$fixture_root/src"
  for dependency in arrow async-trait bytes serde sha2 tokio uuid; do
    write_dependency_package "$fixture_root" "$dependency"
  done
  cat >"$fixture_root/Cargo.toml" <<'EOF'
[package]
name = "novarocks-spi"
version = "0.1.0"
edition = "2024"

[dependencies]
arrow = { path = "deps/arrow" }
async-trait = { path = "deps/async-trait" }
bytes = { path = "deps/bytes" }
sha2 = { path = "deps/sha2" }
serde = { path = "deps/serde" }
tokio = { path = "deps/tokio", optional = true }
uuid = { path = "deps/uuid" }

[features]
state-store-conformance = ["dep:tokio"]
EOF
  : >"$fixture_root/src/lib.rs"
}

assert_rejected() {
  local fixture_root="$1"
  local expected_error="$2"

  if "$CHECKER" --manifest-path "$fixture_root/Cargo.toml" \
      >"$fixture_root/stdout" 2>"$fixture_root/stderr"; then
    echo "SPI dependency boundary mutation was accepted: $fixture_root" >&2
    exit 1
  fi
  grep -Fq "$expected_error" "$fixture_root/stderr"
}

valid_root="$tmpdir/valid"
write_fixture "$valid_root"
"$CHECKER" --manifest-path "$valid_root/Cargo.toml"

extra_dependency_root="$tmpdir/extra-dependency"
cp -R "$valid_root" "$extra_dependency_root"
write_dependency_package "$extra_dependency_root" tracing
sed -i.bak '/sha2 =/a\
tracing = { path = "deps/tracing" }
' "$extra_dependency_root/Cargo.toml"
rm "$extra_dependency_root/Cargo.toml.bak"
assert_rejected "$extra_dependency_root" \
  "required normal dependencies must be exactly: arrow, async-trait, bytes, serde, sha2, uuid"

required_tokio_root="$tmpdir/required-tokio"
cp -R "$valid_root" "$required_tokio_root"
sed -i.bak 's/, optional = true//' "$required_tokio_root/Cargo.toml"
rm "$required_tokio_root/Cargo.toml.bak"
sed -i.bak 's/"dep:tokio"//' "$required_tokio_root/Cargo.toml"
rm "$required_tokio_root/Cargo.toml.bak"
assert_rejected "$required_tokio_root" \
  "Tokio must be an optional normal dependency"

default_tokio_root="$tmpdir/default-tokio"
cp -R "$valid_root" "$default_tokio_root"
sed -i.bak '/\[features\]/a\
default = ["state-store-conformance"]
' "$default_tokio_root/Cargo.toml"
rm "$default_tokio_root/Cargo.toml.bak"
assert_rejected "$default_tokio_root" \
  "default feature graph must not enable Tokio"

transitive_tokio_root="$tmpdir/transitive-tokio"
cp -R "$valid_root" "$transitive_tokio_root"
sed -i.bak '/edition = "2024"/a\
\
[dependencies]\
tokio = { path = "../tokio" }
' "$transitive_tokio_root/deps/bytes/Cargo.toml"
rm "$transitive_tokio_root/deps/bytes/Cargo.toml.bak"
assert_rejected "$transitive_tokio_root" \
  "default normal dependency DAG must not contain Tokio"

echo "spi-dependency-boundary-test: PASS"
