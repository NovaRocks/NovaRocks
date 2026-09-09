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
#
# Mutation test for check-spi-dependency-boundary.py.
#
# Fixture manifests end with a `[dependencies]` table on purpose: a mutation is
# then a plain append.

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
  {
    printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2024"\n' "$package_name"
    printf '\n[dependencies]\n'
  } >"$package_root/Cargo.toml"
  : >"$package_root/src/lib.rs"
}

append_dependency() {
  local manifest="$1"
  local line="$2"

  printf '%s\n' "$line" >>"$manifest"
}

write_fixture() {
  local fixture_root="$1"

  mkdir -p "$fixture_root/src"
  # Arrow is deliberately part of the accepted baseline: Connector contracts
  # own the columnar vocabulary. Tokio is deliberately absent: the
  # state-store-conformance owner that used to justify it is gone.
  for dependency in arrow async-trait bytes novarocks-frontend novarocks-secret \
      novarocks-state-store-api novarocks-types serde sha2 tokio tracing url uuid; do
    write_dependency_package "$fixture_root" "$dependency"
  done
  cat >"$fixture_root/Cargo.toml" <<'EOF'
[package]
name = "novarocks-spi"
version = "0.1.0"
edition = "2024"

[features]
connector-conformance = []

[dependencies]
arrow = { path = "deps/arrow" }
async-trait = { path = "deps/async-trait" }
bytes = { path = "deps/bytes" }
novarocks-secret = { path = "deps/novarocks-secret" }
serde = { path = "deps/serde" }
sha2 = { path = "deps/sha2" }
url = { path = "deps/url" }
uuid = { path = "deps/uuid" }
EOF
  : >"$fixture_root/src/lib.rs"
}

new_mutation() {
  local name="$1"
  local mutation_root="$tmpdir/$name"

  cp -R "$valid_root" "$mutation_root"
  rm -f "$mutation_root/Cargo.lock"
  printf '%s' "$mutation_root"
}

assert_accepted() {
  local fixture_root="$1"

  if ! "$CHECKER" --manifest-path "$fixture_root/Cargo.toml" \
      >"$fixture_root/stdout" 2>"$fixture_root/stderr"; then
    echo "SPI dependency boundary rejected a legal graph: $fixture_root" >&2
    cat "$fixture_root/stderr" >&2
    exit 1
  fi
  grep -Fq "novarocks-spi dependency boundary: PASS" "$fixture_root/stdout"
}

assert_rejected() {
  local fixture_root="$1"
  local expected_error="$2"

  if "$CHECKER" --manifest-path "$fixture_root/Cargo.toml" \
      >"$fixture_root/stdout" 2>"$fixture_root/stderr"; then
    echo "SPI dependency boundary mutation was accepted: $fixture_root" >&2
    exit 1
  fi
  if ! grep -Fq "$expected_error" "$fixture_root/stderr"; then
    echo "missing expected violation in $fixture_root/stderr: $expected_error" >&2
    cat "$fixture_root/stderr" >&2
    exit 1
  fi
}

# --- accept: the real repository ------------------------------------------
"$CHECKER" --manifest-path "$REPO_ROOT/Cargo.toml" >"$tmpdir/repo-stdout"
grep -Fq "novarocks-spi dependency boundary: PASS" "$tmpdir/repo-stdout"

# --- accept: the untouched fixture ----------------------------------------
valid_root="$tmpdir/valid"
write_fixture "$valid_root"
assert_accepted "$valid_root"

# --- accept: a neutral third-party dependency is not frozen out -----------
# The guard protects direction and capability, not an exact dependency array.
extra_dependency_root="$(new_mutation extra-dependency)"
append_dependency "$extra_dependency_root/Cargo.toml" 'tracing = { path = "deps/tracing" }'
assert_accepted "$extra_dependency_root"

# --- reject: a declared async runtime, optional or not --------------------
# The removed state-store-conformance feature was Tokio's only owner, so an
# optional Tokio is no longer a legal shape either.
optional_tokio_root="$(new_mutation optional-tokio)"
append_dependency "$optional_tokio_root/Cargo.toml" \
  'tokio = { path = "deps/tokio", optional = true }'
assert_rejected "$optional_tokio_root" \
  "novarocks-spi declares a normal dependency on a forbidden async runtime: tokio"

# --- reject: a transitive async runtime -----------------------------------
transitive_tokio_root="$(new_mutation transitive-tokio)"
append_dependency "$transitive_tokio_root/deps/bytes/Cargo.toml" \
  'tokio = { path = "../tokio" }'
assert_rejected "$transitive_tokio_root" \
  "novarocks-spi normal dependency closure contains a forbidden async runtime: tokio"

# --- reject: a transitive state-store contract ----------------------------
# The storage contract left SPI in UEA-2 M1 and must not come back through a
# transitive edge.
transitive_state_store_root="$(new_mutation transitive-state-store)"
append_dependency "$transitive_state_store_root/deps/bytes/Cargo.toml" \
  'novarocks-state-store-api = { path = "../novarocks-state-store-api" }'
assert_rejected "$transitive_state_store_root" \
  "novarocks-spi normal dependency closure contains a forbidden state-store contract: novarocks-state-store-api"

# --- reject: a transitive application owner -------------------------------
transitive_frontend_root="$(new_mutation transitive-frontend)"
append_dependency "$transitive_frontend_root/deps/bytes/Cargo.toml" \
  'novarocks-frontend = { path = "../novarocks-frontend" }'
assert_rejected "$transitive_frontend_root" \
  "novarocks-spi normal dependency closure contains a forbidden application/execution owner: novarocks-frontend"

# --- reject: an internal crate no capability rule names -------------------
# The neutral allow-list is the backstop that keeps the original "default
# normal DAG owns no internal crate" protection intact.
transitive_internal_root="$(new_mutation transitive-internal)"
append_dependency "$transitive_internal_root/deps/bytes/Cargo.toml" \
  'novarocks-types = { path = "../novarocks-types" }'
assert_rejected "$transitive_internal_root" \
  "default normal dependency DAG contains internal crates outside the neutral allow-list"

echo "spi-dependency-boundary-test: PASS"
