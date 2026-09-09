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
# Mutation test for check-state-store-dependency-boundary.py.
#
# The accept path runs against the real repository manifest.  The reject path
# builds a throwaway fixture workspace under a temporary directory so no
# mutation ever touches the repository.  Every fixture manifest ends with a
# `[dependencies]` table on purpose: a mutation is then a plain append.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHECKER="$REPO_ROOT/tools/ci/check-state-store-dependency-boundary.py"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

# write_package <fixture-root> <directory> <package-name> [extra manifest block]
#
# The extra block is inserted before the trailing `[dependencies]` table so
# mutations can append normal dependency lines to any package.
write_package() {
  local fixture_root="$1"
  local directory="$2"
  local package_name="$3"
  local extra="${4:-}"
  local package_root="$fixture_root/crates/$directory"

  mkdir -p "$package_root/src"
  {
    printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2024"\n' "$package_name"
    if [ -n "$extra" ]; then
      printf '\n%s\n' "$extra"
    fi
    printf '\n[dependencies]\n'
  } >"$package_root/Cargo.toml"
  : >"$package_root/src/lib.rs"
}

append_dependency() {
  local fixture_root="$1"
  local directory="$2"
  local line="$3"

  printf '%s\n' "$line" >>"$fixture_root/crates/$directory/Cargo.toml"
}

write_fixture() {
  local fixture_root="$1"

  mkdir -p "$fixture_root"
  cat >"$fixture_root/Cargo.toml" <<'EOF'
[workspace]
resolver = "2"
members = [
  "crates/api",
  "crates/arrow",
  "crates/foundationdb",
  "crates/frontend",
  "crates/mysql",
  "crates/neutral-lib",
  "crates/secret",
  "crates/sqlite",
  "crates/testkit",
  "crates/types",
]
EOF

  # Neutral leaves. `arrow` and `novarocks-types` exist only so a mutation can
  # reach a forbidden capability; nothing depends on them in the accept case.
  write_package "$fixture_root" arrow arrow
  write_package "$fixture_root" neutral-lib neutral-lib
  write_package "$fixture_root" secret novarocks-secret
  write_package "$fixture_root" types novarocks-types

  # The storage contract owns no internal crate at all.
  write_package "$fixture_root" api novarocks-state-store-api

  write_package "$fixture_root" testkit novarocks-state-store-testkit
  append_dependency "$fixture_root" testkit \
    'novarocks-state-store-api = { path = "../api" }'

  write_package "$fixture_root" sqlite novarocks-state-store-sqlite
  append_dependency "$fixture_root" sqlite \
    'novarocks-state-store-api = { path = "../api" }'

  # MySQL routes through `neutral-lib`, which is the transitive hop every
  # "not a direct dependency" mutation below uses.
  write_package "$fixture_root" mysql novarocks-state-store-mysql \
    '[dev-dependencies]
novarocks-state-store-testkit = { path = "../testkit" }'
  append_dependency "$fixture_root" mysql \
    'novarocks-state-store-api = { path = "../api" }'
  append_dependency "$fixture_root" mysql \
    'novarocks-secret = { path = "../secret" }'
  append_dependency "$fixture_root" mysql \
    'neutral-lib = { path = "../neutral-lib" }'

  write_package "$fixture_root" foundationdb novarocks-state-store-foundationdb \
    '[dev-dependencies]
novarocks-state-store-testkit = { path = "../testkit" }'
  append_dependency "$fixture_root" foundationdb \
    'novarocks-state-store-api = { path = "../api" }'
  append_dependency "$fixture_root" foundationdb \
    'novarocks-secret = { path = "../secret" }'

  # A production consumer that legitimately uses the testkit for tests only.
  write_package "$fixture_root" frontend novarocks-frontend \
    '[dev-dependencies]
novarocks-state-store-testkit = { path = "../testkit" }'
  append_dependency "$fixture_root" frontend \
    'novarocks-state-store-api = { path = "../api" }'
}

new_mutation() {
  local name="$1"
  local mutation_root="$tmpdir/$name"

  cp -R "$baseline_root" "$mutation_root"
  rm -f "$mutation_root/Cargo.lock"
  printf '%s' "$mutation_root"
}

assert_accepted() {
  local fixture_root="$1"

  if ! "$CHECKER" --manifest-path "$fixture_root/Cargo.toml" \
      >"$fixture_root/stdout" 2>"$fixture_root/stderr"; then
    echo "state-store dependency boundary rejected a legal graph: $fixture_root" >&2
    cat "$fixture_root/stderr" >&2
    exit 1
  fi
  grep -Fq "state-store dependency boundary: PASS" "$fixture_root/stdout"
}

assert_rejected() {
  local fixture_root="$1"
  shift

  if "$CHECKER" --manifest-path "$fixture_root/Cargo.toml" \
      >"$fixture_root/stdout" 2>"$fixture_root/stderr"; then
    echo "state-store dependency boundary mutation was accepted: $fixture_root" >&2
    exit 1
  fi
  local expected
  for expected in "$@"; do
    if ! grep -Fq "$expected" "$fixture_root/stderr"; then
      echo "missing expected violation in $fixture_root/stderr: $expected" >&2
      cat "$fixture_root/stderr" >&2
      exit 1
    fi
  done
}

# --- accept: the real repository ------------------------------------------
"$CHECKER" --manifest-path "$REPO_ROOT/Cargo.toml" >"$tmpdir/repo-stdout"
grep -Fq "state-store dependency boundary: PASS" "$tmpdir/repo-stdout"

# --- accept: the untouched fixture ----------------------------------------
baseline_root="$tmpdir/baseline"
write_fixture "$baseline_root"
assert_accepted "$baseline_root"

# --- accept: a neutral third-party dependency is not frozen out -----------
# The guard protects direction and capability, not an exact dependency array.
neutral_root="$(new_mutation neutral-addition)"
write_package "$neutral_root" tracing tracing
sed -i.bak 's#"crates/types",#"crates/types",\n  "crates/tracing",#' \
  "$neutral_root/Cargo.toml"
rm "$neutral_root/Cargo.toml.bak"
append_dependency "$neutral_root" sqlite 'tracing = { path = "../tracing" }'
assert_accepted "$neutral_root"

# --- reject: transitive columnar runtime ----------------------------------
# `arrow` is reached through `neutral-lib`, never declared by the provider.
transitive_arrow_root="$(new_mutation transitive-arrow)"
append_dependency "$transitive_arrow_root" neutral-lib 'arrow = { path = "../arrow" }'
assert_rejected "$transitive_arrow_root" \
  "novarocks-state-store-mysql normal dependency closure contains a forbidden columnar runtime: arrow"

# --- reject: optional/feature-gated direct columnar runtime ---------------
# No feature enables it, so the resolve graph cannot see it; only the declared
# scan can. This mutation is the witness that the declared scan is load-bearing.
optional_arrow_root="$(new_mutation optional-arrow)"
append_dependency "$optional_arrow_root" sqlite \
  'arrow = { path = "../arrow", optional = true }'
assert_rejected "$optional_arrow_root" \
  "novarocks-state-store-sqlite declares a normal dependency on a forbidden columnar runtime: arrow"

# --- reject: testkit on a declared normal edge ----------------------------
normal_testkit_root="$(new_mutation normal-testkit)"
append_dependency "$normal_testkit_root" frontend \
  'novarocks-state-store-testkit = { path = "../testkit" }'
assert_rejected "$normal_testkit_root" \
  "novarocks-frontend declares novarocks-state-store-testkit as a normal dependency" \
  "resolved normal dependency edges point at novarocks-state-store-testkit from: novarocks-frontend"

# --- reject: testkit reached transitively by a provider -------------------
transitive_testkit_root="$(new_mutation transitive-testkit)"
append_dependency "$transitive_testkit_root" neutral-lib \
  'novarocks-state-store-testkit = { path = "../testkit" }'
assert_rejected "$transitive_testkit_root" \
  "novarocks-state-store-mysql normal dependency closure contains a forbidden test-only harness: novarocks-state-store-testkit" \
  "resolved normal dependency edges point at novarocks-state-store-testkit from: neutral-lib"

# --- reject: the API depending on its own fake ----------------------------
api_testkit_root="$(new_mutation api-testkit)"
cat >>"$api_testkit_root/crates/api/Cargo.toml" <<'EOF'

[dev-dependencies]
novarocks-state-store-testkit = { path = "../testkit" }
EOF
assert_rejected "$api_testkit_root" \
  "novarocks-state-store-api declares novarocks-state-store-testkit as a dev dependency" \
  "novarocks-state-store-api reaches novarocks-state-store-testkit through its dev dependency closure"

# --- reject: transitive application owner ---------------------------------
transitive_frontend_root="$(new_mutation transitive-frontend)"
append_dependency "$transitive_frontend_root" neutral-lib \
  'novarocks-frontend = { path = "../frontend" }'
assert_rejected "$transitive_frontend_root" \
  "novarocks-state-store-mysql normal dependency closure contains a forbidden application/execution owner: novarocks-frontend"

# --- reject: an internal crate no capability rule names -------------------
# The neutral allow-list is the backstop for internal crates nobody enumerated.
transitive_internal_root="$(new_mutation transitive-internal)"
append_dependency "$transitive_internal_root" neutral-lib \
  'novarocks-types = { path = "../types" }'
assert_rejected "$transitive_internal_root" \
  "novarocks-state-store-mysql normal dependency closure contains internal crates outside the neutral allow-list" \
  "novarocks-types"

echo "state-store-dependency-boundary-test: PASS"
