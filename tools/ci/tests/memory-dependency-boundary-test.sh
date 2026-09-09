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
# Mutation test for check-memory-dependency-boundary.py.
#
# The accept path runs against the real repository manifest.  The reject path
# builds a throwaway fixture workspace under a temporary directory so no
# mutation ever touches the repository.  Every fixture manifest ends with a
# `[dependencies]` table on purpose: a mutation is then a plain append.
#
# One fixture is built twice: once with `novarocks-memory-arrow` present and
# once without it, because the adapter crate is optional by contract and the
# absent case has to be a tested behaviour rather than an assumption.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHECKER="$REPO_ROOT/tools/ci/check-memory-dependency-boundary.py"
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

# write_fixture <fixture-root> <with-adapter: yes|no>
#
# `no` omits `novarocks-memory-arrow` entirely -- package, workspace member,
# and the consumer edge that names it -- which is the shape of this branch
# before the adapter change lands.
write_fixture() {
  local fixture_root="$1"
  local with_adapter="$2"

  mkdir -p "$fixture_root"
  {
    printf '[workspace]\nresolver = "2"\nmembers = [\n'
    printf '  "crates/arrow",\n'
    printf '  "crates/arrow-buffer",\n'
    printf '  "crates/frontend",\n'
    printf '  "crates/memory",\n'
    if [ "$with_adapter" = yes ]; then
      printf '  "crates/memory-arrow",\n'
    fi
    printf '  "crates/neutral-lib",\n'
    printf '  "crates/spi",\n'
    printf '  "crates/starrocks",\n'
    printf '  "crates/state-store-api",\n'
    printf '  "crates/types",\n'
    printf ']\n'
  } >"$fixture_root/Cargo.toml"

  # Third-party leaves. `arrow` routes through `arrow-buffer` so the adapter's
  # legal closure is genuinely transitive, not a single direct edge.
  write_package "$fixture_root" arrow-buffer arrow-buffer
  write_package "$fixture_root" arrow arrow
  append_dependency "$fixture_root" arrow 'arrow-buffer = { path = "../arrow-buffer" }'
  # The transitive hop every "not a direct dependency" mutation below uses.
  write_package "$fixture_root" neutral-lib neutral-lib

  # First-party leaves that exist only so a mutation can reach them.
  write_package "$fixture_root" spi novarocks-spi
  write_package "$fixture_root" types novarocks-types
  write_package "$fixture_root" starrocks novarocks-connector-starrocks
  write_package "$fixture_root" state-store-api novarocks-state-store-api

  # The neutral memory core: no dependencies at all, by contract.
  write_package "$fixture_root" memory novarocks-memory

  if [ "$with_adapter" = yes ]; then
    # The Arrow adapter: the memory core plus the Arrow family, nothing else.
    write_package "$fixture_root" memory-arrow novarocks-memory-arrow
    append_dependency "$fixture_root" memory-arrow \
      'novarocks-memory = { path = "../memory" }'
    append_dependency "$fixture_root" memory-arrow \
      'arrow = { path = "../arrow" }'
  fi

  # A legal application consumer of the adapter.
  write_package "$fixture_root" frontend novarocks-frontend
  if [ "$with_adapter" = yes ]; then
    append_dependency "$fixture_root" frontend \
      'novarocks-memory-arrow = { path = "../memory-arrow" }'
  else
    append_dependency "$fixture_root" frontend \
      'novarocks-memory = { path = "../memory" }'
  fi
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
  shift

  if ! "$CHECKER" --manifest-path "$fixture_root/Cargo.toml" \
      >"$fixture_root/stdout" 2>"$fixture_root/stderr"; then
    echo "memory dependency boundary rejected a legal graph: $fixture_root" >&2
    cat "$fixture_root/stderr" >&2
    exit 1
  fi
  grep -Fq "memory dependency boundary: PASS" "$fixture_root/stdout"
  local expected
  for expected in "$@"; do
    if ! grep -Fq "$expected" "$fixture_root/stdout"; then
      echo "missing expected report line in $fixture_root/stdout: $expected" >&2
      cat "$fixture_root/stdout" >&2
      exit 1
    fi
  done
}

assert_rejected() {
  local fixture_root="$1"
  shift

  if "$CHECKER" --manifest-path "$fixture_root/Cargo.toml" \
      >"$fixture_root/stdout" 2>"$fixture_root/stderr"; then
    echo "memory dependency boundary mutation was accepted: $fixture_root" >&2
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
echo "asserting: the real workspace satisfies the memory dependency boundary"
"$CHECKER" --manifest-path "$REPO_ROOT/Cargo.toml" >"$tmpdir/repo-stdout"
grep -Fq "memory dependency boundary: PASS" "$tmpdir/repo-stdout"

# --- accept: case 1, the untouched fixture --------------------------------
echo "asserting: a clean fixture passes (memory core has no deps; adapter names only the core and arrow)"
baseline_root="$tmpdir/baseline"
write_fixture "$baseline_root" yes
assert_accepted "$baseline_root" \
  "novarocks-state-store-api: memory crates in normal closure: none"

# --- reject: case 2, the core acquires a columnar runtime -----------------
echo "asserting: the memory core gaining a normal dependency on arrow-buffer is rejected"
core_arrow_root="$(new_mutation core-arrow)"
append_dependency "$core_arrow_root" memory \
  'arrow-buffer = { path = "../arrow-buffer" }'
assert_rejected "$core_arrow_root" \
  "novarocks-memory normal dependency closure must be empty" \
  "arrow-buffer"

# --- reject: case 3, the core acquires a first-party crate ----------------
echo "asserting: the memory core gaining a normal dependency on novarocks-types is rejected"
core_first_party_root="$(new_mutation core-first-party)"
append_dependency "$core_first_party_root" memory \
  'novarocks-types = { path = "../types" }'
assert_rejected "$core_first_party_root" \
  "novarocks-memory normal dependency closure must be empty" \
  "novarocks-types"

# --- reject: case 4, the adapter acquires another first-party crate -------
echo "asserting: the adapter gaining a normal dependency on novarocks-spi is rejected"
adapter_spi_root="$(new_mutation adapter-spi)"
append_dependency "$adapter_spi_root" memory-arrow \
  'novarocks-spi = { path = "../spi" }'
assert_rejected "$adapter_spi_root" \
  "novarocks-memory-arrow normal dependency closure contains first-party crates outside the allow-list" \
  "novarocks-spi"

# --- accept: case 5, the adapter package is absent entirely ---------------
# The adapter lands in a later change on this branch. Until then the guard must
# skip its rules and still pass, so this case is the optional-crate witness.
echo "asserting: a clean fixture with novarocks-memory-arrow absent still passes and reports the skip"
absent_root="$tmpdir/adapter-absent"
write_fixture "$absent_root" no
assert_accepted "$absent_root" \
  "novarocks-memory-arrow: absent from Cargo metadata (rules skipped)" \
  "skipped: novarocks-memory-arrow is absent from Cargo metadata"

# --- reject: case 6, the retired connector shares a memory consumer -------
echo "asserting: one package reaching both the adapter and novarocks-connector-starrocks is rejected"
retired_pairing_root="$(new_mutation retired-pairing)"
append_dependency "$retired_pairing_root" frontend \
  'novarocks-connector-starrocks = { path = "../starrocks" }'
assert_rejected "$retired_pairing_root" \
  "novarocks-frontend reaches both novarocks-memory-arrow and the retired novarocks-connector-starrocks"

# --- reject: case 7, the storage contract acquires a memory crate ---------
echo "asserting: novarocks-state-store-api gaining a normal dependency on the memory core is rejected"
storage_memory_root="$(new_mutation storage-memory)"
append_dependency "$storage_memory_root" state-store-api \
  'novarocks-memory = { path = "../memory" }'
assert_rejected "$storage_memory_root" \
  "novarocks-state-store-api normal dependency closure contains a memory crate: novarocks-memory" \
  "novarocks-state-store-api declares a normal dependency on a memory crate: novarocks-memory"

# --- reject: case 8, the adapter reaches a first-party crate transitively --
# `novarocks-spi` is reached through `neutral-lib` and never declared by the
# adapter. This mutation is the witness that the closure walk is load-bearing
# and the declared-table scan alone is not sufficient.
echo "asserting: the adapter reaching novarocks-spi transitively through a third-party crate is rejected"
adapter_transitive_root="$(new_mutation adapter-transitive)"
append_dependency "$adapter_transitive_root" memory-arrow \
  'neutral-lib = { path = "../neutral-lib" }'
append_dependency "$adapter_transitive_root" neutral-lib \
  'novarocks-spi = { path = "../spi" }'
assert_rejected "$adapter_transitive_root" \
  "novarocks-memory-arrow normal dependency closure contains first-party crates outside the allow-list" \
  "novarocks-spi"

# --- reject: case 9, a feature-gated columnar runtime on the core ---------
# No feature enables it, so the resolve graph cannot see it; only the declared
# scan can. This mutation is the witness that the declared scan is load-bearing.
echo "asserting: an optional, feature-gated arrow dependency on the memory core is rejected"
core_optional_arrow_root="$(new_mutation core-optional-arrow)"
append_dependency "$core_optional_arrow_root" memory \
  'arrow = { path = "../arrow", optional = true }'
assert_rejected "$core_optional_arrow_root" \
  "novarocks-memory declares normal dependencies (arrow)"

# --- accept: case 10, a neutral third-party crate on the adapter ----------
# The guard constrains the adapter's *first-party* reach. Its third-party side
# is the Arrow family and whatever that family pulls in, which is not an
# enumerable list, so a neutral utility crate is a review question rather than
# a build failure.
echo "asserting: the adapter gaining a neutral third-party crate is accepted"
adapter_neutral_root="$(new_mutation adapter-neutral)"
append_dependency "$adapter_neutral_root" memory-arrow \
  'neutral-lib = { path = "../neutral-lib" }'
assert_accepted "$adapter_neutral_root"

echo "memory-dependency-boundary-test: PASS"
