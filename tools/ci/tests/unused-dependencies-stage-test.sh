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
source "$REPO_ROOT/tools/ci/lib/build_capabilities.sh"
# This small fixture tests the adapter's outcome, independently of the real
# workspace dependency inventory. No build or external service is required.
if [ "$(cargo machete --version)" != 0.9.2 ]; then
  echo "error: unused dependency tests require cargo-machete 0.9.2" >&2
  exit 2
fi
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/clean/src" "$fixture/unused/src" "$fixture/broken"
cat >"$fixture/clean/Cargo.toml" <<'MANIFEST'
[package]
name = "clean"
version = "0.1.0"
edition = "2021"
MANIFEST
printf '%s\n' 'pub fn value() -> u8 { 1 }' >"$fixture/clean/src/lib.rs"
cat >"$fixture/unused/Cargo.toml" <<'MANIFEST'
[package]
name = "unused"
version = "0.1.0"
edition = "2021"
[dependencies]
clean = { path = "../clean" }
MANIFEST
printf '%s\n' 'pub fn value() -> u8 { 2 }' >"$fixture/unused/src/lib.rs"
printf '%s\n' '[package' >"$fixture/broken/Cargo.toml"

assert_exit() {
  local expected="$1" code=0
  shift
  ci_run_cargo_machete "$@" >"$fixture/stdout" 2>"$fixture/stderr" || code=$?
  if [ "$code" -ne "$expected" ]; then
    cat "$fixture/stdout" "$fixture/stderr" >&2
    echo "error: expected cargo-machete adapter exit $expected, got $code" >&2
    exit 1
  fi
}
assert_exit 0 "$fixture/clean"
assert_exit 1 "$fixture/clean" "$fixture/unused"
# 0.9.2 can report this manifest error yet exit zero: incomplete analysis must
# win over a successful analysis of the other package.
assert_exit 2 "$fixture/clean" "$fixture/broken"
grep -q '^error when handling ' "$fixture/stderr"
grep -q '^error: cargo-machete could not analyze every manifest' "$fixture/stderr"

# A pending lockfile edit is a valid local input. The stage must reject only
# changes introduced during this invocation, while preserving tool failures.
mkdir -p "$fixture/workspace/member/src"
cat >"$fixture/workspace/Cargo.toml" <<'MANIFEST'
[workspace]
members = ["member"]
resolver = "2"
MANIFEST
cat >"$fixture/workspace/member/Cargo.toml" <<'MANIFEST'
[package]
name = "lock_guard_member"
version = "0.1.0"
edition = "2021"
MANIFEST
printf '%s\n' 'pub fn value() -> u8 { 1 }' >"$fixture/workspace/member/src/lib.rs"
cargo generate-lockfile --offline --manifest-path "$fixture/workspace/Cargo.toml"
git -C "$fixture/workspace" init -q
git -C "$fixture/workspace" add Cargo.lock
cat >"$fixture/workspace/member/Cargo.toml" <<'MANIFEST'
[package]
name = "lock_guard_member"
version = "0.1.1"
edition = "2021"
MANIFEST
cargo generate-lockfile --offline --manifest-path "$fixture/workspace/Cargo.toml"
if git -C "$fixture/workspace" diff --quiet -- Cargo.lock; then
  echo "error: lockfile fixture has no pending input edit" >&2
  exit 1
fi

assert_workspace_stage() {
  local expected="$1" action="$2" code=0
  (
    cd "$fixture/workspace"
    case "$action" in
      real) ;;
      mutate)
        ci_run_cargo_machete() {
          printf '\n# Unexpected tool write.\n' >>Cargo.lock
        }
        ;;
      fail)
        ci_run_cargo_machete() { return 2; }
        ;;
      *) exit 2 ;;
    esac
    ci_check_unused_dependencies
  ) >"$fixture/workspace-stdout" 2>"$fixture/workspace-stderr" || code=$?
  if [ "$code" -ne "$expected" ]; then
    cat "$fixture/workspace-stdout" "$fixture/workspace-stderr" >&2
    echo "error: expected workspace stage exit $expected for $action, got $code" >&2
    exit 1
  fi
}
assert_workspace_stage 0 real
assert_workspace_stage 1 mutate
grep -q '^error: cargo machete modified Cargo.lock' "$fixture/workspace-stderr"
assert_workspace_stage 2 fail
echo "unused-dependencies-stage-test: PASS"
