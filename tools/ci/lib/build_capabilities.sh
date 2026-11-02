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


# Native Cargo remains the build and test owner. Paths are bound from its JSON.
ci_set_default_build_jobs() {
  if [ "${CARGO_BUILD_JOBS+x}" != x ]; then
    local jobs
    jobs="$(python3 "$(dirname "${BASH_SOURCE[0]}")/build_jobs.py")" || return $?
    export CARGO_BUILD_JOBS="$jobs"
  else
    export CARGO_BUILD_JOBS
  fi
}

ci_validate_build_environment() {
  python3 - <<'ENV'
import os, sys
keys = [k for k,v in os.environ.items() if v and (k in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_TARGET", "RUSTC") or k.startswith("CARGO_PROFILE_") or (k.startswith("CARGO_TARGET_") and k.endswith("_RUSTFLAGS")))]
if keys:
    print("error: unsupported compiler/profile overrides: " + ", ".join(sorted(keys)), file=sys.stderr)
    sys.exit(2)
ENV
}

ci_profile_directory() {
  case "$1" in dev) echo debug ;; release) echo release ;; *) echo "error: unsupported Cargo profile" >&2; return 2 ;; esac
}

ci_target_directory() {
  local kind="$1" root="${2:-${REPO_ROOT:-$PWD}}"
  case "$kind" in
    main) printf '%s\n' "$root/target" ;;
    variants) printf '%s\n' "$root/target/variants" ;;
    *) echo "error: unsupported target kind" >&2; return 2 ;;
  esac
}

ci_validate_build_inputs() {
  ci_validate_build_environment || return $?
  [ "$1" = dev ] || { echo "error: daily test graph requires profile dev" >&2; return 2; }
  ci_set_default_build_jobs
}

ci_main_cargo() {
  local command="$1"; shift
  ci_set_default_build_jobs || return $?
  cargo "$command" --workspace --profile dev --locked --target-dir "$(ci_target_directory main)" "$@"
}

# Pinned cargo-machete can exit successfully after failing to analyze a
# manifest. Preserve its diagnostics and reject incomplete analysis as well.
ci_run_cargo_machete() {
  local log code=0
  log="$(mktemp)" || return 2
  cargo machete --with-metadata "$@" 2>"$log" || code=$?
  cat "$log" >&2
  if grep -q '^error when handling ' "$log"; then
    echo "error: cargo-machete could not analyze every manifest" >&2
    code=2
  elif [ "$code" -ne 0 ] && [ "$code" -ne 1 ]; then
    code=2
  fi
  rm -f "$log"
  return "$code"
}

ci_check_unused_dependencies() {
  local metadata directories directory lock_before lock_after code=0
  local roots=()
  metadata="$(cargo metadata --locked --no-deps --format-version 1)" || return 2
  # Scan member-owned top-level directories, excluding vendor workspaces and
  # the virtual root manifest. Keep paths intact even when they contain spaces.
  directories="$(printf '%s\n' "$metadata" | python3 -c '
import json, sys
from pathlib import Path
data = json.load(sys.stdin)
root = Path(data["workspace_root"])
members = set(data["workspace_members"])
directories = set()
for package in data["packages"]:
    if package["id"] in members:
        relative = Path(package["manifest_path"]).relative_to(root)
        if len(relative.parts) < 2:
            sys.exit("error: expected workspace members below top-level directories")
        directories.add(str(root / relative.parts[0]))
if not directories:
    sys.exit("error: workspace has no members to analyze")
print("\n".join(sorted(directories)))
')" || return 2
  while IFS= read -r directory; do
    roots+=("$directory")
  done <<<"$directories"
  # --with-metadata invokes all-features Cargo metadata internally.
  # Compare this invocation's input, including legitimate pending lock edits.
  lock_before="$(python3 -c 'import hashlib, pathlib; print(hashlib.sha256(pathlib.Path("Cargo.lock").read_bytes()).hexdigest())')" || return 2
  ci_run_cargo_machete "${roots[@]}" || code=$?
  lock_after="$(python3 -c 'import hashlib, pathlib; print(hashlib.sha256(pathlib.Path("Cargo.lock").read_bytes()).hexdigest())')" || return 2
  if [ "$lock_before" != "$lock_after" ]; then
    echo "error: cargo machete modified Cargo.lock" >&2
    [ "$code" -ne 0 ] || code=1
  fi
  return "$code"
}

ci_prepare_main() {
  local receipt="$1" code=0
  ci_validate_build_inputs dev || return $?
  mkdir -p "$(dirname "$receipt")" || return $?
  printf '%s\n' "cargo build --workspace --profile dev --locked --target-dir $(ci_target_directory main) --tests --bins --message-format=json" >"$receipt.cargo.command"
  ci_main_cargo build --tests --bins --message-format=json >"$receipt.cargo.jsonl" 2>"$receipt.cargo.stderr.log" || code=$?
  printf '%s\n' "$code" >"$receipt.cargo.exit"
  [ "$code" -eq 0 ] || return "$code"
  ci_collect_artifacts "$receipt.cargo.jsonl" "$receipt"
}

ci_prepare_nextest() {
  local output="$1"; shift
  ci_validate_build_inputs dev || return $?
  python3 "${REPO_ROOT:-$PWD}/tools/ci/test-run.py" prepare --output "$output" -- "$@"
}

ci_collect_artifacts() {
  python3 "${REPO_ROOT:-$PWD}/tools/ci/cargo-artifacts.py" collect "$1" "$2"
}

ci_bound_binary() {
  local name="$1" receipt="${2:-${NOVA_CI_ARTIFACT_RECEIPT:-}}"
  [ -n "$receipt" ] || { echo "error: prepared Cargo artifact receipt is required" >&2; return 2; }
  python3 "${REPO_ROOT:-$PWD}/tools/ci/cargo-artifacts.py" verify "$receipt" "$name"
}

# Preserve raw self-report and actual exit before interpreting any fields.
ci_capture_build_info() {
  local binary="$1" evidence="$2" expected_identity="${3:-}" code=0
  mkdir -p "$(dirname "$evidence")" || return $?
  "$binary" --build-info >"$evidence.stdout.json" 2>"$evidence.stderr.log" || code=$?
  printf '%s\n' "$code" >"$evidence.exit"
  [ "$code" -eq 0 ] || return "$code"
  python3 - "$evidence.stdout.json" "$expected_identity" <<'INFO'
import json, sys
try:
    v=json.load(open(sys.argv[1]))
    assert v["schema_version"] == 1
    assert v["actual_cfg"]["debug_assertions"] is True
    assert v["capabilities"] and all(isinstance(x["compiled_capabilities"], list) for x in v["capabilities"])
    assert any("test-controls" in x["compiled_capabilities"] for x in v["capabilities"])
    if v.get("package") == "novarocks-server":
        assert v["build_kind"] == "test-controls"
        assert "test-controls" in next(x["compiled_capabilities"] for x in v["capabilities"] if x["owner"] == "novarocks-server")
    if sys.argv[2]:
        assert v["explicit_build_identity"] == sys.argv[2]
except (ValueError, KeyError, AssertionError, TypeError, StopIteration):
    sys.exit("error: invalid actual binary build information")
INFO
}

ci_capture_main_builds() {
  local receipt="$1" evidence="$2" name binary
  for name in novarocks novarocks-system-tests novarocks-sql-test; do
    binary="$(ci_bound_binary "$name" "$receipt")" || return $?
    ci_capture_build_info "$binary" "$evidence/$name" || return $?
  done
}
