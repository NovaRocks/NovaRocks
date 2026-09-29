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

source "$REPO_ROOT/tools/ci/local-full-ci.sh" --source-only

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

if (
  parse_args --cargo-only --suite filter
) >/dev/null 2>&1; then
  echo "--cargo-only must reject SQL and runtime options" >&2
  exit 1
fi

cargo_only_capture="$tmpdir/cargo-only-main"
(
  init_run_dir() {
    CI_RUN_DIR="$tmpdir/cargo-only-run"
    CI_SUMMARY="$CI_RUN_DIR/summary.md"
    mkdir -p "$CI_RUN_DIR"
  }
  validate_explicit_suites_early() {
    printf '%s\n' "validate" >>"$cargo_only_capture"
  }
  run_cargo_gates() {
    printf '%s\n' "cargo" >>"$cargo_only_capture"
  }
  prepare_runtime() {
    printf '%s\n' "runtime" >>"$cargo_only_capture"
    return 97
  }
  run_system_scenarios_stage() {
    printf '%s\n' "system" >>"$cargo_only_capture"
    return 97
  }
  run_sql_suites() {
    printf '%s\n' "sql" >>"$cargo_only_capture"
    return 97
  }
  ci_render_summary() { :; }

  main --cargo-only >/dev/null
)

if [ "$(tr '\n' ' ' <"$cargo_only_capture")" != "validate cargo " ]; then
  echo "--cargo-only must run validation and Cargo gates without runtime or SQL stages" >&2
  cat "$cargo_only_capture" >&2
  exit 1
fi

blocked_run="$tmpdir/blocked-run"
blocked_capture="$tmpdir/blocked-cargo"
blocked_code=0
(
  init_run_dir() {
    CI_RUN_DIR="$blocked_run"
    CI_SUMMARY="$CI_RUN_DIR/summary.md"
    mkdir -p "$CI_RUN_DIR"
    ci_init_summary_state
    ci_set_repo_context "$REPO_ROOT" test test
    ci_render_summary "RUNNING"
  }
  verify_fixture_inputs() {
    printf '%s\n' "verify" >"$blocked_run/verify-called"
    return 75
  }
  run_cargo_gates() {
    printf '%s\n' "cargo" >"$blocked_capture"
  }

  main --tier smoke >/dev/null
) || blocked_code=$?
if [ "$blocked_code" -ne 75 ]; then
  echo "missing fixture BOM must preserve exit 75 and stop CI as BLOCKED" >&2
  exit 1
fi

grep -Fx -- '- Status: BLOCKED' "$blocked_run/summary.md" >/dev/null
grep -F '| fixture prerequisites | BLOCKED |' "$blocked_run/summary.md" >/dev/null
grep -F '| fixture input BOM missing or invalid | env.log |' "$blocked_run/summary.md" >/dev/null
[[ -f "$blocked_run/verify-called" ]]
if [[ -e "$blocked_capture" ]]; then
  echo "BLOCKED fixture preflight must run before Cargo gates" >&2
  exit 1
fi

for owner_error in PortUnavailable RuntimeIdentityMismatch ExternalAttachmentsPresent; do
  runtime_failed_run="$tmpdir/runtime-failed-$owner_error"
  runtime_failed_capture="$tmpdir/runtime-failed-cargo-$owner_error"
  runtime_failed_code=0
  (
    init_run_dir() {
      CI_RUN_DIR="$runtime_failed_run"
      CI_SUMMARY="$CI_RUN_DIR/summary.md"
      mkdir -p "$CI_RUN_DIR"
      ci_init_summary_state
      ci_set_repo_context "$REPO_ROOT" test test
      ci_render_summary "RUNNING"
    }
    verify_fixture_inputs() { return 0; }
    function docker/iceberg-rest/up.sh {
      echo "error: $owner_error: synthetic owner failure" >&2
      return 1
    }
    run_cargo_gates() {
      printf '%s\n' "cargo" >"$runtime_failed_capture"
    }

    main --tier smoke >/dev/null
  ) || runtime_failed_code=$?
  if [ "$runtime_failed_code" -ne 1 ]; then
    echo "owner errors must preserve their failure exit code" >&2
    exit 1
  fi
  grep -Fx -- '- Status: VERIFY FAILED' "$runtime_failed_run/summary.md" >/dev/null
  grep -F '| prepare runtime | VERIFY FAILED |' "$runtime_failed_run/summary.md" >/dev/null
  grep -F "$owner_error" "$runtime_failed_run/env.log" >/dev/null
  if grep -F '| fixture prerequisites | BLOCKED |' "$runtime_failed_run/summary.md" >/dev/null; then
    echo "owner errors must not be classified as fixture BLOCKED" >&2
    exit 1
  fi
  if [[ -e "$runtime_failed_capture" ]]; then
    echo "VERIFY FAILED runtime setup must run before Cargo gates" >&2
    exit 1
  fi
done

publication_a="$tmpdir/publications/a"
publication_b="$tmpdir/publications/b"
mkdir -p "$publication_a" "$publication_b" "$tmpdir/entry"
cat >"$publication_a/env.sh" <<EOF
export NOVA_ENV_REST_ENV_FILE='$publication_a/env.sh'
export NOVA_ENV_RUNTIME_DIR='$tmpdir/stable-runtime'
export NOVA_ENV_OBJECT_STORE_RUNTIME='os-test'
export NOVA_ENV_CATALOG_RUNTIME='cat-test'
export AWS_S3_ENDPOINT='http://127.0.0.1:28000'
export NOVAROCKS_ICEBERG_REST_URI='http://127.0.0.1:28002'
EOF
printf '%s\n' 'return 96' >"$publication_b/env.sh"
ln -s "$publication_a" "$tmpdir/entry/published"
ln -s published/env.sh "$tmpdir/entry/env.sh"
(
  load_fixture_publication "$tmpdir/entry/env.sh"
  rm "$tmpdir/entry/published"
  ln -s "$publication_b" "$tmpdir/entry/published"
  record_fixture_runtime >"$tmpdir/runtime-receipt.log"
  [ "$NOVA_ENV_REST_ENV_FILE" = "$publication_a/env.sh" ]
  [ "$NOVA_ENV_RUNTIME_DIR" = "$tmpdir/stable-runtime" ]
)
for receipt in \
  'NOVA_ENV_OBJECT_STORE_RUNTIME=os-test' \
  'NOVA_ENV_CATALOG_RUNTIME=cat-test' \
  "NOVA_ENV_PUBLICATION_DIR=$publication_a" \
  'AWS_S3_ENDPOINT=http://127.0.0.1:28000' \
  'NOVAROCKS_ICEBERG_REST_URI=http://127.0.0.1:28002'; do
  grep -Fx "$receipt" "$tmpdir/runtime-receipt.log" >/dev/null
done

stage_capture="$tmpdir/cargo-gates"
(
  SKIP_CARGO_TEST="true"
  run_fail_fast_stage() {
    printf '%s|%s|' "$1" "$2" >>"$stage_capture"
    shift 2
    printf '%s\n' "$*" >>"$stage_capture"
  }
  ci_record_stage() { :; }
  ci_render_summary() { :; }
  run_cargo_gates
)

if ! grep -Fx \
  "locked Cargo metadata|cargo-metadata.log|cargo metadata --locked --format-version 1 --no-deps" \
  "$stage_capture" >/dev/null; then
  echo "local full CI must validate locked Cargo metadata" >&2
  exit 1
fi
if ! grep -Fx \
  "Cargo dependency policy|cargo-deny.log|cargo deny --locked check advisories bans licenses sources" \
  "$stage_capture" >/dev/null; then
  echo "local full CI must enforce the resolved dependency policy" >&2
  exit 1
fi
if ! grep -Fx \
  "cargo check all targets|cargo-check-all-targets.log|cargo check --workspace --all-targets --locked" \
  "$stage_capture" >/dev/null; then
  echo "local full CI must check every workspace target with the committed lock" >&2
  exit 1
fi
if ! grep -Fx \
  "DataSketches resolved source|datasketches-source.log|python3 tools/ci/check-datasketches-source.py" \
  "$stage_capture" >/dev/null; then
  echo "local full CI must run the DataSketches resolved-source stage" >&2
  exit 1
fi
if ! grep -Fx \
  "DataSketches source mutations|datasketches-source-test.log|tools/ci/tests/datasketches-source-test.sh" \
  "$stage_capture" >/dev/null; then
  echo "local full CI must run the DataSketches source mutation stage" >&2
  exit 1
fi
if ! grep -Fx \
  "NCP-8 statistics boundary|ncp8-statistics-boundary.log|tools/ci/check-ncp8-statistics-boundary.py" \
  "$stage_capture" >/dev/null; then
  echo "local full CI must run the NCP-8 statistics boundary stage" >&2
  exit 1
fi
if ! grep -Fx \
  "NCP-8 statistics boundary mutations|ncp8-statistics-boundary-test.log|tools/ci/tests/ncp8-statistics-boundary-test.sh" \
  "$stage_capture" >/dev/null; then
  echo "local full CI must run the NCP-8 statistics boundary mutation stage" >&2
  exit 1
fi

baseline="$tmpdir/known-failures.toml"
run_dir="$tmpdir/run"
mkdir -p "$run_dir/sql"

cat >"$baseline" <<'EOF'
[[failure]]
tier = "full"
suite = "tpc-ds"
case = "q93"
error_code = "QueryTimeout"
reason = "synthetic timeout"

[[failure]]
tier = "full"
suite = "tpc-ds"
case = "q94"
error_code = "CommitUnknown"
reason = "synthetic commit unknown"
EOF

cat >"$run_dir/sql/tpc-ds.log" <<'EOF'
[novarocks-sql-test] suite=tpc-ds mode=verify
  [tpc-ds] q94 (steps=1)
    engine_error_code=CommitUnknown target execute failed: ERROR 1105 (HY000): [CommitUnknown] commit outcome unavailable
case timings (all):
  [tpc-ds] q93 PASS 0.01s
  [tpc-ds] q94 FAIL 0.01s
FAIL: total=2 pass=1 fail=1
EOF

CI_TIER="full"
KNOWN_FAILURES_FILE="$baseline"
CI_KNOWN_FAILURE_ROWS=""
CI_FAILURE_TAIL=""

if ci_classify_unexpected_passes "tpc-ds" "$run_dir/sql/tpc-ds.log"; then
  echo "expected mixed pass/fail known-failure log to report an unexpected pass" >&2
  exit 1
fi

grep -q "UNEXPECTED_PASS" <<<"$CI_KNOWN_FAILURE_ROWS"

if (
  CI_FROM_RUN_DIR="$run_dir"
  CI_TIER="full"
  KNOWN_FAILURES_FILE="$baseline"
  reclassify_existing_run >/dev/null
); then
  echo "expected --from reclassification to fail on mixed unexpected pass" >&2
  exit 1
fi

grep -q "UNEXPECTED_PASS" "$run_dir/summary.md"

targeted_suites="$(ci_tier_suites targeted "$REPO_ROOT/tools/ci/suites/stable-correctness-suites.txt")"
grep -qx "optimizer-dist" <<<"$targeted_suites"

if ci_suite_exists "$REPO_ROOT" ssb; then
  echo "benchmark workload must not be accepted as an explicit correctness suite" >&2
  exit 1
fi
if ! ci_suite_exists "$REPO_ROOT" filter; then
  echo "correctness suite must remain selectable" >&2
  exit 1
fi

if [ "$SQL_CLUSTER_MODE" != "cross-process" ]; then
  echo "default SQL cluster mode must be cross-process" >&2
  exit 1
fi
if [ "$SQL_CLUSTER_SIZE" != "3" ]; then
  echo "default SQL cluster size must be 3 BEs" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_mode optimizer)" != "cross-process" ]; then
  echo "ordinary suites must default to cross-process mode" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_size optimizer)" != "3" ]; then
  echo "ordinary suites must default to a 3-BE cluster" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_mode distributed-resilience)" != "cross-process" ]; then
  echo "distributed-resilience must run in cross-process mode" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_size distributed-resilience)" != "3" ]; then
  echo "distributed-resilience must run with 3 BEs" >&2
  exit 1
fi
if ! grep -qx 'distributed-resilience' "$REPO_ROOT/tools/ci/suites/stable-correctness-suites.txt"; then
  echo "distributed-resilience must be part of the stable SQL suite set" >&2
  exit 1
fi
if ci_native_cross_process_enabled; then
  echo "the duplicate native cross-process matrix must be disabled by default" >&2
  exit 1
fi

if ! (
  unset SQL_CLUSTER_MODE SQL_CLUSTER_SIZE
  source "$REPO_ROOT/tools/ci/local-full-ci.sh" --source-only
  parse_args --cluster-mode all-in-one
  [ "$SQL_CLUSTER_MODE" = "all-in-one" ] && [ "$SQL_CLUSTER_SIZE" = "1" ]
); then
  echo "explicit all-in-one mode without a size must infer cluster size 1" >&2
  exit 1
fi

SQL_CLUSTER_MODE="all-in-one"
SQL_CLUSTER_SIZE="1"
if [ "$(ci_suite_cluster_mode optimizer-dist)" != "cross-process" ]; then
  echo "optimizer-dist must force cross-process cluster mode" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_size optimizer-dist)" != "3" ]; then
  echo "optimizer-dist must force a 3-BE cluster" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_mode optimizer)" != "all-in-one" ]; then
  echo "ordinary suites should keep the global cluster mode" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_size optimizer)" != "1" ]; then
  echo "ordinary suites should keep the global cluster size" >&2
  exit 1
fi

native_cross_process_core_suites="$(ci_native_cross_process_core_suites)"
expected_native_cross_process_core_suites="$(printf "%s\n" join filter sort aggregate cte subquery iceberg-rest runtime-filter-distributed)"
if [ "$native_cross_process_core_suites" != "$expected_native_cross_process_core_suites" ]; then
  echo "native cross-process core suites do not match the required matrix" >&2
  printf "expected:\n%s\nactual:\n%s\n" "$expected_native_cross_process_core_suites" "$native_cross_process_core_suites" >&2
  exit 1
fi

NOVA_CI_NATIVE_CROSS_PROCESS_CORE="1"
if ! ci_native_cross_process_enabled; then
  echo "explicit NOVA_CI_NATIVE_CROSS_PROCESS_CORE=1 should enable the native cross-process matrix" >&2
  exit 1
fi

if [ "$(ci_native_cross_process_suites | tr '\n' ' ')" != "$(printf "%s " join filter sort aggregate cte subquery iceberg-rest runtime-filter-distributed)" ]; then
  echo "explicit native cross-process core matrix should use the core suites" >&2
  exit 1
fi

NOVA_CI_NATIVE_CROSS_PROCESS_CORE="0"
NOVA_CI_NATIVE_CROSS_PROCESS_FULL="0"
if ci_native_cross_process_enabled; then
  echo "explicit NOVA_CI_NATIVE_CROSS_PROCESS_CORE=0 should disable the native cross-process matrix when full coverage is off" >&2
  exit 1
fi

NOVA_CI_NATIVE_CROSS_PROCESS_FULL="1"
if ! ci_native_cross_process_enabled; then
  echo "NOVA_CI_NATIVE_CROSS_PROCESS_FULL=1 should enable the matrix even when core coverage is off" >&2
  exit 1
fi
if ! ci_native_cross_process_suites | grep -qx "optimizer-dist"; then
  echo "native cross-process full matrix should include stable full suites" >&2
  exit 1
fi

SQL_CLUSTER_MODE="all-in-one"
SQL_CLUSTER_SIZE="1"
if [ "$(ci_native_cross_process_suite_cluster_mode join)" != "cross-process" ]; then
  echo "native cross-process suites must force cross-process cluster mode" >&2
  exit 1
fi
if [ "$(ci_native_cross_process_suite_cluster_size join)" != "3" ]; then
  echo "native cross-process suites must force a 3-BE cluster" >&2
  exit 1
fi

launcher_capture="$tmpdir/sql-runner-launcher.args"
(
  REPO_ROOT="$tmpdir/current-worktree"
  CI_RUN_DIR="$tmpdir/sql-runner-launcher"
  NOVAROCKS_SQL_TEST_CONFIG="$tmpdir/sql-test.toml"
  NOVA_ENV_RUNTIME_DIR="$tmpdir/stable-runtime"
  NOVA_ENV_REST_ENV_FILE="$publication_a/env.sh"
  RUN_MODE="explicit"
  mkdir -p "$CI_RUN_DIR/sql"

  resolve_suites() {
    SUITES=(iceberg-dml)
  }
  ci_run_logged() {
    printf '%s\n' "$@" >"$launcher_capture"
    return 0
  }
  ci_record_sql_suite() { :; }
  ci_render_summary() { :; }

  run_sql_suites
)

if ! grep -Fx "NOVAROCKS_WORKSPACE_ROOT=$tmpdir/current-worktree" "$launcher_capture" >/dev/null; then
  echo "SQL runner must receive the current CI worktree root" >&2
  exit 1
fi

if ! grep -Fx "NOVA_ENV_REST_ENV_FILE=$publication_a/env.sh" "$launcher_capture" >/dev/null; then
  echo "SQL runner must receive the exact prepared Iceberg REST environment" >&2
  exit 1
fi

if grep -Fx -- "--query-timeout" "$launcher_capture" >/dev/null; then
  echo "iceberg-dml must use the SQL runner's suite timeout when CI has no explicit override" >&2
  exit 1
fi

native_launcher_capture="$tmpdir/native-sql-runner-launcher.args"
(
  REPO_ROOT="$tmpdir/current-worktree"
  CI_RUN_DIR="$tmpdir/native-sql-runner-launcher"
  NOVAROCKS_SQL_TEST_CONFIG="$tmpdir/sql-test.toml"
  NOVA_ENV_RUNTIME_DIR="$tmpdir/stable-runtime"
  NOVA_ENV_REST_ENV_FILE="$publication_a/env.sh"
  SQL_CLUSTER_MODE="cross-process"
  NOVA_CI_NATIVE_CROSS_PROCESS_FULL="1"

  ci_native_cross_process_suites() {
    printf '%s\n' iceberg-dml
  }
  ci_suite_exists() { return 0; }
  ci_run_logged() {
    printf '%s\n' "$@" >"$native_launcher_capture"
    return 0
  }
  ci_record_sql_suite() { :; }
  ci_render_summary() { :; }

  run_native_cross_process_sql_suites
)

if ! grep -Fx "NOVA_ENV_REST_ENV_FILE=$publication_a/env.sh" "$native_launcher_capture" >/dev/null; then
  echo "native SQL runner must receive the exact prepared publication" >&2
  exit 1
fi

if grep -Fx -- "--query-timeout" "$native_launcher_capture" >/dev/null; then
  echo "native iceberg-dml must use the SQL runner's suite timeout when CI has no explicit override" >&2
  exit 1
fi

override_capture="$tmpdir/sql-runner-timeout-override.args"
(
  REPO_ROOT="$tmpdir/current-worktree"
  CI_RUN_DIR="$tmpdir/sql-runner-timeout-override"
  NOVAROCKS_SQL_TEST_CONFIG="$tmpdir/sql-test.toml"
  NOVA_ENV_RUNTIME_DIR="$tmpdir/stable-runtime"
  NOVA_ENV_REST_ENV_FILE="$publication_a/env.sh"
  RUN_MODE="explicit"
  SQL_QUERY_TIMEOUT_SECONDS="75"
  mkdir -p "$CI_RUN_DIR/sql"

  resolve_suites() {
    SUITES=(iceberg-dml)
  }
  ci_run_logged() {
    printf '%s\n' "$@" >"$override_capture"
    return 0
  }
  ci_record_sql_suite() { :; }
  ci_render_summary() { :; }

  run_sql_suites
)

if ! awk '
  previous == "--query-timeout" && $0 == "75" { found = 1 }
  { previous = $0 }
  END { exit(found ? 0 : 1) }
' "$override_capture"; then
  echo "SQL_QUERY_TIMEOUT_SECONDS must override the iceberg-dml suite timeout" >&2
  exit 1
fi

echo "local-full-ci-classification-test: PASS"
