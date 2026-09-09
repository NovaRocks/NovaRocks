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

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/lib/logging.sh"
source "$SCRIPT_DIR/lib/command.sh"
source "$SCRIPT_DIR/lib/sql_suites.sh"
source "$SCRIPT_DIR/lib/known_failures.sh"
source "$SCRIPT_DIR/lib/server.sh"
source "$SCRIPT_DIR/lib/system_scenarios.sh"

STABLE_SUITES_FILE="$SCRIPT_DIR/suites/stable-correctness-suites.txt"
KNOWN_FAILURES_FILE="$SCRIPT_DIR/baselines/known-failures.toml"
RUN_MODE="stable"
CI_TIER="full"
CI_FROM_RUN_DIR=""
CARGO_ONLY="false"
CARGO_ONLY_CONFLICT="false"
ALL_DISCOVERED_REQUESTED="false"
KEEP_RUNTIME="false"
SKIP_CARGO_TEST="false"
SKIP_SYSTEM_SCENARIOS="false"
SYSTEM_SCENARIO_BASE_CONFIG="$SCRIPT_DIR/fixtures/system-scenarios-base.toml"
SYSTEM_SCENARIO_CLUSTER_SIZE="${NOVA_CI_SYSTEM_CLUSTER_SIZE:-3}"
SYSTEM_SCENARIO_TIMEOUT_SECS="${NOVA_CI_SYSTEM_TIMEOUT_SECS:-900}"
REQUESTED_SUITES=()
CI_RUNTIME_PREPARED="false"
PAIMON_FIXTURE_PREPARED="false"
PAIMON_FIXTURE_DIR=""
PAIMON_FIXTURE_RUN_ID=""
NOVA_CI_CARGO_PROFILE="${NOVA_CI_CARGO_PROFILE:-dev-opt}"
SQL_CLUSTER_MODE="${SQL_CLUSTER_MODE:-cross-process}"
if [ -n "${SQL_CLUSTER_SIZE:-}" ]; then
  SQL_CLUSTER_SIZE_EXPLICIT="true"
else
  SQL_CLUSTER_SIZE_EXPLICIT="false"
  case "$SQL_CLUSTER_MODE" in
    all-in-one)
      SQL_CLUSTER_SIZE="1"
      ;;
    *)
      SQL_CLUSTER_SIZE="3"
      ;;
  esac
fi
NOVA_CI_NATIVE_CROSS_PROCESS_CORE="${NOVA_CI_NATIVE_CROSS_PROCESS_CORE:-0}"
NOVA_CI_NATIVE_CROSS_PROCESS_FULL="${NOVA_CI_NATIVE_CROSS_PROCESS_FULL:-0}"
NOVA_CI_NATIVE_CROSS_PROCESS_REQUIRED="${NOVA_CI_NATIVE_CROSS_PROCESS_REQUIRED:-0}"

usage() {
  cat <<'EOF'
Usage: tools/ci/local-full-ci.sh [options]

Runs NovaRocks local full CI with local logs under logs/ci-full/<timestamp>/.
SQL suites are executed with novarocks-sql-test -j 1 because parallel case execution is
not stable in the current local environment.
Rust tests are executed with --test-threads=1 for the same reason.
Cargo build/test/run stages use NOVA_CI_CARGO_PROFILE, defaulting to dev-opt.
Clippy runs in warning-only mode until the repository has a clean strict-clippy
baseline.
Selected SQL suites run once on a native 1FE+3BE cross-process cluster by
default. Set NOVA_CI_NATIVE_CROSS_PROCESS_CORE=1 to append the focused native
cross-process matrix, NOVA_CI_NATIVE_CROSS_PROCESS_FULL=1 to append the stable
full-suite matrix, and NOVA_CI_NATIVE_CROSS_PROCESS_REQUIRED=1 to make failures
in an appended matrix fail CI.

Options:
  --cargo-only          Run only the deterministic Cargo and repository guards.
                        This mode does not prepare runtime services or run SQL.
  --all-discovered      Run every correctness suite through typed runner discovery.
  --suite <name>        Run only the named SQL suite. May be repeated.
  --tier <name>         Stable tier: smoke, targeted, or full. Default: full.
  --from <run-dir>      Reclassify an existing logs/ci-full run without rerun.
  --skip-cargo-test     Skip the Rust test stages. Only for runner debugging.
  --skip-system-scenarios
                        Skip the native 1FE+3BE System Scenario stage. Only for
                        runner debugging; the stable default runs every
                        registered scenario and records a skip as SKIP.
  --cluster-mode <mode> SQL runner cluster mode. Default: cross-process.
  --cluster-size <n>    Number of BE processes. Default: 3, or 1 for all-in-one.
  --keep-runtime        Keep this worktree's docker/iceberg-rest runtime entry.
  -h, --help            Show this help text.
EOF
}

validate_cluster_args() {
  case "$SQL_CLUSTER_MODE" in
    all-in-one|cross-process)
      ;;
    *)
      echo "error: --cluster-mode must be all-in-one or cross-process" >&2
      exit 2
      ;;
  esac

  case "$SQL_CLUSTER_SIZE" in
    ''|*[!0-9]*)
      echo "error: --cluster-size must be a positive integer" >&2
      exit 2
      ;;
  esac

  if [ "$SQL_CLUSTER_SIZE" -lt 1 ]; then
    echo "error: --cluster-size must be >= 1" >&2
    exit 2
  fi

  if [ "$SQL_CLUSTER_MODE" = "all-in-one" ] && [ "$SQL_CLUSTER_SIZE" -ne 1 ]; then
    echo "error: all-in-one mode requires --cluster-size 1" >&2
    exit 2
  fi
}

validate_tier_arg() {
  case "$CI_TIER" in
    smoke|targeted|full)
      ;;
    *)
      echo "error: --tier must be smoke, targeted, or full" >&2
      exit 2
      ;;
  esac
}

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --cargo-only)
        CARGO_ONLY="true"
        shift
        ;;
      --all-discovered)
        CARGO_ONLY_CONFLICT="true"
        ALL_DISCOVERED_REQUESTED="true"
        shift
        ;;
      --suite)
        CARGO_ONLY_CONFLICT="true"
        if [ "$#" -lt 2 ]; then
          echo "error: --suite requires a suite name" >&2
          exit 2
        fi
        REQUESTED_SUITES+=("$2")
        shift 2
        ;;
      --tier)
        CARGO_ONLY_CONFLICT="true"
        if [ "$#" -lt 2 ]; then
          echo "error: --tier requires a tier name" >&2
          exit 2
        fi
        CI_TIER="$2"
        validate_tier_arg
        shift 2
        ;;
      --from)
        CARGO_ONLY_CONFLICT="true"
        if [ "$#" -lt 2 ]; then
          echo "error: --from requires a run directory" >&2
          exit 2
        fi
        CI_FROM_RUN_DIR="$2"
        shift 2
        ;;
      --skip-cargo-test)
        CARGO_ONLY_CONFLICT="true"
        SKIP_CARGO_TEST="true"
        shift
        ;;
      --skip-system-scenarios)
        CARGO_ONLY_CONFLICT="true"
        SKIP_SYSTEM_SCENARIOS="true"
        shift
        ;;
      --cluster-mode)
        CARGO_ONLY_CONFLICT="true"
        if [ "$#" -lt 2 ]; then
          echo "error: --cluster-mode requires a mode" >&2
          exit 2
        fi
        SQL_CLUSTER_MODE="$2"
        if [ "$SQL_CLUSTER_SIZE_EXPLICIT" != "true" ]; then
          case "$SQL_CLUSTER_MODE" in
            all-in-one)
              SQL_CLUSTER_SIZE="1"
              ;;
            *)
              SQL_CLUSTER_SIZE="3"
              ;;
          esac
        fi
        shift 2
        ;;
      --cluster-size)
        CARGO_ONLY_CONFLICT="true"
        if [ "$#" -lt 2 ]; then
          echo "error: --cluster-size requires a count" >&2
          exit 2
        fi
        SQL_CLUSTER_SIZE="$2"
        SQL_CLUSTER_SIZE_EXPLICIT="true"
        shift 2
        ;;
      --keep-runtime)
        CARGO_ONLY_CONFLICT="true"
        KEEP_RUNTIME="true"
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "error: unknown option: $1" >&2
        usage >&2
        exit 2
      ;;
  esac
  done

  validate_cluster_args
  validate_tier_arg

  if [ "$CARGO_ONLY" = "true" ] && [ "$CARGO_ONLY_CONFLICT" = "true" ]; then
    echo "error: --cargo-only cannot be combined with runtime, SQL, reclassification, or skip options" >&2
    exit 2
  fi

  if [ "$ALL_DISCOVERED_REQUESTED" = "true" ] && [ "${#REQUESTED_SUITES[@]}" -gt 0 ]; then
    echo "error: --all-discovered cannot be combined with --suite" >&2
    exit 2
  fi

  if [ "$ALL_DISCOVERED_REQUESTED" = "true" ]; then
    RUN_MODE="all-discovered"
  elif [ "${#REQUESTED_SUITES[@]}" -gt 0 ]; then
    RUN_MODE="explicit"
  else
    RUN_MODE="stable"
  fi
}

init_from_run_dir() {
  local run_dir="$CI_FROM_RUN_DIR"

  case "$run_dir" in
    /*)
      ;;
    *)
      run_dir="$REPO_ROOT/$run_dir"
      ;;
  esac

  if [ ! -d "$run_dir" ]; then
    echo "error: --from run directory does not exist: $CI_FROM_RUN_DIR" >&2
    exit 2
  fi

  if [ ! -d "$run_dir/sql" ]; then
    echo "error: --from run directory has no sql logs: $CI_FROM_RUN_DIR" >&2
    exit 2
  fi

  CI_RUN_DIR="$run_dir"
  CI_SUMMARY="$CI_RUN_DIR/summary.md"
  ci_init_summary_state
  ci_set_repo_context \
    "$REPO_ROOT" \
    "$(git -C "$REPO_ROOT" symbolic-ref --short -q HEAD || echo HEAD)" \
    "$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
}

init_run_dir() {
  local stamp
  stamp="$(date +"%Y%m%d-%H%M%S")"
  CI_RUN_DIR="$REPO_ROOT/logs/ci-full/$stamp"
  if [ -e "$CI_RUN_DIR" ]; then
    CI_RUN_DIR="$REPO_ROOT/logs/ci-full/${stamp}-$$"
  fi

  mkdir -p "$CI_RUN_DIR/sql"
  CI_SUMMARY="$CI_RUN_DIR/summary.md"
  ci_init_summary_state
  ci_set_repo_context \
    "$REPO_ROOT" \
    "$(git -C "$REPO_ROOT" symbolic-ref --short -q HEAD || echo HEAD)" \
    "$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  ci_render_summary "RUNNING"
}

cleanup() {
  local status=$?
  ci_stop_all_in_one_server || true

  if [ "$PAIMON_FIXTURE_PREPARED" = "true" ] && [ -n "$PAIMON_FIXTURE_DIR" ]; then
    {
      echo "+ docker/paimon-read/cleanup.sh --output-dir <ci-run>/paimon-fixture --env-file <runtime-env> --run-id $PAIMON_FIXTURE_RUN_ID"
      docker/paimon-read/cleanup.sh \
        --output-dir "$PAIMON_FIXTURE_DIR" \
        --env-file "$NOVA_ENV_REST_ENV_FILE" \
        --run-id "$PAIMON_FIXTURE_RUN_ID"
    } >>"$CI_RUN_DIR/env.log" 2>&1 || true
  fi

  if [ "$CI_RUNTIME_PREPARED" = "true" ] && [ "$KEEP_RUNTIME" != "true" ]; then
    {
      echo "+ docker/iceberg-rest/down.sh --runtime-only --purge"
      docker/iceberg-rest/down.sh --runtime-only --purge
    } >>"$CI_RUN_DIR/env.log" 2>&1 || true
  fi

  exit "$status"
}

require_runtime_var() {
  local name="$1"
  local value
  eval "value=\${$name:-}"
  if [ -z "$value" ]; then
    echo "missing required runtime variable: $name"
    return 1
  fi
  return 0
}

prepare_runtime() {
  local log_path="$CI_RUN_DIR/env.log"
  local start
  local code
  local duration

  start="$(ci_epoch)"
  {
    echo "+ docker/iceberg-rest/up.sh"
    docker/iceberg-rest/up.sh &&
      echo "+ source docker/iceberg-rest/runtime/current/env.sh" &&
      . docker/iceberg-rest/runtime/current/env.sh &&
      require_runtime_var NOVAROCKS_FE_CONFIG &&
      require_runtime_var NOVAROCKS_BE_CONFIG &&
      require_runtime_var NOVAROCKS_SQL_TEST_CONFIG &&
      require_runtime_var NOVA_ENV_RUNTIME_DIR &&
      require_runtime_var NOVA_ENV_MYSQL_PORT &&
      require_runtime_var NOVAROCKS_ICEBERG_REST_URI &&
      require_runtime_var NOVA_ENV_REST_ENV_FILE &&
      PAIMON_FIXTURE_DIR="$CI_RUN_DIR/paimon-fixture" &&
      PAIMON_FIXTURE_RUN_ID="ci-${CI_COMMIT_SHA:0:12}-$(basename "$CI_RUN_DIR")" &&
      echo "+ docker/paimon-read/prepare.sh --env-file <runtime-env> --run-id $PAIMON_FIXTURE_RUN_ID --output-dir <ci-run>/paimon-fixture" &&
      docker/paimon-read/prepare.sh \
        --env-file "$NOVA_ENV_REST_ENV_FILE" \
        --run-id "$PAIMON_FIXTURE_RUN_ID" \
        --output-dir "$PAIMON_FIXTURE_DIR"
    code=$?
    if [ "$code" -eq 0 ]; then
      export NOVAROCKS_PAIMON_FIXTURE_MANIFEST="$PAIMON_FIXTURE_DIR/manifest.json"
      export SYSTEM_SCENARIO_BASE_CONFIG="$PAIMON_FIXTURE_DIR/base-server.toml"
      export NOVAROCKS_SQL_TEST_CONFIG="$PAIMON_FIXTURE_DIR/sql-runner.toml"
      PAIMON_FIXTURE_PREPARED="true"
      echo "NOVAROCKS_FE_CONFIG=$NOVAROCKS_FE_CONFIG"
      echo "NOVAROCKS_BE_CONFIG=$NOVAROCKS_BE_CONFIG"
      echo "NOVAROCKS_SQL_TEST_CONFIG=$NOVAROCKS_SQL_TEST_CONFIG"
      echo "NOVA_ENV_RUNTIME_DIR=$NOVA_ENV_RUNTIME_DIR"
      echo "NOVA_ENV_MYSQL_PORT=$NOVA_ENV_MYSQL_PORT"
      echo "NOVAROCKS_ICEBERG_REST_URI=$NOVAROCKS_ICEBERG_REST_URI"
      echo "NOVAROCKS_PAIMON_FIXTURE_MANIFEST=$NOVAROCKS_PAIMON_FIXTURE_MANIFEST"
      echo "NOVAROCKS_SPARK_DEFAULTS=${NOVAROCKS_SPARK_DEFAULTS:-}"
      echo "NOVA_CI_CARGO_PROFILE=$NOVA_CI_CARGO_PROFILE"
      echo "SQL_CLUSTER_MODE=$SQL_CLUSTER_MODE"
      echo "SQL_CLUSTER_SIZE=$SQL_CLUSTER_SIZE"
      echo "NOVA_CI_NATIVE_CROSS_PROCESS_CORE=$NOVA_CI_NATIVE_CROSS_PROCESS_CORE"
      echo "NOVA_CI_NATIVE_CROSS_PROCESS_FULL=$NOVA_CI_NATIVE_CROSS_PROCESS_FULL"
      echo "NOVA_CI_NATIVE_CROSS_PROCESS_REQUIRED=$NOVA_CI_NATIVE_CROSS_PROCESS_REQUIRED"
    fi
    test "$code" -eq 0
  } >"$log_path" 2>&1
  code=$?
  duration=$(($(ci_epoch) - start))

  if [ "$code" -ne 0 ]; then
    ci_record_stage "prepare runtime" "FAIL" "$duration" "$log_path"
    ci_mark_failure_tail "prepare runtime failed" "$log_path"
    ci_render_summary "FAIL"
    exit "$code"
  fi

  CI_RUNTIME_PREPARED="true"
  ci_set_runtime_context "$NOVAROCKS_FE_CONFIG" "$NOVAROCKS_SQL_TEST_CONFIG" "$NOVA_ENV_MYSQL_PORT"
  ci_record_stage "prepare runtime" "PASS" "$duration" "$log_path"
  ci_render_summary "RUNNING"
}

reset_frontend_state_store_stage() {
  local log_path="$CI_RUN_DIR/frontend-state-reset.log"
  local start
  local code
  local duration
  local state_store_db

  state_store_db="$NOVA_ENV_RUNTIME_DIR/frontend-state.sqlite"
  start="$(ci_epoch)"
  {
    echo "Reset frontend StateStore before SQL CI."
    echo "state_store_db=$state_store_db"
    rm -f "$state_store_db" "$state_store_db-shm" "$state_store_db-wal"
  } >"$log_path" 2>&1
  code=$?
  duration=$(($(ci_epoch) - start))

  if [ "$code" -ne 0 ]; then
    ci_record_stage "reset frontend StateStore" "FAIL" "$duration" "$log_path"
    ci_mark_failure_tail "reset frontend StateStore failed" "$log_path"
    ci_render_summary "FAIL"
    exit "$code"
  fi

  ci_record_stage "reset frontend StateStore" "PASS" "$duration" "$log_path"
  ci_render_summary "RUNNING"
}

run_fail_fast_stage() {
  local name="$1"
  local log_name="$2"
  shift 2

  local log_path="$CI_RUN_DIR/$log_name"
  local start
  local code
  local duration

  start="$(ci_epoch)"
  ci_run_logged "$log_path" "$@"
  code=$?
  duration=$(($(ci_epoch) - start))

  if [ "$code" -ne 0 ]; then
    ci_record_stage "$name" "FAIL" "$duration" "$log_path"
    ci_mark_failure_tail "$name failed" "$log_path"
    ci_render_summary "FAIL"
    exit "$code"
  fi

  ci_record_stage "$name" "PASS" "$duration" "$log_path"
  ci_render_summary "RUNNING"
}

run_cargo_gates() {
  run_fail_fast_stage "locked Cargo metadata" "cargo-metadata.log" \
    cargo metadata --locked --format-version 1 --no-deps
  run_fail_fast_stage "generated artifact hygiene" "generated-artifact-hygiene.log" \
    tools/ci/check-generated-artifacts.sh
  run_fail_fast_stage "DataSketches resolved source" "datasketches-source.log" \
    python3 tools/ci/check-datasketches-source.py
  run_fail_fast_stage "DataSketches source mutations" "datasketches-source-test.log" \
    tools/ci/tests/datasketches-source-test.sh
  run_fail_fast_stage "native wire dependency boundary" "native-wire-dependency-boundary.log" \
    tools/ci/check-native-wire-dependency-boundary.py
  run_fail_fast_stage "native wire dependency boundary mutations" "native-wire-dependency-boundary-test.log" \
    tools/ci/tests/native-wire-dependency-boundary-test.sh
  run_fail_fast_stage "native trust dependency boundary" "native-trust-dependency-boundary.log" \
    tools/ci/check-native-trust-dependency-boundary.py
  run_fail_fast_stage "native trust dependency boundary mutations" "native-trust-dependency-boundary-test.log" \
    tools/ci/tests/native-trust-dependency-boundary-test.sh
  run_fail_fast_stage "connector role binding boundary" "connector-role-binding-boundary.log" \
    tools/ci/check-connector-role-binding-boundary.py
  run_fail_fast_stage "connector role binding boundary mutations" "connector-role-binding-boundary-test.log" \
    tools/ci/tests/connector-role-binding-boundary-test.sh
  run_fail_fast_stage "StateStore dependency boundary" "state-store-dependency-boundary.log" \
    python3 tools/ci/check-state-store-dependency-boundary.py --manifest-path Cargo.toml
  run_fail_fast_stage "StateStore dependency boundary mutations" "state-store-dependency-boundary-test.log" \
    tools/ci/tests/state-store-dependency-boundary-test.sh
  run_fail_fast_stage "SPI dependency boundary" "spi-dependency-boundary.log" \
    python3 tools/ci/check-spi-dependency-boundary.py --manifest-path Cargo.toml
  run_fail_fast_stage "SPI dependency boundary mutations" "spi-dependency-boundary-test.log" \
    tools/ci/tests/spi-dependency-boundary-test.sh
  run_fail_fast_stage "NCP-8 statistics boundary" "ncp8-statistics-boundary.log" \
    tools/ci/check-ncp8-statistics-boundary.py
  run_fail_fast_stage "NCP-8 statistics boundary mutations" "ncp8-statistics-boundary-test.log" \
    tools/ci/tests/ncp8-statistics-boundary-test.sh
  run_fail_fast_stage "legacy branding" "legacy-branding.log" \
    tools/ci/check-legacy-branding.sh
  run_fail_fast_stage "legacy branding tests" "legacy-branding-test.log" \
    tools/ci/tests/legacy-branding-test.sh
  run_fail_fast_stage "system scenario stage tests" "system-scenarios-stage-test.log" \
    tools/ci/tests/system-scenarios-stage-test.sh
  run_fail_fast_stage "SQL helper environment binding" "sql-helper-env-file.log" \
    docker/iceberg-rest/tests/sql-helper-env-file-test.sh
  run_fail_fast_stage "shared benchmark environment guard" "shared-benchmark-environment.log" \
    docker/iceberg-rest/tests/shared-benchmark-environment-test.sh
  run_fail_fast_stage "isolated fixture current link" "isolated-fixture-current-link.log" \
    docker/iceberg-rest/tests/isolated-fixture-current-link-test.sh
  run_fail_fast_stage "shared benchmark fixture lifecycle" "shared-benchmark-fixture-lifecycle.log" \
    tests/sql/fixtures/benchmarks/tests/bootstrap-lifecycle-test.sh
  run_fail_fast_stage "shared benchmark fixture contract" "shared-benchmark-fixture-contract.log" \
    python3 tests/sql/fixtures/benchmarks/tests/fixture-contract-test.py
  run_fail_fast_stage "cargo fmt" "cargo-fmt.log" cargo fmt --check
  run_fail_fast_stage "cargo check all targets" "cargo-check-all-targets.log" \
    cargo check --workspace --all-targets --locked
  # `--workspace` is load-bearing. Without it Cargo falls back to
  # `default-members = ["novarocks-server"]`, so the lint/build/test gates
  # silently covered one package and every other workspace member's tests
  # never ran.
  run_fail_fast_stage "cargo clippy" "cargo-clippy.log" \
    cargo clippy --workspace --all-targets --locked
  run_fail_fast_stage "cargo build" "cargo-build.log" \
    cargo build --workspace --profile "$NOVA_CI_CARGO_PROFILE" --locked
  # The SQL error manifest is a generated whitelist that other tools consume as
  # fact: the sql-test runner include_str!s it and rejects any @expect_sql_code
  # missing from it. A stale manifest therefore does not surface as "this file
  # is out of date" -- it surfaces as an unrelated suite failing to load its
  # cases. Regenerating is the fix (`cargo run -p novarocks-error-manifest`).
  # The manifest path keeps the entry point explicit while Cargo resolves the
  # package through the repository workspace, profile, and committed lock.
  run_fail_fast_stage "SQL error manifest freshness" "error-manifest-check.log" \
    cargo run --quiet --locked --manifest-path tools/error-manifest/Cargo.toml -- --check

  if [ "$SKIP_CARGO_TEST" = "true" ]; then
    ci_record_stage "workspace component tests" "SKIP" "0" ""
    ci_record_stage "server owner tests" "SKIP" "0" ""
    ci_record_stage "server binary smoke" "SKIP" "0" ""
    ci_render_summary "RUNNING"
    return 0
  fi

  # Each Cargo target belongs to exactly one stage below, so no test runs
  # twice and a failure names its own layer:
  #   - component: every workspace member except the server package;
  #   - server owner: the server lib, its bin (`main.rs` carries unit tests)
  #     and the direct-call config/composition target;
  #   - server smoke: the child-process binary target only.
  run_fail_fast_stage "workspace component tests" "cargo-test-component.log" \
    cargo test --workspace --exclude novarocks-server \
    --profile "$NOVA_CI_CARGO_PROFILE" --locked -- --test-threads=1
  run_fail_fast_stage "server owner tests" "cargo-test-server-owner.log" \
    cargo test -p novarocks-server --lib --bins --test state_store_app_config \
    --profile "$NOVA_CI_CARGO_PROFILE" --locked -- --test-threads=1
  run_fail_fast_stage "server binary smoke" "cargo-test-server-smoke.log" \
    cargo test -p novarocks-server --test server_binary_smoke \
    --profile "$NOVA_CI_CARGO_PROFILE" --locked -- --test-threads=1
}

run_system_scenario_fixture_stage() {
  if [ "$SKIP_SYSTEM_SCENARIOS" = "true" ]; then
    ci_record_stage "system scenario fixtures" "SKIP" "0" ""
    ci_render_summary "RUNNING"
    return 0
  fi

  local binary="target/$NOVA_CI_CARGO_PROFILE/novarocks"
  local fixture_root="$CI_RUN_DIR/system/fixtures"
  local log_path="$CI_RUN_DIR/system-scenario-fixtures.log"
  local start
  local code
  local duration

  start="$(ci_epoch)"
  ci_prepare_system_scenario_binaries \
    "$binary" \
    "$NOVA_CI_CARGO_PROFILE" \
    "$fixture_root" \
    "${CI_COMMIT_SHA:0:12}" \
    "$log_path"
  code=$?
  duration=$(($(ci_epoch) - start))

  if [ "$code" -ne 0 ]; then
    ci_record_stage "system scenario fixtures" "FAIL" "$duration" "$log_path"
    ci_mark_failure_tail "system scenario fixture build failed" "$log_path"
    ci_render_summary "FAIL"
    exit "$code"
  fi

  ci_record_stage "system scenario fixtures" "PASS" "$duration" "$log_path"
  ci_render_summary "RUNNING"
}

run_system_scenarios_stage() {
  if [ "$SKIP_SYSTEM_SCENARIOS" = "true" ]; then
    ci_record_stage "system scenarios" "SKIP" "0" ""
    ci_record_system_scenario "<all>" "SKIP" "0" "" "-"
    ci_render_summary "RUNNING"
    return 0
  fi

  local runner="target/$NOVA_CI_CARGO_PROFILE/novarocks-system-tests"
  local start
  local code
  local duration

  # The workspace build and fixture stage above produced every binary required
  # by the registry. Failing loudly here beats silently skipping the
  # production-topology layer.
  if [ ! -x "$runner" ] || \
    [ ! -x "$SYSTEM_SCENARIO_PRIMARY_BINARY" ] || \
    [ ! -x "$SYSTEM_SCENARIO_COMPATIBLE_BINARY" ] || \
    [ ! -x "$SYSTEM_SCENARIO_OTHER_ISLAND_BINARY" ]; then
    ci_record_stage "system scenarios" "FAIL" "0" ""
    ci_render_summary "FAIL"
    echo "error: system scenario stage needs its runner and all binary fixtures" >&2
    exit 1
  fi

  start="$(ci_epoch)"
  ci_run_system_scenarios \
    "$runner" \
    "$SYSTEM_SCENARIO_PRIMARY_BINARY" \
    "$SYSTEM_SCENARIO_COMPATIBLE_BINARY" \
    "$SYSTEM_SCENARIO_OTHER_ISLAND_BINARY" \
    "$SYSTEM_SCENARIO_BASE_CONFIG" \
    "$CI_RUN_DIR/system/artifacts" \
    "$SYSTEM_SCENARIO_CLUSTER_SIZE" \
    "$SYSTEM_SCENARIO_TIMEOUT_SECS"
  code=$?
  duration=$(($(ci_epoch) - start))

  if [ "$code" -ne 0 ]; then
    ci_record_stage "system scenarios" "FAIL" "$duration" ""
    ci_render_summary "FAIL"
    exit "$code"
  fi

  ci_record_stage "system scenarios" "PASS" "$duration" ""
  ci_render_summary "RUNNING"
}

start_server_stage() {
  local log_path="$CI_RUN_DIR/server.log"
  local start
  local code
  local duration

  start="$(ci_epoch)"
  ci_start_all_in_one_server \
    "$NOVAROCKS_FE_CONFIG" "$NOVAROCKS_BE_CONFIG" "$log_path" 60 "$NOVA_CI_CARGO_PROFILE"
  code=$?
  duration=$(($(ci_epoch) - start))

  if [ "$code" -ne 0 ]; then
    ci_record_stage "all-in-one" "FAIL" "$duration" "$log_path"
    ci_mark_failure_tail "all-in-one failed to become ready" "$log_path"
    ci_render_summary "FAIL"
    exit "$code"
  fi

  ci_record_stage "all-in-one" "PASS" "$duration" "$log_path"
  ci_render_summary "RUNNING"
}

resolve_suites() {
  SUITES=()
  local suites_output

  if [ "$RUN_MODE" = "explicit" ]; then
    local suite
    for suite in "${REQUESTED_SUITES[@]}"; do
      if ! ci_suite_exists "$REPO_ROOT" "$suite"; then
        echo "error: correctness SQL suite does not exist: $suite; benchmark workloads use tools/benchmark/run-sql-benchmark.sh" >&2
        exit 2
      fi
      SUITES+=("$suite")
    done
    return 0
  fi

  if [ "$RUN_MODE" = "all-discovered" ]; then
    suites_output="$(ci_discover_sql_suites "$REPO_ROOT")" || return $?
    while IFS= read -r suite; do
      [ -n "$suite" ] || continue
      SUITES+=("$suite")
    done <<<"$suites_output"
    return 0
  fi

  if ! suites_output="$(ci_tier_suites "$CI_TIER" "$STABLE_SUITES_FILE")"; then
    echo "error: unknown CI tier: $CI_TIER" >&2
    exit 2
  fi

  while IFS= read -r suite; do
    [ -n "$suite" ] || continue
    if ! ci_suite_exists "$REPO_ROOT" "$suite"; then
      echo "error: tier '$CI_TIER' correctness SQL suite does not exist: $suite" >&2
      exit 2
    fi
    SUITES+=("$suite")
  done <<<"$suites_output"
}

validate_explicit_suites_early() {
  local suite
  local log_path

  if [ "$RUN_MODE" != "explicit" ]; then
    return 0
  fi

  for suite in "${REQUESTED_SUITES[@]}"; do
    if ! ci_suite_exists "$REPO_ROOT" "$suite"; then
      log_path="$CI_RUN_DIR/validation.log"
      echo "error: correctness SQL suite does not exist: $suite; benchmark workloads use tools/benchmark/run-sql-benchmark.sh" | tee "$log_path" >&2
      ci_record_stage "validate SQL suites" "FAIL" "0" "$log_path"
      ci_mark_failure_tail "SQL suite validation failed" "$log_path"
      ci_render_summary "FAIL"
      exit 2
    fi
  done
}

ci_sql_failed_case_keys() {
  local log_path="$1"
  ci_sql_case_keys_by_status "$log_path" "FAIL"
}

ci_sql_passed_case_keys() {
  local log_path="$1"
  ci_sql_case_keys_by_status "$log_path" "PASS"
}

ci_sql_case_keys_by_status() {
  local log_path="$1"
  local want_status="$2"
  local line
  local suite_name
  local case_id
  local case_status

  [ -f "$log_path" ] || return 0

  while IFS= read -r line || [ -n "$line" ]; do
    if [[ "$line" =~ ^[[:space:]]+\[([^]]+)\][[:space:]]+([^[:space:]]+)[[:space:]]+([A-Z]+)[[:space:]] ]]; then
      suite_name="${BASH_REMATCH[1]}"
      case_id="${BASH_REMATCH[2]}"
      case_status="${BASH_REMATCH[3]}"
      if [ "$case_status" = "$want_status" ]; then
        printf "|%s|%s|\n" "$suite_name" "$case_id"
      fi
    fi
  done <"$log_path"
}

ci_case_key_contains() {
  local keys="$1"
  local suite="$2"
  local case_name="$3"

  case "$keys" in
    *"|${suite}|${case_name}|"*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

ci_case_key_count() {
  local keys="$1"
  local count=0
  local line

  while IFS= read -r line || [ -n "$line" ]; do
    [ -n "$line" ] || continue
    count=$((count + 1))
  done <<<"$keys"

  printf "%s\n" "$count"
}

ci_suite_cluster_mode() {
  local suite="$1"

  case "$suite" in
    optimizer-dist)
      printf "cross-process\n"
      ;;
    *)
      printf "%s\n" "$SQL_CLUSTER_MODE"
      ;;
  esac
}

ci_suite_cluster_size() {
  local suite="$1"

  case "$suite" in
    optimizer-dist)
      printf "3\n"
      ;;
    *)
      printf "%s\n" "$SQL_CLUSTER_SIZE"
      ;;
  esac
}

ci_native_cross_process_enabled() {
  [ "$NOVA_CI_NATIVE_CROSS_PROCESS_CORE" = "1" ] || [ "$NOVA_CI_NATIVE_CROSS_PROCESS_FULL" = "1" ]
}

ci_native_cross_process_suite_cluster_mode() {
  local suite="$1"
  printf "cross-process\n"
}

ci_native_cross_process_suite_cluster_size() {
  local suite="$1"
  printf "3\n"
}

ci_classify_sql_log() {
  local suite="$1"
  local log_path="$2"
  local require_known_only="${3:-false}"
  local failed_case_keys
  local failed_case_count
  local classified_failed_case_count=0
  local classified_failed_case_keys=""
  local line
  local current_suite="$suite"
  local current_case=""
  local row_suite
  local row_case
  local remaining
  local error_code
  local status_line
  local status
  local reason
  local expires
  local classification_count=0
  local hard_failure=0
  local seen_keys=""
  local seen_key

  [ -f "$log_path" ] || return 1

  # Expected-error PASS lines can include engine_error_code=..., so prefer
  # case timing FAIL rows when the runner emitted them.
  failed_case_keys="$(ci_sql_failed_case_keys "$log_path")"
  failed_case_count="$(ci_case_key_count "$failed_case_keys")"

  while IFS= read -r line || [ -n "$line" ]; do
    if [[ "$line" =~ ^[[:space:]]*\[([^]]+)\][[:space:]]+([^[:space:]]+)[[:space:]]+\(steps= ]]; then
      current_suite="${BASH_REMATCH[1]}"
      current_case="${BASH_REMATCH[2]}"
    elif [[ "$line" =~ (^|[[:space:]])case:[[:space:]]*([A-Za-z0-9_.+-]+) ]]; then
      current_case="${BASH_REMATCH[2]}"
    fi

    case "$line" in
      *PASS*engine_error_code=*)
        continue
        ;;
    esac

    remaining="$line"
    while [[ "$remaining" =~ engine_error_code=([A-Za-z0-9_]+) ]]; do
      error_code="${BASH_REMATCH[1]}"
      remaining="${remaining#*engine_error_code=$error_code}"
      row_suite="${current_suite:-$suite}"
      row_case="${current_case:-unknown}"

      if [ -n "$failed_case_keys" ] && ! ci_case_key_contains "$failed_case_keys" "$row_suite" "$row_case"; then
        continue
      fi

      seen_key="|${row_suite}|${row_case}|${error_code}|"
      case "$seen_keys" in
        *"$seen_key"*)
          continue
          ;;
      esac
      seen_keys="${seen_keys}${seen_key}
"
      if [ -n "$failed_case_keys" ] && ! ci_case_key_contains "$classified_failed_case_keys" "$row_suite" "$row_case"; then
        classified_failed_case_keys="${classified_failed_case_keys}|${row_suite}|${row_case}|
"
        classified_failed_case_count=$((classified_failed_case_count + 1))
      fi

      status_line="$(ci_known_failure_status "$KNOWN_FAILURES_FILE" "$CI_TIER" "$row_suite" "$row_case" "$error_code")"
      IFS='|' read -r status reason expires <<EOF
$status_line
EOF
      ci_record_sql_classification "$row_suite" "$row_case" "$status" "$error_code" "$reason"
      classification_count=$((classification_count + 1))
      if [ "$status" != "KNOWN_FAIL" ]; then
        hard_failure=1
      fi
    done
  done <"$log_path"

  if [ "$require_known_only" = "true" ]; then
    [ "$classification_count" -gt 0 ] \
      && [ "$hard_failure" -eq 0 ] \
      && [ "$failed_case_count" -eq "$classified_failed_case_count" ]
    return $?
  fi

  return 0
}

ci_classify_unexpected_passes() {
  local suite="$1"
  local log_path="$2"
  local passed_case_keys
  local row
  local row_case
  local error_code
  local reason
  local expires
  local unexpected=0

  [ -f "$log_path" ] || return 0

  passed_case_keys="$(ci_sql_passed_case_keys "$log_path")"
  [ -n "$passed_case_keys" ] || return 0

  while IFS='|' read -r row_case error_code reason expires || [ -n "$row_case" ]; do
    [ -n "$row_case" ] || continue
    if ci_case_key_contains "$passed_case_keys" "$suite" "$row_case"; then
      ci_record_sql_classification "$suite" "$row_case" "UNEXPECTED_PASS" "$error_code" "$reason"
      unexpected=1
    fi
  done < <(ci_known_failure_rows_for_suite "$KNOWN_FAILURES_FILE" "$CI_TIER" "$suite")

  [ "$unexpected" -eq 0 ]
}

ci_sql_suite_status_from_log() {
  local log_path="$1"
  local line

  [ -f "$log_path" ] || {
    printf "FAIL\n"
    return 0
  }

  while IFS= read -r line || [ -n "$line" ]; do
    if [[ "$line" =~ ^(exit_code|exit_status)=([0-9]+)$ ]]; then
      if [ "${BASH_REMATCH[2]}" -ne 0 ]; then
        printf "FAIL\n"
        return 0
      fi
    elif [[ "$line" =~ ^fail=([0-9]+)$ ]]; then
      if [ "${BASH_REMATCH[1]}" -ne 0 ]; then
        printf "FAIL\n"
        return 0
      fi
    elif [[ "$line" =~ ^[[:space:]]+\[[^]]+\][[:space:]]+[^[:space:]]+[[:space:]]+FAIL[[:space:]] ]]; then
      printf "FAIL\n"
      return 0
    elif [ "$line" = "failed cases:" ]; then
      printf "FAIL\n"
      return 0
    fi
  done <"$log_path"

  printf "PASS\n"
}

ci_should_use_runner_suite_timeout() {
  local suite="$1"

  # The prepared-write-set entry-budget case intentionally materializes 16,385
  # files and can run for roughly 90 seconds. Keep the runner's suite default
  # authoritative unless the caller explicitly requests a CI-wide override.
  [ "$suite" = "iceberg-dml" ] && [ -z "${SQL_QUERY_TIMEOUT_SECONDS:-}" ]
}

run_sql_suites() {
  local failed=0
  local suite
  local log_path
  local start
  local code
  local duration
  local -a suite_extra_args
  local -a suite_query_timeout_args
  local query_timeout
  local novarocks_bin
  local suite_cluster_mode
  local suite_cluster_size

  novarocks_bin="$REPO_ROOT/$(ci_novarocks_binary_path "$NOVA_CI_CARGO_PROFILE")"

  resolve_suites
  if [ "${#SUITES[@]}" -eq 0 ]; then
    echo "error: no SQL suites selected" >&2
    exit 2
  fi

  for suite in "${SUITES[@]}"; do
    log_path="$CI_RUN_DIR/sql/${suite}.log"
    start="$(ci_epoch)"
    suite_extra_args=()
    suite_query_timeout_args=()
    query_timeout="${SQL_QUERY_TIMEOUT_SECONDS:-60}"
    suite_cluster_mode="$(ci_suite_cluster_mode "$suite")"
    suite_cluster_size="$(ci_suite_cluster_size "$suite")"
    case "$suite" in
      complex-type)
        query_timeout="${SQL_QUERY_TIMEOUT_SECONDS:-120}"
        ;;
      optimizer-dist)
        query_timeout="${SQL_QUERY_TIMEOUT_SECONDS:-300}"
        ;;
    esac
    if ! ci_should_use_runner_suite_timeout "$suite"; then
      suite_query_timeout_args=(--query-timeout "$query_timeout")
    fi
    if [ "$RUN_MODE" != "explicit" ]; then
      case "$suite" in
        analytic)
          # Keep the default stable run focused. These migrated window matrices
          # remain available in direct analytic suite runs.
          suite_extra_args=(
            --skip
            "analytic_test_basic_window_function,analytic_test_basic_window_function_2"
          )
          ;;
      esac
    fi
    ci_run_logged "$log_path" \
      env NO_PROXY=127.0.0.1,localhost \
      NOVAROCKS_BIN="$novarocks_bin" \
      NOVAROCKS_WORKSPACE_ROOT="$REPO_ROOT" \
      NOVA_ENV_REST_ENV_FILE="$NOVA_ENV_RUNTIME_DIR/env.sh" \
      cargo run --manifest-path tests/sql/runner/Cargo.toml --profile "$NOVA_CI_CARGO_PROFILE" --bin novarocks-sql-test -- \
        --config "$NOVAROCKS_SQL_TEST_CONFIG" \
        --suite "$suite" \
        --mode verify \
        "${suite_query_timeout_args[@]}" \
        --cluster-mode "$suite_cluster_mode" \
        --cluster-size "$suite_cluster_size" \
        "${suite_extra_args[@]}" \
        -j 1
    code=$?
    duration=$(($(ci_epoch) - start))

    if ! ci_classify_unexpected_passes "$suite" "$log_path"; then
      if [ "$code" -ne 0 ]; then
        ci_classify_sql_log "$suite" "$log_path" "false" >/dev/null || true
      fi
      failed=1
      ci_record_sql_suite "$suite" "UNEXPECTED_PASS" "$duration" "$log_path"
      if [ -z "$CI_FAILURE_TAIL" ]; then
        ci_mark_failure_tail "SQL suite has unexpected known-failure passes: $suite" "$log_path"
      fi
    elif [ "$code" -eq 0 ]; then
      ci_record_sql_suite "$suite" "PASS" "$duration" "$log_path"
    elif ci_classify_sql_log "$suite" "$log_path" "true"; then
      ci_record_sql_suite "$suite" "KNOWN_FAIL" "$duration" "$log_path"
    else
      failed=1
      ci_record_sql_suite "$suite" "FAIL" "$duration" "$log_path"
      if [ -z "$CI_FAILURE_TAIL" ]; then
        ci_mark_failure_tail "SQL suite failed: $suite" "$log_path"
      fi
    fi
    ci_render_summary "RUNNING"
  done

  if [ "$failed" -ne 0 ]; then
    ci_render_summary "FAIL"
    exit 1
  fi
}

stop_server_for_native_cross_process_stage() {
  local log_path="$CI_RUN_DIR/server-stop-for-native-cross-process.log"
  local start
  local code
  local duration

  start="$(ci_epoch)"
  {
    echo "Stopping all-in-one before native cross-process SQL suites."
    ci_stop_all_in_one_server
  } >"$log_path" 2>&1
  code=$?
  duration=$(($(ci_epoch) - start))

  if [ "$code" -ne 0 ]; then
    ci_record_stage "all-in-one stop for native cross-process" "FAIL" "$duration" "$log_path"
    ci_mark_failure_tail "all-in-one stop for native cross-process failed" "$log_path"
    ci_render_summary "FAIL"
    exit "$code"
  fi

  ci_record_stage "all-in-one stop for native cross-process" "PASS" "$duration" "$log_path"
  ci_render_summary "RUNNING"
}

run_native_cross_process_sql_suites() {
  local failed=0
  local suite
  local log_path
  local start
  local code
  local duration
  local query_timeout
  local novarocks_bin
  local suite_cluster_mode
  local suite_cluster_size
  local -a native_cross_process_suites
  local -a suite_query_timeout_args
  local suites_output

  if ! ci_native_cross_process_enabled; then
    return 0
  fi

  mkdir -p "$CI_RUN_DIR/sql-native-cross-process"
  novarocks_bin="$REPO_ROOT/$(ci_novarocks_binary_path "$NOVA_CI_CARGO_PROFILE")"

  if ! suites_output="$(ci_native_cross_process_suites)"; then
    echo "error: failed to resolve native cross-process SQL suites" >&2
    exit 2
  fi

  native_cross_process_suites=()
  while IFS= read -r suite; do
    [ -n "$suite" ] || continue
    if ! ci_suite_exists "$REPO_ROOT" "$suite"; then
      echo "error: native cross-process SQL suite does not exist: $suite" >&2
      exit 2
    fi
    native_cross_process_suites+=("$suite")
  done <<<"$suites_output"

  if [ "${#native_cross_process_suites[@]}" -eq 0 ]; then
    echo "error: no native cross-process SQL suites selected" >&2
    exit 2
  fi

  if [ "$SQL_CLUSTER_MODE" = "all-in-one" ]; then
    stop_server_for_native_cross_process_stage
  fi

  for suite in "${native_cross_process_suites[@]}"; do
    log_path="$CI_RUN_DIR/sql-native-cross-process/${suite}.log"
    start="$(ci_epoch)"
    suite_query_timeout_args=()
    query_timeout="${SQL_QUERY_TIMEOUT_SECONDS:-60}"
    suite_cluster_mode="$(ci_native_cross_process_suite_cluster_mode "$suite")"
    suite_cluster_size="$(ci_native_cross_process_suite_cluster_size "$suite")"
    case "$suite" in
      tpc-ds|tpc-h)
        query_timeout="${SQL_QUERY_TIMEOUT_SECONDS:-300}"
        ;;
      complex-type|ssb)
        query_timeout="${SQL_QUERY_TIMEOUT_SECONDS:-120}"
        ;;
      optimizer-dist)
        query_timeout="${SQL_QUERY_TIMEOUT_SECONDS:-300}"
        ;;
    esac
    if ! ci_should_use_runner_suite_timeout "$suite"; then
      suite_query_timeout_args=(--query-timeout "$query_timeout")
    fi

    ci_run_logged "$log_path" \
      env NO_PROXY=127.0.0.1,localhost \
      NOVAROCKS_BIN="$novarocks_bin" \
      NOVA_ENV_REST_ENV_FILE="$NOVA_ENV_RUNTIME_DIR/env.sh" \
      cargo run --manifest-path tests/sql/runner/Cargo.toml --profile "$NOVA_CI_CARGO_PROFILE" -- \
        --config "$NOVAROCKS_SQL_TEST_CONFIG" \
        --suite "$suite" \
        --mode verify \
        "${suite_query_timeout_args[@]}" \
        --cluster-mode "$suite_cluster_mode" \
        --cluster-size "$suite_cluster_size" \
        -j 1
    code=$?
    duration=$(($(ci_epoch) - start))

    if [ "$code" -eq 0 ]; then
      ci_record_sql_suite "native-cross-process:$suite" "PASS" "$duration" "$log_path"
    else
      failed=1
      ci_classify_sql_log "$suite" "$log_path" "false" >/dev/null || true
      if [ "$NOVA_CI_NATIVE_CROSS_PROCESS_REQUIRED" = "1" ]; then
        ci_record_sql_suite "native-cross-process:$suite" "FAIL" "$duration" "$log_path"
        if [ -z "$CI_FAILURE_TAIL" ]; then
          ci_mark_failure_tail "native cross-process SQL suite failed: $suite" "$log_path"
        fi
      else
        ci_record_sql_suite "native-cross-process:$suite" "DISCOVERY_FAIL" "$duration" "$log_path"
      fi
    fi
    ci_render_summary "RUNNING"
  done

  if [ "$failed" -ne 0 ] && [ "$NOVA_CI_NATIVE_CROSS_PROCESS_REQUIRED" = "1" ]; then
    ci_render_summary "FAIL"
    exit 1
  fi
}

reclassify_existing_run() {
  local failed=0
  local any_logs=0
  local log_path
  local suite
  local status

  init_from_run_dir

  for log_path in "$CI_RUN_DIR"/sql/*.log; do
    [ -f "$log_path" ] || continue
    any_logs=1
    suite="${log_path##*/}"
    suite="${suite%.log}"
    status="$(ci_sql_suite_status_from_log "$log_path")"

    if ! ci_classify_unexpected_passes "$suite" "$log_path"; then
      if [ "$status" != "PASS" ]; then
        ci_classify_sql_log "$suite" "$log_path" "false" >/dev/null || true
      fi
      failed=1
      ci_record_sql_suite "$suite" "UNEXPECTED_PASS" "0" "$log_path"
      if [ -z "$CI_FAILURE_TAIL" ]; then
        ci_mark_failure_tail "SQL suite has unexpected known-failure passes: $suite" "$log_path"
      fi
    elif [ "$status" = "PASS" ]; then
      ci_record_sql_suite "$suite" "PASS" "0" "$log_path"
    elif ci_classify_sql_log "$suite" "$log_path" "true"; then
      ci_record_sql_suite "$suite" "KNOWN_FAIL" "0" "$log_path"
    else
      failed=1
      ci_record_sql_suite "$suite" "FAIL" "0" "$log_path"
      if [ -z "$CI_FAILURE_TAIL" ]; then
        ci_mark_failure_tail "SQL suite failed: $suite" "$log_path"
      fi
    fi
  done

  if [ "$any_logs" -eq 0 ]; then
    failed=1
    CI_FAILURE_TAIL="$(cat <<EOF
### reclassification failed

\`\`\`text
no sql/*.log files found under $CI_RUN_DIR
\`\`\`
EOF
)"
  fi

  if [ "$failed" -ne 0 ]; then
    ci_render_summary "FAIL"
    echo "FAIL: $CI_SUMMARY"
    exit 1
  fi

  ci_render_summary "PASS"
  echo "PASS: $CI_SUMMARY"
}

main() {
  parse_args "$@"
  cd "$REPO_ROOT" || exit 1

  if [ -n "$CI_FROM_RUN_DIR" ]; then
    reclassify_existing_run
    exit 0
  fi

  init_run_dir
  validate_explicit_suites_early

  if [ "$CARGO_ONLY" = "true" ]; then
    run_cargo_gates
    ci_render_summary "PASS"
    echo "PASS: $CI_SUMMARY"
    exit 0
  fi

  trap cleanup EXIT

  prepare_runtime
  run_cargo_gates
  # System Scenarios run before the SQL suites so a topology, readiness,
  # restart, fault or cleanup break surfaces before the much longer SQL tier.
  # The stage needs no Docker fixture: every registered scenario builds its
  # Iceberg warehouse on the local filesystem.
  run_system_scenario_fixture_stage
  run_system_scenarios_stage
  reset_frontend_state_store_stage
  if [ "$SQL_CLUSTER_MODE" = "all-in-one" ]; then
    start_server_stage
  else
    ci_record_stage "standalone" "SKIP" "0" ""
    ci_render_summary "RUNNING"
  fi
  run_sql_suites
  run_native_cross_process_sql_suites

  ci_render_summary "PASS"
  echo "PASS: $CI_SUMMARY"
}

if [ "${1:-}" != "--source-only" ]; then
  main "$@"
fi
