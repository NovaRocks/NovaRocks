#!/usr/bin/env bash
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/lib/logging.sh"
source "$SCRIPT_DIR/lib/command.sh"
source "$SCRIPT_DIR/lib/sql_suites.sh"
source "$SCRIPT_DIR/lib/server.sh"

STABLE_SUITES_FILE="$SCRIPT_DIR/suites/stable-sql-suites.txt"
RUN_MODE="stable"
ALL_DISCOVERED_REQUESTED="false"
KEEP_RUNTIME="false"
SKIP_CARGO_TEST="false"
REQUESTED_SUITES=()
CI_RUNTIME_PREPARED="false"
NOVA_CI_CARGO_PROFILE="${NOVA_CI_CARGO_PROFILE:-dev-opt}"
SQL_CLUSTER_MODE="${SQL_CLUSTER_MODE:-all-in-one}"
SQL_CLUSTER_SIZE="${SQL_CLUSTER_SIZE:-1}"

usage() {
  cat <<'EOF'
Usage: tools/ci/local-full-ci.sh [options]

Runs NovaRocks local full CI with local logs under logs/ci-full/<timestamp>/.
SQL suites are executed with sql-tests -j 1 because parallel case execution is
not stable in the current local environment.
Rust tests are executed with --test-threads=1 for the same reason.
Cargo build/test/run stages use NOVA_CI_CARGO_PROFILE, defaulting to dev-opt.
Clippy runs in warning-only mode until the repository has a clean strict-clippy
baseline.

Options:
  --all-discovered      Run every suite discovered from sql-tests/*/sql.
  --suite <name>        Run only the named SQL suite. May be repeated.
  --skip-cargo-test     Skip cargo test. Intended only for runner debugging.
  --cluster-mode <mode> SQL runner cluster mode: all-in-one or cross-process.
  --cluster-size <n>    Number of BE processes for cross-process mode.
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

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --all-discovered)
        ALL_DISCOVERED_REQUESTED="true"
        shift
        ;;
      --suite)
        if [ "$#" -lt 2 ]; then
          echo "error: --suite requires a suite name" >&2
          exit 2
        fi
        REQUESTED_SUITES+=("$2")
        shift 2
        ;;
      --skip-cargo-test)
        SKIP_CARGO_TEST="true"
        shift
        ;;
      --cluster-mode)
        if [ "$#" -lt 2 ]; then
          echo "error: --cluster-mode requires a mode" >&2
          exit 2
        fi
        SQL_CLUSTER_MODE="$2"
        shift 2
        ;;
      --cluster-size)
        if [ "$#" -lt 2 ]; then
          echo "error: --cluster-size requires a count" >&2
          exit 2
        fi
        SQL_CLUSTER_SIZE="$2"
        shift 2
        ;;
      --keep-runtime)
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
  ci_stop_standalone_server || true

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
    docker/iceberg-rest/up.sh
    echo "+ source docker/iceberg-rest/runtime/current/env.sh"
    . docker/iceberg-rest/runtime/current/env.sh
    require_runtime_var NOVAROCKS_STANDALONE_CONFIG
    require_runtime_var NOVAROCKS_SQL_TEST_CONFIG
    require_runtime_var NOVA_ENV_RUNTIME_DIR
    require_runtime_var NOVA_ENV_MYSQL_PORT
    require_runtime_var NOVAROCKS_ICEBERG_REST_URI
    echo "NOVAROCKS_STANDALONE_CONFIG=$NOVAROCKS_STANDALONE_CONFIG"
    echo "NOVAROCKS_SQL_TEST_CONFIG=$NOVAROCKS_SQL_TEST_CONFIG"
    echo "NOVA_ENV_RUNTIME_DIR=$NOVA_ENV_RUNTIME_DIR"
    echo "NOVA_ENV_MYSQL_PORT=$NOVA_ENV_MYSQL_PORT"
    echo "NOVAROCKS_ICEBERG_REST_URI=$NOVAROCKS_ICEBERG_REST_URI"
    echo "NOVAROCKS_SPARK_DEFAULTS=${NOVAROCKS_SPARK_DEFAULTS:-}"
    echo "NOVA_CI_CARGO_PROFILE=$NOVA_CI_CARGO_PROFILE"
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
  ci_set_runtime_context "$NOVAROCKS_STANDALONE_CONFIG" "$NOVAROCKS_SQL_TEST_CONFIG" "$NOVA_ENV_MYSQL_PORT"
  ci_record_stage "prepare runtime" "PASS" "$duration" "$log_path"
  ci_render_summary "RUNNING"
}

reset_managed_lake_metadata_stage() {
  local log_path="$CI_RUN_DIR/metadata-reset.log"
  local start
  local code
  local duration
  local metadata_db

  metadata_db="$NOVA_ENV_RUNTIME_DIR/standalone-managed-lake.sqlite"
  start="$(ci_epoch)"
  {
    echo "Reset managed-lake metadata before SQL CI."
    echo "metadata_db=$metadata_db"
    rm -f "$metadata_db" "$metadata_db-shm" "$metadata_db-wal"
  } >"$log_path" 2>&1
  code=$?
  duration=$(($(ci_epoch) - start))

  if [ "$code" -ne 0 ]; then
    ci_record_stage "reset metadata" "FAIL" "$duration" "$log_path"
    ci_mark_failure_tail "reset metadata failed" "$log_path"
    ci_render_summary "FAIL"
    exit "$code"
  fi

  ci_record_stage "reset metadata" "PASS" "$duration" "$log_path"
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
  run_fail_fast_stage "cargo fmt" "cargo-fmt.log" cargo fmt --check
  run_fail_fast_stage "cargo clippy" "cargo-clippy.log" cargo clippy --all-targets
  run_fail_fast_stage "cargo build" "cargo-build.log" cargo build --profile "$NOVA_CI_CARGO_PROFILE"

  if [ "$SKIP_CARGO_TEST" = "true" ]; then
    ci_record_stage "cargo test" "SKIP" "0" ""
    ci_render_summary "RUNNING"
  else
    run_fail_fast_stage "cargo test" "cargo-test.log" cargo test --profile "$NOVA_CI_CARGO_PROFILE" -- --test-threads=1
  fi
}

start_server_stage() {
  local log_path="$CI_RUN_DIR/server.log"
  local start
  local code
  local duration

  start="$(ci_epoch)"
  ci_start_standalone_server "$NOVAROCKS_STANDALONE_CONFIG" "$log_path" 60 "$NOVA_CI_CARGO_PROFILE"
  code=$?
  duration=$(($(ci_epoch) - start))

  if [ "$code" -ne 0 ]; then
    ci_record_stage "standalone-server" "FAIL" "$duration" "$log_path"
    ci_mark_failure_tail "standalone-server failed to become ready" "$log_path"
    ci_render_summary "FAIL"
    exit "$code"
  fi

  ci_record_stage "standalone-server" "PASS" "$duration" "$log_path"
  ci_render_summary "RUNNING"
}

resolve_suites() {
  SUITES=()

  if [ "$RUN_MODE" = "explicit" ]; then
    local suite
    for suite in "${REQUESTED_SUITES[@]}"; do
      if ! ci_suite_exists "$REPO_ROOT" "$suite"; then
        echo "error: SQL suite does not exist: $suite" >&2
        exit 2
      fi
      SUITES+=("$suite")
    done
    return 0
  fi

  if [ "$RUN_MODE" = "all-discovered" ]; then
    while IFS= read -r suite; do
      [ -n "$suite" ] || continue
      SUITES+=("$suite")
    done < <(ci_discover_sql_suites "$REPO_ROOT")
    return 0
  fi

  while IFS= read -r suite; do
    [ -n "$suite" ] || continue
    if ! ci_suite_exists "$REPO_ROOT" "$suite"; then
      echo "error: stable SQL suite does not exist: $suite" >&2
      exit 2
    fi
    SUITES+=("$suite")
  done < <(ci_load_stable_suites "$STABLE_SUITES_FILE")
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
      echo "error: SQL suite does not exist: $suite" | tee "$log_path" >&2
      ci_record_stage "validate SQL suites" "FAIL" "0" "$log_path"
      ci_mark_failure_tail "SQL suite validation failed" "$log_path"
      ci_render_summary "FAIL"
      exit 2
    fi
  done
}

run_sql_suites() {
  local failed=0
  local suite
  local log_path
  local start
  local code
  local duration
  local -a suite_extra_args
  local query_timeout
  local novarocks_bin

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
    query_timeout="${SQL_QUERY_TIMEOUT_SECONDS:-60}"
    case "$suite" in
      tpc-ds|tpc-h)
        query_timeout="${SQL_QUERY_TIMEOUT_SECONDS:-180}"
        ;;
      complex-type|ssb)
        query_timeout="${SQL_QUERY_TIMEOUT_SECONDS:-120}"
        ;;
    esac
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
        tpc-ds)
          # q85 is still excluded from unattended stable CI while its plan-shape
          # issue is being addressed. Direct tpc-ds runs keep full coverage.
          suite_extra_args=(
            --skip
            "q85"
          )
          ;;
      esac
    fi
    ci_run_logged "$log_path" \
      env NO_PROXY=127.0.0.1,localhost \
      NOVAROCKS_BIN="$novarocks_bin" \
      cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests --profile "$NOVA_CI_CARGO_PROFILE" -- \
        --config "$NOVAROCKS_SQL_TEST_CONFIG" \
        --suite "$suite" \
        --mode verify \
        --query-timeout "$query_timeout" \
        --cluster-mode "$SQL_CLUSTER_MODE" \
        --cluster-size "$SQL_CLUSTER_SIZE" \
        "${suite_extra_args[@]}" \
        -j 1
    code=$?
    duration=$(($(ci_epoch) - start))

    if [ "$code" -eq 0 ]; then
      ci_record_sql_suite "$suite" "PASS" "$duration" "$log_path"
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

main() {
  parse_args "$@"
  cd "$REPO_ROOT" || exit 1
  init_run_dir
  validate_explicit_suites_early
  trap cleanup EXIT

  prepare_runtime
  run_cargo_gates
  reset_managed_lake_metadata_stage
  if [ "$SQL_CLUSTER_MODE" = "all-in-one" ]; then
    start_server_stage
  else
    ci_record_stage "standalone-server" "SKIP" "0" ""
    ci_render_summary "RUNNING"
  fi
  run_sql_suites

  ci_render_summary "PASS"
  echo "PASS: $CI_SUMMARY"
}

main "$@"
