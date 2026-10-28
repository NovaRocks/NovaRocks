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
source "$SCRIPT_DIR/lib/build_capabilities.sh"
source "$SCRIPT_DIR/lib/sql_suites.sh"
source "$SCRIPT_DIR/lib/server.sh"
source "$SCRIPT_DIR/lib/system_scenarios.sh"

DISPOSITIONS_FILE="$SCRIPT_DIR/baselines/dispositions.json"
RUN_MODE="stable"
CI_TIER="full"
CI_FROM_RUN_DIR=""
CARGO_ONLY="false"
CI_COLLECT_ALL="false"
CI_COLLECTION_REACHED_END="false"
CI_RUN_REACHED_END="false"
CI_COLLECTION_RESULT=0
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
NOVA_CI_CARGO_PROFILE="${NOVA_CI_CARGO_PROFILE:-dev}"
SQL_CLUSTER_MODE="${SQL_CLUSTER_MODE:-cross-process}"
SQL_CLUSTER_LAUNCH_PROFILE=""
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

# Design: ADR-0200 (docs/adr/ADR-0200-advisory-discovery-is-independent-of-admission.md)
ci_list_stages() {
  cat <<'STAGES'
{
  "schema": 1,
  "stages": [
    {"name": "CI build inputs", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "locked Cargo metadata", "tier": "admission", "domain": ["**"]},
    {"name": "Cargo dependency policy", "tier": "admission", "domain": ["**"]},
    {"name": "unused dependencies", "tier": "admission", "domain": ["**"]},
    {"name": "unused dependencies tests", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "legacy branding", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "legacy branding tests", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "system scenario stage tests", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "Rust executor regression tests", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "SQL helper environment binding", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "shared benchmark environment guard", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "isolated fixture current link", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "shared benchmark fixture lifecycle", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "shared benchmark fixture contract", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "cargo fmt", "tier": "admission", "domain": ["**"]},
    {"name": "cargo clippy", "tier": "admission", "domain": ["**"]},
    {"name": "joint tests and binaries preparation", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "main binary build information", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "SQL error manifest freshness", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "functional default scenario selection", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "selected system program preparation", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "workspace nextest and doctests", "tier": "admission", "domain": ["**"]},
    {"name": "identity snapshot tests", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "invocation record tests", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "verification composition tests", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "local CI classification tests", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "disabled SQL suite tests", "tier": "admission", "domain": ["tools/ci/**"]},
    {"name": "Cargo dependency advisories", "tier": "full", "domain": ["Cargo.lock", "deny.toml", "tools/ci/dependency-advisories.py", ".github/workflows/dependency-advisories.yml"]},
    {"name": "memory-model:loom", "tier": "full", "domain": ["novarocks/memory/**", "tools/ci/memory-model-checks.sh"]},
    {"name": "memory-model:miri", "tier": "full", "domain": ["novarocks/memory/**", "tools/ci/memory-model-checks.sh"]},
    {"name": "native-compatibility", "tier": "full", "domain": ["tools/ci/**"]}
  ]
}
STAGES
}

usage() {
  cat <<'EOF'
Usage: tools/ci/local-full-ci.sh [options]

Runs NovaRocks local full CI with local logs under logs/ci-full/<timestamp>/.
SQL suites are executed with novarocks-sql-test -j 1 because parallel case execution is
not stable in the current local environment.
Rust tests use native nextest's four slots and configured resource groups;
Cargo doctests run as a separate execution domain.
Cargo check/clippy/build/test/run stages use NOVA_CI_CARGO_PROFILE, defaulting to dev.
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
  --collect-all         Collect the complete default native set after ordinary test
                        failures with confirmed exit, identity and cleanup. Raw
                        failures remain failures; unsafe or incomplete work stops.
                        This is local collection, not admission or full-layer PASS.
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
  --cluster-launch-profile <fault-scenario|performance>
                        Pass an explicit native launch profile to every SQL round.
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
      --collect-all)
        CARGO_ONLY_CONFLICT="true"
        CI_COLLECT_ALL="true"
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
      --cluster-launch-profile)
        CARGO_ONLY_CONFLICT="true"
        if [ "$#" -lt 2 ]; then
          echo "error: --cluster-launch-profile requires a profile" >&2
          exit 2
        fi
        case "$2" in
          fault-scenario|performance) SQL_CLUSTER_LAUNCH_PROFILE="$2" ;;
          *) echo "error: unsupported --cluster-launch-profile: $2" >&2; exit 2 ;;
        esac
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

  if [ "$CI_COLLECT_ALL" = true ] && {
    [ -n "$CI_FROM_RUN_DIR" ] || [ "$SKIP_CARGO_TEST" = true ] ||
    [ "$SKIP_SYSTEM_SCENARIOS" = true ] || [ "$ALL_DISCOVERED_REQUESTED" = true ] ||
    [ "${#REQUESTED_SUITES[@]}" -ne 0 ] || [ "$CI_TIER" != full ] ||
    [ "$SQL_CLUSTER_MODE" != cross-process ] || [ "$SQL_CLUSTER_SIZE" -ne 3 ] ||
    [ "$SYSTEM_SCENARIO_CLUSTER_SIZE" -ne 3 ];
  }; then
    echo "error: --collect-all requires the complete default set and native 1FE+3BE" >&2
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
  python3 "$SCRIPT_DIR/lib/identity_snapshot.py" source --repo "$REPO_ROOT" --output "$CI_RUN_DIR/identity/source-start.json"
}

cleanup() {
  local status=$? source_code=0 stop_code=0 runtime_code=0 runtime_attempted=false
  local summary_code=0 collection_code=0 finalization_status=PASS final_status=PASS
  local summary_ready=false finalization_log=""
  if [ -n "${CI_RUN_DIR:-}" ] && [ -d "$CI_RUN_DIR" ] &&
     [ -n "${CI_SUMMARY:-}" ] && [ -n "${CI_STARTED_EPOCH:-}" ]; then
    summary_ready=true
  fi
  if [ -n "${CI_RUN_DIR:-}" ] && [ -d "$CI_RUN_DIR" ]; then
    python3 "$SCRIPT_DIR/lib/identity_snapshot.py" source --repo "$REPO_ROOT" --output "$CI_RUN_DIR/identity/source-end.json" || source_code=$?
  else
    source_code=1
  fi
  ci_stop_all_in_one_server || stop_code=$?

  if [ "$CI_RUNTIME_PREPARED" = "true" ] && [ "$KEEP_RUNTIME" != "true" ]; then
    runtime_attempted=true
    {
      echo "+ docker/iceberg-rest/down.sh --runtime-only --purge"
      docker/iceberg-rest/down.sh --runtime-only --purge
    } >>"$CI_RUN_DIR/env.log" 2>&1 || runtime_code=$?
  fi

  if [ "$source_code" -ne 0 ] || [ "$stop_code" -ne 0 ] || [ "$runtime_code" -ne 0 ] ||
     [ "${CI_RUN_REACHED_END:-false}" != true ]; then
    [ "$status" -ne 0 ] || status=1
    finalization_status=FAIL
  fi
  [ "$status" -eq 0 ] || final_status=FAIL
  if [ "$summary_ready" = true ]; then
    [ "$CI_COLLECT_ALL" != true ] || finalization_log="$CI_RUN_DIR/collection.json"
    ci_record_stage "run terminal and cleanup facts (source=$source_code stop=$stop_code runtime=$runtime_code)" "$finalization_status" "0" "$finalization_log"
    ci_render_summary "$final_status" || summary_code=$?
  else
    summary_code=1
    echo "error: run summary state is incomplete at exit" >&2
  fi
  [ "$summary_code" -eq 0 ] || { [ "$status" -ne 0 ] || status=1; }

  if [ "$CI_COLLECT_ALL" = true ] && [ -n "${CI_RUN_DIR:-}" ] && [ -d "$CI_RUN_DIR" ]; then
    # Collection is an additional local record, never a formal receipt. The
    # caller must still observe this invocation's actual exit independently.
    python3 - "$CI_RUN_DIR/collection.json" "$CI_COLLECTION_REACHED_END" "$status" "$source_code" "$stop_code" "$runtime_code" "$runtime_attempted" "$KEEP_RUNTIME" "$summary_code" "${CI_RUN_REACHED_END:-false}" <<'COLLECTION' || collection_code=$?
import json, sys
from pathlib import Path
path=Path(sys.argv[1])
source,stop,runtime=map(int,sys.argv[4:7])
summary=int(sys.argv[9])
attempted=sys.argv[7]=='true'
reached=sys.argv[10]=='true'
value={'schema':1,'mode':'collect-all','collection_complete':sys.argv[2]=='true' and reached and not any((source,stop,runtime,summary)),
       'exit_handler_status':int(sys.argv[3]),'cleanup':{'source_end_exit':source,'server_stop_exit':stop,'runtime_cleanup_exit':runtime if attempted else None,'runtime_cleanup_attempted':attempted,'keep_runtime':sys.argv[8]=='true'},
       'run_reached_end':reached,'summary_write_exit':summary,'invocation_exit':'caller-must-observe','evidence_class':'local-collection'}
path.write_text(json.dumps(value,indent=2)+'\n')
COLLECTION
    if [ "$collection_code" -ne 0 ]; then
      [ "$status" -ne 0 ] || status=1
      [ "$summary_ready" != true ] || ci_record_stage "collection record persistence" "FAIL" "0" "$CI_RUN_DIR/collection.json"
    fi
  fi
  if [ "$summary_ready" = true ]; then
    if [ "$collection_code" -ne 0 ] || [ "$summary_code" -ne 0 ]; then
      [ "$summary_code" -eq 0 ] || ci_record_stage "summary persistence" "FAIL" "0" "$CI_SUMMARY"
      ci_render_summary "FAIL" || true
    fi
    if [ "$status" -eq 0 ]; then
      echo "PASS: $CI_SUMMARY"
    else
      echo "FAIL: $CI_SUMMARY"
    fi
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
    echo "+ docker/fixture-inputs/verify.sh --consumer iceberg-rest"
    verify_fixture_inputs &&
    echo "+ docker/iceberg-rest/up.sh" &&
    docker/iceberg-rest/up.sh &&
      echo "+ resolve and source fixture publication" &&
      load_fixture_publication docker/iceberg-rest/runtime/current/env.sh &&
      require_runtime_var NOVAROCKS_FE_CONFIG &&
      require_runtime_var NOVAROCKS_BE_CONFIG &&
      require_runtime_var NOVAROCKS_SQL_TEST_CONFIG &&
      require_runtime_var NOVA_ENV_RUNTIME_DIR &&
      require_runtime_var NOVA_ENV_MYSQL_PORT &&
      require_runtime_var NOVAROCKS_ICEBERG_REST_URI &&
      require_runtime_var NOVAROCKS_ICEBERG_REST_MV_URI &&
      require_runtime_var NOVA_ENV_REST_ENV_FILE &&
      require_runtime_var NOVA_ENV_MANIFEST &&
      require_runtime_var NOVA_ENV_OBJECT_STORE_RUNTIME &&
      require_runtime_var NOVA_ENV_CATALOG_RUNTIME &&
      require_runtime_var AWS_S3_ENDPOINT &&
      record_fixture_runtime &&
      echo "+ python3 docker/iceberg-rest/rest-mv/probe.py contract --manifest <runtime-manifest>" &&
      python3 docker/iceberg-rest/rest-mv/probe.py contract --manifest "$NOVA_ENV_MANIFEST"
    code=$?
    if [ "$code" -eq 0 ]; then
      echo "PAIMON_TESTS=DISABLED (pending detailed connector design; fixtures are not prepared)"
      echo "NOVAROCKS_FE_CONFIG=$NOVAROCKS_FE_CONFIG"
      echo "NOVAROCKS_BE_CONFIG=$NOVAROCKS_BE_CONFIG"
      echo "NOVAROCKS_SQL_TEST_CONFIG=$NOVAROCKS_SQL_TEST_CONFIG"
      echo "NOVA_ENV_RUNTIME_DIR=$NOVA_ENV_RUNTIME_DIR"
      echo "NOVA_ENV_MYSQL_PORT=$NOVA_ENV_MYSQL_PORT"
      echo "NOVAROCKS_ICEBERG_REST_URI=$NOVAROCKS_ICEBERG_REST_URI"
      echo "NOVAROCKS_SPARK_DEFAULTS=${NOVAROCKS_SPARK_DEFAULTS:-}"
      echo "NOVA_CI_CARGO_PROFILE=$NOVA_CI_CARGO_PROFILE"
      echo "SQL_CLUSTER_MODE=$SQL_CLUSTER_MODE"
      echo "SQL_CLUSTER_SIZE=$SQL_CLUSTER_SIZE"
      echo "NOVA_CI_NATIVE_CROSS_PROCESS_CORE=$NOVA_CI_NATIVE_CROSS_PROCESS_CORE"
      echo "NOVA_CI_NATIVE_CROSS_PROCESS_FULL=$NOVA_CI_NATIVE_CROSS_PROCESS_FULL"
      echo "NOVA_CI_NATIVE_CROSS_PROCESS_REQUIRED=$NOVA_CI_NATIVE_CROSS_PROCESS_REQUIRED"
    fi
    (exit "$code")
  } >"$log_path" 2>&1
  code=$?
  duration=$(($(ci_epoch) - start))

  if [ "$code" -ne 0 ]; then
    if [ "$code" -eq 75 ]; then
      ci_record_stage "fixture prerequisites" "BLOCKED" "$duration" "$log_path"
      ci_record_blocked "fixture input BOM missing or invalid" "$log_path"
      ci_render_summary "BLOCKED"
    else
      ci_record_stage "prepare runtime" "VERIFY FAILED" "$duration" "$log_path"
      ci_mark_failure_tail "prepare runtime verification failed" "$log_path"
      ci_render_summary "VERIFY FAILED"
    fi
    exit "$code"
  fi

  CI_RUNTIME_PREPARED="true"
  ci_set_runtime_context "$NOVAROCKS_FE_CONFIG" "$NOVAROCKS_SQL_TEST_CONFIG" "$NOVA_ENV_MYSQL_PORT"
  ci_record_stage "prepare runtime" "PASS" "$duration" "$log_path"
  ci_render_summary "RUNNING"
}

verify_fixture_inputs() {
  docker/fixture-inputs/verify.sh --consumer iceberg-rest
}

load_fixture_publication() {
  local publication_env
  publication_env="$(python3 -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve(strict=True))' "$1")" || return
  . "$publication_env"
}

record_fixture_runtime() {
  echo "NOVA_FIXTURE_MACHINE_ID=$NOVA_FIXTURE_MACHINE_ID"
  echo "NOVA_FIXTURE_MACHINE_DAEMON=$NOVA_FIXTURE_MACHINE_DAEMON"
  echo "NOVA_FIXTURE_MACHINE_VERSION=$NOVA_FIXTURE_MACHINE_VERSION"
  echo "NOVA_FIXTURE_MACHINE_GENERATION=$NOVA_FIXTURE_MACHINE_GENERATION"
  echo "NOVA_ENV_REST_ENV_FILE=$NOVA_ENV_REST_ENV_FILE"
  echo "NOVA_ENV_PUBLICATION_DIR=$(dirname "$NOVA_ENV_REST_ENV_FILE")"
  echo "AWS_S3_ENDPOINT=$AWS_S3_ENDPOINT"
  echo "NOVAROCKS_ICEBERG_REST_URI=$NOVAROCKS_ICEBERG_REST_URI"
  echo "NOVAROCKS_ICEBERG_REST_MV_URI=$NOVAROCKS_ICEBERG_REST_MV_URI"
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

# Describe at the actual command boundary; no second stage command registry.
ci_stage_native_command() {
  local name="$1"; shift
  if [ "${CI_DESCRIBE_STAGE:-false}" = true ]; then
    python3 "$SCRIPT_DIR/lib/invocation_record.py" describe-stage --name "$name" -- "$@"
    exit $?
  fi
  "$@"
}

run_fail_fast_stage() {
  local name="$1"
  local log_name="$2"
  shift 2

  if [ -n "${CI_SELECTED_STAGE:-}" ]; then
    [ "$CI_SELECTED_STAGE" = "$name" ] || return 0
    CI_STAGE_EXECUTED=true
    ci_stage_native_command "$name" "$@"
    exit $?
  fi
  local log_path="$CI_RUN_DIR/$log_name"
  local start
  local code
  local duration

  start="$(ci_epoch)"
  ci_run_recorded_logged "$log_path" stage "$name" '{}' "$@"
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

run_rust_batch_stage() {
  local log_path="$CI_RUN_DIR/rust-batch.log" start code duration
  start="$(ci_epoch)"
  printf '%s\n' "execution_index=$CI_RUN_DIR/rust/execution-index.json" >"$log_path"
  # test-run.py already owns exclusive raw logs and bounded display writers.
  # Preserve the inherited terminal here; another redirect/tee would either
  # swallow its real-time display or introduce a second blocking output owner.
  local record materials
  materials="$(python3 - "$CI_RUN_DIR/rust" <<'MATERIALS'
import json, sys
from pathlib import Path
root=Path(sys.argv[1])
print(json.dumps({"execution_index":str(root/'execution-index.json'),"execution_identity":[str(root/'identity/nextest.json'),str(root/'identity/doctest.json')]}))
MATERIALS
)"
  record="$(ci_invocation_start stage "workspace nextest and doctests" python3 tools/ci/test-run.py batch --output "$CI_RUN_DIR/rust")" || return $?
  python3 tools/ci/test-run.py batch --output "$CI_RUN_DIR/rust"
  code=$?
  ci_invocation_finish "$record" "$code" "$log_path" "$materials" || return $?
  duration=$(($(ci_epoch) - start))
  printf 'wrapper_exit=%s\n' "$code" >>"$log_path"
  local collect_safe=false
  if [ "$CI_COLLECT_ALL" = true ]; then
    mkdir -p "$CI_RUN_DIR/collection"
    if (cd "$SCRIPT_DIR" && python3 -m verification.collection rust "$CI_RUN_DIR/rust" "$code") >"$CI_RUN_DIR/collection/rust.json"; then
      collect_safe=true
    elif [ "$code" -eq 0 ]; then
      ci_record_stage "workspace nextest and doctests" "FAIL" "$duration" "$log_path"
      ci_mark_failure_tail "Rust collection has incomplete exit, identity or cleanup facts" "$log_path"
      return 1
    fi
  fi
  if [ "$code" -ne 0 ]; then
    ci_record_stage "workspace nextest and doctests" "FAIL" "$duration" "$log_path"
    ci_mark_failure_tail "workspace Rust batch failed; inspect its execution index" "$log_path"
    ci_render_summary "FAIL"
    if [ "$CI_COLLECT_ALL" = true ] && [ "$collect_safe" = true ]; then
      CI_COLLECTION_RESULT=1
      return 0
    fi
    return "$code"
  fi
  ci_record_stage "workspace nextest and doctests" "PASS" "$duration" "$log_path"
  ci_render_summary "RUNNING"
}

run_cargo_gates() {
  ci_set_default_build_jobs || return $?
  run_fail_fast_stage "CI build inputs" "build-inputs.log" \
    ci_validate_build_inputs "$NOVA_CI_CARGO_PROFILE"
  local main_target
  main_target="$(ci_target_directory main)" || return $?
  if [ -z "${CI_SELECTED_STAGE:-}" ]; then NOVA_CI_ARTIFACT_RECEIPT="$CI_RUN_DIR/rust/final-prepare/artifacts.json"; fi
  export NOVA_CI_ARTIFACT_RECEIPT
  run_fail_fast_stage "locked Cargo metadata" "cargo-metadata.log" \
    cargo metadata --locked --format-version 1
  run_fail_fast_stage "Cargo dependency policy" "cargo-deny.log" \
    cargo deny --locked check bans licenses sources
  run_fail_fast_stage "unused dependencies" "cargo-machete.log" \
    ci_check_unused_dependencies
  run_fail_fast_stage "unused dependencies tests" "cargo-machete-test.log" \
    bash tools/ci/tests/unused-dependencies-stage-test.sh
  run_fail_fast_stage "legacy branding" "legacy-branding.log" \
    tools/ci/check-legacy-branding.sh
  run_fail_fast_stage "legacy branding tests" "legacy-branding-test.log" \
    tools/ci/tests/legacy-branding-test.sh
  run_fail_fast_stage "system scenario stage tests" "system-scenarios-stage-test.log" \
    tools/ci/tests/system-scenarios-stage-test.sh
  run_fail_fast_stage "verification composition tests" "verification-composition-tests.log" \
    bash -c 'for t in tools/ci/tests/verification-*-test.py; do python3 "$t" || exit $?; done'
  run_fail_fast_stage "local CI classification tests" "local-full-ci-classification-test.log" \
    bash tools/ci/tests/local-full-ci-classification-test.sh
  run_fail_fast_stage "disabled SQL suite tests" "paimon-tests-disabled-test.log" \
    bash tools/ci/tests/paimon-tests-disabled-test.sh
  run_fail_fast_stage "identity snapshot tests" "identity-snapshot-test.log" python3 tools/ci/tests/identity-snapshot-test.py
  run_fail_fast_stage "invocation record tests" "invocation-record-test.log" python3 tools/ci/tests/invocation-record-test.py
  CI_STAGE_OUTPUT_ARGS=1 run_fail_fast_stage "Rust executor regression tests" "test-run-test.log" \
    env NOVA_CI_TEST_EVIDENCE_ROOT="$CI_RUN_DIR/executor-regression" \
    python3 tools/ci/tests/test-run-test.py
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
  # Design: ADR-0194 (docs/adr/ADR-0194-strict-clippy-ci-baseline.md)
  run_fail_fast_stage "cargo clippy" "cargo-clippy.log" \
    ci_main_cargo clippy --all-targets -- -D warnings
  # The SQL error manifest is a generated whitelist that other tools consume as
  # fact: the sql-test runner include_str!s it and rejects any @expect_sql_code
  # missing from it. A stale manifest therefore does not surface as "this file
  # is out of date" -- it surfaces as an unrelated suite failing to load its
  # cases. Regenerating is the fix (`cargo run -p novarocks-error-manifest`).
  # The manifest path keeps the entry point explicit while Cargo resolves the
  # package through the repository workspace, profile, and committed lock.
  if [ "$SKIP_CARGO_TEST" = true ]; then
    CI_STAGE_OUTPUT_ARGS=1 run_fail_fast_stage "joint tests and binaries preparation" "cargo-prepare.log" \
      ci_prepare_main "$NOVA_CI_ARTIFACT_RECEIPT"
    CI_STAGE_INPUT_ARGS=1 CI_STAGE_OUTPUT_ARGS=2 run_fail_fast_stage "main binary build information" "main-build-info.log" \
      ci_capture_main_builds "$NOVA_CI_ARTIFACT_RECEIPT" "$CI_RUN_DIR/build-info/main"
    local error_manifest
    error_manifest="$(ci_bound_binary novarocks-error-manifest)" || return $?
    run_fail_fast_stage "SQL error manifest freshness" "error-manifest-check.log" \
      "$error_manifest" --check
    ci_record_stage "workspace tests" "SKIP" "0" ""
    ci_record_stage "workspace doctests" "SKIP" "0" ""
    return 0
  fi
  # The batch owns failure control: an ordinary nextest assertion failure still
  # runs independent doctests; cancellation or incomplete cleanup gates them.
  # Its final joint preparation refreshes executable provenance after Cargo.
  run_rust_batch_stage

}

run_system_scenario_fixture_stage() {
  [ "$SKIP_SYSTEM_SCENARIOS" != true ] || return 0
  local runner binary
  runner="$(ci_bound_binary novarocks-system-tests)" || return $?
  binary="$(ci_bound_binary novarocks)" || return $?
  if [ "${CI_DESCRIBE_STAGE:-false}" != true ]; then mkdir -p "$CI_RUN_DIR/system"; fi
  SYSTEM_SCENARIO_SELECTION="$CI_RUN_DIR/system/selected.txt"
  CI_STAGE_OUTPUT_ARGS=3,4 run_fail_fast_stage "functional default scenario selection" "system-selection.log" \
    ci_select_system_scenarios "$runner" functional "$SYSTEM_SCENARIO_SELECTION" "$CI_RUN_DIR/system/requirements.txt"
  CI_STAGE_INPUT_ARGS=6 CI_STAGE_OUTPUT_ARGS=3,5 run_fail_fast_stage "selected system program preparation" "system-preparation.log" \
    ci_prepare_system_scenario_binaries "$binary" dev "$CI_RUN_DIR/system/fixtures" "${CI_COMMIT_SHA:0:12}" \
    "$CI_RUN_DIR/system-scenario-fixtures.log" "$CI_RUN_DIR/system/requirements.txt"
}

run_system_scenarios_stage() {
  if [ "$SKIP_SYSTEM_SCENARIOS" = "true" ]; then
    ci_record_stage "system scenarios" "SKIP" "0" ""
    ci_record_system_scenario "<all>" "SKIP" "0" "" "-"
    ci_render_summary "RUNNING"
    return 0
  fi

  local runner
  runner="$(ci_bound_binary novarocks-system-tests)" || return $?
  local start
  local code
  local duration

  [ -x "$runner" ] && [ -x "$SYSTEM_SCENARIO_PRIMARY_BINARY" ] || return 1
  local collection_policy=fail-fast
  [ "$CI_COLLECT_ALL" != true ] || collection_policy=collect-all
  start="$(ci_epoch)"
  ci_run_system_scenarios \
    "$runner" \
    "$SYSTEM_SCENARIO_PRIMARY_BINARY" \
    "$SYSTEM_SCENARIO_COMPATIBLE_BINARY" \
    "$SYSTEM_SCENARIO_OTHER_ISLAND_BINARY" \
    "$SYSTEM_SCENARIO_BASE_CONFIG" \
    "$CI_RUN_DIR/system/artifacts" \
    "$SYSTEM_SCENARIO_CLUSTER_SIZE" \
    "$SYSTEM_SCENARIO_TIMEOUT_SECS" \
    "$SYSTEM_SCENARIO_SELECTION" \
    "$collection_policy"
  code=$?
  duration=$(($(ci_epoch) - start))

  if [ "$code" -ne 0 ]; then
    ci_record_stage "system scenarios" "FAIL" "$duration" ""
    ci_render_summary "FAIL"
    if [ "$CI_COLLECT_ALL" = true ] && [ "${CI_SYSTEM_COLLECTION_COMPLETE:-false}" = true ]; then
      CI_COLLECTION_RESULT=1
      return 0
    fi
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

  if ! suites_output="$(ci_tier_suites "$CI_TIER" "$REPO_ROOT")"; then
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
    if [ "$suite" = "paimon" ]; then
      echo "error: SQL suite paimon is disabled pending detailed connector design" >&2
      exit 2
    fi
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

ci_classify_sql_results() {
  local suite="$1" results="$2" output code=0
  local membership_args=()
  [ "$CI_COLLECT_ALL" = true ] || membership_args=(--admission-membership)
  output="$(cd "$SCRIPT_DIR" && python3 -m verification.classification --results "$results" --dispositions "$DISPOSITIONS_FILE" "${membership_args[@]}")" || code=$?
  if [ "$CI_COLLECT_ALL" = true ] && [ -n "$output" ]; then
    printf '%s\n' "$output" >"${results%.results.jsonl}.classification.json"
  fi
  [ -n "$output" ] || { CI_SQL_RESULT_STATUS=FAIL; return 1; }
  local rows row_suite row_case row_status row_code row_reason
  rows="$(python3 -c 'import json,sys; d=json.load(sys.stdin); print("\n".join("|".join([r["suite"],r["case"],r["status"],str((r.get("failure") or {}).get("error_code") or ""),""]) for r in d["rows"]))' <<<"$output")"
  while IFS='|' read -r row_suite row_case row_status row_code row_reason; do
    [ -n "$row_suite" ] || continue
    [ "$row_status" = PASS ] || ci_record_sql_classification "$row_suite" "$row_case" "$row_status" "$row_code" "$row_reason"
  done <<<"$rows"
  CI_SQL_RESULT_STATUS="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<<"$output")"
  CI_SQL_ACCEPTS_CASE_FAILURE="$(python3 -c 'import json,sys; d=json.load(sys.stdin); print("true" if d["status"]=="KNOWN_FAIL" or (d["status"]=="PASS" and d["membership"]["excluded"]) else "false")' <<<"$output")"
  CI_SQL_RECORDED_EXIT_CODE="$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("native_exit_code"))' <<<"$output")"
  return "$code"
}

ci_should_use_runner_suite_timeout() {
  local suite="$1"

  # The prepared-write-set entry-budget case intentionally materializes 16,385
  # files and can run for roughly 90 seconds. Keep the runner's suite default
  # authoritative unless the caller explicitly requests a CI-wide override.
  [ "$suite" = "iceberg-dml" ] && [ -z "${SQL_QUERY_TIMEOUT_SECONDS:-}" ]
}

ci_sql_native_exit_acceptable() {
  [ "$1" = "$CI_SQL_RECORDED_EXIT_CODE" ] || return 1
  [ "$1" -eq 0 ] || { [ "$1" -eq 1 ] && [ "$CI_SQL_ACCEPTS_CASE_FAILURE" = true ]; }
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
  local -a suite_launch_profile_args
  local query_timeout
  local novarocks_bin
  local suite_cluster_mode
  local suite_cluster_size

  novarocks_bin="$(ci_novarocks_binary_path "$NOVA_CI_CARGO_PROFILE")" || return $?
  local sql_runner
  sql_runner="$(ci_bound_binary novarocks-sql-test)" || return $?

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
    suite_launch_profile_args=()
    if [ -n "$SQL_CLUSTER_LAUNCH_PROFILE" ]; then
      suite_launch_profile_args=(--cluster-launch-profile "$SQL_CLUSTER_LAUNCH_PROFILE")
    fi
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
    novarocks_bin="$(ci_bound_binary novarocks)" || return $?
    sql_runner="$(ci_bound_binary novarocks-sql-test)" || return $?
    local results_file="${log_path%.log}.results.jsonl" sql_materials
    sql_materials="$(python3 - "$results_file" <<'MATERIALS'
import json, sys
print(json.dumps({"sql_results":sys.argv[1]}))
MATERIALS
)"
    ci_run_recorded_logged "$log_path" sql-suite "$suite" "$sql_materials" \
      env NO_PROXY=127.0.0.1,localhost \
      NOVAROCKS_BIN="$novarocks_bin" \
      NOVA_ENV_REST_ENV_FILE="$NOVA_ENV_REST_ENV_FILE" \
      NOVAROCKS_WORKSPACE_ROOT="$REPO_ROOT" \
      "$sql_runner" \
        --config "$NOVAROCKS_SQL_TEST_CONFIG" \
        --suite "$suite" \
        --mode verify \
        --results-file "$results_file" \
        "${suite_query_timeout_args[@]}" \
        --cluster-mode "$suite_cluster_mode" \
        --cluster-size "$suite_cluster_size" \
        "${suite_launch_profile_args[@]}" \
        "${suite_extra_args[@]}" \
        -j 1
    code=$?
    duration=$(($(ci_epoch) - start))

    if [ "$CI_COLLECT_ALL" = true ]; then
      mkdir -p "$CI_RUN_DIR/collection/sql"
      if ! (cd "$SCRIPT_DIR" && python3 -m verification.collection sql "$results_file" "$code" --invocation "${CI_LAST_INVOCATION_RECORD:-}") >"$CI_RUN_DIR/collection/sql/${suite}.$(basename "$(dirname "$results_file")").json"; then
        ci_record_sql_suite "$suite" "FAIL" "$duration" "$log_path"
        ci_mark_failure_tail "SQL collection has incomplete exit, identity or cleanup facts: $suite" "$log_path"
        ci_render_summary "FAIL"
        exit 1
      fi
      [ "$code" -eq 0 ] || CI_COLLECTION_RESULT=1
    fi
    local status classification_code=0
    ci_classify_sql_results "$suite" "$results_file" || classification_code=$?
    status="$CI_SQL_RESULT_STATUS"
    # Preserve a raw invocation failure even if its selected case records pass.
    if ! ci_sql_native_exit_acceptable "$code"; then status=FAIL; classification_code=1; fi
    ci_record_sql_suite "$suite" "$status" "$duration" "$log_path"
    if [ "$classification_code" -ne 0 ]; then
      failed=1
      [ "$CI_COLLECT_ALL" != true ] || CI_COLLECTION_RESULT=1
      if [ -z "$CI_FAILURE_TAIL" ]; then ci_mark_failure_tail "SQL suite requires attention: $suite" "$log_path"; fi
    fi
    ci_render_summary "RUNNING"
  done

  if [ "$failed" -ne 0 ]; then
    ci_render_summary "FAIL"
    [ "$CI_COLLECT_ALL" != true ] || return 0
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
  local -a suite_launch_profile_args
  local suites_output

  if ! ci_native_cross_process_enabled; then
    return 0
  fi

  mkdir -p "$CI_RUN_DIR/sql-native-cross-process"
  novarocks_bin="$(ci_novarocks_binary_path "$NOVA_CI_CARGO_PROFILE")" || return $?
  local sql_runner
  sql_runner="$(ci_bound_binary novarocks-sql-test)" || return $?

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
    suite_launch_profile_args=()
    if [ -n "$SQL_CLUSTER_LAUNCH_PROFILE" ]; then
      suite_launch_profile_args=(--cluster-launch-profile "$SQL_CLUSTER_LAUNCH_PROFILE")
    fi
    query_timeout="${SQL_QUERY_TIMEOUT_SECONDS:-60}"
    suite_cluster_mode="$(ci_native_cross_process_suite_cluster_mode "$suite")"
    suite_cluster_size="$(ci_native_cross_process_suite_cluster_size "$suite")"
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

    novarocks_bin="$(ci_bound_binary novarocks)" || return $?
    sql_runner="$(ci_bound_binary novarocks-sql-test)" || return $?
    local results_file="${log_path%.log}.results.jsonl" sql_materials
    sql_materials="$(python3 - "$results_file" <<'MATERIALS'
import json, sys
print(json.dumps({"sql_results":sys.argv[1]}))
MATERIALS
)"
    ci_run_recorded_logged "$log_path" sql-suite "$suite" "$sql_materials" \
      env NO_PROXY=127.0.0.1,localhost \
      NOVAROCKS_BIN="$novarocks_bin" \
      NOVA_ENV_REST_ENV_FILE="$NOVA_ENV_REST_ENV_FILE" \
      NOVAROCKS_WORKSPACE_ROOT="$REPO_ROOT" \
      "$sql_runner" \
        --config "$NOVAROCKS_SQL_TEST_CONFIG" \
        --suite "$suite" \
        --mode verify \
        --results-file "$results_file" \
        "${suite_query_timeout_args[@]}" \
        --cluster-mode "$suite_cluster_mode" \
        --cluster-size "$suite_cluster_size" \
        "${suite_launch_profile_args[@]}" \
        -j 1
    code=$?
    duration=$(($(ci_epoch) - start))

    if [ "$CI_COLLECT_ALL" = true ]; then
      mkdir -p "$CI_RUN_DIR/collection/sql"
      if ! (cd "$SCRIPT_DIR" && python3 -m verification.collection sql "$results_file" "$code" --invocation "${CI_LAST_INVOCATION_RECORD:-}") >"$CI_RUN_DIR/collection/sql/${suite}.$(basename "$(dirname "$results_file")").json"; then
        ci_record_sql_suite "$suite" "FAIL" "$duration" "$log_path"
        ci_mark_failure_tail "SQL collection has incomplete exit, identity or cleanup facts: $suite" "$log_path"
        ci_render_summary "FAIL"
        exit 1
      fi
      [ "$code" -eq 0 ] || CI_COLLECTION_RESULT=1
    fi
    local status classification_code=0
    ci_classify_sql_results "$suite" "$results_file" || classification_code=$?
    status="$CI_SQL_RESULT_STATUS"
    if ! ci_sql_native_exit_acceptable "$code"; then status=FAIL; classification_code=1; fi
    if [ "$classification_code" -eq 0 ]; then
      ci_record_sql_suite "native-cross-process:$suite" "$status" "$duration" "$log_path"
    else
      failed=1
      [ "$CI_COLLECT_ALL" != true ] || CI_COLLECTION_RESULT=1
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
    [ "$CI_COLLECT_ALL" != true ] || return 0
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

  local results_file classification_code
  for results_file in "$CI_RUN_DIR"/sql/*.results.jsonl; do
    [ -f "$results_file" ] || continue
    any_logs=1
    suite="${results_file##*/}"; suite="${suite%.results.jsonl}"
    log_path="${results_file%.results.jsonl}.log"
    classification_code=0
    ci_classify_sql_results "$suite" "$results_file" || classification_code=$?
    status="$CI_SQL_RESULT_STATUS"
    ci_record_sql_suite "$suite" "$status" "0" "$log_path"
    if [ "$classification_code" -ne 0 ]; then failed=1; fi
  done

  if [ "$any_logs" -eq 0 ]; then
    failed=1
    CI_FAILURE_TAIL="$(cat <<EOF
### reclassification failed

\`\`\`text
no sql/*.results.jsonl files found under $CI_RUN_DIR
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

# Development selection delegates to the same native stage commands. This
# dispatcher executes one declared stage and creates no parallel command map.
ci_execute_selected_stage() (
  ci_set_default_build_jobs || return $?
  CI_SELECTED_STAGE="$1"
  NOVA_CI_ARTIFACT_RECEIPT="$2"
  CI_RUN_DIR="$3"
  CI_SUMMARY="$CI_RUN_DIR/summary.md"
  CI_STAGE_EXECUTED=false
  CI_COMMIT_SHA="${CI_COMMIT_SHA:-$(git -C "$REPO_ROOT" rev-parse HEAD)}"
  SKIP_CARGO_TEST=true
  ci_init_summary_state
  case "$CI_SELECTED_STAGE" in
    'workspace nextest and doctests')
      ci_stage_native_command "$CI_SELECTED_STAGE" python3 "$SCRIPT_DIR/test-run.py" batch --output "$CI_RUN_DIR/rust-stage"
      return $? ;;
    'Cargo dependency advisories')
      ci_stage_native_command "$CI_SELECTED_STAGE" python3 "$SCRIPT_DIR/dependency-advisories.py" --output "$CI_RUN_DIR/dependency-advisories"
      return $? ;;
    memory-model:loom) ci_stage_native_command "$CI_SELECTED_STAGE" bash "$SCRIPT_DIR/memory-model-checks.sh" --loom; return $? ;;
    memory-model:miri) ci_stage_native_command "$CI_SELECTED_STAGE" bash "$SCRIPT_DIR/memory-model-checks.sh" --miri; return $? ;;
    native-compatibility) ci_stage_native_command "$CI_SELECTED_STAGE" bash "$SCRIPT_DIR/native-compatibility-ci.sh"; return $? ;;
    'functional default scenario selection'|'selected system program preparation')
      run_system_scenario_fixture_stage ;;
    *) run_cargo_gates ;;
  esac
  [ "$CI_STAGE_EXECUTED" = true ] || { echo "error: stage has no native command: $CI_SELECTED_STAGE" >&2; return 2; }
)

main() {
  if [ "${1:-}" = --describe-stage ]; then
    [ "$#" -eq 4 ] || { echo "usage: local-full-ci.sh --describe-stage <name> <artifact-receipt> <output>" >&2; return 2; }
    CI_DESCRIBE_STAGE=true
    ci_execute_selected_stage "$2" "$3" "$4"
    return $?
  fi
  if [ "${1:-}" = --run-stage ]; then
    [ "$#" -eq 4 ] || { echo "usage: local-full-ci.sh --run-stage <name> <artifact-receipt> <output>" >&2; return 2; }
    ci_execute_selected_stage "$2" "$3" "$4"
    return $?
  fi
  if [ "${1:-}" = --list-stages ]; then ci_list_stages; return 0; fi
  parse_args "$@"
  cd "$REPO_ROOT" || exit 1

  if [ -n "$CI_FROM_RUN_DIR" ]; then
    reclassify_existing_run
    exit 0
  fi

  trap cleanup EXIT
  init_run_dir || return $?
  validate_explicit_suites_early

  if [ "$CARGO_ONLY" = "true" ]; then
    run_cargo_gates || return $?
    CI_RUN_REACHED_END=true
    ci_render_summary "RUNNING"
    return 0
  fi

  trap cleanup EXIT

  prepare_runtime
  run_cargo_gates || return $?
  # System Scenarios run before the SQL suites so a topology, readiness,
  # restart, fault or cleanup break surfaces before the much longer SQL tier.
  # Most default scenarios use local warehouses. Target replacement owns an
  # isolated REST/S3 fixture using the inputs verified during runtime preparation.
  run_system_scenario_fixture_stage || return $?
  run_system_scenarios_stage || return $?
  reset_frontend_state_store_stage
  if [ "$SQL_CLUSTER_MODE" = "all-in-one" ]; then
    start_server_stage
  else
    ci_record_stage "standalone" "SKIP" "0" ""
    ci_render_summary "RUNNING"
  fi
  if [ "$CI_COLLECT_ALL" = true ]; then
    run_sql_suites || return $?
    run_native_cross_process_sql_suites || return $?
  else
    run_sql_suites
    run_native_cross_process_sql_suites
  fi

  CI_RUN_REACHED_END=true
  if [ "$CI_COLLECT_ALL" = true ]; then
    CI_COLLECTION_REACHED_END=true
    if [ "$CI_COLLECTION_RESULT" -ne 0 ]; then
      ci_render_summary "FAIL"
      echo "Collection stages finished with raw failures or classification obligations; final status follows cleanup: $CI_SUMMARY"
      return "$CI_COLLECTION_RESULT"
    fi
  fi
  # Every normal run publishes its final status only after actual source-end
  # and cleanup; read-only dispatch paths return before installing this trap.
  ci_render_summary "RUNNING"
  return 0
}

if [ "${1:-}" != "--source-only" ]; then
  main "$@"
fi
