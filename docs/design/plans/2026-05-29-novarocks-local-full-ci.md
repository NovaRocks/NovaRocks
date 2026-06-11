# NovaRocks Local Full CI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local, repeatable full CI runner for NovaRocks that can later be called by Codex daily automation.

**Architecture:** Add a layered Bash runner under `tools/ci/`: one main entrypoint, focused helper libraries, and a stable SQL suite manifest. The runner prepares the existing Docker Iceberg runtime, runs fail-fast Rust gates, starts the standalone server using the `NOVAROCKS_READY` contract, runs SQL suites with continue-on-failure, writes a local summary, and cleans only resources it created.

**Tech Stack:** Bash, Cargo, NovaRocks `docker/iceberg-rest` runtime scripts, `tests/sql-test-runner`, local Markdown logs.

---

## File Structure

- Modify: `.gitignore`
  Add `/logs/ci-full/` so local CI output never enters git.
- Create: `tools/ci/suites/stable-sql-suites.txt`
  Default daily SQL suite list. Keep benchmark suites out of the default list until benchmark bootstrap is part of this runner.
- Create: `tools/ci/lib/logging.sh`
  Summary state, duration helpers, stage rows, SQL rows, and failure-tail rendering.
- Create: `tools/ci/lib/command.sh`
  One small command wrapper that logs the exact command and captures stdout/stderr.
- Create: `tools/ci/lib/sql_suites.sh`
  Stable suite loading, all-discovered suite scanning, and suite existence checks.
- Create: `tools/ci/lib/server.sh`
  Standalone-server start/ready/stop helpers. This file owns the `NOVAROCKS_READY mysql_port=` wait loop.
- Create: `tools/ci/local-full-ci.sh`
  Main CLI, argument parsing, runtime preparation, Rust gates, server lifecycle, SQL execution, summary, cleanup, and exit status.

## Task 1: Ignore Local CI Logs And Add Stable Suite Manifest

**Files:**
- Modify: `.gitignore`
- Create: `tools/ci/suites/stable-sql-suites.txt`

- [ ] **Step 1: Add local CI log ignore**

Append this line to `.gitignore` near other local output directories:

```gitignore
/logs/ci-full/
```

- [ ] **Step 2: Create the stable suite manifest**

Create `tools/ci/suites/stable-sql-suites.txt` with this exact content:

```text
# Stable SQL suites for tools/ci/local-full-ci.sh default daily mode.
# Benchmark suites are excluded from default daily until benchmark data bootstrap is explicitly part of this runner:
# ssb
# tpc-h
# tpc-ds
# iceberg-mv-scheduler is excluded until scheduler timing expectations are verified for unattended daily runs.
aggregate
analytic
complex-type
cte
decimal
filter
function
iceberg
iceberg-compatibility
iceberg-ddl
iceberg-dml
iceberg-ivm
iceberg-rest
join
limit
low-cardinality
materialized-view
optimizer
project
runtime-filter
set-op
sort
table-function
```

- [ ] **Step 3: Verify manifest contents**

Run:

```bash
grep -v '^#' tools/ci/suites/stable-sql-suites.txt | grep -v '^$' | wc -l
```

Expected output:

```text
23
```

- [ ] **Step 4: Commit**

```bash
git add .gitignore tools/ci/suites/stable-sql-suites.txt
git commit -m "chore: add local CI suite manifest"
```

## Task 2: Add Logging And Command Helpers

**Files:**
- Create: `tools/ci/lib/logging.sh`
- Create: `tools/ci/lib/command.sh`

- [ ] **Step 1: Create logging helper**

Create `tools/ci/lib/logging.sh` with this exact content:

```bash
#!/usr/bin/env bash

ci_now_utc() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

ci_epoch() {
  date +%s
}

ci_init_summary_state() {
  CI_STARTED_AT="$(ci_now_utc)"
  CI_STARTED_EPOCH="$(ci_epoch)"
  CI_STAGE_ROWS=""
  CI_SQL_ROWS=""
  CI_FAILURE_TAIL=""
  CI_REPO_PATH=""
  CI_BRANCH_NAME=""
  CI_COMMIT_SHA=""
  CI_RUNTIME_CONFIG=""
  CI_SQL_CONFIG=""
  CI_MYSQL_PORT=""
}

ci_set_repo_context() {
  CI_REPO_PATH="$1"
  CI_BRANCH_NAME="$2"
  CI_COMMIT_SHA="$3"
}

ci_set_runtime_context() {
  CI_RUNTIME_CONFIG="$1"
  CI_SQL_CONFIG="$2"
  CI_MYSQL_PORT="$3"
}

ci_rel_log() {
  local log_path="$1"
  if [ -z "$log_path" ]; then
    printf "-"
    return 0
  fi

  case "$log_path" in
    "$CI_RUN_DIR"/*)
      printf "%s" "${log_path#"$CI_RUN_DIR"/}"
      ;;
    *)
      printf "%s" "$log_path"
      ;;
  esac
}

ci_record_stage() {
  local name="$1"
  local status="$2"
  local duration="$3"
  local log_path="$4"
  local rel_log
  rel_log="$(ci_rel_log "$log_path")"
  CI_STAGE_ROWS="${CI_STAGE_ROWS}| ${name} | ${status} | ${duration}s | ${rel_log} |
"
}

ci_record_sql_suite() {
  local suite="$1"
  local status="$2"
  local duration="$3"
  local log_path="$4"
  local rel_log
  rel_log="$(ci_rel_log "$log_path")"
  CI_SQL_ROWS="${CI_SQL_ROWS}| ${suite} | ${status} | ${duration}s | ${rel_log} |
"
}

ci_mark_failure_tail() {
  local title="$1"
  local log_path="$2"
  local tail_text

  if [ -f "$log_path" ]; then
    tail_text="$(tail -40 "$log_path" 2>/dev/null || true)"
  else
    tail_text="log file not found: $log_path"
  fi

  CI_FAILURE_TAIL="$(cat <<EOF
### ${title}

\`\`\`text
${tail_text}
\`\`\`
EOF
)"
}

ci_render_summary() {
  local status="$1"
  local finished_at
  local finished_epoch
  local duration

  finished_at="$(ci_now_utc)"
  finished_epoch="$(ci_epoch)"
  duration=$((finished_epoch - CI_STARTED_EPOCH))

  {
    printf "# NovaRocks Local Full CI Summary\n\n"
    printf -- "- Status: %s\n" "$status"
    printf -- "- Started at: %s\n" "$CI_STARTED_AT"
    printf -- "- Finished at: %s\n" "$finished_at"
    printf -- "- Duration: %ss\n" "$duration"
    printf -- "- Repo: %s\n" "$CI_REPO_PATH"
    printf -- "- Branch: %s\n" "$CI_BRANCH_NAME"
    printf -- "- Commit: %s\n" "$CI_COMMIT_SHA"
    printf -- "- Run dir: %s\n" "$CI_RUN_DIR"
    printf -- "- Runtime config: %s\n" "${CI_RUNTIME_CONFIG:-unknown}"
    printf -- "- SQL config: %s\n" "${CI_SQL_CONFIG:-unknown}"
    printf -- "- MySQL port: %s\n\n" "${CI_MYSQL_PORT:-unknown}"

    printf "## Stages\n\n"
    printf "| Stage | Status | Duration | Log |\n"
    printf "| --- | --- | --- | --- |\n"
    if [ -n "$CI_STAGE_ROWS" ]; then
      printf "%s" "$CI_STAGE_ROWS"
    fi
    printf "\n"

    printf "## SQL Suites\n\n"
    printf "| Suite | Status | Duration | Log |\n"
    printf "| --- | --- | --- | --- |\n"
    if [ -n "$CI_SQL_ROWS" ]; then
      printf "%s" "$CI_SQL_ROWS"
    fi
    printf "\n"

    if [ -n "$CI_FAILURE_TAIL" ]; then
      printf "## Failure Tail\n\n"
      printf "%s\n" "$CI_FAILURE_TAIL"
    fi
  } >"$CI_SUMMARY"
}
```

- [ ] **Step 2: Create command helper**

Create `tools/ci/lib/command.sh` with this exact content:

```bash
#!/usr/bin/env bash

ci_run_logged() {
  local log_path="$1"
  shift

  {
    printf "+"
    for arg in "$@"; do
      printf " %q" "$arg"
    done
    printf "\n"
    "$@"
  } >"$log_path" 2>&1
}
```

- [ ] **Step 3: Run syntax checks**

Run:

```bash
for f in tools/ci/lib/logging.sh tools/ci/lib/command.sh; do
  bash -n "$f"
done
```

Expected output: no output, exit code 0.

- [ ] **Step 4: Run logging smoke check**

Run:

```bash
bash -c '
  source tools/ci/lib/logging.sh
  tmp="$(mktemp -d)"
  CI_RUN_DIR="$tmp"
  CI_SUMMARY="$tmp/summary.md"
  ci_init_summary_state
  ci_set_repo_context "/repo" "branch" "abc123"
  ci_record_stage "cargo fmt" "PASS" "1" "$tmp/fmt.log"
  ci_render_summary "PASS"
  grep -q "| cargo fmt | PASS | 1s | fmt.log |" "$CI_SUMMARY"
'
```

Expected output: no output, exit code 0.

- [ ] **Step 5: Commit**

```bash
git add tools/ci/lib/logging.sh tools/ci/lib/command.sh
git commit -m "chore: add local CI logging helpers"
```

## Task 3: Add SQL Suite Helper

**Files:**
- Create: `tools/ci/lib/sql_suites.sh`

- [ ] **Step 1: Create SQL suite helper**

Create `tools/ci/lib/sql_suites.sh` with this exact content:

```bash
#!/usr/bin/env bash

ci_load_stable_suites() {
  local manifest="$1"
  local line

  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
      ""|\#*)
        continue
        ;;
      *)
        printf "%s\n" "$line"
        ;;
    esac
  done <"$manifest"
}

ci_discover_sql_suites() {
  local repo_root="$1"
  local dir
  local suite

  for dir in "$repo_root"/sql-tests/*/sql; do
    [ -d "$dir" ] || continue
    suite="${dir%/sql}"
    printf "%s\n" "${suite##*/}"
  done | sort
}

ci_suite_exists() {
  local repo_root="$1"
  local suite="$2"
  [ -d "$repo_root/sql-tests/$suite/sql" ]
}
```

- [ ] **Step 2: Run syntax check**

Run:

```bash
bash -n tools/ci/lib/sql_suites.sh
```

Expected output: no output, exit code 0.

- [ ] **Step 3: Verify stable suite loading**

Run:

```bash
bash -c '
  source tools/ci/lib/sql_suites.sh
  ci_load_stable_suites tools/ci/suites/stable-sql-suites.txt | head -5
'
```

Expected output:

```text
aggregate
analytic
complex-type
cte
decimal
```

- [ ] **Step 4: Verify discovered suite scanning**

Run:

```bash
bash -c '
  source tools/ci/lib/sql_suites.sh
  ci_discover_sql_suites "$(pwd)" | grep -E "^(filter|iceberg-rest|optimizer)$"
'
```

Expected output contains these three lines, in sorted order:

```text
filter
iceberg-rest
optimizer
```

- [ ] **Step 5: Commit**

```bash
git add tools/ci/lib/sql_suites.sh
git commit -m "chore: add local CI SQL suite helper"
```

## Task 4: Add Standalone Server Helper

**Files:**
- Create: `tools/ci/lib/server.sh`

- [ ] **Step 1: Create server helper**

Create `tools/ci/lib/server.sh` with this exact content:

```bash
#!/usr/bin/env bash

CI_SERVER_PID=""

ci_start_standalone_server() {
  local config_path="$1"
  local log_path="$2"
  local timeout_seconds="$3"
  local i

  NO_PROXY=127.0.0.1,localhost \
    target/debug/novarocks standalone-server \
      --config "$config_path" >"$log_path" 2>&1 &
  CI_SERVER_PID=$!

  i=0
  while [ "$i" -lt "$timeout_seconds" ]; do
    if grep -q '^NOVAROCKS_READY mysql_port=' "$log_path" 2>/dev/null; then
      return 0
    fi

    if ! kill -0 "$CI_SERVER_PID" 2>/dev/null; then
      return 1
    fi

    sleep 1
    i=$((i + 1))
  done

  if kill -0 "$CI_SERVER_PID" 2>/dev/null; then
    kill "$CI_SERVER_PID" 2>/dev/null || true
    wait "$CI_SERVER_PID" 2>/dev/null || true
  fi
  CI_SERVER_PID=""
  return 2
}

ci_stop_standalone_server() {
  if [ -n "$CI_SERVER_PID" ] && kill -0 "$CI_SERVER_PID" 2>/dev/null; then
    kill "$CI_SERVER_PID" 2>/dev/null || true
    wait "$CI_SERVER_PID" 2>/dev/null || true
  fi
  CI_SERVER_PID=""
}
```

- [ ] **Step 2: Run syntax check**

Run:

```bash
bash -n tools/ci/lib/server.sh
```

Expected output: no output, exit code 0.

- [ ] **Step 3: Verify marker contract is present**

Run:

```bash
grep -n "NOVAROCKS_READY mysql_port=" tools/ci/lib/server.sh
```

Expected output contains:

```text
NOVAROCKS_READY mysql_port=
```

- [ ] **Step 4: Commit**

```bash
git add tools/ci/lib/server.sh
git commit -m "chore: add local CI server helper"
```

## Task 5: Add Main Local Full CI Runner

**Files:**
- Create: `tools/ci/local-full-ci.sh`

- [ ] **Step 1: Create main runner**

Create `tools/ci/local-full-ci.sh` with this exact content:

```bash
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
KEEP_RUNTIME="false"
SKIP_CARGO_TEST="false"
REQUESTED_SUITES=()
CI_RUNTIME_PREPARED="false"

usage() {
  cat <<'EOF'
Usage: tools/ci/local-full-ci.sh [options]

Runs NovaRocks local full CI with local logs under logs/ci-full/<timestamp>/.

Options:
  --all-discovered      Run every suite discovered from sql-tests/*/sql.
  --suite <name>        Run only the named SQL suite. May be repeated.
  --skip-cargo-test     Skip cargo test. Intended only for runner debugging.
  --keep-runtime        Keep this worktree's docker/iceberg-rest runtime entry.
  -h, --help            Show this help text.
EOF
}

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --all-discovered)
        RUN_MODE="all-discovered"
        shift
        ;;
      --suite)
        if [ "$#" -lt 2 ]; then
          echo "error: --suite requires a suite name" >&2
          exit 2
        fi
        RUN_MODE="explicit"
        REQUESTED_SUITES+=("$2")
        shift 2
        ;;
      --skip-cargo-test)
        SKIP_CARGO_TEST="true"
        shift
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

  if [ "$RUN_MODE" = "all-discovered" ] && [ "${#REQUESTED_SUITES[@]}" -gt 0 ]; then
    echo "error: --all-discovered cannot be combined with --suite" >&2
    exit 2
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
    require_runtime_var NOVA_ENV_MYSQL_PORT
    require_runtime_var NOVAROCKS_ICEBERG_REST_URI
    echo "NOVAROCKS_STANDALONE_CONFIG=$NOVAROCKS_STANDALONE_CONFIG"
    echo "NOVAROCKS_SQL_TEST_CONFIG=$NOVAROCKS_SQL_TEST_CONFIG"
    echo "NOVA_ENV_MYSQL_PORT=$NOVA_ENV_MYSQL_PORT"
    echo "NOVAROCKS_ICEBERG_REST_URI=$NOVAROCKS_ICEBERG_REST_URI"
    echo "NOVAROCKS_SPARK_DEFAULTS=${NOVAROCKS_SPARK_DEFAULTS:-}"
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
  run_fail_fast_stage "cargo clippy" "cargo-clippy.log" cargo clippy --all-targets -- -D warnings
  run_fail_fast_stage "cargo build" "cargo-build.log" cargo build

  if [ "$SKIP_CARGO_TEST" = "true" ]; then
    ci_record_stage "cargo test" "SKIP" "0" ""
    ci_render_summary "RUNNING"
  else
    run_fail_fast_stage "cargo test" "cargo-test.log" cargo test
  fi
}

start_server_stage() {
  local log_path="$CI_RUN_DIR/server.log"
  local start
  local code
  local duration

  start="$(ci_epoch)"
  ci_start_standalone_server "$NOVAROCKS_STANDALONE_CONFIG" "$log_path" 60
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

  if [ "$RUN_MODE" != "explicit" ]; then
    return 0
  fi

  for suite in "${REQUESTED_SUITES[@]}"; do
    if ! ci_suite_exists "$REPO_ROOT" "$suite"; then
      echo "error: SQL suite does not exist: $suite" >&2
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

  resolve_suites
  if [ "${#SUITES[@]}" -eq 0 ]; then
    echo "error: no SQL suites selected" >&2
    exit 2
  fi

  for suite in "${SUITES[@]}"; do
    log_path="$CI_RUN_DIR/sql/${suite}.log"
    start="$(ci_epoch)"
    ci_run_logged "$log_path" \
      env NO_PROXY=127.0.0.1,localhost \
      cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
        --config "$NOVAROCKS_SQL_TEST_CONFIG" \
        --suite "$suite" \
        --mode verify
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
  start_server_stage
  run_sql_suites

  ci_render_summary "PASS"
  echo "PASS: $CI_SUMMARY"
}

main "$@"
```

- [ ] **Step 2: Make the runner executable**

Run:

```bash
chmod +x tools/ci/local-full-ci.sh
```

- [ ] **Step 3: Run syntax checks**

Run:

```bash
for f in tools/ci/local-full-ci.sh tools/ci/lib/*.sh; do
  bash -n "$f"
done
```

Expected output: no output, exit code 0.

- [ ] **Step 4: Verify help output**

Run:

```bash
tools/ci/local-full-ci.sh --help
```

Expected output contains:

```text
Usage: tools/ci/local-full-ci.sh [options]
--all-discovered
--suite <name>
--skip-cargo-test
--keep-runtime
```

- [ ] **Step 5: Verify invalid suite handling**

Run:

```bash
tools/ci/local-full-ci.sh --suite does-not-exist --skip-cargo-test
```

Expected output contains:

```text
error: SQL suite does not exist: does-not-exist
```

Expected exit code: 2. This command must not run `docker/iceberg-rest/up.sh`, Cargo gates, or standalone-server startup, because `validate_explicit_suites_early` runs immediately after the run directory is initialized.

- [ ] **Step 6: Commit**

```bash
git add tools/ci/local-full-ci.sh
git commit -m "chore: add local full CI runner"
```

## Task 6: Run Targeted End-To-End Smoke

**Files:**
- Verify: `tools/ci/local-full-ci.sh`
- Verify output: `logs/ci-full/<timestamp>/summary.md`

- [ ] **Step 1: Run a narrow smoke suite**

Run:

```bash
tools/ci/local-full-ci.sh --suite filter --skip-cargo-test
```

Expected final stdout contains:

```text
PASS:
```

If the command fails because the current checkout has a real compile, clippy, runtime, or SQL regression, do not change the CI runner to hide it. Open the latest `logs/ci-full/<timestamp>/summary.md`, identify the failing stage, and report the failure as validation evidence.

- [ ] **Step 2: Inspect summary structure**

Replace `<run-dir>` with the run directory printed by Step 1:

```bash
grep -E "^- Status: |^## Stages|^## SQL Suites|filter" <run-dir>/summary.md
```

Expected output contains:

```text
- Status: PASS
## Stages
## SQL Suites
| filter | PASS |
```

- [ ] **Step 3: Verify runtime cleanup happened**

Run:

```bash
tail -20 <run-dir>/env.log | grep "down.sh --runtime-only --purge"
```

Expected output contains:

```text
docker/iceberg-rest/down.sh --runtime-only --purge
```

- [ ] **Step 4: Verify no standalone-server process from this runner remains**

Run:

```bash
if pgrep -fl "target/debug/novarocks standalone-server" | grep -F "$(pwd)" >/tmp/novarocks-local-ci-pgrep.txt; then
  cat /tmp/novarocks-local-ci-pgrep.txt
  exit 1
fi
rm -f /tmp/novarocks-local-ci-pgrep.txt
```

Expected output: no output, exit code 0.

- [ ] **Step 5: Commit smoke-result-neutral fixes if needed**

If the smoke run exposed runner bugs and you fixed them, commit only runner-related changes:

```bash
git add tools/ci .gitignore
git commit -m "fix: harden local full CI runner"
```

If the smoke run exposed a real NovaRocks compile/runtime/SQL failure unrelated to the runner, do not commit unrelated product fixes in this plan.

## Task 7: Final Static Review

**Files:**
- Review: `.gitignore`
- Review: `tools/ci/local-full-ci.sh`
- Review: `tools/ci/lib/*.sh`
- Review: `tools/ci/suites/stable-sql-suites.txt`

- [ ] **Step 1: Run shell syntax check**

Run:

```bash
for f in tools/ci/local-full-ci.sh tools/ci/lib/*.sh; do
  bash -n "$f"
done
```

Expected output: no output, exit code 0.

- [ ] **Step 2: Run diff whitespace check**

Run:

```bash
git diff --check
```

Expected output: no output, exit code 0.

- [ ] **Step 3: Verify forbidden cleanup commands are absent**

Run:

```bash
if rg -n "down\\.sh --docker|pkill|killall|lsof|fuser" tools/ci; then
  exit 1
fi
```

Expected output: no output, exit code 0.

- [ ] **Step 4: Verify readiness marker is used**

Run:

```bash
rg -n "NOVAROCKS_READY mysql_port=" tools/ci/lib/server.sh
```

Expected output contains:

```text
tools/ci/lib/server.sh
```

- [ ] **Step 5: Verify stable manifest excludes benchmark suites**

Run:

```bash
if grep -E '^(ssb|tpc-h|tpc-ds)$' tools/ci/suites/stable-sql-suites.txt; then
  exit 1
fi
```

Expected output: no output, exit code 0.

- [ ] **Step 6: Commit any final review-only corrections**

If Steps 1-5 required small runner-only corrections:

```bash
git add tools/ci .gitignore
git commit -m "fix: polish local full CI runner"
```

If no corrections were needed, do not create an empty commit.

## Self-Review Checklist

- Spec coverage:
  - Local runner entrypoint: Task 5.
  - Stable suite list and all-discovered mode: Tasks 1, 3, and 5.
  - Local logs and `summary.md`: Tasks 2 and 5.
  - Fail-fast Rust gates: Task 5.
  - `NOVAROCKS_READY` server wait: Task 4.
  - SQL continue-on-failure: Task 5.
  - Self-owned cleanup only: Tasks 4, 5, and 7.
  - No GitHub Actions, notifications, Obsidian, or external credentials: preserved by file set and commands.
- Placeholder scan: no plan step uses unspecified file paths, undefined functions, or open-ended implementation instructions.
- Type and name consistency:
  - Main script sources `logging.sh`, `command.sh`, `sql_suites.sh`, and `server.sh`.
  - Helper function names referenced in `local-full-ci.sh` are defined in earlier tasks.
  - `CI_RUN_DIR` and `CI_SUMMARY` are initialized before logging helpers render summary output.
