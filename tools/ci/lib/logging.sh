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
  CI_SQL_CASE_ROWS=""
  CI_KNOWN_FAILURE_ROWS=""
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
  local suite_status="$2"
  local duration="$3"
  local log_path="$4"
  local rel_log
  rel_log="$(ci_rel_log "$log_path")"
  CI_SQL_ROWS="${CI_SQL_ROWS}| ${suite} | ${suite_status} | ${duration}s | ${rel_log} |
"
  ci_record_sql_case_timings "$log_path"
}

ci_record_sql_case_timings() {
  local log_path="$1"
  local in_case_timings="false"
  local line
  local suite_name
  local case_id
  local case_status
  local elapsed

  [ -f "$log_path" ] || return 0

  while IFS= read -r line || [ -n "$line" ]; do
    if [ "$line" = "case timings (all):" ]; then
      in_case_timings="true"
      continue
    fi

    if [ "$in_case_timings" != "true" ]; then
      continue
    fi

    case "$line" in
      =*)
        break
        ;;
    esac

    if [[ "$line" =~ ^[[:space:]]+\[([^]]+)\][[:space:]]+([^[:space:]]+)[[:space:]]+([A-Z]+)[[:space:]]+([0-9]+(\.[0-9]+)?)s$ ]]; then
      suite_name="${BASH_REMATCH[1]}"
      case_id="${BASH_REMATCH[2]}"
      case_status="${BASH_REMATCH[3]}"
      elapsed="${BASH_REMATCH[4]}"
      CI_SQL_CASE_ROWS="${CI_SQL_CASE_ROWS}| ${suite_name} | ${case_status} | ${case_id} | ${elapsed}s |
"
    fi
  done <"$log_path"
}

ci_record_sql_classification() {
  local suite="$1"
  local case_name="$2"
  local status="$3"
  local error_code="$4"
  local reason="$5"

  suite="${suite//|/\\|}"
  case_name="${case_name//|/\\|}"
  status="${status//|/\\|}"
  error_code="${error_code//|/\\|}"
  reason="${reason//|/\\|}"

  CI_KNOWN_FAILURE_ROWS="${CI_KNOWN_FAILURE_ROWS}| ${suite} | ${case_name} | ${status} | ${error_code} | ${reason} |
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

    if [ -n "$CI_SQL_CASE_ROWS" ]; then
      printf "## SQL Case Timings\n\n"
      printf "| Suite | Status | Case | Duration |\n"
      printf "| --- | --- | --- | --- |\n"
      printf "%s" "$CI_SQL_CASE_ROWS"
      printf "\n"
    fi

    if [ -n "$CI_KNOWN_FAILURE_ROWS" ]; then
      printf "## SQL Failure Classification\n\n"
      printf "| Suite | Case | Status | Error Code | Reason |\n"
      printf "| --- | --- | --- | --- | --- |\n"
      printf "%s" "$CI_KNOWN_FAILURE_ROWS"
      printf "\n"
    fi

    if [ -n "$CI_FAILURE_TAIL" ]; then
      printf "## Failure Tail\n\n"
      printf "%s\n" "$CI_FAILURE_TAIL"
    fi
  } >"$CI_SUMMARY"
}
