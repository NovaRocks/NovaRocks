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


ci_load_stable_suites() {
  local manifest="$1"
  local line
  local suite

  while IFS= read -r line || [ -n "$line" ]; do
    suite="${line#"${line%%[![:blank:]]*}"}"
    suite="${suite%"${suite##*[![:blank:]]}"}"

    case "$suite" in
      ""|\#*)
        continue
        ;;
      *)
        printf "%s\n" "$suite"
        ;;
    esac
  done <"$manifest"
}

ci_tier_suites() {
  local tier="$1"
  local stable_manifest="$2"

  case "$tier" in
    smoke)
      printf "%s\n" filter project optimizer
      ;;
    targeted)
      printf "%s\n" optimizer optimizer-dist iceberg-rest aggregate runtime-filter
      ;;
    full)
      ci_load_stable_suites "$stable_manifest"
      ;;
    *)
      return 1
      ;;
  esac
}

ci_native_cross_process_core_suites() {
  printf "%s\n" join filter sort aggregate cte subquery iceberg-rest runtime-filter-distributed
}

ci_native_cross_process_suites() {
  if [ "${NOVA_CI_NATIVE_CROSS_PROCESS_FULL:-0}" = "1" ]; then
    ci_tier_suites full "$STABLE_SUITES_FILE"
    return $?
  fi

  ci_native_cross_process_core_suites
}

ci_suite_is_explicit_only() {
  local repo_root="$1"
  local suite="$2"
  local manifest="$repo_root/sql-tests/$suite/suite.toml"
  local explicit_only
  local status

  case "$suite" in
    *[!A-Za-z0-9_.+-]*|""|"."|"..")
      echo "error: invalid SQL suite name: $suite" >&2
      return 2
      ;;
  esac

  if [ ! -f "$manifest" ]; then
    return 1
  fi

  explicit_only="$(awk '
      function trim(value) {
        sub(/^[[:space:]]+/, "", value)
        sub(/[[:space:]]+$/, "", value)
        return value
      }

      BEGIN {
        in_table = 0
        assignments = 0
        invalid = 0
        value = "false"
      }

      {
        line = $0
        sub(/\r$/, "", line)
        line = trim(line)

        if (line == "" || line ~ /^#/) {
          next
        }
        if (line ~ /^\[/) {
          in_table = 1
          next
        }
        if (!in_table && line ~ /^explicit_only[[:space:]]*=/) {
          assignments++
          sub(/^explicit_only[[:space:]]*=[[:space:]]*/, "", line)
          sub(/[[:space:]]*#.*/, "", line)
          line = trim(line)
          if (line != "true" && line != "false") {
            invalid = 1
          }
          value = line
        }
      }

      END {
        if (assignments != 1 || invalid) {
          exit 2
        }
        print value
      }
    ' "$manifest")" || {
    status=$?
    echo "error: invalid explicit_only metadata in $manifest" >&2
    return "$status"
  }

  [ "$explicit_only" = "true" ]
}

ci_discover_sql_suites() {
  local repo_root="$1"
  local dir
  local suite
  local status
  local -a suites=()

  for dir in "$repo_root"/sql-tests/*/sql; do
    [ -d "$dir" ] || continue
    suite="${dir%/sql}"
    suite="${suite##*/}"
    if ci_suite_is_explicit_only "$repo_root" "$suite"; then
      continue
    else
      status=$?
    fi
    if [ "$status" -ne 1 ]; then
      return "$status"
    fi
    suites+=("$suite")
  done

  if [ "${#suites[@]}" -gt 0 ]; then
    printf "%s\n" "${suites[@]}" | sort
  fi
}

ci_suite_exists() {
  local repo_root="$1"
  local suite="$2"

  case "$suite" in
    *[!A-Za-z0-9_.+-]*|""|"."|"..")
      return 1
      ;;
  esac

  [ -d "$repo_root/sql-tests/$suite/sql" ]
}
