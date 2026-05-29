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
