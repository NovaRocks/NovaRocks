#!/usr/bin/env bash

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
      printf "%s\n" optimizer iceberg-rest aggregate runtime-filter
      ;;
    full)
      ci_load_stable_suites "$stable_manifest"
      ;;
    *)
      return 1
      ;;
  esac
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

  case "$suite" in
    *[!A-Za-z0-9_.+-]*|""|"."|"..")
      return 1
      ;;
  esac

  [ -d "$repo_root/sql-tests/$suite/sql" ]
}
