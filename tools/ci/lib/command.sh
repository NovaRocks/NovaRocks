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
