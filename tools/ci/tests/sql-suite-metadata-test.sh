#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
source "$REPO_ROOT/tools/ci/lib/sql_suites.sh"

! ci_suite_is_explicit_only "$REPO_ROOT" filter

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

assert_metadata_error() {
  local repo_root="$1"
  local suite="$2"
  if ci_suite_is_explicit_only "$repo_root" "$suite" 2>"$repo_root/predicate.err"; then
    echo "malformed metadata was accepted for suite: $suite" >&2
    exit 1
  fi
  if ci_discover_sql_suites "$repo_root" >/dev/null 2>"$repo_root/discovery.err"; then
    echo "shell discovery ignored malformed metadata for suite: $suite" >&2
    exit 1
  fi
}

duplicate_root="$tmpdir/duplicate"
mkdir -p "$duplicate_root/sql-tests/duplicate/sql"
printf '%s\n' 'explicit_only = true' 'explicit_only = false' >"$duplicate_root/sql-tests/duplicate/suite.toml"
assert_metadata_error "$duplicate_root" duplicate

unknown_field_root="$tmpdir/unknown-field"
mkdir -p "$unknown_field_root/sql-tests/unknown-field/sql"
printf '%s\n' 'server_mode = "native"' >"$unknown_field_root/sql-tests/unknown-field/suite.toml"
assert_metadata_error "$unknown_field_root" unknown-field

echo "sql-suite-metadata-test: PASS"
