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

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

source "$REPO_ROOT/tools/ci/local-full-ci.sh" --source-only

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

baseline="$tmpdir/known-failures.toml"
run_dir="$tmpdir/run"
mkdir -p "$run_dir/sql"

cat >"$baseline" <<'EOF'
[[failure]]
tier = "full"
suite = "tpc-ds"
case = "q93"
error_code = "QueryTimeout"
reason = "synthetic timeout"

[[failure]]
tier = "full"
suite = "tpc-ds"
case = "q94"
error_code = "CommitUnknown"
reason = "synthetic commit unknown"
EOF

cat >"$run_dir/sql/tpc-ds.log" <<'EOF'
[sql-tests] suite=tpc-ds mode=verify
  [tpc-ds] q94 (steps=1)
    engine_error_code=CommitUnknown target execute failed: ERROR 1105 (HY000): [CommitUnknown] commit outcome unavailable
case timings (all):
  [tpc-ds] q93 PASS 0.01s
  [tpc-ds] q94 FAIL 0.01s
FAIL: total=2 pass=1 fail=1
EOF

CI_TIER="full"
KNOWN_FAILURES_FILE="$baseline"
CI_KNOWN_FAILURE_ROWS=""
CI_FAILURE_TAIL=""

if ci_classify_unexpected_passes "tpc-ds" "$run_dir/sql/tpc-ds.log"; then
  echo "expected mixed pass/fail known-failure log to report an unexpected pass" >&2
  exit 1
fi

grep -q "UNEXPECTED_PASS" <<<"$CI_KNOWN_FAILURE_ROWS"

if (
  CI_FROM_RUN_DIR="$run_dir"
  CI_TIER="full"
  KNOWN_FAILURES_FILE="$baseline"
  reclassify_existing_run >/dev/null
); then
  echo "expected --from reclassification to fail on mixed unexpected pass" >&2
  exit 1
fi

grep -q "UNEXPECTED_PASS" "$run_dir/summary.md"

targeted_suites="$(ci_tier_suites targeted "$REPO_ROOT/tools/ci/suites/stable-sql-suites.txt")"
grep -qx "optimizer-dist" <<<"$targeted_suites"

resolve_root="$tmpdir/resolve-malformed"
mkdir -p \
  "$resolve_root/sql-tests/a-valid/sql" \
  "$resolve_root/sql-tests/z-malformed/sql"
printf '%s\n' 'explicit_only = false' \
  >"$resolve_root/sql-tests/a-valid/suite.toml"
printf '%s\n' 'explicit_only = "false"' \
  >"$resolve_root/sql-tests/z-malformed/suite.toml"
resolve_status=0
(
  REPO_ROOT="$resolve_root"
  RUN_MODE="all-discovered"
  resolve_suites
) 2>"$resolve_root/resolve.err" || resolve_status=$?
if [ "$resolve_status" -eq 0 ]; then
  echo "all-discovered resolution must propagate malformed metadata failure" >&2
  exit 1
fi
if [ "$resolve_status" -ne 2 ]; then
  echo "all-discovered resolution returned $resolve_status instead of discovery status 2" >&2
  exit 1
fi

if [ "$SQL_CLUSTER_MODE" != "cross-process" ]; then
  echo "default SQL cluster mode must be cross-process" >&2
  exit 1
fi
if [ "$SQL_CLUSTER_SIZE" != "3" ]; then
  echo "default SQL cluster size must be 3 BEs" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_mode optimizer)" != "cross-process" ]; then
  echo "ordinary suites must default to cross-process mode" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_size optimizer)" != "3" ]; then
  echo "ordinary suites must default to a 3-BE cluster" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_mode distributed-resilience)" != "cross-process" ]; then
  echo "distributed-resilience must run in cross-process mode" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_size distributed-resilience)" != "3" ]; then
  echo "distributed-resilience must run with 3 BEs" >&2
  exit 1
fi
if ! grep -qx 'distributed-resilience' "$REPO_ROOT/tools/ci/suites/stable-sql-suites.txt"; then
  echo "distributed-resilience must be part of the stable SQL suite set" >&2
  exit 1
fi
if ci_native_cross_process_enabled; then
  echo "the duplicate native cross-process matrix must be disabled by default" >&2
  exit 1
fi

if ! (
  unset SQL_CLUSTER_MODE SQL_CLUSTER_SIZE
  source "$REPO_ROOT/tools/ci/local-full-ci.sh" --source-only
  parse_args --cluster-mode all-in-one
  [ "$SQL_CLUSTER_MODE" = "all-in-one" ] && [ "$SQL_CLUSTER_SIZE" = "1" ]
); then
  echo "explicit all-in-one mode without a size must infer cluster size 1" >&2
  exit 1
fi

SQL_CLUSTER_MODE="all-in-one"
SQL_CLUSTER_SIZE="1"
if [ "$(ci_suite_cluster_mode optimizer-dist)" != "cross-process" ]; then
  echo "optimizer-dist must force cross-process cluster mode" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_size optimizer-dist)" != "3" ]; then
  echo "optimizer-dist must force a 3-BE cluster" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_mode optimizer)" != "all-in-one" ]; then
  echo "ordinary suites should keep the global cluster mode" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_size optimizer)" != "1" ]; then
  echo "ordinary suites should keep the global cluster size" >&2
  exit 1
fi

native_cross_process_core_suites="$(ci_native_cross_process_core_suites)"
expected_native_cross_process_core_suites="$(printf "%s\n" join filter sort aggregate cte subquery iceberg-rest runtime-filter-distributed)"
if [ "$native_cross_process_core_suites" != "$expected_native_cross_process_core_suites" ]; then
  echo "native cross-process core suites do not match the required matrix" >&2
  printf "expected:\n%s\nactual:\n%s\n" "$expected_native_cross_process_core_suites" "$native_cross_process_core_suites" >&2
  exit 1
fi

NOVA_CI_NATIVE_CROSS_PROCESS_CORE="1"
if ! ci_native_cross_process_enabled; then
  echo "explicit NOVA_CI_NATIVE_CROSS_PROCESS_CORE=1 should enable the native cross-process matrix" >&2
  exit 1
fi

if [ "$(ci_native_cross_process_suites | tr '\n' ' ')" != "$(printf "%s " join filter sort aggregate cte subquery iceberg-rest runtime-filter-distributed)" ]; then
  echo "explicit native cross-process core matrix should use the core suites" >&2
  exit 1
fi

NOVA_CI_NATIVE_CROSS_PROCESS_CORE="0"
NOVA_CI_NATIVE_CROSS_PROCESS_FULL="0"
if ci_native_cross_process_enabled; then
  echo "explicit NOVA_CI_NATIVE_CROSS_PROCESS_CORE=0 should disable the native cross-process matrix when full coverage is off" >&2
  exit 1
fi

NOVA_CI_NATIVE_CROSS_PROCESS_FULL="1"
if ! ci_native_cross_process_enabled; then
  echo "NOVA_CI_NATIVE_CROSS_PROCESS_FULL=1 should enable the matrix even when core coverage is off" >&2
  exit 1
fi
if ! ci_native_cross_process_suites | grep -qx "optimizer-dist"; then
  echo "native cross-process full matrix should include stable full suites" >&2
  exit 1
fi

SQL_CLUSTER_MODE="all-in-one"
SQL_CLUSTER_SIZE="1"
if [ "$(ci_native_cross_process_suite_cluster_mode join)" != "cross-process" ]; then
  echo "native cross-process suites must force cross-process cluster mode" >&2
  exit 1
fi
if [ "$(ci_native_cross_process_suite_cluster_size join)" != "3" ]; then
  echo "native cross-process suites must force a 3-BE cluster" >&2
  exit 1
fi

echo "local-full-ci-classification-test: PASS"
