#!/usr/bin/env bash
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

proto_core_suites="$(ci_proto_core_suites)"
expected_proto_core_suites="$(printf "%s\n" join filter sort aggregate cte subquery iceberg-rest runtime-filter-distributed)"
if [ "$proto_core_suites" != "$expected_proto_core_suites" ]; then
  echo "proto core suites do not match the NIDL-5 M1 matrix" >&2
  printf "expected:\n%s\nactual:\n%s\n" "$expected_proto_core_suites" "$proto_core_suites" >&2
  exit 1
fi

if ! ci_proto_enabled; then
  echo "proto core matrix should be enabled by default" >&2
  exit 1
fi

if [ "$(ci_proto_suites | tr '\n' ' ')" != "$(printf "%s " join filter sort aggregate cte subquery iceberg-rest runtime-filter-distributed)" ]; then
  echo "default proto matrix should use the core suites" >&2
  exit 1
fi

NOVA_CI_PROTO_CORE="0"
NOVA_CI_PROTO_FULL="0"
if ci_proto_enabled; then
  echo "explicit NOVA_CI_PROTO_CORE=0 should disable proto when full proto is off" >&2
  exit 1
fi

NOVA_CI_PROTO_FULL="1"
if ! ci_proto_enabled; then
  echo "NOVA_CI_PROTO_FULL=1 should enable proto even when core proto is off" >&2
  exit 1
fi
if ! ci_proto_suites | grep -qx "optimizer-dist"; then
  echo "proto full matrix should include stable full suites" >&2
  exit 1
fi

SQL_CLUSTER_MODE="all-in-one"
SQL_CLUSTER_SIZE="1"
if [ "$(ci_proto_suite_cluster_mode join)" != "cross-process" ]; then
  echo "proto suites must force cross-process cluster mode" >&2
  exit 1
fi
if [ "$(ci_proto_suite_cluster_size join)" != "3" ]; then
  echo "proto suites must force a 3-BE cluster" >&2
  exit 1
fi

proto_args="$(ci_proto_runner_extra_args join)"
if ! grep -q -- "--plan-wire-format proto" <<<"$proto_args"; then
  echo "proto runner args must include --plan-wire-format proto" >&2
  exit 1
fi

local_full_ci_text="$(cat "$REPO_ROOT/tools/ci/local-full-ci.sh")"
if ! grep -q 'run_fail_fast_stage "cargo clippy compat"' <<<"$local_full_ci_text"; then
  echo "local-full-ci must run a compat clippy stage" >&2
  exit 1
fi
if ! grep -q -- 'cargo clippy --all-targets --features compat' <<<"$local_full_ci_text"; then
  echo "compat clippy stage must pass --features compat" >&2
  exit 1
fi
if ! grep -q 'run_fail_fast_stage "cargo build compat"' <<<"$local_full_ci_text"; then
  echo "local-full-ci must run a compat build stage" >&2
  exit 1
fi
if ! grep -q -- 'cargo build --profile "$NOVA_CI_CARGO_PROFILE" --features compat' <<<"$local_full_ci_text"; then
  echo "compat build stage must pass --features compat" >&2
  exit 1
fi
if ! grep -q 'run_fail_fast_stage "cargo test compat"' <<<"$local_full_ci_text"; then
  echo "local-full-ci must run a compat test stage" >&2
  exit 1
fi
if ! grep -q -- 'cargo test --profile "$NOVA_CI_CARGO_PROFILE" --features compat' <<<"$local_full_ci_text"; then
  echo "compat test stage must pass --features compat" >&2
  exit 1
fi
