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
