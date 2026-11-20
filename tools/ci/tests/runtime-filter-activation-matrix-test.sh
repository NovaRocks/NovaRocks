#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

fake_bin="$tmpdir/bin"
mkdir -p "$fake_bin"

cat >"$fake_bin/cargo" <<'FAKE_CARGO'
#!/usr/bin/env bash
set -euo pipefail

count_file="$FAKE_CARGO_LOG/count"
count=0
if [[ -f "$count_file" ]]; then
    count="$(<"$count_file")"
fi
count=$((count + 1))
printf '%s\n' "$count" >"$count_file"
printf '%s\n' "$@" >"$FAKE_CARGO_LOG/args.$count"
printf 'fake topology: 1 FE + 3 BE\n'

if [[ "${FAIL_INVOCATION:-0}" == "$count" ]]; then
    exit 23
fi
FAKE_CARGO
chmod +x "$fake_bin/cargo"

assert_arg_pair() {
    local file="$1"
    local option="$2"
    local value="$3"
    awk -v option="$option" -v value="$value" '
        previous == option && $0 == value { found = 1 }
        { previous = $0 }
        END { exit(found ? 0 : 1) }
    ' "$file"
}

run_matrix() {
    local output_dir="$1"
    shift
    mkdir -p "$FAKE_CARGO_LOG"
    PATH="$fake_bin:$PATH" \
        NOVAROCKS_SQL_TEST_CONFIG="$tmpdir/sql-test.toml" \
        bash "$repo_root/tools/ci/runtime-filter-activation-matrix.sh" \
        --suite "suite-with-value" \
        --only "case_a,case_b" \
        --output-dir "$output_dir" \
        "$@"
}

export FAKE_CARGO_LOG="$tmpdir/success-log"
run_matrix "$tmpdir/success-output"

[[ "$(<"$FAKE_CARGO_LOG/count")" == "2" ]]
for invocation in 1 2; do
    args="$FAKE_CARGO_LOG/args.$invocation"
    assert_arg_pair "$args" --suite "suite-with-value"
    assert_arg_pair "$args" --only "case_a,case_b"
    assert_arg_pair "$args" --cluster-mode "cross-process"
    assert_arg_pair "$args" --cluster-size "3"
    assert_arg_pair "$args" --mode "verify"
    assert_arg_pair "$args" -j "1"
done
assert_arg_pair "$FAKE_CARGO_LOG/args.1" --target-session-sql \
    'SET SESSION enable_global_runtime_filter = true'
assert_arg_pair "$FAKE_CARGO_LOG/args.2" --target-session-sql \
    'SET SESSION enable_global_runtime_filter = false'
[[ -f "$tmpdir/success-output/rf-on.log" ]]
[[ -f "$tmpdir/success-output/rf-off.log" ]]

export FAKE_CARGO_LOG="$tmpdir/failure-log"
export FAIL_INVOCATION=2
if run_matrix "$tmpdir/failure-output"; then
    echo "matrix script unexpectedly ignored the second invocation failure" >&2
    exit 1
fi
[[ "$(<"$FAKE_CARGO_LOG/count")" == "2" ]]

echo "runtime-filter activation matrix source test PASS"
