#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: $0 --suite <suite> --only <cases> [--output-dir <dir>]" >&2
}

suite=""
only=""
output_dir="/tmp/rfd-6g-rf-matrix"

while (($# > 0)); do
    case "$1" in
        --suite)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            suite="$2"
            shift 2
            ;;
        --only)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            only="$2"
            shift 2
            ;;
        --output-dir)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            output_dir="$2"
            shift 2
            ;;
        *)
            usage
            echo "unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

[[ -n "$suite" ]] || { usage; echo "--suite is required" >&2; exit 2; }
[[ -n "$only" ]] || { usage; echo "--only is required" >&2; exit 2; }
: "${NOVAROCKS_SQL_TEST_CONFIG:?NOVAROCKS_SQL_TEST_CONFIG must name the generated runner config}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
mkdir -p "$output_dir"

run_matrix_side() {
    local label="$1"
    local enabled="$2"
    local rewrite_runtime_filter_bindings="${3:-false}"
    local log_path="$output_dir/rf-$label.log"

    {
        printf 'final_head=%s\n' "$(git -C "$repo_root" rev-parse HEAD)"
        printf 'suite=%s\nonly=%s\n' "$suite" "$only"
        printf 'runtime_filter=%s\n' "$enabled"
        cd "$repo_root"
        matrix_args=()
        if [[ "$rewrite_runtime_filter_bindings" == "true" ]]; then
            matrix_args+=(
                --rewrite-explain-contains-as-not-contains "producer binding"
                --rewrite-explain-contains-as-not-contains "consumer binding"
                --rewrite-explain-contains-as-not-contains "expr = (t3.k)"
                --rewrite-explain-contains-as-not-contains "expr = (t1.k)"
                --rewrite-explain-contains-as-not-contains "expr = (t2.k)"
            )
        fi
        cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
            --config "$NOVAROCKS_SQL_TEST_CONFIG" \
            --suite "$suite" \
            --only "$only" \
            --mode verify \
            -j 1 \
            --cluster-mode cross-process \
            --cluster-size 3 \
            --target-session-sql "SET enable_global_runtime_filter = $enabled" \
            "${matrix_args[@]}"
    } 2>&1 | tee "$log_path"
}

run_matrix_side on true
# RF-off retains every structural EXPLAIN assertion, but rewrites the RF-on
# binding and transitive-consumer assertions so it proves they are absent.
run_matrix_side off false true
