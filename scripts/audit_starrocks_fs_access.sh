#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

targets=(
  src/connector/starrocks/scan
  src/connector/starrocks/sink
  src/connector/starrocks/table/txn.rs
  src/connector/starrocks/table/erase.rs
  src/connector/starrocks/lake
  src/runtime/starlet_shard_registry.rs
)

for target in "${targets[@]}"; do
  if [[ ! -e "$target" ]]; then
    printf 'StarRocks FS access audit target is missing: %s\n' "$target" >&2
    exit 1
  fi
done

deny_pattern='classify_scan_paths|resolve_opendal_paths|resolve_object_store_operator_and_path|ScanPathScheme|oss_config_for_path'
allow_pattern='#\[cfg\(test\)\]|mod tests|src/runtime/starlet_shard_registry.rs:[0-9]+:    fn |src/runtime/starlet_shard_registry.rs:[0-9]+:        let \(_op, key\) = crate::fs::path::resolve_object_store_operator_and_path|src/runtime/starlet_shard_registry.rs:[0-9]+:pub\(crate\) fn oss_config_for_path|src/runtime/starlet_shard_registry.rs:[0-9]+:    s3_config_for_path\(path\)\.map'

if hits="$(rg -n "$deny_pattern" "${targets[@]}" 2>&1)"; then
  rg_status=0
else
  rg_status=$?
fi

if [[ "$rg_status" -eq 1 ]]; then
  exit 0
fi
if [[ "$rg_status" -gt 1 ]]; then
  printf 'StarRocks FS access audit scan failed:\n%s\n' "$hits" >&2
  exit "$rg_status"
fi

# Allow test code in the runtime registry to exercise legacy compatibility until FS-6.
# Do not allow production StarRocks lake modules to use these old helpers.
if blocked="$(printf '%s\n' "$hits" | rg -v "$allow_pattern" 2>&1)"; then
  filter_status=0
else
  filter_status=$?
fi
if [[ "$filter_status" -eq 1 ]]; then
  exit 0
fi
if [[ "$filter_status" -gt 1 ]]; then
  printf 'StarRocks FS access audit allow-list filter failed:\n%s\n' "$blocked" >&2
  exit "$filter_status"
fi
if [[ -n "$blocked" ]]; then
  printf 'StarRocks FS access boundary violations:\n%s\n' "$blocked" >&2
  exit 1
fi
