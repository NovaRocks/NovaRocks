#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

scan_targets=(src tools/src/bin)

deny_pattern='FileIOBuilder::new|S3StorageFactory|build_oss_operator|normalize_oss_path|resolve_oss_operator_and_path_with_config|resolve_object_store_operator_and_path|classify_scan_paths|resolve_opendal_paths|opendal::services::S3::default|opendal::services::Fs::default|novarocks_fs_oss|novarocks_fs_path|crate::fs::oss'

hits=()
while IFS= read -r hit; do
  hits+=("$hit")
done < <(rg -n --no-heading "$deny_pattern" "${scan_targets[@]}" || true)

is_test_line() {
  local file="$1"
  local line="$2"

  awk -v target="$line" '
    function brace_delta(text, copy, opens, closes) {
      copy = text
      opens = gsub(/\{/, "{", copy)
      copy = text
      closes = gsub(/\}/, "}", copy)
      return opens - closes
    }

    {
      if (in_tests) {
        if (NR == target) {
          found = 1
        }
        depth += brace_delta($0)
        if (depth <= 0) {
          in_tests = 0
        }
      }

      if (!in_tests && prev_cfg_test && $0 ~ /^[[:space:]]*mod[[:space:]]+tests[[:space:]]*\{/) {
        in_tests = 1
        depth = brace_delta($0)
        if (NR == target) {
          found = 1
        }
        if (depth <= 0) {
          in_tests = 0
        }
      }

      prev_cfg_test = ($0 ~ /^[[:space:]]*#\[cfg\(test\)\][[:space:]]*$/)
    }

    END {
      exit found ? 0 : 1
    }
  ' "$file"
}

is_allowed_core_hit() {
  local file="$1"
  local line="$2"
  local text="$3"

  if is_test_line "$file" "$line"; then
    return 0
  fi
  case "$file:$text" in
    src/connector/iceberg/fs_io.rs:*FileIOBuilder::new* ) return 0 ;;
    src/fs/object_store.rs:*'fn build_object_store_operator'* ) return 0 ;;
    src/fs/access.rs:*'crate::fs::object_store::build_object_store_operator'* ) return 0 ;;
  esac
  return 1
}

blocked=()
for hit in "${hits[@]}"; do
  IFS=: read -r file line text <<<"$hit"
  if ! is_allowed_core_hit "$file" "$line" "$text"; then
    blocked+=("$hit")
  fi
done

aws_hits=()
while IFS= read -r hit; do
  aws_hits+=("$hit")
done < <(rg -n --no-heading 'aws\.s3\.' "${scan_targets[@]}" || true)

is_allowed_aws_hit() {
  local file="$1"
  local line="$2"
  local text="$3"

  if is_test_line "$file" "$line"; then
    return 0
  fi
  case "$file" in
    src/fs/object_store_credentials.rs) return 0 ;;
    src/fs/object_store.rs) return 0 ;;
    src/connector/starrocks/object_store_profile.rs) return 0 ;;
    src/connector/iceberg/fs_io.rs) return 0 ;;
    src/connector/iceberg/catalog/registry.rs) return 0 ;;
    src/connector/iceberg/stats.rs) return 0 ;;
    src/connector/iceberg/sink_plan.rs) return 0 ;;
  esac
  case "$file:$text" in
    src/connector/starrocks/scan/op.rs:*'provide aws.s3.*'* ) return 0 ;;
  esac
  return 1
}

for hit in "${aws_hits[@]}"; do
  IFS=: read -r file line text <<<"$hit"
  if ! is_allowed_aws_hit "$file" "$line" "$text"; then
    blocked+=("$hit")
  fi
done

if [[ ${#blocked[@]} -ne 0 ]]; then
  printf 'FS access boundary violations:\n' >&2
  printf '%s\n' "${blocked[@]}" >&2
  exit 1
fi
