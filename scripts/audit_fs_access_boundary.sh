#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

scan_targets=(src tools/src/bin)

deny_pattern='FileIOBuilder::new|S3StorageFactory|build_oss_operator|normalize_oss_path|resolve_oss_operator_and_path_with_config|resolve_object_store_operator_and_path|classify_scan_paths|resolve_opendal_paths|opendal::services::S3::default|opendal::services::Fs::default|(^|[^[:alnum:]_:])(S3|Fs)::default[[:space:]]*\(|use[[:space:]]+opendal::services(::|::\{).*\b(S3|Fs)\b|novarocks_fs_oss|novarocks_fs_path|crate::fs::oss'

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
    src/fs/local.rs:*'opendal::services::Fs::default'* ) return 0 ;;
    src/fs/object_store.rs:*'opendal::services::S3::default'* ) return 0 ;;
    src/fs/opendal.rs:*'opendal::services::Fs::default'* ) return 0 ;;
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

collect_opendal_service_import_hits() {
  local file

  while IFS= read -r file; do
    awk -v file="$file" '
      function brace_delta(text, copy, opens, closes) {
        copy = text
        opens = gsub(/\{/, "{", copy)
        copy = text
        closes = gsub(/\}/, "}", copy)
        return opens - closes
      }

      function has_service_import(text) {
        return text ~ /(^|[^[:alnum:]_])(S3|Fs)([[:space:]]+as[[:space:]]+[[:alnum:]_]+)?([^[:alnum:]_]|$)/
      }

      function starts_services_import(text) {
        return text ~ /(^|[^[:alnum:]_])services::[[:space:]]*\{/
      }

      function emit_hit() {
        printf "%s:%d:%s\n", file, NR, $0
      }

      {
        if (in_nested_services_import) {
          if (has_service_import($0)) {
            emit_hit()
          }
          nested_services_depth += brace_delta($0)
          if (nested_services_depth <= 0) {
            in_nested_services_import = 0
          }
          if ($0 ~ /;/) {
            in_opendal_import = 0
            in_nested_services_import = 0
          }
          next
        }

        if (in_services_import) {
          if (has_service_import($0)) {
            emit_hit()
          }
          if ($0 ~ /;/) {
            in_services_import = 0
          }
          next
        }

        if (in_opendal_import) {
          if (starts_services_import($0)) {
            if (has_service_import($0)) {
              emit_hit()
            }
            nested_services_depth = brace_delta($0)
            if (nested_services_depth > 0) {
              in_nested_services_import = 1
            }
          }
          if ($0 ~ /;/) {
            in_opendal_import = 0
            in_nested_services_import = 0
          }
          next
        }

        if ($0 ~ /^[[:space:]]*use[[:space:]]+opendal::services::[[:space:]]*\{/) {
          in_services_import = 1
          if (has_service_import($0)) {
            emit_hit()
          }
          if ($0 ~ /;/) {
            in_services_import = 0
          }
          next
        }

        if ($0 ~ /^[[:space:]]*use[[:space:]]+opendal::[[:space:]]*\{/) {
          in_opendal_import = 1
          if (starts_services_import($0)) {
            if (has_service_import($0)) {
              emit_hit()
            }
            nested_services_depth = brace_delta($0)
            if (nested_services_depth > 0) {
              in_nested_services_import = 1
            }
          }
          if ($0 ~ /;/) {
            in_opendal_import = 0
            in_nested_services_import = 0
          }
          next
        }

        if ($0 ~ /^[[:space:]]*use[[:space:]]+opendal::services::[[:space:]]*(S3|Fs)([[:space:]]+as[[:space:]]+[[:alnum:]_]+)?[[:space:]]*;/) {
          emit_hit()
        }
      }
    ' "$file"
  done < <(rg --files "${scan_targets[@]}" -g '*.rs')
}

while IFS= read -r hit; do
  IFS=: read -r file line text <<<"$hit"
  if ! is_allowed_core_hit "$file" "$line" "$text"; then
    blocked+=("$hit")
  fi
done < <(collect_opendal_service_import_hits)

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
    src/runtime/starlet_shard_registry.rs:*'"aws.s3.endpoint"'* ) return 0 ;;
    src/runtime/starlet_shard_registry.rs:*'"aws.s3.accessKeyId"'* ) return 0 ;;
    src/runtime/starlet_shard_registry.rs:*'"aws.s3.accessKeySecret"'* ) return 0 ;;
    src/runtime/starlet_shard_registry.rs:*'"aws.s3.region"'* ) return 0 ;;
    src/runtime/starlet_shard_registry.rs:*'"aws.s3.enable_path_style_access"'* ) return 0 ;;
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
