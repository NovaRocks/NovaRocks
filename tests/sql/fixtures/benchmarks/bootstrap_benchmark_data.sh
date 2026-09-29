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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
WORKSPACE_ROOT="$(cd "${NOVAROCKS_WORKSPACE_ROOT:-$SOURCE_ROOT}" && pwd)"
# Normal runs replace this discovery hint with the publication returned by
# the current bind. Dry-run never requires a ready entry.
ENV_FILE="${NOVA_ENV_REST_ENV_FILE:-$WORKSPACE_ROOT/docker/iceberg-rest/runtime/current/env.sh}"
# shellcheck source=benchmark_fixture_publication.sh
source "$SCRIPT_DIR/benchmark_fixture_publication.sh"

SSB_VERSION="d006a6c49ff1a145a7d4ac7d837427627b213091"
SSB_ARCHIVE_URL="https://github.com/greenlion/ssb-dbgen/archive/d006a6c49ff1a145a7d4ac7d837427627b213091.zip"
SSB_ARCHIVE_SHA256="fe38fc04bfffec954dd9a5264be295768edc2227fbafc2cb58fa7ca3ad459f3d"
SSB_ARCHIVE_ROOT="ssb-dbgen-d006a6c49ff1a145a7d4ac7d837427627b213091"
SSB_ARCHIVE_FILE="ssb-dbgen-$SSB_VERSION.zip"
SSB_TABLES=(customer dates lineorder part supplier)

TPCH_VERSION="6985da461c641fd0d255b214f2d693f1bf08bc33"
TPCH_ARCHIVE_URL="https://codeload.github.com/databricks/tpch-dbgen/tar.gz/$TPCH_VERSION"
TPCH_ARCHIVE_SHA256="0357de7004ad47ede32e2ace83f7a468bbd8bedb7dcfc7e317751efe2b399f1a"
TPCH_ARCHIVE_ROOT="tpch-dbgen-$TPCH_VERSION"
TPCH_ARCHIVE_FILE="tpch-dbgen-$TPCH_VERSION.tar.gz"
TPCH_TABLES=(customer lineitem nation orders part partsupp region supplier)

TPCDS_VERSION="1b7fb7529edae091684201fab142d956d6afd881"
TPCDS_ARCHIVE_URL="https://codeload.github.com/databricks/tpcds-kit/tar.gz/$TPCDS_VERSION"
TPCDS_ARCHIVE_SHA256="c67d62cfdab1571a7625aaab29771e123cf6be3f9dd615606d822bf7e1bb4221"
TPCDS_ARCHIVE_ROOT="tpcds-kit-$TPCDS_VERSION"
TPCDS_ARCHIVE_FILE="tpcds-kit-$TPCDS_VERSION.tar.gz"
TPCDS_TABLES=(
  call_center catalog_page catalog_returns catalog_sales customer
  customer_address customer_demographics date_dim household_demographics
  income_band inventory item promotion reason ship_mode store store_returns
  store_sales time_dim warehouse web_page web_returns web_sales web_site
)

suite=""
scale=""
resolved_dataset_file=""
check_only=0
rebuild=0
ensure=0
dry_run=0

usage() {
  cat <<'EOF'
Usage: bootstrap_benchmark_data.sh --suite <ssb|tpc-h|tpc-ds> --scale <scale> [options]

Options:
  --suite <name>             Benchmark suite: ssb, tpc-h, or tpc-ds.
  --scale <scale>            Standard scale. Defaults: ssb=1, tpc-h=1, tpc-ds=1GB.
  --resolved-dataset <file>  T01 resolver JSON for this exact dataset (required).
  --check                    Check READY and emit a typed result/error.
  --ensure                   Reuse READY or build only when it is absent (default).
  --rebuild                  Rebuild even if readiness check succeeds.
  --dry-run                  Print resolved paths without generating or uploading.
  --help                     Show this help.
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

log() {
  echo "$*"
}

require_value() {
  local option="$1"
  local value="${2:-}"
  [[ -n "$value" && "$value" != --* ]] || die "$option requires a value"
}

parse_args() {
  while (($#)); do
    case "$1" in
      --suite)
        require_value "$1" "${2:-}"
        suite="${2:-}"
        shift 2
        ;;
      --scale)
        require_value "$1" "${2:-}"
        scale="${2:-}"
        shift 2
        ;;
      --resolved-dataset)
        require_value "$1" "${2:-}"
        resolved_dataset_file="${2:-}"
        shift 2
        ;;
      --check)
        check_only=1
        shift
        ;;
      --rebuild)
        rebuild=1
        shift
        ;;
      --ensure)
        ensure=1
        shift
        ;;
      --dry-run)
        dry_run=1
        shift
        ;;
      --help)
        usage
        exit 0
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
  done
}

scale_to_generator_value() {
  local raw="$1"
  local lowered
  lowered="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  if [[ "$suite" == "tpc-ds" ]]; then
    lowered="${lowered%gb}"
    lowered="${lowered%g}"
  fi
  printf '%s' "$lowered"
}

scale_to_slug() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9._-]/_/g'
}

validate_suite_and_scale() {
  : "${suite:=ssb}"
  case "$suite" in
    ssb)
      : "${scale:=1}"
      ;;
    tpc-h)
      : "${scale:=1}"
      ;;
    tpc-ds)
      : "${scale:=1GB}"
      ;;
    *)
      die "unsupported --suite: $suite"
      ;;
  esac
  [[ -n "$resolved_dataset_file" ]] || die "--resolved-dataset is required"
  [[ -f "$resolved_dataset_file" ]] || die "resolved dataset is missing: $resolved_dataset_file"
  (( check_only + rebuild + ensure <= 1 )) || die "choose at most one of --check, --ensure, or --rebuild"

  generator_scale="$(scale_to_generator_value "$scale")"
  [[ "$generator_scale" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "invalid scale for $suite: $scale"
  scale_slug="$(scale_to_slug "$scale")"
}

configure_suite() {
  case "$suite" in
    ssb)
      suite_database="ssb"
      generator_name="ssb-dbgen"
      generator_version="$SSB_VERSION"
      archive_url="$SSB_ARCHIVE_URL"
      archive_sha256="$SSB_ARCHIVE_SHA256"
      archive_root="$SSB_ARCHIVE_ROOT"
      archive_basename="$SSB_ARCHIVE_FILE"
      build_kind="ssb"
      suite_tables=("${SSB_TABLES[@]}")
      ;;
    tpc-h)
      suite_database="tpch"
      generator_name="tpch-dbgen"
      generator_version="$TPCH_VERSION"
      archive_url="$TPCH_ARCHIVE_URL"
      archive_sha256="$TPCH_ARCHIVE_SHA256"
      archive_root="$TPCH_ARCHIVE_ROOT"
      archive_basename="$TPCH_ARCHIVE_FILE"
      build_kind="tpch"
      suite_tables=("${TPCH_TABLES[@]}")
      ;;
    tpc-ds)
      suite_database="tpcds"
      generator_name="tpcds-kit"
      generator_version="$TPCDS_VERSION"
      archive_url="$TPCDS_ARCHIVE_URL"
      archive_sha256="$TPCDS_ARCHIVE_SHA256"
      archive_root="$TPCDS_ARCHIVE_ROOT"
      archive_basename="$TPCDS_ARCHIVE_FILE"
      build_kind="tpcds"
      suite_tables=("${TPCDS_TABLES[@]}")
      ;;
  esac
}

source_env() {
  [[ -f "$ENV_FILE" ]] || die "environment is not initialized: $ENV_FILE; run docker/iceberg-rest/up.sh --prepare-only"
  # shellcheck disable=SC1090
  source "$ENV_FILE"

  : "${NOVA_ENV_COMPOSE_ENV:?missing NOVA_ENV_COMPOSE_ENV in $ENV_FILE}"
  : "${NOVA_ENV_COMPOSE_PROJECT:?missing NOVA_ENV_COMPOSE_PROJECT in $ENV_FILE}"
  : "${NOVA_ENV_COMPOSE_FILE:?missing NOVA_ENV_COMPOSE_FILE in $ENV_FILE}"
  : "${NOVA_ENV_SHARED_BENCHMARK_ROOT:?missing NOVA_ENV_SHARED_BENCHMARK_ROOT in $ENV_FILE}"
  : "${AWS_S3_ENDPOINT:?missing AWS_S3_ENDPOINT in $ENV_FILE}"
  : "${AWS_S3_ACCESS_KEY_ID:?missing AWS_S3_ACCESS_KEY_ID in $ENV_FILE}"
  : "${AWS_S3_SECRET_ACCESS_KEY:?missing AWS_S3_SECRET_ACCESS_KEY in $ENV_FILE}"
}

resolve_paths() {
  cache_dir="$WORKSPACE_ROOT/tests/sql/fixtures/benchmarks/cache"
  generated_dir="$WORKSPACE_ROOT/tests/sql/fixtures/benchmarks/generated/$suite/$scale_slug"
  raw_dir="$generated_dir/raw"
  archive_file="$cache_dir/$archive_basename"
  source_dir="$cache_dir/$archive_root"
  spark_loader="$SOURCE_ROOT/tests/sql/fixtures/benchmarks/spark/write_standard_benchmark.py"

  schema_ddl_file=""
  case "$suite" in
    tpc-h)
      schema_ddl_file="$source_dir/dss.ddl"
      ;;
    tpc-ds)
      schema_ddl_file="$source_dir/tools/tpcds.sql"
      ;;
  esac
}

print_dry_run() {
  cat <<EOF
DRY_RUN suite=$suite scale=$scale generator_scale=$generator_scale
workspace=$WORKSPACE_ROOT
env_file=$ENV_FILE
database=$suite_database
raw_dir=$raw_dir
dataset_key=$dataset_key_json
ready_uri=$ready_uri
staging_parent=$staging_parent
cache_dir=$cache_dir
source_dir=$source_dir
schema_ddl_file=$schema_ddl_file
spark_loader=$spark_loader
iceberg_format_version=3
puffin_ndv=spark_compute_table_stats
producer_identity=not_checked_dry_run
EOF
}

emit_result() {
  local reused="$1" built="$2" etag="$3"
  python3 - "$reused" "$built" "$etag" "$resolved_dataset_file" "$ready_exact_warehouse" "$ready_manifest_uri" "$ready_identity" >&3 <<'PY'
import json, sys
reused, built, etag, resolved_path, warehouse, manifest, identity = sys.argv[1:]
resolved = json.load(open(resolved_path, encoding="utf-8"))
print(json.dumps({"schema_version": 1, "dataset_key": resolved["dataset_key"], "state": "ReadyValid",
  "reused": reused == "true", "built": built == "true", "exact_warehouse": warehouse,
  "manifest_uri": manifest, "publication": {"ready_uri": resolved["ready_uri"], "etag": etag, "identity": identity}}, separators=(",", ":")))
PY
}

emit_error() {
  local error="$1" message="$2"
  python3 - "$error" "$message" "$resolved_dataset_file" >&3 <<'PY'
import json, sys
resolved = json.load(open(sys.argv[3], encoding="utf-8"))
print(json.dumps({"schema_version": 1, "error": sys.argv[1], "dataset_key": resolved["dataset_key"], "message": sys.argv[2]}, separators=(",", ":")))
PY
}

load_resolved_dataset() {
  local expected actual shared_root
  shared_root="$(python3 - "$resolved_dataset_file" <<'PYROOT'
import json, sys
r=json.load(open(sys.argv[1]))
suffix='/' + r['dataset_key']['suite'] + '/' + r['dataset_key']['scale'].lower() + '/' + r['fixture_contract_id']
assert r['dataset_root'].endswith(suffix)
print(r['dataset_root'][:-len(suffix)])
PYROOT
)" || die "invalid resolved dataset root"
  expected="$(python3 "$SOURCE_ROOT/tests/sql/fixtures/benchmarks/resolve_benchmark_fixture.py" --workspace-root "$SOURCE_ROOT" --suite "$suite" --scale "$scale" --shared-root "$shared_root")" || die "unable to resolve fixture contract"
  actual="$(python3 - "$resolved_dataset_file" <<'PY'
import json, sys
print(json.dumps(json.load(open(sys.argv[1], encoding='utf-8')), sort_keys=True, separators=(',', ':')))
PY
)" || die "invalid resolved dataset JSON"
  [[ "$actual" == "$expected" ]] || die "resolved dataset does not match the T01 resolver contract"
  dataset_key_json="$(python3 - "$resolved_dataset_file" <<'PY'
import json, sys
print(json.dumps(json.load(open(sys.argv[1]))['dataset_key'], sort_keys=True, separators=(',', ':')))
PY
)"
  ready_uri="$(python3 - "$resolved_dataset_file" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))['ready_uri'])
PY
)"
  staging_parent="$(python3 - "$resolved_dataset_file" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))['staging_parent'])
PY
)"
  fixture_contract_id="$(python3 - "$resolved_dataset_file" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))['fixture_contract_id'])
PY
)"
  normalized_scale="$(python3 - "$resolved_dataset_file" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))['dataset_key']['scale'])
PY
)"
}

validate_ready_json() {
  local ready_json="$1"
  python3 - "$resolved_dataset_file" "$ready_json" <<'PY'
import json, sys
r=json.load(open(sys.argv[1], encoding='utf-8')); value=json.loads(sys.argv[2])
required={'schema_version','dataset_key','state','exact_warehouse','manifest_uri','contract','producer_fingerprint','publication'}
if set(value) < required or value['schema_version'] != 1 or value['dataset_key'] != r['dataset_key'] or value['state'] != 'ReadyValid': raise SystemExit(1)
if value['contract'] != r['contract'] or value['producer_fingerprint'] != r['producer_fingerprint']: raise SystemExit(1)
if value['publication'].get('ready_uri') != r['ready_uri'] or not value['publication'].get('identity'): raise SystemExit(1)
if not value['exact_warehouse'].startswith(r['staging_parent'] + '/') or not value['manifest_uri'].startswith(value['exact_warehouse'] + '/'): raise SystemExit(1)
print(value['exact_warehouse']); print(value['manifest_uri']); print(value['publication']['identity'])
PY
}

normalize_s3_uri() {
  case "$1" in
    s3://*) printf '%s\n' "$1" ;;
    s3a://*) printf 's3://%s\n' "${1#s3a://}" ;;
    *) return 1 ;;
  esac
}

check_manifest_objects() {
  local manifest_prefix="$1" objects manifest_object manifest_json object_uris uri
  objects="$(fixture_publication_list_prefix "${manifest_prefix%/}/")" || return 2
  grep -Fxq "${manifest_prefix%/}/_SUCCESS" <<<"$objects" || return 2
  manifest_object="$(awk -F/ '$NF ~ /^part-/ {print; exit}' <<<"$objects")"
  [[ -n "$manifest_object" ]] || return 2
  fixture_publication_get "$manifest_object" || return 2
  manifest_json="$FIXTURE_PUBLICATION_BODY"
  object_uris="$(python3 - "$resolved_dataset_file" "$manifest_json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1], encoding='utf-8'))
manifest = json.loads(sys.argv[2])
if manifest.get('dataset_key') != r['dataset_key']:
    raise SystemExit(1)
if manifest.get('fixture_contract') != r['contract']:
    raise SystemExit(1)
if manifest.get('producer_fingerprint') != r['producer_fingerprint']:
    raise SystemExit(1)
tables = manifest.get('tables')
if not isinstance(tables, list) or {row.get('name') for row in tables} != set(r['contract']['tables']):
    raise SystemExit(1)
for row in tables:
    metadata, statistics = row.get('metadata_uri'), row.get('statistics_file')
    if not isinstance(metadata, str) or not metadata.startswith(('s3://', 's3a://')):
        raise SystemExit(1)
    if not isinstance(statistics, str) or not statistics.startswith(('s3://', 's3a://')):
        raise SystemExit(1)
    print(metadata)
    print(statistics)
PY
)" || return 2
  while IFS= read -r uri; do
    [[ -n "$uri" ]] || continue
    fixture_publication_head "$(normalize_s3_uri "$uri")" || return 2
  done <<<"$object_uris"
}

check_readiness() {
  local ready_json parsed
  fixture_publication_get "$ready_uri" || return 1
  ready_json="$FIXTURE_PUBLICATION_BODY"
  ready_etag="$FIXTURE_PUBLICATION_ETAG"
  parsed="$(validate_ready_json "$ready_json")" || return 2
  ready_exact_warehouse="$(sed -n '1p' <<<"$parsed")"
  ready_manifest_uri="$(sed -n '2p' <<<"$parsed")"
  ready_identity="$(sed -n '3p' <<<"$parsed")"
  # These are direct object checks; no FE/MySQL/catalog state is created here.
  check_manifest_objects "$ready_manifest_uri" || return 2
  return 0
}

sha256_file() {
  local file="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{print $1}'
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{print $1}'
    return
  fi
  die "sha256sum or shasum is required"
}

download_and_verify_generator() {
  mkdir -p "$cache_dir"
  if [[ ! -f "$archive_file" ]]; then
    log "Downloading $generator_name $generator_version..."
    curl -fsSL "$archive_url" -o "$archive_file.tmp"
    mv "$archive_file.tmp" "$archive_file"
  fi

  local actual_sha
  actual_sha="$(sha256_file "$archive_file")"
  [[ "$actual_sha" == "$archive_sha256" ]] || die "$generator_name archive checksum mismatch: $actual_sha"
}

extract_generator_source() {
  download_and_verify_generator
  if [[ "$rebuild" == "1" ]]; then
    rm -rf "$source_dir"
  fi
  if [[ -d "$source_dir" ]]; then
    return
  fi

  case "$archive_file" in
    *.zip)
      unzip -q "$archive_file" -d "$cache_dir"
      ;;
    *.tar.gz|*.tgz)
      tar -xzf "$archive_file" -C "$cache_dir"
      ;;
    *)
      die "unsupported generator archive type: $archive_file"
      ;;
  esac
  [[ -d "$source_dir" ]] || die "generator archive did not create expected directory: $source_dir"
}

patch_generator_source() {
  if [[ "$suite" != "ssb" ]]; then
    return
  fi

  local source_file="$source_dir/bm_utils.c"
  [[ -f "$source_file" ]] || die "SSB generator source is missing: $source_file"
  log "Patching SSB generator for modern libc open(O_CREAT) checks..."
  local patched_file="$source_file.tmp"
  awk '
    {
      if ($0 ~ /^[[:space:]]*if .*S_ISFIFO\(fstats\.st_mode\)/) {
        print "    if (!retcode && S_ISFIFO(fstats.st_mode))"
        next
      }
      if ($0 ~ /open\(fullpath, .*O_CREAT\);/) {
        sub(/\|O_CREAT\);/, "|O_CREAT, 0644);")
      }
      print
    }
  ' "$source_file" > "$patched_file"
  mv "$patched_file" "$source_file"

  grep -q "if (!retcode && S_ISFIFO(fstats.st_mode))" "$source_file" \
    || die "failed to patch SSB generator FIFO guard"
  grep -q "O_CREAT, 0644" "$source_file" \
    || die "failed to patch SSB generator open mode"
}

cleanup_spark_tmp_dir() {
  local tmp_dir="$1"
  "${compose_args[@]}" exec -T spark /bin/bash -lc "rm -rf '$tmp_dir'" >/dev/null 2>&1 || true
}

tar_source_to_spark() {
  local tmp_dir="$1"
  if tar --help 2>&1 | grep -q -- '--disable-copyfile'; then
    COPYFILE_DISABLE=1 tar --disable-copyfile -C "$source_dir" -cf - .
  else
    COPYFILE_DISABLE=1 tar -C "$source_dir" -cf - .
  fi | "${compose_args[@]}" exec -T spark tar --warning=no-unknown-keyword -C "$tmp_dir/source" -xf -
}

remote_generation_command() {
  case "$build_kind" in
    ssb)
      cat <<EOF
cd '$tmp_dir/source'
make clean >/dev/null 2>&1 || true
if ! make dbgen MACHINE=LINUX >/tmp/novarocks-ssb-dbgen-build.log 2>&1; then
  cat /tmp/novarocks-ssb-dbgen-build.log >&2
  exit 1
fi
DSS_CONFIG='$tmp_dir/source' DSS_PATH='$tmp_dir/raw' ./dbgen -s '$generator_scale' -T a
EOF
      ;;
    tpch)
      cat <<EOF
cd '$tmp_dir/source'
make clean >/dev/null 2>&1 || true
if ! make dbgen >/tmp/novarocks-tpch-dbgen-build.log 2>&1; then
  cat /tmp/novarocks-tpch-dbgen-build.log >&2
  exit 1
fi
DSS_CONFIG='$tmp_dir/source' DSS_PATH='$tmp_dir/raw' ./dbgen -f -s '$generator_scale'
EOF
      ;;
    tpcds)
      cat <<EOF
cd '$tmp_dir/source/tools'
make clean >/dev/null 2>&1 || true
if ! make dsdgen OS=LINUX >/tmp/novarocks-tpcds-dsdgen-build.log 2>&1; then
  cat /tmp/novarocks-tpcds-dsdgen-build.log >&2
  exit 1
fi
./dsdgen -DIR '$tmp_dir/raw' -SCALE '$generator_scale' -FORCE Y
EOF
      ;;
    *)
      die "unknown build kind: $build_kind"
      ;;
  esac
}

raw_file_for_table() {
  local table="$1"
  case "$suite" in
    ssb)
      if [[ "$table" == "dates" ]]; then
        printf 'date.tbl'
      else
        printf '%s.tbl' "$table"
      fi
      ;;
    tpc-h)
      printf '%s.tbl' "$table"
      ;;
    tpc-ds)
      printf '%s.dat' "$table"
      ;;
  esac
}

verify_raw_files() {
  local table
  local raw_file
  for table in "${suite_tables[@]}"; do
    raw_file="$(raw_file_for_table "$table")"
    [[ -s "$raw_dir/$raw_file" ]] || die "missing generated raw file: $raw_dir/$raw_file"
  done
}

generate_raw_files() {
  local tmp_dir="/tmp/novarocks-$suite-generator-${NOVA_ENV_ID:-env}-$$"
  rm -rf "$raw_dir"
  mkdir -p "$raw_dir"
  log "Generating $suite raw files with $generator_name..."

  "${compose_args[@]}" exec -T spark /bin/bash -lc "rm -rf '$tmp_dir' && mkdir -p '$tmp_dir/source' '$tmp_dir/raw'"
  if ! tar_source_to_spark "$tmp_dir"; then
    cleanup_spark_tmp_dir "$tmp_dir"
    return 1
  fi

  local generation_command
  generation_command="$(remote_generation_command)"
  if ! "${compose_args[@]}" exec -T spark /bin/bash -lc "
    set -euo pipefail
    $generation_command
  "; then
    cleanup_spark_tmp_dir "$tmp_dir"
    return 1
  fi
  if ! "${compose_args[@]}" exec -T spark tar -C "$tmp_dir/raw" -cf - . | tar -C "$raw_dir" -xf -; then
    cleanup_spark_tmp_dir "$tmp_dir"
    return 1
  fi
  cleanup_spark_tmp_dir "$tmp_dir"
  verify_raw_files
}

s3_to_mc_path() {
  local uri="$1"
  [[ "$uri" == s3://* ]] || die "expected s3 URI, got: $uri"
  printf 'minio/%s' "${uri#s3://}"
}

upload_raw_files() {
  local target_path
  target_path="$(s3_to_mc_path "$raw_uri")"
  log "Uploading raw files to $raw_uri..."
  "${compose_args[@]}" run --rm -T \
    -e "MINIO_ROOT_USER=$AWS_S3_ACCESS_KEY_ID" \
    -e "MINIO_ROOT_PASSWORD=$AWS_S3_SECRET_ACCESS_KEY" \
    --volume "$raw_dir:/benchmark-raw:ro" \
    --entrypoint /bin/sh mc -c "
    set -eu
    /usr/bin/mc alias set minio http://minio:9000 \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" >/dev/null
    # raw_uri is this writer's unique staging prefix, never a shared dataset key.
    /usr/bin/mc rm --recursive --force '$target_path' >/dev/null 2>&1 || true
    /usr/bin/mc cp --recursive /benchmark-raw/ '$target_path/'
  "
}

run_spark_loader() {
  log "Writing $suite data to Iceberg with Spark..."
  local loader_tmp_dir="/tmp/novarocks-benchmark-bootstrap-${NOVA_ENV_ID:-env}-$$"
  local fixture_contract_in_spark="$loader_tmp_dir/fixture-contract.json"
  local schema_arg=""
  local spark_master="${NOVAROCKS_BENCHMARK_SPARK_MASTER:-local[2]}"
  local spark_memory="${NOVAROCKS_BENCHMARK_SPARK_MEMORY:-2g}"
  local spark_shuffle_partitions="${NOVAROCKS_BENCHMARK_SPARK_SHUFFLE_PARTITIONS:-8}"
  local spark_default_parallelism="${NOVAROCKS_BENCHMARK_SPARK_DEFAULT_PARALLELISM:-8}"
  "${compose_args[@]}" exec -T spark /bin/bash -lc "rm -rf '$loader_tmp_dir' && mkdir -p '$loader_tmp_dir'"
  "${compose_args[@]}" cp "$spark_loader" "spark:$loader_tmp_dir/write_standard_benchmark.py"
  "${compose_args[@]}" cp "$fixture_contract_file" "spark:$fixture_contract_in_spark"
  if [[ -n "$schema_ddl_file" ]]; then
    [[ -f "$schema_ddl_file" ]] || die "schema DDL is missing: $schema_ddl_file"
    "${compose_args[@]}" cp "$schema_ddl_file" "spark:$loader_tmp_dir/schema.sql"
    schema_arg="--schema-ddl '$loader_tmp_dir/schema.sql'"
  fi
  "${compose_args[@]}" exec -T spark /bin/bash -lc "
    set -euo pipefail
    trap 'rm -rf $loader_tmp_dir' EXIT
    spark_submit_bin=\"\${SPARK_SUBMIT_BIN:-}\"
    if [[ -z \"\$spark_submit_bin\" ]]; then
      spark_submit_bin=\"\$(command -v spark-submit || true)\"
    fi
    if [[ -z \"\$spark_submit_bin\" && -x /opt/spark/bin/spark-submit ]]; then
      spark_submit_bin=/opt/spark/bin/spark-submit
    fi
    if [[ -z \"\$spark_submit_bin\" ]]; then
      echo 'spark-submit binary not found' >&2
      exit 127
    fi
    \"\$spark_submit_bin\" \
      --master '$spark_master' \
      --driver-memory '$spark_memory' \
      --conf 'spark.executor.memory=$spark_memory' \
      --conf 'spark.sql.shuffle.partitions=$spark_shuffle_partitions' \
      --conf 'spark.default.parallelism=$spark_default_parallelism' \
      '$loader_tmp_dir/write_standard_benchmark.py' \
      --suite '$suite' \
      --scale '$normalized_scale' \
      --raw-base-uri '$raw_uri' \
      --catalog '$spark_catalog' \
      --database '$suite_database' \
      --warehouse '$exact_warehouse' \
      --manifest-output '$manifest_uri' \
      --s3-endpoint '${NOVAROCKS_SPARK_S3_ENDPOINT:-http://minio:9000}' \
      --s3-access-key '$AWS_S3_ACCESS_KEY_ID' \
      --s3-secret-key '$AWS_S3_SECRET_ACCESS_KEY' \
      --generator '$generator_name' \
      --generator-version '$generator_version' \
      --fixture-contract-json '$fixture_contract_in_spark' \
      $schema_arg
  "
}

ensure_docker_services() {
  local binding publication spark_container actual_image
  if [[ -n "${BENCHMARK_FIXTURE_UP_COMMAND:-}" ]]; then
    binding="$("$BENCHMARK_FIXTURE_UP_COMMAND")" || return 1
  else
    binding="$("$SOURCE_ROOT/docker/iceberg-rest/up.sh")" || return 1
  fi
  publication="$(python3 - "$binding" <<'PYBIND'
import json, pathlib, sys
path=pathlib.Path(json.loads(sys.argv[1])["published_dir"])
assert path.is_absolute() and path.is_dir()
print(path.resolve())
PYBIND
)" || return 1
  ENV_FILE="$publication/env.sh"
  source_env
  compose_args=(python3 "$SOURCE_ROOT/docker/iceberg-rest/runtime_entry.py" compose
    --env-file "$NOVA_ENV_COMPOSE_ENV" -p "$NOVA_ENV_COMPOSE_PROJECT" -f "$NOVA_ENV_COMPOSE_FILE")
  spark_container="$("${compose_args[@]}" ps -q spark)" || return 1
  [[ -n "$spark_container" && "$spark_container" != *$'\n'* ]] || return 1
  actual_image="$(docker inspect "$spark_container" --format '{{.Image}}')" || return 1
  python3 "$SOURCE_ROOT/tests/sql/fixtures/benchmarks/resolve_benchmark_fixture.py" \
    --workspace-root "$SOURCE_ROOT" --validate-producer "$publication" \
    --resolved-dataset "$resolved_dataset_file" --actual-container "$spark_container" --actual-image "$actual_image"
}

new_identity() {
  local random
  random="$(openssl rand -hex 12 2>/dev/null || printf '%s' "$RANDOM$RANDOM$(date +%s)")"
  printf '%s-%s' "$(date +%s)" "$random"
}

run_with_timeout() {
  # Each writer has a unique staging prefix. The conditional READY publication
  # below is the only shared mutation and decides the immutable winner.
  local started deadline child_status
  deadline="${NOVA_ENV_BENCHMARK_BUILD_TIMEOUT_SECONDS:-7200}"
  [[ "$deadline" =~ ^[1-9][0-9]*$ ]] || return 64
  "$@" &
  owner_child_pid="$!"
  started="$SECONDS"
  while kill -0 "$owner_child_pid" 2>/dev/null; do
    sleep 1
    if (( SECONDS - started > deadline )); then
      stop_owner_child
      return 124
    fi
  done
  child_status=0
  wait "$owner_child_pid" || child_status="$?"
  owner_child_pid=""
  return "$child_status"
}

collect_owner_process_tree() {
  local pid="$1" child
  while IFS= read -r child; do
    [[ -n "$child" ]] || continue
    collect_owner_process_tree "$child"
  done < <(pgrep -P "$pid" 2>/dev/null || true)
  printf '%s\n' "$pid"
}

stop_owner_child() {
  local deadline pid
  local -a owner_processes=()
  [[ -n "${owner_child_pid:-}" ]] || return 0
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && owner_processes+=("$pid")
  done < <(collect_owner_process_tree "$owner_child_pid")
  kill -TERM "${owner_processes[@]}" 2>/dev/null || true
  deadline=$((SECONDS + 5))
  while (( SECONDS < deadline )); do
    for pid in "${owner_processes[@]}"; do
      kill -0 "$pid" 2>/dev/null && { sleep 1; continue 2; }
    done
    break
  done
  for pid in "${owner_processes[@]}"; do
    kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null || true
  done
  wait "$owner_child_pid" 2>/dev/null || true
  owner_child_pid=""
}

cleanup_owner() {
  stop_owner_child
  [[ -z "${fixture_contract_file:-}" ]] || rm -f "$fixture_contract_file"
  [[ -z "${candidate:-}" ]] || rm -f "$candidate"
}

handle_owner_signal() {
  cleanup_owner
  exit "$1"
}

publish_ready() {
  local candidate="$1" mode="$2" observed_etag="${3:-}" status
  status="$(fixture_publication_put_conditional "$ready_uri" "$candidate" "$mode" "$observed_etag")" || return 4
  case "$status" in
    200|201) return 0 ;;
    409|412)
      # A conditional loser can only reuse a fully valid exact winner.
      if check_readiness; then return 10; fi
      return 5
      ;;
    *) return 4 ;;
  esac
}

write_ready_candidate() {
  local candidate="$1"
  python3 - "$resolved_dataset_file" "$exact_warehouse" "$manifest_uri" "$staging_identity" > "$candidate" <<'PY'
import json, sys
r=json.load(open(sys.argv[1], encoding='utf-8'))
warehouse, manifest, staging = sys.argv[2:]
value={"schema_version": 1, "dataset_key": r["dataset_key"], "state": "ReadyValid", "exact_warehouse": warehouse,
       "manifest_uri": manifest, "contract": r["contract"], "producer_fingerprint": r["producer_fingerprint"],
       "publication": {"ready_uri": r["ready_uri"], "identity": staging}}
print(json.dumps(value, sort_keys=True, separators=(',', ':')))
PY
}

main() {
  # The runner treats stdout as a one-object protocol.  All command output,
  # including Docker/Spark diagnostics, belongs on stderr.
  exec 3>&1
  exec 1>&2
  local candidate=""
  parse_args "$@"
  validate_suite_and_scale
  configure_suite
  load_resolved_dataset
  resolve_paths

  if [[ "$dry_run" == "1" ]]; then
    print_dry_run
    exit 0
  fi

  if ! ensure_docker_services; then
    emit_error writer_failed "ProducerIdentityMismatch: unable to bind and verify the current Spark producer"
    exit 1
  fi
  ready_etag=""
  if check_readiness; then
    if [[ "$rebuild" != 1 ]]; then emit_result true false "$ready_etag"; exit 0; fi
    observed_ready_etag="$ready_etag"
  else
    ready_status="$?"
    if [[ "$check_only" == 1 ]]; then emit_error ready_invalid "READY is absent or invalid"; exit 1; fi
    if [[ "$ready_status" == 2 && "$rebuild" != 1 ]]; then emit_error ready_invalid "READY exists but fails the exact fixture contract"; exit 1; fi
    # A ReadyInvalid object still has an observed ETag.  Explicit rebuild is
    # permitted only as If-Match against precisely that object; absence has no
    # ETag and therefore remains an If-None-Match publication.
    observed_ready_etag="${ready_etag:-}"
  fi

  staging_identity="$(new_identity)"
  fixture_contract_file="$(mktemp)"; chmod 600 "$fixture_contract_file"; cp "$resolved_dataset_file" "$fixture_contract_file"
  trap cleanup_owner EXIT
  trap 'handle_owner_signal 130' INT
  trap 'handle_owner_signal 143' TERM
  # A concurrent absent writer can finish while this process allocates its
  # private staging prefix. Reuse the exact READY instead of coordinating via
  # a Docker resource.
  if check_readiness; then
    if [[ "$rebuild" != 1 ]]; then emit_result true false "$ready_etag"; exit 0; fi
  else
    holder_ready_status="$?"
    if [[ "$holder_ready_status" == 2 && "$rebuild" != 1 ]]; then
      emit_error ready_invalid "READY exists but fails the exact fixture contract"; exit 1
    fi
  fi

  exact_warehouse="$staging_parent/$staging_identity/warehouse"
  raw_uri="$staging_parent/$staging_identity/raw"
  manifest_uri="$exact_warehouse/manifest"
  spark_catalog="fixture_${fixture_contract_id:0:12}_${staging_identity//[^a-zA-Z0-9]/_}"
  fixture_publication_curl_capable || [[ -n "${BENCHMARK_FIXTURE_STORAGE_DIR:-}" ]] || { emit_error publication_failed "curl lacks --aws-sigv4"; exit 1; }
  extract_generator_source || { emit_error writer_failed "generator setup failed"; exit 1; }
  patch_generator_source || { emit_error writer_failed "generator patch failed"; exit 1; }
  run_with_timeout generate_raw_files || { emit_error writer_failed "raw generation failed"; exit 1; }
  run_with_timeout upload_raw_files || { emit_error writer_failed "raw upload failed"; exit 1; }
  run_with_timeout run_spark_loader || { emit_error writer_failed "Spark loader failed"; exit 1; }
  fixture_publication_head "$manifest_uri/_SUCCESS" || { emit_error ready_invalid "candidate manifest is incomplete"; exit 1; }
  candidate="$(mktemp)"; chmod 600 "$candidate"
  write_ready_candidate "$candidate"
  if publish_ready "$candidate" "$( [[ "$rebuild" == 1 ]] && echo rebuild || echo absent )" "$observed_ready_etag"; then
    check_readiness || { emit_error publication_failed "published READY failed direct validation"; exit 1; }
    emit_result false true "$ready_etag"; exit 0
  else
    publish_status="$?"
  fi
  if [[ "$publish_status" == 10 ]] && check_readiness; then emit_result true false "$ready_etag";
  elif [[ "$publish_status" == 5 ]]; then emit_error publication_conflict "conditional READY publication lost without a valid winner";
  else emit_error publication_failed "conditional READY publication failed"; fi
  exit 1
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
