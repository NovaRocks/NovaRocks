#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${NOVAROCKS_WORKSPACE_ROOT:-$SCRIPT_DIR/../..}" && pwd)"
ENV_FILE="$WORKSPACE_ROOT/docker/iceberg-rest/runtime/current/env.sh"

SSB_VERSION="219403ad7d1dd32ae1f97b5553abf92129fccd7f"
SSB_ARCHIVE_URL="https://github.com/electrum/ssb-dbgen/archive/219403ad7d1dd32ae1f97b5553abf92129fccd7f.zip"
SSB_ARCHIVE_SHA256="2653bb57c165bbf9b41ea37d1da6cd2f81c0505a6ed20839111985f53dec2ee6"
SSB_ARCHIVE_ROOT="ssb-dbgen-219403ad7d1dd32ae1f97b5553abf92129fccd7f"
SSB_BINARY="dbgen"
SSB_TABLES=(customer dates lineorder part supplier)

suite=""
scale=""
target_catalog="iceberg_cat"
mysql_host="127.0.0.1"
mysql_port=""
mysql_user="root"
mysql_password=""
check_only=0
rebuild=0
dry_run=0

usage() {
  cat <<'EOF'
Usage: bootstrap_benchmark_data.sh --suite ssb --scale 1 [options]

Options:
  --suite <ssb>              Benchmark suite. First phase supports only ssb.
  --scale <1>                Scale factor. First phase supports only 1.
  --target-catalog <name>    Target Iceberg catalog name. Default: iceberg_cat.
  --mysql-host <host>        NovaRocks MySQL host. Default: 127.0.0.1.
  --mysql-port <port>        NovaRocks MySQL port. Default: env NOVA_ENV_MYSQL_PORT.
  --mysql-user <user>        NovaRocks MySQL user. Default: root.
  --mysql-password <pass>    NovaRocks MySQL password. Default: empty.
  --check                    Check existing bootstrap readiness and exit.
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

quote_ident() {
  local ident="$1"
  printf '`%s`' "${ident//\`/\`\`}"
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
      --target-catalog)
        require_value "$1" "${2:-}"
        target_catalog="${2:-}"
        shift 2
        ;;
      --mysql-host)
        require_value "$1" "${2:-}"
        mysql_host="${2:-}"
        shift 2
        ;;
      --mysql-port)
        require_value "$1" "${2:-}"
        mysql_port="${2:-}"
        shift 2
        ;;
      --mysql-user)
        require_value "$1" "${2:-}"
        mysql_user="${2:-}"
        shift 2
        ;;
      --mysql-password)
        require_value "$1" "${2:-}"
        mysql_password="${2:-}"
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

validate_first_phase() {
  [[ "$suite" == "ssb" ]] || die "first phase supports only --suite ssb"
  [[ "$scale" == "1" ]] || die "first phase supports only --scale 1"
  [[ -n "$target_catalog" ]] || die "--target-catalog must not be empty"
}

source_env() {
  [[ -f "$ENV_FILE" ]] || die "environment is not initialized: $ENV_FILE; run docker/iceberg-rest/up.sh --prepare-only"
  # shellcheck disable=SC1090
  source "$ENV_FILE"

  mysql_port="${mysql_port:-${NOVA_ENV_MYSQL_PORT:-}}"
  [[ -n "$mysql_port" ]] || die "--mysql-port is required when NOVA_ENV_MYSQL_PORT is unset"
  : "${NOVA_ENV_COMPOSE_ENV:?missing NOVA_ENV_COMPOSE_ENV in $ENV_FILE}"
  : "${NOVA_ENV_COMPOSE_PROJECT:?missing NOVA_ENV_COMPOSE_PROJECT in $ENV_FILE}"
  : "${NOVA_ENV_COMPOSE_FILE:?missing NOVA_ENV_COMPOSE_FILE in $ENV_FILE}"
  : "${CATALOG_WAREHOUSE_URI:?missing CATALOG_WAREHOUSE_URI in $ENV_FILE}"
  : "${AWS_S3_ENDPOINT:?missing AWS_S3_ENDPOINT in $ENV_FILE}"
  : "${AWS_S3_ACCESS_KEY_ID:?missing AWS_S3_ACCESS_KEY_ID in $ENV_FILE}"
  : "${AWS_S3_SECRET_ACCESS_KEY:?missing AWS_S3_SECRET_ACCESS_KEY in $ENV_FILE}"
}

resolve_paths() {
  cache_dir="$WORKSPACE_ROOT/sql-tests/bootstrap/cache"
  generated_dir="$WORKSPACE_ROOT/sql-tests/bootstrap/generated/ssb/1"
  raw_dir="$generated_dir/raw"
  lock_dir="$WORKSPACE_ROOT/sql-tests/bootstrap/.bootstrap-ssb-sf1.lock"
  archive_file="$cache_dir/ssb-dbgen-$SSB_VERSION.zip"
  source_dir="$cache_dir/$SSB_ARCHIVE_ROOT"
  dbgen_bin="$source_dir/$SSB_BINARY"
  warehouse_uri="${CATALOG_WAREHOUSE_URI%/}"
  raw_uri="$warehouse_uri/_benchmark_raw/ssb/sf1"
  manifest_uri="$warehouse_uri/_bootstrap_manifest/ssb/sf1"
  spark_loader="$WORKSPACE_ROOT/sql-tests/bootstrap/spark/write_standard_benchmark.py"
}

print_dry_run() {
  cat <<EOF
DRY_RUN suite=$suite scale=$scale
workspace=$WORKSPACE_ROOT
env_file=$ENV_FILE
target_catalog=$target_catalog
database=ssb
mysql=$mysql_user@$mysql_host:$mysql_port
warehouse=$warehouse_uri
raw_dir=$raw_dir
raw_uri=$raw_uri
manifest_uri=$manifest_uri
cache_dir=$cache_dir
dbgen_bin=$dbgen_bin
spark_loader=$spark_loader
compose_project=$NOVA_ENV_COMPOSE_PROJECT
EOF
}

mysql_client() {
  if command -v mysql >/dev/null 2>&1; then
    echo mysql
    return
  fi
  if command -v mariadb >/dev/null 2>&1; then
    echo mariadb
    return
  fi
  die "mysql or mariadb client is required"
}

run_sql() {
  local sql="$1"
  local client
  client="$(mysql_client)"
  local args=(-h "$mysql_host" -P "$mysql_port" -u "$mysql_user" --protocol=TCP --batch --raw --skip-column-names)
  if [[ -n "$mysql_password" ]]; then
    args+=("-p$mysql_password")
  fi
  "$client" "${args[@]}" -e "$sql"
}

create_target_catalog() {
  local catalog_sql
  catalog_sql=$(cat <<EOF
CREATE EXTERNAL CATALOG IF NOT EXISTS $(quote_ident "$target_catalog")
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "$warehouse_uri",
  "aws.s3.endpoint" = "$AWS_S3_ENDPOINT",
  "aws.s3.access_key" = "$AWS_S3_ACCESS_KEY_ID",
  "aws.s3.secret_key" = "$AWS_S3_SECRET_ACCESS_KEY",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
EOF
)
  run_sql "$catalog_sql"
}

check_manifest() {
  local path
  path="$(s3_to_mc_path "$manifest_uri")"
  "${compose_args[@]}" run --rm -T --entrypoint /bin/sh mc -c "
    set -eu
    /usr/bin/mc alias set minio http://minio:9000 \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" >/dev/null
    /usr/bin/mc ls '$path' >/dev/null
  "
}

check_readiness() {
  log "Checking benchmark data readiness..."
  create_target_catalog
  run_sql "USE $(quote_ident "$target_catalog").$(quote_ident ssb);"
  local table
  for table in "${SSB_TABLES[@]}"; do
    run_sql "USE $(quote_ident "$target_catalog").$(quote_ident ssb); SELECT * FROM $(quote_ident "$table") LIMIT 1;" >/dev/null
  done
  check_manifest
  log "Benchmark data is ready: suite=$suite scale=$scale catalog=$target_catalog"
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

download_and_verify_ssb() {
  mkdir -p "$cache_dir"
  if [[ ! -f "$archive_file" ]]; then
    log "Downloading ssb-dbgen $SSB_VERSION..."
    curl -fsSL "$SSB_ARCHIVE_URL" -o "$archive_file.tmp"
    mv "$archive_file.tmp" "$archive_file"
  fi

  local actual_sha
  actual_sha="$(sha256_file "$archive_file")"
  [[ "$actual_sha" == "$SSB_ARCHIVE_SHA256" ]] || die "ssb-dbgen archive checksum mismatch: $actual_sha"
}

build_ssb_dbgen() {
  download_and_verify_ssb
  if [[ "$rebuild" == "1" ]]; then
    rm -rf "$source_dir"
  fi
  if [[ ! -d "$source_dir" ]]; then
    unzip -q "$archive_file" -d "$cache_dir"
  fi
  if [[ "$rebuild" == "1" || ! -x "$dbgen_bin" ]]; then
    log "Building ssb-dbgen..."
    make -C "$source_dir"
  fi
  [[ -x "$dbgen_bin" ]] || die "ssb-dbgen binary was not built: $dbgen_bin"
}

generate_raw_files() {
  rm -rf "$raw_dir"
  mkdir -p "$raw_dir"
  log "Generating SSB raw files..."
  (
    cd "$raw_dir"
    "$dbgen_bin" -s "$scale" -T a
  )
  local raw_file
  for raw_file in customer.tbl date.tbl lineorder.tbl part.tbl supplier.tbl; do
    [[ -s "$raw_dir/$raw_file" ]] || die "missing generated raw file: $raw_dir/$raw_file"
  done
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
  tar -C "$raw_dir" -cf - . | "${compose_args[@]}" run --rm -T --entrypoint /bin/sh mc -c "
    set -eu
    tmp_dir=/tmp/novarocks-ssb-raw-\$\$
    rm -rf \"\$tmp_dir\"
    mkdir -p \"\$tmp_dir\"
    tar -C \"\$tmp_dir\" -xf -
    /usr/bin/mc alias set minio http://minio:9000 \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" >/dev/null
    /usr/bin/mc rm --recursive --force '$target_path' >/dev/null 2>&1 || true
    /usr/bin/mc cp --recursive \"\$tmp_dir/\" '$target_path/'
    rm -rf \"\$tmp_dir\"
  "
}

run_spark_loader() {
  log "Writing SSB data to Iceberg with Spark..."
  local tmp_dir="/tmp/novarocks-benchmark-bootstrap-${NOVA_ENV_ID:-env}-$$"
  "${compose_args[@]}" exec -T spark /bin/bash -lc "rm -rf '$tmp_dir' && mkdir -p '$tmp_dir'"
  "${compose_args[@]}" cp "$spark_loader" "spark:$tmp_dir/write_standard_benchmark.py"
  "${compose_args[@]}" exec -T spark /bin/bash -lc "
    set -euo pipefail
    trap 'rm -rf $tmp_dir' EXIT
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
    \"\$spark_submit_bin\" '$tmp_dir/write_standard_benchmark.py' \
      --suite '$suite' \
      --scale '$scale' \
      --raw-base-uri '$raw_uri' \
      --catalog '$target_catalog' \
      --database ssb \
      --warehouse '$warehouse_uri' \
      --manifest-output '$manifest_uri' \
      --s3-endpoint '${NOVAROCKS_SPARK_S3_ENDPOINT:-http://minio:9000}' \
      --s3-access-key '$AWS_S3_ACCESS_KEY_ID' \
      --s3-secret-key '$AWS_S3_SECRET_ACCESS_KEY' \
      --generator ssb-dbgen \
      --generator-version '$SSB_VERSION'
  "
}

acquire_lock() {
  if ! mkdir "$lock_dir" 2>/dev/null; then
    die "bootstrap is already running or lock is stale: $lock_dir"
  fi
  trap 'rm -rf "$lock_dir"' EXIT
}

ensure_docker_services() {
  "$WORKSPACE_ROOT/docker/iceberg-rest/up.sh"
}

main() {
  parse_args "$@"
  : "${suite:=ssb}"
  : "${scale:=1}"
  validate_first_phase
  source_env
  resolve_paths

  compose_args=(
    docker compose
    --env-file "$NOVA_ENV_COMPOSE_ENV"
    -p "$NOVA_ENV_COMPOSE_PROJECT"
    -f "$NOVA_ENV_COMPOSE_FILE"
  )

  if [[ "$dry_run" == "1" ]]; then
    print_dry_run
    exit 0
  fi

  acquire_lock
  ensure_docker_services

  if [[ "$rebuild" != "1" ]]; then
    if check_readiness; then
      [[ "$check_only" == "1" ]] && exit 0
      log "Existing benchmark data is ready; use --rebuild to regenerate."
      exit 0
    fi
    log "Existing benchmark data is not ready; bootstrapping..."
  fi

  [[ "$check_only" != "1" ]] || die "benchmark data is not ready"

  build_ssb_dbgen
  generate_raw_files
  upload_raw_files
  run_spark_loader
  check_readiness
}

main "$@"
