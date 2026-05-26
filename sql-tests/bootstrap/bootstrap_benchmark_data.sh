#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${NOVAROCKS_WORKSPACE_ROOT:-$SCRIPT_DIR/../..}" && pwd)"
ENV_FILE="$WORKSPACE_ROOT/docker/iceberg-rest/runtime/current/env.sh"

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
Usage: bootstrap_benchmark_data.sh --suite <ssb|tpc-h|tpc-ds> --scale <scale> [options]

Options:
  --suite <name>             Benchmark suite: ssb, tpc-h, or tpc-ds.
  --scale <scale>            Standard scale. Defaults: ssb=1, tpc-h=1, tpc-ds=1GB.
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
  [[ -n "$target_catalog" ]] || die "--target-catalog must not be empty"

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

  mysql_port="${mysql_port:-${NOVA_ENV_MYSQL_PORT:-}}"
  [[ -n "$mysql_port" ]] || die "--mysql-port is required when NOVA_ENV_MYSQL_PORT is unset"
  : "${NOVA_ENV_COMPOSE_ENV:?missing NOVA_ENV_COMPOSE_ENV in $ENV_FILE}"
  : "${NOVA_ENV_COMPOSE_PROJECT:?missing NOVA_ENV_COMPOSE_PROJECT in $ENV_FILE}"
  : "${NOVA_ENV_COMPOSE_FILE:?missing NOVA_ENV_COMPOSE_FILE in $ENV_FILE}"
  : "${CATALOG_WAREHOUSE_URI:?missing CATALOG_WAREHOUSE_URI in $ENV_FILE}"
  : "${AWS_S3_ENDPOINT:?missing AWS_S3_ENDPOINT in $ENV_FILE}"
  AWS_S3_ACCESS_KEY_ID="${AWS_S3_ACCESS_KEY_ID:-admin}"
  AWS_S3_SECRET_ACCESS_KEY="${AWS_S3_SECRET_ACCESS_KEY:-admin123}"
}

resolve_paths() {
  cache_dir="$WORKSPACE_ROOT/sql-tests/bootstrap/cache"
  generated_dir="$WORKSPACE_ROOT/sql-tests/bootstrap/generated/$suite/$scale_slug"
  raw_dir="$generated_dir/raw"
  lock_dir="$WORKSPACE_ROOT/sql-tests/bootstrap/.bootstrap-$suite-$scale_slug.lock"
  archive_file="$cache_dir/$archive_basename"
  source_dir="$cache_dir/$archive_root"
  warehouse_uri="${CATALOG_WAREHOUSE_URI%/}"
  raw_uri="$warehouse_uri/_benchmark_raw/$suite/$scale_slug"
  manifest_uri="$warehouse_uri/_bootstrap_manifest/$suite/$scale_slug"
  spark_loader="$WORKSPACE_ROOT/sql-tests/bootstrap/spark/write_standard_benchmark.py"

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
target_catalog=$target_catalog
database=$suite_database
mysql=$mysql_user@$mysql_host:$mysql_port
warehouse=$warehouse_uri
raw_dir=$raw_dir
raw_uri=$raw_uri
manifest_uri=$manifest_uri
cache_dir=$cache_dir
source_dir=$source_dir
schema_ddl_file=$schema_ddl_file
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
  "${compose_args[@]}" run --rm -T \
    -e "MINIO_ROOT_USER=$AWS_S3_ACCESS_KEY_ID" \
    -e "MINIO_ROOT_PASSWORD=$AWS_S3_SECRET_ACCESS_KEY" \
    --entrypoint /bin/sh mc -c "
    set -eu
    /usr/bin/mc alias set minio http://minio:9000 \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" >/dev/null
    /usr/bin/mc ls '$path' >/dev/null
  "
}

check_readiness() {
  log "Checking benchmark data readiness..."
  create_target_catalog || return 1
  run_sql "USE $(quote_ident "$target_catalog").$(quote_ident "$suite_database");" || return 1
  local table
  for table in "${suite_tables[@]}"; do
    run_sql "SHOW CREATE TABLE $(quote_ident "$target_catalog").$(quote_ident "$suite_database").$(quote_ident "$table");" >/dev/null || return 1
  done
  check_manifest || return 1
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
    /usr/bin/mc rm --recursive --force '$target_path' >/dev/null 2>&1 || true
    /usr/bin/mc cp --recursive /benchmark-raw/ '$target_path/'
  "
}

run_spark_loader() {
  log "Writing $suite data to Iceberg with Spark..."
  local loader_tmp_dir="/tmp/novarocks-benchmark-bootstrap-${NOVA_ENV_ID:-env}-$$"
  local schema_arg=""
  "${compose_args[@]}" exec -T spark /bin/bash -lc "rm -rf '$loader_tmp_dir' && mkdir -p '$loader_tmp_dir'"
  "${compose_args[@]}" cp "$spark_loader" "spark:$loader_tmp_dir/write_standard_benchmark.py"
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
    \"\$spark_submit_bin\" '$loader_tmp_dir/write_standard_benchmark.py' \
      --suite '$suite' \
      --scale '$scale' \
      --raw-base-uri '$raw_uri' \
      --catalog '$target_catalog' \
      --database '$suite_database' \
      --warehouse '$warehouse_uri' \
      --manifest-output '$manifest_uri' \
      --s3-endpoint '${NOVAROCKS_SPARK_S3_ENDPOINT:-http://minio:9000}' \
      --s3-access-key '$AWS_S3_ACCESS_KEY_ID' \
      --s3-secret-key '$AWS_S3_SECRET_ACCESS_KEY' \
      --generator '$generator_name' \
      --generator-version '$generator_version' \
      $schema_arg
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
  validate_suite_and_scale
  configure_suite
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

  extract_generator_source
  generate_raw_files
  upload_raw_files
  run_spark_loader
  check_readiness
}

main "$@"
