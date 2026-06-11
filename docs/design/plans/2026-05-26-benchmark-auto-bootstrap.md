# Benchmark Auto Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `sql-test-runner --suite ssb` automatically prepare deterministic SSB SF1 benchmark data in the standard Docker test environment when the data is missing.

**Architecture:** Keep the runner as an orchestrator: it detects benchmark suites, calls a repository bootstrap script, and rechecks before suite init. The bootstrap script owns SSB generator download/build, raw file generation, raw upload to MinIO, Spark conversion to Iceberg, and manifest writing. Spark only converts standard generator output; it does not create benchmark data itself.

**Tech Stack:** Rust `clap`/`std::process::Command` in `tests/sql-test-runner`, Bash for bootstrap orchestration, SSB `ssb-dbgen`, Docker Compose MinIO/Spark fixture, PySpark + Iceberg Spark catalog.

---

## File Structure

- Modify `.gitignore`: stop ignoring the whole `sql-tests/bootstrap` tree; ignore only generated/cache/data subtrees.
- Create `sql-tests/bootstrap/benchmark_tools.toml`: pinned SSB generator metadata.
- Create `sql-tests/bootstrap/bootstrap_benchmark_data.sh`: SSB bootstrap orchestration and check mode.
- Create `sql-tests/bootstrap/spark/write_standard_benchmark.py`: PySpark loader for SSB raw `.tbl` files.
- Keep or create `sql-tests/bootstrap/ddl/ssb.sql`: static NovaRocks-readable SSB DDL.
- Modify `tests/sql-test-runner/src/main.rs`: add CLI flags and call the bootstrap orchestration before suite init.
- Create `tests/sql-test-runner/src/benchmark_bootstrap.rs`: pure benchmark-suite helpers plus command execution wrapper.
- Modify `tests/sql-test-runner/src/types.rs`: add runner options struct if keeping the option data outside `main.rs` improves clarity.
- Update `sql-tests/bootstrap/README.md`: document first-phase SSB flow and manual cache recovery.
- Update `sql-tests/ssb/result/*.result`: record results from generated SSB SF1 data after implementation verification.

---

### Task 1: Make Bootstrap Scripts Trackable

**Files:**
- Modify: `.gitignore`
- Create or preserve: `sql-tests/bootstrap/README.md`
- Create or preserve: `sql-tests/bootstrap/ddl/ssb.sql`

- [ ] **Step 1: Write the expected gitignore change**

Edit `.gitignore` so this block:

```gitignore
/sql-tests/bootstrap
```

becomes:

```gitignore
/sql-tests/bootstrap/cache/
/sql-tests/bootstrap/generated/
/sql-tests/bootstrap/parquet/
```

This keeps large or local data out of git while allowing scripts, DDL, and Spark loader code to be tracked.

- [ ] **Step 2: Verify ignored data stays ignored**

Run:

```bash
git check-ignore -v sql-tests/bootstrap/parquet/ssb/customer/data/example.parquet
git check-ignore -v sql-tests/bootstrap/cache/example.zip
git check-ignore -v sql-tests/bootstrap/generated/ssb/1/raw/customer.tbl
```

Expected: each command prints a `.gitignore` line and exits 0.

- [ ] **Step 3: Verify scripts are no longer ignored**

Run:

```bash
git check-ignore -v sql-tests/bootstrap/bootstrap_benchmark_data.sh || true
git check-ignore -v sql-tests/bootstrap/benchmark_tools.toml || true
git check-ignore -v sql-tests/bootstrap/spark/write_standard_benchmark.py || true
```

Expected: no output for these three tracked-source paths.

- [ ] **Step 4: Commit**

```bash
git add .gitignore
git commit -m "chore: track benchmark bootstrap sources"
```

---

### Task 2: Add Pinned SSB Tool Metadata

**Files:**
- Create: `sql-tests/bootstrap/benchmark_tools.toml`
- Test: shell checksum command

- [ ] **Step 1: Add SSB generator metadata**

Create `sql-tests/bootstrap/benchmark_tools.toml` with:

```toml
[ssb]
name = "ssb-dbgen"
version = "219403ad7d1dd32ae1f97b5553abf92129fccd7f"
archive_url = "https://github.com/electrum/ssb-dbgen/archive/219403ad7d1dd32ae1f97b5553abf92129fccd7f.zip"
archive_sha256 = "2653bb57c165bbf9b41ea37d1da6cd2f81c0505a6ed20839111985f53dec2ee6"
archive_root = "ssb-dbgen-219403ad7d1dd32ae1f97b5553abf92129fccd7f"
build_command = "make"
binary = "dbgen"
default_scale = "1"
raw_tables = ["customer", "date", "lineorder", "part", "supplier"]
```

- [ ] **Step 2: Verify the pinned archive hash**

Run:

```bash
tmp="$(mktemp -d)"
curl -L --fail -o "$tmp/ssb-dbgen.zip" https://github.com/electrum/ssb-dbgen/archive/219403ad7d1dd32ae1f97b5553abf92129fccd7f.zip
shasum -a 256 "$tmp/ssb-dbgen.zip"
rm -rf "$tmp"
```

Expected output includes:

```text
2653bb57c165bbf9b41ea37d1da6cd2f81c0505a6ed20839111985f53dec2ee6
```

- [ ] **Step 3: Commit**

```bash
git add sql-tests/bootstrap/benchmark_tools.toml
git commit -m "test: add pinned SSB generator metadata"
```

---

### Task 3: Add the SSB Spark Loader

**Files:**
- Create: `sql-tests/bootstrap/spark/write_standard_benchmark.py`
- Test: Python syntax check

- [ ] **Step 1: Create the Spark loader**

Create `sql-tests/bootstrap/spark/write_standard_benchmark.py` with:

```python
#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


SSB_TABLES = {
    "customer": [
        ("c_custkey", "int"),
        ("c_name", "string"),
        ("c_address", "string"),
        ("c_city", "string"),
        ("c_nation", "string"),
        ("c_region", "string"),
        ("c_phone", "string"),
        ("c_mktsegment", "string"),
    ],
    "dates": [
        ("d_datekey", "int"),
        ("d_date", "string"),
        ("d_dayofweek", "string"),
        ("d_month", "string"),
        ("d_year", "int"),
        ("d_yearmonthnum", "int"),
        ("d_yearmonth", "string"),
        ("d_daynuminweek", "int"),
        ("d_daynuminmonth", "int"),
        ("d_daynuminyear", "int"),
        ("d_monthnuminyear", "int"),
        ("d_weeknuminyear", "int"),
        ("d_sellingseason", "string"),
        ("d_lastdayinweekfl", "int"),
        ("d_lastdayinmonthfl", "int"),
        ("d_holidayfl", "int"),
        ("d_weekdayfl", "int"),
    ],
    "lineorder": [
        ("lo_orderkey", "int"),
        ("lo_linenumber", "int"),
        ("lo_custkey", "int"),
        ("lo_partkey", "int"),
        ("lo_suppkey", "int"),
        ("lo_orderdate", "int"),
        ("lo_orderpriority", "string"),
        ("lo_shippriority", "int"),
        ("lo_quantity", "int"),
        ("lo_extendedprice", "int"),
        ("lo_ordtotalprice", "int"),
        ("lo_discount", "int"),
        ("lo_revenue", "int"),
        ("lo_supplycost", "int"),
        ("lo_tax", "int"),
        ("lo_commitdate", "int"),
        ("lo_shipmode", "string"),
    ],
    "part": [
        ("p_partkey", "int"),
        ("p_name", "string"),
        ("p_mfgr", "string"),
        ("p_category", "string"),
        ("p_brand", "string"),
        ("p_color", "string"),
        ("p_type", "string"),
        ("p_size", "int"),
        ("p_container", "string"),
    ],
    "supplier": [
        ("s_suppkey", "int"),
        ("s_name", "string"),
        ("s_address", "string"),
        ("s_city", "string"),
        ("s_nation", "string"),
        ("s_region", "string"),
        ("s_phone", "string"),
    ],
}

SSB_RAW_NAMES = {
    "customer": "customer.tbl",
    "dates": "date.tbl",
    "lineorder": "lineorder.tbl",
    "part": "part.tbl",
    "supplier": "supplier.tbl",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Write standard benchmark raw files to Iceberg")
    parser.add_argument("--suite", required=True)
    parser.add_argument("--scale", required=True)
    parser.add_argument("--raw-base-uri", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--manifest-output", required=True)
    parser.add_argument("--s3-endpoint", required=True)
    parser.add_argument("--s3-access-key", required=True)
    parser.add_argument("--s3-secret-key", required=True)
    parser.add_argument("--generator", required=True)
    parser.add_argument("--generator-version", required=True)
    return parser.parse_args()


def configure_catalog(spark, args):
    spark.conf.set(f"spark.sql.catalog.{args.catalog}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"spark.sql.catalog.{args.catalog}.type", "hadoop")
    spark.conf.set(f"spark.sql.catalog.{args.catalog}.warehouse", args.warehouse)
    spark.conf.set(f"spark.sql.catalog.{args.catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    spark.conf.set(f"spark.sql.catalog.{args.catalog}.s3.endpoint", args.s3_endpoint)
    spark.conf.set(f"spark.sql.catalog.{args.catalog}.s3.path-style-access", "true")
    spark.conf.set(f"spark.sql.catalog.{args.catalog}.s3.access-key-id", args.s3_access_key)
    spark.conf.set(f"spark.sql.catalog.{args.catalog}.s3.secret-access-key", args.s3_secret_key)
    spark.conf.set(f"spark.sql.catalog.{args.catalog}.s3.region", "us-east-1")
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint)
    spark.conf.set("spark.hadoop.fs.s3a.access.key", args.s3_access_key)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key)
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    spark.conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def spark_type(type_name):
    if type_name == "int":
        return IntegerType()
    if type_name == "string":
        return StringType()
    raise ValueError(f"unsupported SSB type: {type_name}")


def read_pipe_table(spark, path, columns):
    raw_schema = StructType(
        [StructField(f"c{idx}", StringType(), True) for idx in range(len(columns) + 1)]
    )
    df = (
        spark.read.option("delimiter", "|")
        .option("header", "false")
        .schema(raw_schema)
        .csv(path)
    )
    projected = []
    for idx, (name, type_name) in enumerate(columns):
        value = trim(col(f"c{idx}"))
        if type_name == "int":
            value = value.cast(IntegerType())
        projected.append(value.alias(name))
    return df.select(*projected)


def main():
    args = parse_args()
    if args.suite != "ssb":
        raise ValueError(f"unsupported suite in first phase: {args.suite}")

    spark = (
        SparkSession.builder.appName("NovaRocksBenchmarkBootstrap")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )
    configure_catalog(spark, args)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.catalog}.{args.database}")

    row_counts = []
    for table, columns in SSB_TABLES.items():
        raw_name = SSB_RAW_NAMES[table]
        raw_path = f"{args.raw_base_uri.rstrip('/')}/{raw_name}"
        df = read_pipe_table(spark, raw_path, columns)
        target = f"{args.catalog}.{args.database}.{table}"
        spark.sql(f"DROP TABLE IF EXISTS {target}")
        df.writeTo(target).using("iceberg").tableProperty("format-version", "2").create()
        row_counts.append({"name": table, "rows": df.count()})

    manifest = {
        "suite": args.suite,
        "scale": args.scale,
        "catalog": args.catalog,
        "database": args.database,
        "generator": args.generator,
        "generator_version": args.generator_version,
        "schema_version": "2026-05-26",
        "warehouse": args.warehouse,
        "tables": row_counts,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_df = spark.createDataFrame([(json.dumps(manifest, sort_keys=True),)], ["value"])
    manifest_df.coalesce(1).write.mode("overwrite").text(args.manifest_output)
    spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run Python syntax check**

Run:

```bash
python3 -m py_compile sql-tests/bootstrap/spark/write_standard_benchmark.py
```

Expected: command exits 0.

- [ ] **Step 3: Commit**

```bash
git add sql-tests/bootstrap/spark/write_standard_benchmark.py
git commit -m "test: add SSB Spark bootstrap loader"
```

---

### Task 4: Add SSB Bootstrap Script

**Files:**
- Create: `sql-tests/bootstrap/bootstrap_benchmark_data.sh`
- Test: shell syntax and dry-run/check behavior

- [ ] **Step 1: Create the bootstrap script**

Create `sql-tests/bootstrap/bootstrap_benchmark_data.sh` with:

```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNTIME_ENV="$REPO_ROOT/docker/iceberg-rest/runtime/current/env.sh"
TOOLS_TOML="$SCRIPT_DIR/benchmark_tools.toml"

SUITE=""
SCALE=""
TARGET_CATALOG="sql_test_catalog"
MYSQL_HOST="127.0.0.1"
MYSQL_PORT=""
MYSQL_USER="root"
MYSQL_PASSWORD=""
MODE="bootstrap"
REBUILD=0
DRY_RUN=0

SSB_VERSION="219403ad7d1dd32ae1f97b5553abf92129fccd7f"
SSB_URL="https://github.com/electrum/ssb-dbgen/archive/219403ad7d1dd32ae1f97b5553abf92129fccd7f.zip"
SSB_SHA256="2653bb57c165bbf9b41ea37d1da6cd2f81c0505a6ed20839111985f53dec2ee6"
SSB_ARCHIVE_ROOT="ssb-dbgen-219403ad7d1dd32ae1f97b5553abf92129fccd7f"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
  printf '[%s] ERROR: %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2
  exit 1
}

usage() {
  cat >&2 <<'USAGE'
usage: bootstrap_benchmark_data.sh --suite ssb [--scale 1] [--target-catalog sql_test_catalog]
       bootstrap_benchmark_data.sh --check --suite ssb [--scale 1]

options:
  --suite NAME             Supported first-phase suite: ssb
  --scale SCALE            Default: 1
  --target-catalog NAME    Default: sql_test_catalog
  --mysql-host HOST        Default: 127.0.0.1
  --mysql-port PORT        Default: $NOVA_ENV_MYSQL_PORT
  --mysql-user USER        Default: root
  --mysql-password PASS    Optional
  --check                  Check only, no writes
  --rebuild                Drop/rebuild SSB tables through Spark
  --dry-run                Print major actions without running generator or Spark
USAGE
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --suite) SUITE="${2:-}"; shift 2 ;;
    --scale) SCALE="${2:-}"; shift 2 ;;
    --target-catalog) TARGET_CATALOG="${2:-}"; shift 2 ;;
    --mysql-host) MYSQL_HOST="${2:-}"; shift 2 ;;
    --mysql-port) MYSQL_PORT="${2:-}"; shift 2 ;;
    --mysql-user) MYSQL_USER="${2:-}"; shift 2 ;;
    --mysql-password) MYSQL_PASSWORD="${2:-}"; shift 2 ;;
    --check) MODE="check"; shift ;;
    --rebuild) REBUILD=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage ;;
    *) die "unknown argument: $1" ;;
  esac
done

[[ -n "$SUITE" ]] || usage
[[ "$SUITE" == "ssb" ]] || die "unsupported suite in first phase: $SUITE"
SCALE="${SCALE:-1}"
[[ "$SCALE" == "1" ]] || die "unsupported SSB scale in first phase: $SCALE"
[[ -f "$TOOLS_TOML" ]] || die "missing tool metadata: $TOOLS_TOML"
[[ -f "$RUNTIME_ENV" ]] || die "missing runtime env: $RUNTIME_ENV; run docker/iceberg-rest/up.sh first"

# shellcheck disable=SC1090
source "$RUNTIME_ENV"

MYSQL_PORT="${MYSQL_PORT:-${NOVA_ENV_MYSQL_PORT:-}}"
[[ -n "$MYSQL_PORT" ]] || die "mysql port is missing; source runtime/current/env.sh or pass --mysql-port"

TOOL_CACHE="${NOVAROCKS_BENCHMARK_TOOL_CACHE:-$SCRIPT_DIR/cache/tools}"
DOWNLOAD_DIR="${NOVAROCKS_BENCHMARK_DOWNLOAD_DIR:-$SCRIPT_DIR/cache/downloads}"
GENERATED_DIR="$SCRIPT_DIR/generated/$SUITE/$SCALE"
RAW_DIR="$GENERATED_DIR/raw"
LOG_DIR="$GENERATED_DIR/logs"
LOCK_DIR="$SCRIPT_DIR/generated/locks/${NOVA_ENV_ID:-env}-$SUITE-$SCALE.lock"
WAREHOUSE="${CATALOG_WAREHOUSE_URI:-${iceberg_catalog_warehouse:-${NOVAROCKS_ICEBERG_REST_WAREHOUSE:-}}}"
if [[ -z "$WAREHOUSE" || "$WAREHOUSE" == s3://warehouse/* ]]; then
  WAREHOUSE="${NOVA_ENV_ICEBERG_CATALOG_WAREHOUSE:-s3://novarocks/${NOVA_ENV_ID:-default}/iceberg-catalog}"
fi
RAW_S3_BASE="${WAREHOUSE%/}/_benchmark_raw/$SUITE/sf$SCALE"
MANIFEST_S3_BASE="${WAREHOUSE%/}/_bootstrap_manifest/$SUITE/sf$SCALE"

mysql_query() {
  local sql="$1"
  local args=(-h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" --batch --raw --skip-column-names --default-character-set=utf8mb4)
  if [[ -n "$MYSQL_PASSWORD" ]]; then
    MYSQL_PWD="$MYSQL_PASSWORD" mysql "${args[@]}" -e "$sql"
  else
    mysql "${args[@]}" -e "$sql"
  fi
}

create_catalog_sql() {
  cat <<SQL
CREATE EXTERNAL CATALOG IF NOT EXISTS \`$TARGET_CATALOG\`
PROPERTIES (
  "type"="iceberg",
  "iceberg.catalog.type"="hadoop",
  "iceberg.catalog.warehouse"="$WAREHOUSE",
  "aws.s3.access_key"="${AWS_S3_ACCESS_KEY_ID:-admin}",
  "aws.s3.secret_key"="${AWS_S3_SECRET_ACCESS_KEY:-admin123}",
  "aws.s3.endpoint"="${AWS_S3_ENDPOINT:-http://127.0.0.1:9000}",
  "aws.s3.enable_path_style_access"="true"
);
SQL
}

check_ssb_tables() {
  local sql
  sql="$(create_catalog_sql)
SET catalog $TARGET_CATALOG;
USE ssb;
SELECT 1 FROM customer LIMIT 1;
SELECT 1 FROM dates LIMIT 1;
SELECT 1 FROM lineorder LIMIT 1;
SELECT 1 FROM part LIMIT 1;
SELECT 1 FROM supplier LIMIT 1;"
  mysql_query "$sql" >/dev/null 2>&1
}

parse_s3_uri() {
  local uri="$1"
  local stripped="${uri#s3://}"
  [[ "$stripped" != "$uri" ]] || die "expected s3 URI: $uri"
  local bucket="${stripped%%/*}"
  local prefix=""
  if [[ "$stripped" == */* ]]; then
    prefix="${stripped#*/}"
  fi
  printf '%s\t%s\n' "$bucket" "$prefix"
}

mc_target() {
  local uri="$1"
  local bucket prefix
  IFS=$'\t' read -r bucket prefix < <(parse_s3_uri "$uri")
  if [[ -n "$prefix" ]]; then
    printf 'minio/%s/%s\n' "$bucket" "$prefix"
  else
    printf 'minio/%s\n' "$bucket"
  fi
}

run_mc_container() {
  local script="$1"
  docker run --rm \
    --network "${NOVA_ENV_COMPOSE_PROJECT:-nr-iceberg-rest}_iceberg_net" \
    --entrypoint /bin/sh \
    -e MINIO_CONTAINER_ENDPOINT="http://minio:9000" \
    -e MINIO_ROOT_USER="${AWS_S3_ACCESS_KEY_ID:-admin}" \
    -e MINIO_ROOT_PASSWORD="${AWS_S3_SECRET_ACCESS_KEY:-admin123}" \
    -v "$RAW_DIR:/raw:ro" \
    quay.io/minio/mc:latest \
    -c "$script"
}

run_mc_container_without_raw_mount() {
  local script="$1"
  docker run --rm \
    --network "${NOVA_ENV_COMPOSE_PROJECT:-nr-iceberg-rest}_iceberg_net" \
    --entrypoint /bin/sh \
    -e MINIO_CONTAINER_ENDPOINT="http://minio:9000" \
    -e MINIO_ROOT_USER="${AWS_S3_ACCESS_KEY_ID:-admin}" \
    -e MINIO_ROOT_PASSWORD="${AWS_S3_SECRET_ACCESS_KEY:-admin123}" \
    quay.io/minio/mc:latest \
    -c "$script"
}

check_manifest() {
  local target
  target="$(mc_target "$MANIFEST_S3_BASE")"
  run_mc_container_without_raw_mount "mc alias set minio \"\$MINIO_CONTAINER_ENDPOINT\" \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" >/dev/null && mc find '$target' --name 'part-*' | grep -q ." >/dev/null 2>&1
}

check_ready() {
  check_ssb_tables && check_manifest
}

download_tool() {
  mkdir -p "$DOWNLOAD_DIR" "$TOOL_CACHE"
  local archive="$DOWNLOAD_DIR/ssb-dbgen-$SSB_VERSION.zip"
  if [[ -n "${NOVAROCKS_BENCHMARK_TOOL_SSB_ARCHIVE:-}" ]]; then
    archive="$NOVAROCKS_BENCHMARK_TOOL_SSB_ARCHIVE"
  elif [[ ! -f "$archive" ]]; then
    log "download ssb-dbgen $SSB_VERSION"
    curl -L --fail -o "$archive" "$SSB_URL" || die "failed to download $SSB_URL; place archive at $archive and retry"
  fi
  local actual
  actual="$(shasum -a 256 "$archive" | awk '{print $1}')"
  [[ "$actual" == "$SSB_SHA256" ]] || die "ssb-dbgen sha256 mismatch: expected $SSB_SHA256 got $actual"
  local source_dir="$TOOL_CACHE/$SSB_ARCHIVE_ROOT"
  if [[ ! -x "$source_dir/dbgen" ]]; then
    rm -rf "$source_dir"
    unzip -q "$archive" -d "$TOOL_CACHE"
    (cd "$source_dir" && make CC="${CC:-cc}" MACHINE="${SSB_DBGEN_MACHINE:-LINUX}" DATABASE=DB2 WORKLOAD=SSBM) >"$LOG_DIR/ssb-dbgen-build.log" 2>&1 || die "ssb-dbgen build failed; see $LOG_DIR/ssb-dbgen-build.log"
  fi
  [[ -x "$source_dir/dbgen" ]] || die "dbgen binary missing after build: $source_dir/dbgen"
  printf '%s\n' "$source_dir/dbgen"
}

generate_raw() {
  local dbgen="$1"
  mkdir -p "$RAW_DIR"
  if [[ -s "$RAW_DIR/customer.tbl" && -s "$RAW_DIR/date.tbl" && -s "$RAW_DIR/lineorder.tbl" && -s "$RAW_DIR/part.tbl" && -s "$RAW_DIR/supplier.tbl" ]]; then
    log "raw SSB files already exist at $RAW_DIR"
    return 0
  fi
  rm -rf "$RAW_DIR"
  mkdir -p "$RAW_DIR"
  log "generate SSB raw files with scale $SCALE"
  (cd "$RAW_DIR" && "$dbgen" -s "$SCALE" -T a) >"$LOG_DIR/ssb-dbgen-generate.log" 2>&1 || die "SSB generation failed; see $LOG_DIR/ssb-dbgen-generate.log"
  for file in customer.tbl date.tbl lineorder.tbl part.tbl supplier.tbl; do
    [[ -s "$RAW_DIR/$file" ]] || die "generated file is missing or empty: $RAW_DIR/$file"
  done
}

upload_raw() {
  local target
  target="$(mc_target "$RAW_S3_BASE")"
  local bucket_target="${target#minio/}"
  bucket_target="minio/${bucket_target%%/*}"
  log "upload raw files to $RAW_S3_BASE"
  run_mc_container "set -e; mc alias set minio \"\$MINIO_CONTAINER_ENDPOINT\" \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" >/dev/null; mc mb --ignore-existing '$bucket_target' >/dev/null; mc mirror --overwrite /raw '$target'"
}

run_spark_loader() {
  local compose_args=(docker compose --env-file "$NOVA_ENV_COMPOSE_ENV" -p "$NOVA_ENV_COMPOSE_PROJECT" -f "$NOVA_ENV_COMPOSE_FILE")
  local tmp_dir="/tmp/novarocks-benchmark-bootstrap-${NOVA_ENV_ID:-env}-$$"
  local container_script="$tmp_dir/write_standard_benchmark.py"
  log "load SSB raw files into Iceberg with Spark"
  "${compose_args[@]}" exec -T spark /bin/bash -lc "mkdir -p '$tmp_dir'"
  "${compose_args[@]}" exec -T spark /bin/bash -lc "cat > '$container_script'" < "$SCRIPT_DIR/spark/write_standard_benchmark.py"
  "${compose_args[@]}" exec -T spark /bin/bash -lc "
    set -euo pipefail
    trap 'rm -rf $tmp_dir' EXIT
    spark-submit '$container_script' \
      --suite ssb \
      --scale '$SCALE' \
      --raw-base-uri '$RAW_S3_BASE' \
      --catalog '$TARGET_CATALOG' \
      --database ssb \
      --warehouse '$WAREHOUSE' \
      --manifest-output '$MANIFEST_S3_BASE' \
      --s3-endpoint 'http://minio:9000' \
      --s3-access-key '${AWS_S3_ACCESS_KEY_ID:-admin}' \
      --s3-secret-key '${AWS_S3_SECRET_ACCESS_KEY:-admin123}' \
      --generator ssb-dbgen \
      --generator-version '$SSB_VERSION'
  " >"$LOG_DIR/spark-loader.log" 2>&1 || die "Spark loader failed; see $LOG_DIR/spark-loader.log"
}

main() {
  mkdir -p "$LOG_DIR"
  if [[ "$MODE" == "check" ]]; then
    check_ready
    return $?
  fi
  if [[ "$DRY_RUN" == "1" ]]; then
    log "DRY_RUN suite=$SUITE scale=$SCALE warehouse=$WAREHOUSE raw=$RAW_S3_BASE manifest=$MANIFEST_S3_BASE"
    return 0
  fi
  if check_ready && [[ "$REBUILD" == "0" ]]; then
    log "SSB benchmark data already exists for scale $SCALE"
    return 0
  fi
  mkdir "$LOCK_DIR" 2>/dev/null || die "bootstrap lock exists: $LOCK_DIR"
  trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT
  local dbgen
  dbgen="$(download_tool)"
  generate_raw "$dbgen"
  upload_raw
  run_spark_loader
  check_ready || die "bootstrap completed but SSB readiness check failed"
  log "SSB benchmark data is ready"
}

main "$@"
```

- [ ] **Step 2: Make the script executable**

Run:

```bash
chmod +x sql-tests/bootstrap/bootstrap_benchmark_data.sh
```

- [ ] **Step 3: Run syntax check**

Run:

```bash
bash -n sql-tests/bootstrap/bootstrap_benchmark_data.sh
```

Expected: command exits 0.

- [ ] **Step 4: Run dry-run check**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
sql-tests/bootstrap/bootstrap_benchmark_data.sh --suite ssb --scale 1 --dry-run
```

Expected output includes:

```text
DRY_RUN suite=ssb scale=1
```

- [ ] **Step 5: Commit**

```bash
git add sql-tests/bootstrap/bootstrap_benchmark_data.sh
git commit -m "test: add SSB benchmark bootstrap script"
```

---

### Task 5: Add Runner Bootstrap Helpers

**Files:**
- Create: `tests/sql-test-runner/src/benchmark_bootstrap.rs`
- Modify: `tests/sql-test-runner/src/main.rs`
- Test: `cargo test --manifest-path tests/sql-test-runner/Cargo.toml benchmark_bootstrap`

- [ ] **Step 1: Add the module declaration**

At the top of `tests/sql-test-runner/src/main.rs`, add:

```rust
mod benchmark_bootstrap;
```

- [ ] **Step 2: Create the helper module**

Create `tests/sql-test-runner/src/benchmark_bootstrap.rs` with:

```rust
use crate::types::RunnerConfig;
use anyhow::{Context, Result, bail};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BenchmarkBootstrapOptions {
    pub enabled: bool,
    pub rebuild: bool,
    pub scales: BTreeMap<String, String>,
}

pub fn is_benchmark_suite(suite: &str) -> bool {
    matches!(suite, "ssb" | "tpc-h" | "tpc-ds")
}

pub fn default_scale(suite: &str) -> Option<&'static str> {
    match suite {
        "ssb" => Some("1"),
        "tpc-h" => Some("1"),
        "tpc-ds" => Some("1GB"),
        _ => None,
    }
}

pub fn parse_scale_override(raw: &str) -> Result<(String, String)> {
    let Some((suite, scale)) = raw.split_once('=') else {
        bail!("invalid --benchmark-scale '{}'; expected <suite>=<scale>", raw);
    };
    let suite = suite.trim();
    let scale = scale.trim();
    if !is_benchmark_suite(suite) {
        bail!("invalid benchmark suite in --benchmark-scale '{}': {}", raw, suite);
    }
    if scale.is_empty() {
        bail!("empty benchmark scale in --benchmark-scale '{}'", raw);
    }
    Ok((suite.to_string(), scale.to_string()))
}

pub fn parse_scale_overrides(values: &[String]) -> Result<BTreeMap<String, String>> {
    let mut parsed = BTreeMap::new();
    for raw in values {
        let (suite, scale) = parse_scale_override(raw)?;
        parsed.insert(suite, scale);
    }
    Ok(parsed)
}

pub fn scale_for_suite(options: &BenchmarkBootstrapOptions, suite: &str) -> Result<String> {
    if let Some(scale) = options.scales.get(suite) {
        return Ok(scale.clone());
    }
    default_scale(suite)
        .map(ToString::to_string)
        .with_context(|| format!("no default benchmark scale for suite {}", suite))
}

fn bootstrap_script(base_dir: &Path) -> PathBuf {
    base_dir
        .join("sql-tests")
        .join("bootstrap")
        .join("bootstrap_benchmark_data.sh")
}

#[derive(Debug, Clone)]
pub struct BootstrapInvocation<'a> {
    pub base_dir: &'a Path,
    pub suite: &'a str,
    pub scale: &'a str,
    pub host: &'a str,
    pub port: &'a str,
    pub user: &'a str,
    pub password: Option<&'a str>,
    pub target_catalog: &'a str,
    pub check_only: bool,
    pub rebuild: bool,
}

pub fn build_command(invocation: &BootstrapInvocation<'_>) -> Command {
    let mut command = Command::new(bootstrap_script(invocation.base_dir));
    command.current_dir(invocation.base_dir);
    command.arg("--suite").arg(invocation.suite);
    command.arg("--scale").arg(invocation.scale);
    command.arg("--target-catalog").arg(invocation.target_catalog);
    command.arg("--mysql-host").arg(invocation.host);
    command.arg("--mysql-port").arg(invocation.port);
    command.arg("--mysql-user").arg(invocation.user);
    if let Some(password) = invocation.password.filter(|v| !v.is_empty()) {
        command.arg("--mysql-password").arg(password);
    }
    if invocation.check_only {
        command.arg("--check");
    }
    if invocation.rebuild {
        command.arg("--rebuild");
    }
    command
}

pub fn command_preview(command: &Command) -> String {
    let mut parts = Vec::new();
    parts.push(command.get_program().to_string_lossy().to_string());
    for arg in command.get_args() {
        parts.push(arg.to_string_lossy().to_string());
    }
    parts.join(" ")
}

fn run_command(mut command: Command, label: &str) -> Result<bool> {
    let preview = command_preview(&command);
    let output = command
        .output()
        .with_context(|| format!("failed to run {} command: {}", label, preview))?;
    if output.status.success() {
        return Ok(true);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if label == "benchmark bootstrap check" {
        return Ok(false);
    }
    bail!(
        "{} failed: {}\nstdout:\n{}\nstderr:\n{}",
        label,
        preview,
        stdout,
        stderr
    );
}

pub fn ensure_benchmark_data(
    base_dir: &Path,
    suite: &str,
    options: &BenchmarkBootstrapOptions,
    runner_config: &RunnerConfig,
    host: &str,
    port: &str,
    user: &str,
    password: Option<&str>,
) -> Result<()> {
    if !options.enabled || !is_benchmark_suite(suite) {
        return Ok(());
    }

    let scale = scale_for_suite(options, suite)?;
    let target_catalog = runner_config
        .values
        .get("benchmark_bootstrap_catalog")
        .map(String::as_str)
        .unwrap_or("sql_test_catalog");

    let check_invocation = BootstrapInvocation {
        base_dir,
        suite,
        scale: &scale,
        host,
        port,
        user,
        password,
        target_catalog,
        check_only: true,
        rebuild: false,
    };
    if run_command(build_command(&check_invocation), "benchmark bootstrap check")? {
        return Ok(());
    }

    let bootstrap_invocation = BootstrapInvocation {
        check_only: false,
        rebuild: options.rebuild,
        ..check_invocation
    };
    run_command(build_command(&bootstrap_invocation), "benchmark bootstrap")?;

    if run_command(build_command(&check_invocation), "benchmark bootstrap check")? {
        return Ok(());
    }

    bail!(
        "benchmark bootstrap for suite {} scale {} completed but readiness check still failed",
        suite,
        scale
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifies_supported_benchmark_suites() {
        assert!(is_benchmark_suite("ssb"));
        assert!(is_benchmark_suite("tpc-h"));
        assert!(is_benchmark_suite("tpc-ds"));
        assert!(!is_benchmark_suite("iceberg"));
    }

    #[test]
    fn parses_scale_overrides() {
        let parsed = parse_scale_overrides(&[
            "ssb=1".to_string(),
            "tpc-h=1".to_string(),
            "tpc-ds=1GB".to_string(),
        ])
        .expect("valid scale overrides");
        assert_eq!(parsed.get("ssb").map(String::as_str), Some("1"));
        assert_eq!(parsed.get("tpc-h").map(String::as_str), Some("1"));
        assert_eq!(parsed.get("tpc-ds").map(String::as_str), Some("1GB"));
    }

    #[test]
    fn rejects_bad_scale_override() {
        let err = parse_scale_override("ssb").expect_err("missing equals must fail");
        assert!(err.to_string().contains("expected <suite>=<scale>"));
    }

    #[test]
    fn command_contains_check_arguments() {
        let base_dir = Path::new("/repo");
        let invocation = BootstrapInvocation {
            base_dir,
            suite: "ssb",
            scale: "1",
            host: "127.0.0.1",
            port: "9132",
            user: "root",
            password: None,
            target_catalog: "sql_test_catalog",
            check_only: true,
            rebuild: false,
        };
        let command = build_command(&invocation);
        let preview = command_preview(&command);
        assert!(preview.contains("/repo/sql-tests/bootstrap/bootstrap_benchmark_data.sh"));
        assert!(preview.contains("--suite ssb"));
        assert!(preview.contains("--scale 1"));
        assert!(preview.contains("--mysql-port 9132"));
        assert!(preview.contains("--check"));
    }
}
```

- [ ] **Step 3: Run the new module tests and confirm they pass**

Run:

```bash
cargo test --manifest-path tests/sql-test-runner/Cargo.toml benchmark_bootstrap
```

Expected: output includes:

```text
test result: ok.
```

- [ ] **Step 4: Commit**

```bash
git add tests/sql-test-runner/src/main.rs tests/sql-test-runner/src/benchmark_bootstrap.rs
git commit -m "test: add benchmark bootstrap runner helpers"
```

---

### Task 6: Wire Runner CLI and Preparation Flow

**Files:**
- Modify: `tests/sql-test-runner/src/main.rs`
- Test: runner help and unit tests

- [ ] **Step 1: Add imports**

Near the existing `use crate::config::` import block in `tests/sql-test-runner/src/main.rs`, add:

```rust
use crate::benchmark_bootstrap::{
    BenchmarkBootstrapOptions, ensure_benchmark_data, parse_scale_overrides,
};
```

- [ ] **Step 2: Add CLI fields**

Inside `struct Cli`, after the `jobs` field, add:

```rust
    /// Disable automatic benchmark data bootstrap for suites such as ssb.
    #[arg(long, action = ArgAction::SetTrue)]
    no_auto_bootstrap_benchmark_data: bool,

    /// Override benchmark scale as <suite>=<scale>, for example ssb=1.
    #[arg(long = "benchmark-scale", action = ArgAction::Append)]
    benchmark_scale: Vec<String>,

    /// Rebuild benchmark data when automatic bootstrap runs.
    #[arg(long, action = ArgAction::SetTrue)]
    benchmark_bootstrap_rebuild: bool,
```

- [ ] **Step 3: Build options after CLI validation**

After the `float_epsilon` validation block in `main()`, add:

```rust
    let benchmark_bootstrap_options = BenchmarkBootstrapOptions {
        enabled: !cli.no_auto_bootstrap_benchmark_data,
        rebuild: cli.benchmark_bootstrap_rebuild,
        scales: parse_scale_overrides(&cli.benchmark_scale)?,
    };
```

- [ ] **Step 4: Call auto bootstrap while preparing each suite**

In the suite preparation loop, after `target_user`, `target_password`, `query_timeout`, and `target_port` have been resolved, but before the `prepared_suites.push(PreparedSuite {` call, add:

```rust
        ensure_benchmark_data(
            &base_dir,
            &suite.name,
            &benchmark_bootstrap_options,
            &runner_config,
            &target_host,
            &target_port,
            &target_user,
            target_password.as_deref(),
        )
        .with_context(|| format!("failed to prepare benchmark data for suite {}", suite.name))?;
```

Place this call before the suite header is printed if the implementation keeps preparation output separate from run output. If the current code prints the suite header before this point, place it immediately after that header so users can see which suite triggered bootstrap.

- [ ] **Step 5: Verify help output contains the flags**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --help | rg "auto-bootstrap|benchmark-scale|benchmark-bootstrap"
```

Expected output includes:

```text
--no-auto-bootstrap-benchmark-data
--benchmark-scale <BENCHMARK_SCALE>
--benchmark-bootstrap-rebuild
```

- [ ] **Step 6: Run sql-test-runner tests**

Run:

```bash
cargo test --manifest-path tests/sql-test-runner/Cargo.toml
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add tests/sql-test-runner/src/main.rs tests/sql-test-runner/src/benchmark_bootstrap.rs
git commit -m "feat: auto bootstrap benchmark suite data"
```

---

### Task 7: Document the SSB Bootstrap Workflow

**Files:**
- Modify or create: `sql-tests/bootstrap/README.md`
- Test: README command dry-run

- [ ] **Step 1: Write the README**

Create or replace `sql-tests/bootstrap/README.md` with:

````markdown
# SQL Test Benchmark Bootstrap

This directory contains source-controlled scripts for preparing standard benchmark data for SQL suites. Large generated data is not stored in git.

## First-Phase Scope

The first implemented suite is `ssb` at scale factor 1. `tpc-h` and `tpc-ds` are represented in the design, but their full bootstrap and result recording are separate implementation work.

## Data Source Rule

Benchmark data must come from the standard generator for the suite. The SSB flow uses `ssb-dbgen`. Spark only converts generated `.tbl` files into Iceberg tables.

## Generated Paths

These paths are local and ignored:

- `cache/`: downloaded and compiled generators
- `generated/`: raw `.tbl` files and logs
- `parquet/`: legacy local parquet snapshots

## Manual SSB Bootstrap

Start the standard Docker fixture and standalone server first:

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
```

In another shell:

```bash
source docker/iceberg-rest/runtime/current/env.sh
sql-tests/bootstrap/bootstrap_benchmark_data.sh \
  --suite ssb \
  --scale 1 \
  --mysql-port "$NOVA_ENV_MYSQL_PORT"
```

## Runner Auto Bootstrap

The SQL runner triggers SSB bootstrap automatically when data is missing:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite ssb --mode verify
```

Disable automatic bootstrap:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite ssb --mode verify \
  --no-auto-bootstrap-benchmark-data
```

## Manual Generator Cache

If automatic download fails, download the pinned SSB archive manually:

```bash
mkdir -p sql-tests/bootstrap/cache/downloads
curl -L --fail \
  -o sql-tests/bootstrap/cache/downloads/ssb-dbgen-219403ad7d1dd32ae1f97b5553abf92129fccd7f.zip \
  https://github.com/electrum/ssb-dbgen/archive/219403ad7d1dd32ae1f97b5553abf92129fccd7f.zip
shasum -a 256 sql-tests/bootstrap/cache/downloads/ssb-dbgen-219403ad7d1dd32ae1f97b5553abf92129fccd7f.zip
```

Expected checksum:

```text
2653bb57c165bbf9b41ea37d1da6cd2f81c0505a6ed20839111985f53dec2ee6
```
````

- [ ] **Step 2: Verify the documented dry-run command**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
sql-tests/bootstrap/bootstrap_benchmark_data.sh --suite ssb --scale 1 --mysql-port "$NOVA_ENV_MYSQL_PORT" --dry-run
```

Expected output includes:

```text
DRY_RUN suite=ssb scale=1
```

- [ ] **Step 3: Commit**

```bash
git add sql-tests/bootstrap/README.md
git commit -m "docs: document benchmark bootstrap workflow"
```

---

### Task 8: Record and Verify SSB Results

**Files:**
- Modify: `sql-tests/ssb/result/*.result`
- Runtime: standard Docker fixture and standalone-server

- [ ] **Step 1: Start Docker fixture**

Run:

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
```

Expected output from `docker/iceberg-rest/up.sh`: it completes successfully and `docker/iceberg-rest/runtime/current/env.sh` exists.

- [ ] **Step 2: Start standalone-server**

Run in a long-running shell:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
```

Expected output includes:

```text
NOVAROCKS_READY mysql_port=
```

- [ ] **Step 3: Verify auto bootstrap on one SSB case**

Run in another shell:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite ssb --only q1.1 --mode verify
```

Expected during the first run: output from `bootstrap_benchmark_data.sh` appears before the case runs. Expected final summary includes:

```text
summary (suite=ssb, mode=verify)
total=1
fail=0
```

- [ ] **Step 4: Record SSB results from generated data**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite ssb --mode record --record-from target
```

Expected: each `sql-tests/ssb/result/*.result` file is updated to match generated SSB SF1 data.

- [ ] **Step 5: Verify full SSB suite**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite ssb --mode verify
```

Expected final summary includes:

```text
summary (suite=ssb, mode=verify)
total=13
fail=0
```

- [ ] **Step 6: Stop standalone-server**

Press `Ctrl-C` in the standalone-server shell. Confirm the shell returns to a prompt.

- [ ] **Step 7: Commit**

```bash
git add sql-tests/ssb/result
git commit -m "test: record SSB results from generated data"
```

---

### Task 9: Final Regression and Review

**Files:**
- All files changed by Tasks 1-8

- [ ] **Step 1: Run formatting checks**

Run:

```bash
cargo fmt --check
```

Expected: command exits 0.

- [ ] **Step 2: Run SQL runner tests**

Run:

```bash
cargo test --manifest-path tests/sql-test-runner/Cargo.toml
```

Expected: all tests pass.

- [ ] **Step 3: Run shell syntax checks**

Run:

```bash
bash -n sql-tests/bootstrap/bootstrap_benchmark_data.sh
python3 -m py_compile sql-tests/bootstrap/spark/write_standard_benchmark.py
```

Expected: both commands exit 0.

- [ ] **Step 4: Run final SSB verification**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite ssb --mode verify
```

Expected:

```text
total=13
fail=0
```

- [ ] **Step 5: Inspect git history**

Run:

```bash
git status --short
git log --oneline -8
```

Expected: `git status --short` has no unrelated changes. The recent commits are scoped to bootstrap sources, runner integration, docs, and SSB result recording.

---

## Self-Review

Spec coverage:

- Standard generator only: Tasks 2 and 4 pin and call `ssb-dbgen`.
- Spark conversion only: Task 3 reads `.tbl` files and writes Iceberg tables.
- Runner auto trigger: Tasks 5 and 6 add detection and command execution.
- Ignored data: Task 1 narrows `.gitignore`.
- Manifest/check path: Tasks 3 and 4 write and check the S3 manifest path.
- First-stage SSB scope: Tasks 8 and 9 verify `ssb`; TPC-H/TPC-DS are not included in the first implementation.

No unsupported scope is included:

- No TPC-H/TPC-DS generation code is required in this plan.
- No custom random benchmark generator is introduced.
- No generated data is committed.
