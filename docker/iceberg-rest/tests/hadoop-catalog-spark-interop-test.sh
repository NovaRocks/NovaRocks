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

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
fixture_dir=$(cd "$script_dir/.." && pwd)
repo_root=$(cd "$fixture_dir/../.." && pwd)

# Bind before reading paths; an old current pointer is not this request's result.
publication="$("$fixture_dir/up.sh" | python3 -c 'import json, sys; print(json.load(sys.stdin)["published_dir"])')"
source "$publication/env.sh"

run_token="$(date +%s)_$$"
warehouse_base="${NOVAROCKS_ICEBERG_TEST_WAREHOUSE%/}"
warehouse="${warehouse_base}/hadoop-spark-interop-${run_token}"
warehouse="${warehouse/s3:\/\//s3a:\/\/}"
catalog="nr_hadoop_interop_${run_token}"
namespace="interop_${run_token}"
spark_table="spark_created"
novarocks_table="novarocks_created"
artifact="$NOVA_ENV_RUNTIME_DIR/hadoop-catalog-spark-interop-${run_token}.log"
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/novarocks-hadoop-spark-interop.XXXXXX")
server_log="$tmp_dir/novarocks.log"
server_pid=""

cleanup() {
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" >/dev/null 2>&1; then
        kill "$server_pid" >/dev/null 2>&1 || true
        wait "$server_pid" >/dev/null 2>&1 || true
    fi
    rm -rf "$tmp_dir"
}
trap cleanup EXIT

compose=(
    docker compose
    --env-file "$NOVA_ENV_COMPOSE_ENV"
    -p "$NOVA_ENV_COMPOSE_PROJECT"
    -f "$NOVA_ENV_COMPOSE_FILE"
)

case "$warehouse" in
    s3a://*/*)
        bucket_and_prefix=${warehouse#s3a://}
        bucket=${bucket_and_prefix%%/*}
        prefix=${bucket_and_prefix#*/}
        ;;
    *)
        echo "Spark HadoopCatalog interop requires an s3a warehouse, got: $warehouse" >&2
        exit 1
        ;;
esac

spark_v1_object="$bucket/$prefix/$namespace/$spark_table/metadata/v1.metadata.json"
novarocks_v1_object="$bucket/$prefix/$namespace/$novarocks_table/metadata/v1.metadata.json"
novarocks_hint_object="$bucket/$prefix/$namespace/$novarocks_table/metadata/version-hint.text"

log() {
    printf '%s\n' "$*" | tee -a "$artifact"
}

fail() {
    log "FAIL: $*"
    exit 1
}

mc_run() {
    local command=$1
    "${compose[@]}" run --rm --no-deps --entrypoint /bin/sh mc -c \
        "/usr/bin/mc alias set interop http://minio:9000 '$MINIO_ROOT_USER' '$MINIO_ROOT_PASSWORD' >/dev/null && $command"
}

mc_cat() {
    local object=$1
    mc_run "/usr/bin/mc cat 'interop/$object'"
}

mc_remove() {
    local object=$1
    mc_run "/usr/bin/mc rm 'interop/$object' >/dev/null"
}

metadata_field() {
    local field=$1
    python3 -c 'import json, sys; print(json.load(sys.stdin)[sys.argv[1]])' "$field"
}

metadata_digest() {
    shasum -a 256 | awk '{print $1}'
}

spark_sql() {
    local sql_file=$1
    NOVAROCKS_SPARK_EXTRA_DEFAULTS="$tmp_dir/spark-hadoop-defaults.conf" \
        "$fixture_dir/spark-sql.sh" "$sql_file"
}

spark_shell() {
    local scala_file=$1
    NOVAROCKS_SPARK_EXTRA_DEFAULTS="$tmp_dir/spark-hadoop-defaults.conf" \
        "$fixture_dir/spark-shell.sh" "$scala_file"
}

mysql_exec() {
    local sql=$1
    "$mysql_bin" \
        --protocol=TCP \
        --host=127.0.0.1 \
        --port="$NOVA_ENV_MYSQL_PORT" \
        --user=root \
        --batch \
        --raw \
        --skip-column-names \
        --execute "$sql"
}

cat > "$tmp_dir/spark-hadoop-defaults.conf" <<EOF
spark.sql.catalog.nr_hadoop org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.nr_hadoop.type hadoop
spark.sql.catalog.nr_hadoop.warehouse $warehouse
spark.sql.catalog.nr_hadoop.io-impl org.apache.iceberg.hadoop.HadoopFileIO
EOF

cat > "$tmp_dir/spark-create.sql" <<EOF
CREATE NAMESPACE IF NOT EXISTS nr_hadoop.$namespace;
CREATE TABLE nr_hadoop.$namespace.$spark_table (
  id BIGINT,
  payload STRING
) USING iceberg
TBLPROPERTIES ('format-version' = '2');
SELECT COUNT(*) AS row_count FROM nr_hadoop.$namespace.$spark_table;
EOF

: > "$artifact"
log "warehouse=$warehouse"
log "spark_version=$NOVA_ENV_SPARK_VERSION iceberg_version=$NOVA_ENV_ICEBERG_VERSION"
log "phase=spark-create"
spark_sql "$tmp_dir/spark-create.sql" 2>&1 | tee -a "$artifact"

spark_metadata=$(mc_cat "$spark_v1_object")
spark_uuid=$(printf '%s' "$spark_metadata" | metadata_field table-uuid)
spark_location=$(printf '%s' "$spark_metadata" | metadata_field location)
spark_digest=$(printf '%s' "$spark_metadata" | metadata_digest)
[[ "$spark_location" == "$warehouse/$namespace/$spark_table" ]] || \
    fail "Spark metadata location mismatch: $spark_location"
log "spark_created_uuid=$spark_uuid"
log "spark_created_location=$spark_location"
log "spark_created_v1_sha256=$spark_digest"

mysql_bin=$(command -v mysql || true)
[[ -n "$mysql_bin" ]] || fail "mysql client is required for NovaRocks protocol verification"

log "phase=novarocks-start"
cd "$repo_root"
cargo build -p novarocks-server 2>&1 | tee -a "$artifact"
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone \
    --role all-in-one \
    --fe-config "$NOVAROCKS_FE_CONFIG" \
    --be-config "$NOVAROCKS_BE_CONFIG" >"$server_log" 2>&1 &
server_pid=$!
for _ in $(seq 1 60); do
    if grep -q '^NOVAROCKS_READY ' "$server_log"; then
        break
    fi
    if ! kill -0 "$server_pid" >/dev/null 2>&1; then
        tail -40 "$server_log" | tee -a "$artifact"
        fail "NovaRocks exited before readiness"
    fi
    sleep 1
done
grep -q '^NOVAROCKS_READY ' "$server_log" || fail "timed out waiting for NovaRocks readiness"
grep '^NOVAROCKS_READY ' "$server_log" | tee -a "$artifact"

log "phase=novarocks-load-spark-table"
mysql_exec "CREATE EXTERNAL CATALOG $catalog PROPERTIES(\
\"type\"=\"iceberg\",\
\"iceberg.catalog.type\"=\"hadoop\",\
\"iceberg.catalog.warehouse\"=\"$warehouse\",\
\"aws.s3.endpoint\"=\"$AWS_S3_ENDPOINT\",\
\"credential.object-store-metadata.consumer-role\"=\"frontend\",\
\"credential.object-store-metadata.mode\"=\"static\",\
\"credential.object-store-metadata.name\"=\"$iceberg_object_store_credential_name\",\
\"credential.object-store-metadata.generation\"=\"$iceberg_object_store_credential_generation\",\
\"credential.object-store-data.consumer-role\"=\"backend\",\
\"credential.object-store-data.mode\"=\"static\",\
\"credential.object-store-data.name\"=\"$iceberg_object_store_credential_name\",\
\"credential.object-store-data.generation\"=\"$iceberg_object_store_credential_generation\",\
\"aws.s3.region\"=\"us-east-1\",\
\"aws.s3.enable_path_style_access\"=\"true\"\
);"
# Iceberg's HadoopCatalog has no durable namespace object. NovaRocks uses its
# own namespace marker for SQL admission, so install that marker before testing
# table-protocol interoperability over Spark's already-created table.
mysql_exec "CREATE DATABASE IF NOT EXISTS $catalog.$namespace;"
spark_rows=$(mysql_exec "SELECT COUNT(*) FROM $catalog.$namespace.$spark_table;")
[[ "$spark_rows" == "0" ]] || fail "NovaRocks returned unexpected Spark table row count: $spark_rows"
mysql_exec "CREATE TABLE IF NOT EXISTS $catalog.$namespace.$spark_table (id BIGINT, payload STRING);"

if mysql_exec "CREATE TABLE $catalog.$namespace.$spark_table (id BIGINT, payload STRING);" \
    >"$tmp_dir/novarocks-strict.out" 2>&1; then
    fail "NovaRocks strict CREATE unexpectedly replaced the Spark table"
fi
grep -q 'AlreadyExists' "$tmp_dir/novarocks-strict.out" || {
    cat "$tmp_dir/novarocks-strict.out" | tee -a "$artifact"
    fail "NovaRocks strict CREATE did not return the typed AlreadyExists error"
}

spark_metadata_after_novarocks=$(mc_cat "$spark_v1_object")
spark_digest_after_novarocks=$(printf '%s' "$spark_metadata_after_novarocks" | metadata_digest)
[[ "$spark_digest_after_novarocks" == "$spark_digest" ]] || \
    fail "NovaRocks strict/no-op handling changed Spark v1 metadata"
log "novarocks_spark_table_rows=$spark_rows"
log "novarocks_strict_outcome=AlreadyExists"
log "novarocks_noop_outcome=success"

log "phase=novarocks-create"
mysql_exec "CREATE TABLE $catalog.$namespace.$novarocks_table (id BIGINT, payload STRING);"
novarocks_metadata=$(mc_cat "$novarocks_v1_object")
novarocks_uuid=$(printf '%s' "$novarocks_metadata" | metadata_field table-uuid)
novarocks_location=$(printf '%s' "$novarocks_metadata" | metadata_field location)
novarocks_digest=$(printf '%s' "$novarocks_metadata" | metadata_digest)
[[ "$novarocks_location" == "$warehouse/$namespace/$novarocks_table" ]] || \
    fail "NovaRocks metadata location mismatch: $novarocks_location"
log "novarocks_created_uuid=$novarocks_uuid"
log "novarocks_created_location=$novarocks_location"
log "novarocks_created_v1_sha256=$novarocks_digest"

cat > "$tmp_dir/spark-load.scala" <<EOF
import org.apache.iceberg.spark.Spark3Util

val sparkCreated = Spark3Util.loadIcebergTable(spark, "nr_hadoop.$namespace.$spark_table")
require(sparkCreated.uuid().toString == "$spark_uuid", "Spark-created UUID changed")
require(sparkCreated.location() == "$spark_location", "Spark-created location changed")
println("SPARK_TABLE_FACTS table=$spark_table uuid=" + sparkCreated.uuid() + " location=" + sparkCreated.location())

val novarocksCreated = Spark3Util.loadIcebergTable(spark, "nr_hadoop.$namespace.$novarocks_table")
require(novarocksCreated.uuid().toString == "$novarocks_uuid", "NovaRocks-created UUID mismatch")
require(novarocksCreated.location() == "$novarocks_location", "NovaRocks-created location mismatch")
println("SPARK_TABLE_FACTS table=$novarocks_table uuid=" + novarocksCreated.uuid() + " location=" + novarocksCreated.location())
EOF

log "phase=spark-load-novarocks-table"
spark_shell "$tmp_dir/spark-load.scala" 2>&1 | tee -a "$artifact"

log "phase=spark-no-hint-overwrite-probe"
mc_remove "$novarocks_hint_object"
cat > "$tmp_dir/spark-strict-create.sql" <<EOF
CREATE TABLE nr_hadoop.$namespace.$novarocks_table (
  id BIGINT,
  payload STRING
) USING iceberg
TBLPROPERTIES ('format-version' = '2');
EOF
if spark_sql "$tmp_dir/spark-strict-create.sql" >"$tmp_dir/spark-strict.out" 2>&1; then
    fail "Spark strict CREATE unexpectedly replaced a NovaRocks v1 without version-hint.text"
fi
tail -80 "$tmp_dir/spark-strict.out" | tee -a "$artifact"

novarocks_metadata_after_spark=$(mc_cat "$novarocks_v1_object")
novarocks_digest_after_spark=$(printf '%s' "$novarocks_metadata_after_spark" | metadata_digest)
novarocks_uuid_after_spark=$(printf '%s' "$novarocks_metadata_after_spark" | metadata_field table-uuid)
[[ "$novarocks_digest_after_spark" == "$novarocks_digest" ]] || \
    fail "Spark strict CREATE overwrote NovaRocks v1 metadata after version-hint removal"
[[ "$novarocks_uuid_after_spark" == "$novarocks_uuid" ]] || \
    fail "Spark strict CREATE changed NovaRocks table UUID after version-hint removal"

log "spark_no_hint_strict_outcome=failed_without_overwrite"
log "spark_no_hint_v1_sha256=$novarocks_digest_after_spark"
log "PASS: Spark and NovaRocks loaded the same UUID/location in both directions"
log "artifact=$artifact"
