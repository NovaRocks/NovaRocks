-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @sequential=true
-- @tags=mv,iceberg,rest,minio,storage-contract,concurrency
-- The real REST service holds the MV target commit after validating its
-- original main requirement and before the JDBC compare-and-swap. Spark
-- advances main while that exact request is held.

-- query 1
-- @skip_result_check=true
CREATE CATALOG mvretry_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');

-- query 2
-- @skip_result_check=true
CREATE SCHEMA mvretry_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvretry_${uuid0}.ns_${uuid0}.fact (k VARCHAR, v BIGINT) WITH (format_version = 3);
INSERT INTO mvretry_${uuid0}.ns_${uuid0}.fact VALUES ROW('east', 10);

-- query 4
-- @skip_result_check=true
USE mvretry_${uuid0}.ns_${uuid0};
CREATE MATERIALIZED VIEW target_mv WITH (refresh_policy = 'MANUAL') AS SELECT k, v FROM fact;

-- query 5
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=1,slot-changes=0,full-refreshes=1
REFRESH MATERIALIZED VIEW target_mv;

-- query 6
-- @skip_result_check=true
INSERT INTO mvretry_${uuid0}.ns_${uuid0}.fact VALUES ROW('west', 20);

-- query 7
-- Spark moves main before the held commit. FULL builds a fresh pair on that
-- exact parent and publishes all rows from the original source.
-- @skip_result_check=true
-- @publication_service_hold=ns_${uuid0}.target_mv
-- @publication_catalog_concurrent_shell=set -eu; tmp_sql=$(mktemp "${TMPDIR:-/tmp}/novarocks-mv-occ-XXXXXX.sql"); trap 'rm -f "$tmp_sql"' EXIT; printf '%s\n' "DELETE FROM ice_rest.ns_${uuid0}.target_mv WHERE k = 'east';" > "$tmp_sql"; "${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
USE mvretry_${uuid0}.ns_${uuid0};
SET SESSION "mvretry_${uuid0}".incremental_refresh_enabled = false;
REFRESH MATERIALIZED VIEW target_mv;

-- query 8
-- @result_contains=MV_FULL_REBASED
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-mv-occ-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("MV_FULL_REBASED") {
  val qualified = "ice_rest.ns_${uuid0}.target_mv"
  val table = Iru5SparkCommitInterop.table(spark, qualified)
  val current = table.currentSnapshot()
  val rebuiltDelete = table.snapshot(current.parentId())
  val external = table.snapshot(rebuiltDelete.parentId())
  val original = table.snapshot(external.parentId())
  val oldDelete = table.snapshot(original.parentId())
  require(table.snapshots().asScala.map(_.snapshotId()).toSet ==
    Set(current, rebuiltDelete, external, original, oldDelete).map(_.snapshotId()),
    "Expected exactly the original pair, one external commit and one rebuilt pair")
  require(current.operation() == "append" && rebuiltDelete.operation() == "delete")
  require(original.summary().containsKey("novarocks.doc.v2.record.novarocks.mv~publication") &&
    !external.summary().containsKey("novarocks.doc.v2.record.novarocks.mv~publication") &&
    current.summary().containsKey("novarocks.doc.v2.record.novarocks.mv~publication") &&
    current.summary().get("novarocks.doc.v2.record.novarocks.mv~publication") != original.summary().get("novarocks.doc.v2.record.novarocks.mv~publication"))
  require(oldDelete.summary().containsKey("novarocks.mv.full-refresh-stage.v1") &&
    rebuiltDelete.summary().containsKey("novarocks.mv.full-refresh-stage.v1"))
  val rows = spark.sql(s"SELECT k, v FROM $qualified ORDER BY k, v").collect().toVector
  require(rows.map(r => (r.getString(0), r.getLong(1))) == Vector(("east", 10L), ("west", 20L)))
}
SPARK_SCALA
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala"

-- query 9
-- An unpublished MV with the first slot snapshot still requires a full build.
-- @skip_result_check=true
CREATE MATERIALIZED VIEW refusal_mv WITH (refresh_policy = 'MANUAL') AS SELECT k, v FROM fact;
ALTER MATERIALIZED VIEW refusal_mv SET PROPERTIES refresh_paused = false;

-- query 10
-- @result_contains=MV_RETRY_LIMIT_SET
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-mv-retry-limit-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
printf '%s\n' "ALTER TABLE ice_rest.ns_${uuid0}.refusal_mv SET TBLPROPERTIES ('commit.retry.num-retries'='0');" > "$tmp_sql"
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf '%s\n' MV_RETRY_LIMIT_SET

-- query 11
-- @result_contains=MV_OCC_BASELINE
shell: set -eu
curl --silent --show-error --fail '${iceberg_rest_uri}/v1/namespaces/ns_${uuid0}/tables/refusal_mv' \
  | python3 -c 'import json, sys; json.dump(json.load(sys.stdin)["metadata"], open(sys.argv[1], "w")); print("MV_OCC_BASELINE")' "${TMPDIR:-/tmp}/novarocks-mv-no-retry-${uuid0}.json"

-- query 12
-- @db=mvretry_${uuid0}.ns_${uuid0}
-- @publication_service_hold=ns_${uuid0}.refusal_mv
-- @publication_catalog_concurrent_shell=set -eu; tmp_sql=$(mktemp "${TMPDIR:-/tmp}/novarocks-mv-occ-XXXXXX.sql"); trap 'rm -f "$tmp_sql"' EXIT; printf '%s\n' "INSERT INTO ice_rest.ns_${uuid0}.refusal_mv VALUES ('external', -1);" > "$tmp_sql"; "${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
-- @expect_trino_error=TRANSACTION_CONFLICT,USER_ERROR
-- @expect_error=Failed to commit the transaction during refresh materialized view:
REFRESH MATERIALIZED VIEW refusal_mv;

-- query 13
-- @result_contains=MV_OCC_FIXED
shell: set -eu
baseline="${TMPDIR:-/tmp}/novarocks-mv-no-retry-${uuid0}.json"
trap 'rm -f "$baseline"' EXIT
curl --silent --show-error --fail '${iceberg_rest_uri}/v1/namespaces/ns_${uuid0}/tables/refusal_mv' \
  | python3 -c '
import json
import sys

before = json.load(open(sys.argv[1]))
after = json.load(sys.stdin)["metadata"]
old = before["current-snapshot-id"]
prior = {s["snapshot-id"]: s for s in before["snapshots"]}
actual = {s["snapshot-id"]: s for s in after["snapshots"]}
assert all(actual.get(snapshot_id) == snapshot for snapshot_id, snapshot in prior.items())
added = [snapshot for snapshot_id, snapshot in actual.items() if snapshot_id not in prior]
assert len(added) == 1 and added[0]["snapshot-id"] == after["current-snapshot-id"]
external = added[0]
assert external["parent-snapshot-id"] == old
assert "novarocks.doc.v2.record.novarocks.mv~publication" not in external["summary"]
assert "novarocks.mv.full-refresh-stage.v1" not in external["summary"]
for key in ("current-schema-id", "default-spec-id", "schemas", "partition-specs"):
    assert before[key] == after[key], key
assert {k: v for k, v in before["properties"].items() if k.startswith("novarocks.doc.")} == {
    k: v for k, v in after["properties"].items() if k.startswith("novarocks.doc.")}
print("MV_OCC_FIXED")
' "$baseline"

-- query 14
-- @skip_result_check=true
CREATE MATERIALIZED VIEW repartition_mv WITH (refresh_policy = 'MANUAL') AS SELECT k, v FROM fact;
REFRESH MATERIALIZED VIEW repartition_mv;

-- query 15
-- @result_contains=MV_OCC_BASELINE
shell: set -eu
curl --silent --show-error --fail '${iceberg_rest_uri}/v1/namespaces/ns_${uuid0}/tables/repartition_mv' \
  | python3 -c 'import json, sys; json.dump(json.load(sys.stdin)["metadata"], open(sys.argv[1], "w")); print("MV_OCC_BASELINE")' "${TMPDIR:-/tmp}/novarocks-mv-repartition-${uuid0}.json"

-- query 16
-- @db=mvretry_${uuid0}.ns_${uuid0}
-- @publication_service_hold=ns_${uuid0}.repartition_mv
-- @publication_catalog_concurrent_shell=set -eu; tmp_sql=$(mktemp "${TMPDIR:-/tmp}/novarocks-mv-occ-XXXXXX.sql"); trap 'rm -f "$tmp_sql"' EXIT; printf '%s\n' "DELETE FROM ice_rest.ns_${uuid0}.repartition_mv WHERE k = 'east';" > "$tmp_sql"; "${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
-- @expect_trino_error=ICEBERG_COMMIT_ERROR,EXTERNAL
-- @expect_error=Failed to commit the transaction during refresh materialized view: Materialized view storage table changed during repartition
ALTER MATERIALIZED VIEW repartition_mv SET PROPERTIES partitioning = ARRAY['truncate(k, 2)'];

-- query 17
-- @result_contains=MV_OCC_FIXED
shell: set -eu
baseline="${TMPDIR:-/tmp}/novarocks-mv-repartition-${uuid0}.json"
trap 'rm -f "$baseline"' EXIT
curl --silent --show-error --fail '${iceberg_rest_uri}/v1/namespaces/ns_${uuid0}/tables/repartition_mv' \
  | python3 -c '
import json
import sys

before = json.load(open(sys.argv[1]))
after = json.load(sys.stdin)["metadata"]
old = before["current-snapshot-id"]
prior = {s["snapshot-id"]: s for s in before["snapshots"]}
actual = {s["snapshot-id"]: s for s in after["snapshots"]}
assert all(actual.get(snapshot_id) == snapshot for snapshot_id, snapshot in prior.items())
added = [snapshot for snapshot_id, snapshot in actual.items() if snapshot_id not in prior]
assert len(added) == 1 and added[0]["snapshot-id"] == after["current-snapshot-id"]
external = added[0]
assert external["parent-snapshot-id"] == old
assert "novarocks.doc.v2.record.novarocks.mv~publication" not in external["summary"]
assert "novarocks.mv.full-refresh-stage.v1" not in external["summary"]
for key in ("current-schema-id", "default-spec-id", "schemas", "partition-specs"):
    assert before[key] == after[key], key
assert {k: v for k, v in before["properties"].items() if k.startswith("novarocks.doc.")} == {
    k: v for k, v in after["properties"].items() if k.startswith("novarocks.doc.")}
print("MV_OCC_FIXED")
' "$baseline"

-- query 18
-- @cleanup=true
-- @skip_result_check=true
shell: rm -f "${TMPDIR:-/tmp}/novarocks-mv-no-retry-${uuid0}.json" "${TMPDIR:-/tmp}/novarocks-mv-repartition-${uuid0}.json"

-- query 19
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS mvretry_${uuid0}.ns_${uuid0}.repartition_mv;
DROP MATERIALIZED VIEW IF EXISTS mvretry_${uuid0}.ns_${uuid0}.refusal_mv;

-- query 20
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS mvretry_${uuid0}.ns_${uuid0}.target_mv;
DROP TABLE IF EXISTS mvretry_${uuid0}.ns_${uuid0}.fact;
DROP SCHEMA IF EXISTS mvretry_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS mvretry_${uuid0};
