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
CREATE CATALOG mvincr_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');

-- query 2
-- @skip_result_check=true
CREATE SCHEMA mvincr_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvincr_${uuid0}.ns_${uuid0}.fact (k VARCHAR, v BIGINT) WITH (format_version = 3);
INSERT INTO mvincr_${uuid0}.ns_${uuid0}.fact VALUES ROW('east', 10);

-- query 4
-- @skip_result_check=true
USE mvincr_${uuid0}.ns_${uuid0};
CREATE MATERIALIZED VIEW target_mv WITH (refresh_policy = 'MANUAL') AS SELECT k, v FROM fact;

-- query 5
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=1,slot-changes=0,full-refreshes=1
REFRESH MATERIALIZED VIEW target_mv;

-- query 6
-- @skip_result_check=true
INSERT INTO mvincr_${uuid0}.ns_${uuid0}.fact VALUES ROW('west', 20);

-- query 7
-- @result_contains=MV_OCC_BASELINE
shell: set -eu
curl --silent --show-error --fail '${iceberg_rest_uri}/v1/namespaces/ns_${uuid0}/tables/target_mv' \
  | python3 -c 'import json, sys; json.dump(json.load(sys.stdin)["metadata"], open(sys.argv[1], "w")); print("MV_OCC_BASELINE")' "${TMPDIR:-/tmp}/novarocks-mv-incremental-${uuid0}.json"

-- query 8
-- Make the already-dispatched incremental write lose to a physical advance.
-- @publication_service_hold=ns_${uuid0}.target_mv
-- @publication_catalog_concurrent_shell=set -eu; tmp_sql=$(mktemp "${TMPDIR:-/tmp}/novarocks-mv-occ-XXXXXX.sql"); trap 'rm -f "$tmp_sql"' EXIT; printf '%s\n' "DELETE FROM ice_rest.ns_${uuid0}.target_mv WHERE k = 'east';" > "$tmp_sql"; "${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
-- @expect_trino_error=ICEBERG_COMMIT_ERROR,EXTERNAL
-- @expect_error=Failed to commit the transaction during refresh materialized view: Materialized view storage table changed during incremental refresh
REFRESH MATERIALIZED VIEW mvincr_${uuid0}.ns_${uuid0}.target_mv;

-- query 9
-- @result_contains=MV_OCC_FIXED
shell: set -eu
baseline="${TMPDIR:-/tmp}/novarocks-mv-incremental-${uuid0}.json"
trap 'rm -f "$baseline"' EXIT
curl --silent --show-error --fail '${iceberg_rest_uri}/v1/namespaces/ns_${uuid0}/tables/target_mv' \
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

-- query 10
-- @cleanup=true
-- @skip_result_check=true
shell: rm -f "${TMPDIR:-/tmp}/novarocks-mv-incremental-${uuid0}.json"

-- query 11
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS mvincr_${uuid0}.ns_${uuid0}.target_mv;
DROP TABLE IF EXISTS mvincr_${uuid0}.ns_${uuid0}.fact;
DROP SCHEMA IF EXISTS mvincr_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS mvincr_${uuid0};
