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
-- @order_sensitive=true
-- @tags=iceberg,field_domain,schema_history,metadata_depth
-- Semantic depth is exactly 64, independently verified from actual SDK fields.
-- Full TableMetadata schemas exceed serde's structural 128 depth. Native append
-- and CTAS commit/load run before the SDK drops the deep field; later loads must
-- still decode its accurately retained schema and domain history.
-- SDK/Parquet complete bags use original raw STRING/INT carriers, not engine codecs.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG metadata64_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-metadata.name" = "${iceberg_object_store_metadata_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-data.name" = "${iceberg_object_store_data_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.region" = "${oss_region}",
  "aws.s3.enable_path_style_access" = "${oss_path_style}"
);
CREATE DATABASE metadata64_${uuid0}.${case_db};
SET CATALOG metadata64_${uuid0};
USE ${case_db};
SET enable_materialized_view_rewrite = false;

-- query 2
-- @skip_result_check=true
-- @result_contains=METADATA_DEPTH_READY
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:?resolved fixture publication required}")"
uea_receipts="$uea_workspace/tests/sql/.runtime/iceberg-read-policy/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/initial.scala"
uea_log="$uea_receipts/initial.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/iceberg-read-policy/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
    DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "${case_db}", "metadata_history")
    MetadataHistoryFixture.depthInitialize("${case_db}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^METADATA_DEPTH_READY$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/initial.jsonl"
printf 'METADATA_DEPTH_READY\n'

-- query 3
SELECT id,deep FROM metadata_depth ORDER BY id;

-- query 4
SELECT typeof(deep.n1.n2.n3.n4.n5.n6.n7.n8.n9.n10.n11.n12.n13.n14.n15.n16.n17.n18.n19.n20.n21.n22.n23.n24.n25.n26.n27.n28.n29.n30.n31.n32.n33.n34.n35.n36.n37.n38.n39.n40.n41.n42.n43.n44.n45.n46.n47.n48.n49.n50.n51.n52.n53.n54.n55.n56.n57.n58.n59.n60.n61.n62.n63) AS leaf_type,COUNT(*) AS row_count FROM metadata_depth GROUP BY 1;

-- query 5
-- @skip_result_check=true
INSERT INTO metadata_depth(id) VALUES (3);
CREATE TABLE metadata_depth_copy AS SELECT id,deep FROM metadata_depth;

-- query 6
SELECT id,deep FROM metadata_depth_copy ORDER BY id;

-- query 7
-- @skip_result_check=true
-- @result_contains=METADATA_DEPTH_NATIVE_COMMIT_OBSERVED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:?resolved fixture publication required}")"
uea_receipts="$uea_workspace/tests/sql/.runtime/iceberg-read-policy/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/native-commit.scala"
uea_log="$uea_receipts/native-commit.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/iceberg-read-policy/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
    DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "${case_db}", "metadata_history")
    MetadataHistoryFixture.depthObserveAppend("${case_db}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^METADATA_DEPTH_NATIVE_COMMIT_OBSERVED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/native-commit.jsonl"
printf 'METADATA_DEPTH_NATIVE_COMMIT_OBSERVED\n'

-- query 8
-- @skip_result_check=true
-- @result_contains=METADATA_DEPTH_HISTORY_READY
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:?resolved fixture publication required}")"
uea_receipts="$uea_workspace/tests/sql/.runtime/iceberg-read-policy/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/retained.scala"
uea_log="$uea_receipts/retained.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/iceberg-read-policy/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
    DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "${case_db}", "metadata_history")
    MetadataHistoryFixture.depthRetainHistory("${case_db}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^METADATA_DEPTH_HISTORY_READY$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/retained.jsonl"
printf 'METADATA_DEPTH_HISTORY_READY\n'

-- query 9
SELECT id FROM metadata_depth ORDER BY id;

-- query 10
SELECT id FROM metadata_depth FOR VERSION AS OF 'depth_initial' ORDER BY id;

-- query 11
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS metadata64_${uuid0}.${case_db}.metadata_depth_copy FORCE;
DROP TABLE IF EXISTS metadata64_${uuid0}.${case_db}.metadata_depth FORCE;
DROP DATABASE metadata64_${uuid0}.${case_db};
DROP CATALOG metadata64_${uuid0};
