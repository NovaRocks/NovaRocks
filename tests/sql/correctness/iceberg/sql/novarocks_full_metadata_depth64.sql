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

-- Maximum-depth coverage is temporarily outside the supported scope (2026-11-03).
-- Exact case quarantine: tools/ci/baselines/dispositions.json. Local B validation
-- uses --skip; preserve this input and its golden until explicit capability review.
-- @sequential=true
-- @order_sensitive=true
-- @tags=iceberg,schema_history,metadata_depth
-- The SDK schema has 63 ARRAY containers and one INT leaf (logical depth64).
-- Table schema JSON stays inside the SDK manifest decoder's boundary; the
-- ARROW:schema hint still exceeds FlatBuffer's default depth. Native NULL and
-- non-NULL appends, CTAS and historical reads must preserve the SDK-fixed bags.

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
SELECT typeof(deep[1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1]) AS leaf_type,COUNT(*) AS row_count FROM metadata_depth GROUP BY 1;

-- query 5
-- @skip_result_check=true
INSERT INTO metadata_depth(id) VALUES (3);
INSERT INTO metadata_depth(id,deep) VALUES (5,[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[7]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]);
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
SELECT id,deep FROM metadata_depth FOR VERSION AS OF 'depth_initial' ORDER BY id;

-- query 11
-- @expect_error_tier=drift
-- @expect_error=Iceberg schema JSON exceeds 127 container levels
CREATE TABLE metadata_depth_rejected (id BIGINT,deep STRUCT<n1 STRUCT<n2 STRUCT<n3 STRUCT<n4 STRUCT<n5 STRUCT<n6 STRUCT<n7 STRUCT<n8 STRUCT<n9 STRUCT<n10 STRUCT<n11 STRUCT<n12 STRUCT<n13 STRUCT<n14 STRUCT<n15 STRUCT<n16 STRUCT<n17 STRUCT<n18 STRUCT<n19 STRUCT<n20 STRUCT<n21 STRUCT<n22 STRUCT<n23 STRUCT<n24 STRUCT<n25 STRUCT<n26 STRUCT<n27 STRUCT<n28 STRUCT<n29 STRUCT<n30 STRUCT<n31 STRUCT<n32 STRUCT<n33 STRUCT<n34 STRUCT<n35 STRUCT<n36 STRUCT<n37 STRUCT<n38 STRUCT<n39 STRUCT<n40 STRUCT<n41 STRUCT<n42 STRUCT<n43 STRUCT<n44 STRUCT<n45 STRUCT<n46 STRUCT<n47 STRUCT<n48 STRUCT<n49 STRUCT<n50 STRUCT<n51 STRUCT<n52 STRUCT<n53 STRUCT<n54 STRUCT<n55 STRUCT<n56 STRUCT<n57 STRUCT<n58 STRUCT<n59 STRUCT<n60 STRUCT<n61 STRUCT<n62 STRUCT<n63 INT>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>);

-- query 12
-- @skip_result_check=true
-- A same-name CREATE without IF NOT EXISTS proves rejection left no table.
CREATE TABLE metadata_depth_rejected (id BIGINT);
DROP TABLE metadata_depth_rejected;

-- query 13
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS metadata64_${uuid0}.${case_db}.metadata_depth_rejected FORCE;
DROP TABLE IF EXISTS metadata64_${uuid0}.${case_db}.metadata_depth_copy FORCE;
DROP TABLE IF EXISTS metadata64_${uuid0}.${case_db}.metadata_depth FORCE;
DROP DATABASE metadata64_${uuid0}.${case_db};
DROP CATALOG metadata64_${uuid0};
