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
-- @tags=iceberg,metadata_depth,schema_history
-- MinIO-only Hadoop test catalog (ADR-0196).
-- This plain-table catalog test uses 63 ARRAY containers plus INT (logical64).
-- Native id-only and non-NULL deep appends run before DROP; the saved tag
-- retains the exact deep schema. An independent SDK reads both complete bags.

-- query 1
-- @skip_result_check=true
-- The Hadoop object-store test is admitted only on the MinIO fixture.
shell: test "${fixture_object_store_kind}" = minio

-- query 2
-- @skip_result_check=true
CREATE EXTERNAL CATALOG hadoopmetadata64_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_test_warehouse}/metadata64_hadoop_${uuid0}",
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
CREATE DATABASE hadoopmetadata64_${uuid0}.${case_db};
SET CATALOG hadoopmetadata64_${uuid0};
USE ${case_db};

-- query 3
-- @skip_result_check=true
CREATE TABLE metadata_depth_native (id BIGINT,deep ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<INT>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>)
TBLPROPERTIES ("format-version"="3","write.row-lineage"="true");

-- query 4
-- @skip_result_check=true
INSERT INTO metadata_depth_native(id) VALUES (1),(1),(2);
INSERT INTO metadata_depth_native(id,deep) VALUES (4,[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[7]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]);

-- query 5
SELECT id,deep FROM metadata_depth_native ORDER BY id;

-- query 6
SELECT typeof(deep[1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1][1]) AS leaf_type,COUNT(*) AS row_count FROM metadata_depth_native GROUP BY 1;

-- query 7
-- @skip_result_check=true
ALTER TABLE metadata_depth_native CREATE TAG depth_before_drop;
ALTER TABLE metadata_depth_native DROP COLUMN deep;
INSERT INTO metadata_depth_native(id) VALUES (3);

-- query 8
SELECT id FROM metadata_depth_native ORDER BY id;

-- query 9
SELECT id,deep FROM metadata_depth_native FOR VERSION AS OF 'depth_before_drop' ORDER BY id;

-- query 10
-- @skip_result_check=true
-- @result_contains=HADOOP_METADATA_DEPTH_OBSERVED
shell: set -eu
workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
receipts="$workspace/tests/sql/.runtime/iceberg-read-policy/${uuid0}"
mkdir -p "$receipts"
scala_file="$receipts/hadoop-observe.scala"
cat "$workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$workspace/tests/sql/fixtures/iceberg-read-policy/fixture.scala" > "$scala_file"
cat >> "$scala_file" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "${case_db}", "hadoop_read_policy")
  MetadataHistoryFixture.hadoopObserve("${case_db}", "${iceberg_test_warehouse}/metadata64_hadoop_${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$workspace/docker/iceberg-rest/spark-shell.sh" "$scala_file" > "$receipts/hadoop-observe.log" 2>&1; then tail -80 "$receipts/hadoop-observe.log" >&2; exit 1; fi
grep -q '^HADOOP_METADATA_DEPTH_OBSERVED$' "$receipts/hadoop-observe.log" || { tail -80 "$receipts/hadoop-observe.log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$receipts/hadoop-observe.log" > "$receipts/hadoop-observe.jsonl"
printf 'HADOOP_METADATA_DEPTH_OBSERVED\n'

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
DROP TABLE IF EXISTS hadoopmetadata64_${uuid0}.${case_db}.metadata_depth_rejected FORCE;
DROP TABLE IF EXISTS hadoopmetadata64_${uuid0}.${case_db}.metadata_depth_native FORCE;
DROP DATABASE hadoopmetadata64_${uuid0}.${case_db};
DROP CATALOG hadoopmetadata64_${uuid0};
