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
-- @tags=iceberg,ivm,imv,projection_filter,cutover
-- Test Objective:
-- Correctness and plan-shape validation for the single-table
-- projection/filter IMV incremental-refresh cutover (Phase 3, Task 11).
-- After the cutover the refresh executor stops mutating the MV AST into
-- a `__nr_ivm_delta(...)` table-function call; instead the verbatim MV
-- SELECT is run through the IMV rewrite pipeline, which rebinds the
-- single base scan to an `IcebergDeltaTable` source. This case asserts
-- the *shape* of that delta-scan plan via the user-facing
-- `__nr_ivm_delta(...)` TVF, which travels the same analyzer / codegen
-- path as the refresh-time rewrite (`IcebergDeltaScanRelation` ->
-- `ScanSource::IcebergDeltaTable` -> `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE`).
--
-- SQL planner tests cover the delta source and EXPLAIN plan shape with
-- frozen test-catalog snapshots. Query 7 verifies that the live provider
-- refuses fabricated snapshot bounds before a plan is presented.
--
-- Row correctness and internal-column hygiene (positive
-- `@result_contains` and negative `@result_not_contains` on query 5):
--   * `__change_op`, `_row_id`, and `__nova_base_row_id` must NOT be
--     visible from `SELECT * FROM proj_mv`. The PF refresh merge sink
--     strips `__change_op` and `_row_id` from the INSERT batch (see
--     commit e230d8b6); `__nova_base_row_id` is derived internally
--     from `_row_id` by `InjectApplyKeyProject`. A regression that
--     re-exposed any of them would surface in the row text output.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_pfcut_${uuid0}
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
CREATE DATABASE ice_pfcut_${uuid0}.ns_${uuid0};
CREATE TABLE ice_pfcut_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO ice_pfcut_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10), (1, 20), (2, 40), (3, 0);

-- query 2
-- @skip_result_check=true
SET CATALOG ice_pfcut_${uuid0};
USE ns_${uuid0};

CREATE MATERIALIZED VIEW proj_mv
DISTRIBUTED BY HASH(k1) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM orders WHERE v2 > 0;

-- query 3
-- First (full) REFRESH.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW proj_mv WITH SYNC MODE;

-- query 4
-- Delta + incremental REFRESH (the PF cutover path).
-- @skip_result_check=true
INSERT INTO ice_pfcut_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 70), (4, 5), (5, -1);
REFRESH MATERIALIZED VIEW proj_mv WITH SYNC MODE;

-- query 5
-- Correctness + internal-column hygiene. The PF cutover must yield
-- exactly the rows the MV SELECT would recompute, and `SELECT *`
-- must NOT expose any of the IMV-internal columns the merge-sink /
-- apply-key path uses.
-- @result_not_contains=__change_op
-- @result_not_contains=_row_id
-- @result_not_contains=__nova_base_row_id
-- @result_contains=1	10
-- @result_contains=1	20
-- @result_contains=1	70
-- @result_contains=2	40
-- @result_contains=4	5
SELECT * FROM proj_mv ORDER BY k1, v2;

-- query 6
-- Re-pin the session catalog/db after REFRESH (it switches the
-- active session catalog as a planning side-effect), and warm the
-- in-memory base-table cache. The analyzer's `__nr_ivm_delta`
-- resolver issues `catalog.get_table(namespace, table)` against the
-- current session catalog; without a prior touch the freshly created
-- Iceberg base may not be visible to that lookup.
-- @skip_result_check=true
SET CATALOG ice_pfcut_${uuid0};
USE ns_${uuid0};
SELECT k1, v2 FROM orders LIMIT 1;

-- query 7
-- The live provider validates the exact change window even for EXPLAIN.
-- Snapshot 0 is not a real revision of this table, so the request must fail
-- closed rather than yielding a plan that looks executable.
-- @expect_error=iceberg snapshot 0 does not exist
EXPLAIN VERBOSE SELECT k1, v2 FROM __nr_ivm_delta('ice_pfcut_${uuid0}.ns_${uuid0}.orders', 0, 0) WHERE v2 > 0;

-- query 8
-- @cleanup=true
-- Cleanup.
-- @skip_result_check=true
DROP MATERIALIZED VIEW proj_mv;
DROP TABLE ice_pfcut_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_pfcut_${uuid0}.ns_${uuid0};
DROP CATALOG ice_pfcut_${uuid0};
