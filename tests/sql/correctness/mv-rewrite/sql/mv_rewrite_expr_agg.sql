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
-- @tags=optimizer,mv,rewrite,iceberg
-- Test Objective (migrated from materialized-view/test_materialized_view_rewrite2,
-- test_mv_rewrite_with_agg and test_sync_materialized_view_rewrite_with_case_when):
-- Expression aggregates and rollup edge semantics:
-- 1. SUM(IF(cond, col, 0)) matches structurally (FunctionCall path) for both
--    direct 1:1 and strict-subset rollup, including range compensation on a
--    group-by key (open range / equality / fully-below-range empty result).
-- 2. SUM(CASE WHEN ... THEN ... ELSE ... END) matches structurally (Case
--    normalization) for direct and rollup hits; a DIFFERENT case condition
--    must not match.
-- 3. Conditional-aggregate derivation (query SUM(CASE WHEN k6>1 ...) onto an
--    MV storing only plain SUM(k9) grouped by (k1,k6)) is NOT supported ->
--    deterministic miss.
-- 4. HAVING in the query is applied above the rewritten rollup aggregate.
-- 5. AVG: allowed in direct 1:1 group-by-equal mapping, rejected for
--    strict-subset rollup (no sum/count decomposition in the MVP).
--
-- Data scale: ~2400 rows so the MV alternatives are a genuine cost win.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mvrw_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/mvrw_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.enable_path_style_access" = "true"
);

-- query 2
-- @skip_result_check=true
CREATE DATABASE mvrw_${uuid0}.ns_${uuid0};

-- query 3
-- k1: 4 distinct dates; k6: small int driving the conditions; k9: payload
-- @skip_result_check=true
CREATE TABLE mvrw_${uuid0}.ns_${uuid0}.t1 (
  k1 STRING,
  k6 INT,
  k9 BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- @skip_result_check=true
INSERT INTO mvrw_${uuid0}.ns_${uuid0}.t1
SELECT
  concat('2023-08-', CAST(10 + (n % 4) AS STRING)) AS k1,
  CAST(n % 5 AS INT) AS k6,
  CAST(n % 100 AS BIGINT) AS k9
FROM TABLE(generate_series(1, 2400)) t(n);

-- query 5
-- @skip_result_check=true
SET CATALOG mvrw_${uuid0};

-- query 6
-- @skip_result_check=true
USE ns_${uuid0};

-- query 7
-- plain-SUM MV grouped by (k1, k6): the conditional-derivation miss guard
-- @skip_result_check=true
CREATE MATERIALIZED VIEW mv_plain
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, k6, SUM(k9) AS s9 FROM t1 GROUP BY k1, k6;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_plain WITH SYNC MODE;

-- query 9
-- deriving SUM(CASE WHEN k6 > 1 ...) from plain SUM(k9)+group-key k6 would
-- require conditional-aggregate lifting -> unsupported, deterministic miss
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT k1, SUM(CASE WHEN k6 > 1 THEN k9 ELSE 0 END) FROM t1 GROUP BY k1;

-- query 10
SELECT k1, SUM(CASE WHEN k6 > 1 THEN k9 ELSE 0 END) AS s FROM t1 GROUP BY k1 ORDER BY k1;

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW mv_plain;

-- query 12
-- expression-aggregate MV: SUM(IF(...)), SUM(CASE WHEN ...), SUM, COUNT, AVG
-- @skip_result_check=true
CREATE MATERIALIZED VIEW mv_cond
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT
  k1,
  k6,
  SUM(IF(k6 > 1, k9, 0)) AS s_if,
  SUM(CASE WHEN k6 > 1 THEN k9 ELSE 0 END) AS s_case,
  SUM(k9) AS s9,
  COUNT(k9) AS c9,
  AVG(k9) AS a9
FROM t1 GROUP BY k1, k6;

-- query 13
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_cond WITH SYNC MODE;

-- query 14
-- direct 1:1: identical expression aggregates, group-by equal (AVG included)
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: mv_cond
SELECT k1, k6, SUM(IF(k6 > 1, k9, 0)), SUM(CASE WHEN k6 > 1 THEN k9 ELSE 0 END), AVG(k9) FROM t1 GROUP BY k1, k6;

-- query 15
SELECT k1, k6, SUM(IF(k6 > 1, k9, 0)) AS s_if, SUM(CASE WHEN k6 > 1 THEN k9 ELSE 0 END) AS s_case, AVG(k9) AS a9
FROM t1 GROUP BY k1, k6 ORDER BY k1, k6;

-- query 16
-- strict-subset rollup of the expression aggregates (SUM->SUM, args matched
-- structurally)
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: mv_cond
SELECT k1, SUM(IF(k6 > 1, k9, 0)), SUM(CASE WHEN k6 > 1 THEN k9 ELSE 0 END) FROM t1 GROUP BY k1;

-- query 17
SELECT k1, SUM(IF(k6 > 1, k9, 0)) AS s_if, SUM(CASE WHEN k6 > 1 THEN k9 ELSE 0 END) AS s_case
FROM t1 GROUP BY k1 ORDER BY k1;

-- query 18
-- a DIFFERENT case condition (k6 >= 1) must not match structurally
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT k1, SUM(CASE WHEN k6 >= 1 THEN k9 ELSE 0 END) FROM t1 GROUP BY k1;

-- query 19
SELECT k1, SUM(CASE WHEN k6 >= 1 THEN k9 ELSE 0 END) AS s FROM t1 GROUP BY k1 ORDER BY k1;

-- query 20
-- range compensation on group-by key k1: open range
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: mv_cond
SELECT k1, SUM(k9) FROM t1 WHERE k1 >= '2023-08-12' GROUP BY k1;

-- query 21
SELECT k1, SUM(k9) AS s FROM t1 WHERE k1 >= '2023-08-12' GROUP BY k1 ORDER BY k1;

-- query 22
-- equality compensation on group-by key k1
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: mv_cond
SELECT k1, SUM(k9), COUNT(k9) FROM t1 WHERE k1 = '2023-08-11' GROUP BY k1;

-- query 23
SELECT k1, SUM(k9) AS s, COUNT(k9) AS c FROM t1 WHERE k1 = '2023-08-11' GROUP BY k1 ORDER BY k1;

-- query 24
-- range below all data: rewritten plan must return the empty set
SELECT k1, SUM(k9) AS s FROM t1 WHERE k1 < '2023-08-10' GROUP BY k1 ORDER BY k1;

-- query 25
-- HAVING stays above the rewritten rollup aggregate
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: mv_cond
SELECT k1, SUM(k9) AS s FROM t1 GROUP BY k1 HAVING SUM(k9) > 1000;

-- query 26
SELECT k1, SUM(k9) AS s FROM t1 GROUP BY k1 HAVING SUM(k9) > 1000 ORDER BY k1;

-- query 27
-- AVG with a coarser group-by needs sum/count decomposition -> unsupported
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT k1, AVG(k9) FROM t1 GROUP BY k1;

-- query 28
SELECT k1, AVG(k9) AS a FROM t1 GROUP BY k1 ORDER BY k1;

-- query 29
-- @skip_result_check=true
DROP MATERIALIZED VIEW mv_cond;

-- query 30
-- @skip_result_check=true
DROP TABLE mvrw_${uuid0}.ns_${uuid0}.t1 FORCE;

-- query 31
-- @skip_result_check=true
DROP DATABASE mvrw_${uuid0}.ns_${uuid0};

-- query 32
-- @skip_result_check=true
DROP CATALOG mvrw_${uuid0};
