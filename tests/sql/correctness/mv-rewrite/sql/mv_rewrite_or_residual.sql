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
-- Test Objective (migrated from materialized-view/test_mv_or_predicate_rewrite
-- and the residual-predicate slice of test_materialized_view_union_all_rewrite):
-- Residual-predicate (exact-match) semantics over WHERE-scoped MVs:
-- 1. A top-level OR predicate matches exactly, including with the arms in the
--    opposite order (commutative normalization) and with extra query-only
--    conjuncts re-applied as compensation.
-- 2. OR implication (query range inside one disjunct) is NOT supported ->
--    deterministic miss.
-- 3. AND-of-ORs: exact match hits; the implication query misses.
-- 4. `!=` residuals: exact match hits; `!=` with a different constant, or a
--    range the residual would only imply, misses.
-- 5. On an aggregate MV with a residual filter, a query-only residual on a
--    group-by key is re-applied as compensation.
--
-- Data scale: ~2400 rows so the SPJ MVs (roughly half the base rows) and the
-- aggregate MV are real cost wins.

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
-- @skip_result_check=true
CREATE TABLE mvrw_${uuid0}.ns_${uuid0}.lineorder (
  lo_orderkey INT,
  lo_linenumber INT,
  lo_quantity INT,
  lo_revenue INT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- @skip_result_check=true
INSERT INTO mvrw_${uuid0}.ns_${uuid0}.lineorder
SELECT
  CAST(10000 + (n % 8) AS INT) AS lo_orderkey,
  CAST(n % 5 AS INT) AS lo_linenumber,
  CAST(n % 50 AS INT) AS lo_quantity,
  CAST(n % 1000 AS INT) AS lo_revenue
FROM TABLE(generate_series(1, 2400)) t(n);

-- query 5
-- @skip_result_check=true
SET CATALOG mvrw_${uuid0};

-- query 6
-- @skip_result_check=true
USE ns_${uuid0};

-- query 7
-- SPJ MV with a top-level OR filter
-- @skip_result_check=true
CREATE MATERIALIZED VIEW or_mv
DISTRIBUTED BY HASH(lo_orderkey) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT lo_orderkey, lo_linenumber, lo_quantity, lo_revenue
FROM lineorder
WHERE lo_orderkey > 10003 OR lo_linenumber < 2;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW or_mv WITH SYNC MODE;

-- query 9
-- exact OR match
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: or_mv
SELECT lo_orderkey, lo_linenumber, lo_quantity FROM lineorder WHERE lo_orderkey > 10003 OR lo_linenumber < 2;

-- query 10
SELECT lo_orderkey, lo_linenumber, COUNT(*) AS c
FROM lineorder WHERE lo_orderkey > 10003 OR lo_linenumber < 2
GROUP BY lo_orderkey, lo_linenumber ORDER BY lo_orderkey, lo_linenumber;

-- query 11
-- arms in the opposite order: commutative normalization must still match
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: or_mv
SELECT lo_orderkey, lo_linenumber FROM lineorder WHERE lo_linenumber < 2 OR lo_orderkey > 10003;

-- query 12
-- exact OR plus a query-only conjunct -> compensation above the MV scan
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: or_mv
SELECT lo_orderkey, lo_quantity FROM lineorder WHERE (lo_orderkey > 10003 OR lo_linenumber < 2) AND lo_quantity >= 30;

-- query 13
SELECT lo_orderkey, COUNT(*) AS c
FROM lineorder WHERE (lo_orderkey > 10003 OR lo_linenumber < 2) AND lo_quantity >= 30
GROUP BY lo_orderkey ORDER BY lo_orderkey;

-- query 14
-- OR implication (range inside one disjunct) is unsupported -> miss
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT lo_orderkey FROM lineorder WHERE lo_orderkey > 10004;

-- query 15
SELECT lo_orderkey, COUNT(*) AS c FROM lineorder WHERE lo_orderkey > 10004 GROUP BY lo_orderkey ORDER BY lo_orderkey;

-- query 16
-- single-arm query (the other disjunct alone) is also only implied -> miss
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT lo_linenumber FROM lineorder WHERE lo_linenumber < 2;

-- query 17
-- @skip_result_check=true
DROP MATERIALIZED VIEW or_mv;

-- query 18
-- AND-of-ORs MV
-- @skip_result_check=true
CREATE MATERIALIZED VIEW andor_mv
DISTRIBUTED BY HASH(lo_orderkey) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT lo_orderkey, lo_linenumber, lo_revenue
FROM lineorder
WHERE (lo_orderkey > 10003 OR lo_linenumber > 2) AND (lo_orderkey < 10006 OR lo_linenumber < 1);

-- query 19
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW andor_mv WITH SYNC MODE;

-- query 20
-- exact AND-of-ORs match
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: andor_mv
SELECT lo_orderkey, lo_linenumber FROM lineorder
WHERE (lo_orderkey > 10003 OR lo_linenumber > 2) AND (lo_orderkey < 10006 OR lo_linenumber < 1);

-- query 21
SELECT lo_orderkey, lo_linenumber, COUNT(*) AS c FROM lineorder
WHERE (lo_orderkey > 10003 OR lo_linenumber > 2) AND (lo_orderkey < 10006 OR lo_linenumber < 1)
GROUP BY lo_orderkey, lo_linenumber ORDER BY lo_orderkey, lo_linenumber;

-- query 22
-- the conjunction of single arms is only implied by the MV predicate -> miss
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT lo_orderkey FROM lineorder WHERE lo_orderkey > 10003 AND lo_linenumber < 1;

-- query 23
-- @skip_result_check=true
DROP MATERIALIZED VIEW andor_mv;

-- query 24
-- `!=` residual MV (from test_materialized_view_union_all_rewrite MV-A/B)
-- @skip_result_check=true
CREATE MATERIALIZED VIEW ne_mv
DISTRIBUTED BY HASH(lo_orderkey) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT lo_orderkey, lo_linenumber, SUM(lo_revenue) AS s1
FROM lineorder
WHERE lo_linenumber != 2
GROUP BY lo_orderkey, lo_linenumber;

-- query 25
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ne_mv WITH SYNC MODE;

-- query 26
-- exact != residual match with strict-subset rollup
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: ne_mv
SELECT lo_orderkey, SUM(lo_revenue) FROM lineorder WHERE lo_linenumber != 2 GROUP BY lo_orderkey;

-- query 27
SELECT lo_orderkey, SUM(lo_revenue) AS s
FROM lineorder WHERE lo_linenumber != 2 GROUP BY lo_orderkey ORDER BY lo_orderkey;

-- query 28
-- query-only residual on a group-by key is re-applied as compensation
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: ne_mv
SELECT lo_orderkey, SUM(lo_revenue) FROM lineorder WHERE lo_linenumber != 2 AND lo_orderkey != 10001 GROUP BY lo_orderkey;

-- query 29
SELECT lo_orderkey, SUM(lo_revenue) AS s
FROM lineorder WHERE lo_linenumber != 2 AND lo_orderkey != 10001 GROUP BY lo_orderkey ORDER BY lo_orderkey;

-- query 30
-- different != constant -> residual mismatch, miss
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT lo_orderkey, SUM(lo_revenue) FROM lineorder WHERE lo_linenumber != 3 GROUP BY lo_orderkey;

-- query 31
-- a range the != residual would only imply (lo_linenumber >= 3) -> miss
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT lo_orderkey, SUM(lo_revenue) FROM lineorder WHERE lo_linenumber >= 3 GROUP BY lo_orderkey;

-- query 32
-- bare query without the MV residual -> miss
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT lo_orderkey, SUM(lo_revenue) FROM lineorder GROUP BY lo_orderkey;

-- query 33
-- @skip_result_check=true
DROP MATERIALIZED VIEW ne_mv;

-- query 34
-- @skip_result_check=true
DROP TABLE mvrw_${uuid0}.ns_${uuid0}.lineorder FORCE;

-- query 35
-- @skip_result_check=true
DROP DATABASE mvrw_${uuid0}.ns_${uuid0};

-- query 36
-- @skip_result_check=true
DROP CATALOG mvrw_${uuid0};
