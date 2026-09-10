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
-- Test Objective (migrated from materialized-view/test_sync_materialized_view_unorder
-- and the portable slice of test_materialized_view_text_based_rewrite):
-- Normalization robustness of the structural matcher:
-- 1. The MV defines its query through a TABLE ALIAS and a function-expression
--    group-by (substring month bucket); queries written WITHOUT the alias, or
--    with a DIFFERENT alias, must still match.
-- 2. The MV select list is UNORDERED relative to both the base-table column
--    order and the (group-keys-first) aggregate layout.
-- 3. The MV WHERE equality filter must match exactly; a query missing the
--    filter, or filtering a different value on that non-group-key column,
--    must miss.
-- 4. Freshness: a base insert advances the snapshot -> miss with correct
--    base-table results. (No re-refresh here: incremental IMV refresh does
--    not yet support expression GROUP BY keys — "cannot map GROUP BY
--    expression substring(...) to aggregate output column"; the
--    refresh-restores-hit cycle is covered by mv_rewrite_freshness.sql on a
--    plain-key MV.)
--
-- Data scale: ~2400 rows, NULL k2 rows included (excluded by the MV filter).

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
CREATE TABLE mvrw_${uuid0}.ns_${uuid0}.t1 (
  k2 STRING,
  k3 BIGINT,
  k4 STRING,
  v1 BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- k2 cycles a/b/NULL; k4 covers two months; k3 has 3 buckets
-- @skip_result_check=true
INSERT INTO mvrw_${uuid0}.ns_${uuid0}.t1
SELECT
  CASE WHEN n % 3 = 0 THEN 'a' WHEN n % 3 = 1 THEN 'b' ELSE NULL END AS k2,
  CAST(n % 3 AS BIGINT) AS k3,
  concat('2024-0', CAST(7 + (n % 2) AS STRING), '-15') AS k4,
  CAST(n % 10 AS BIGINT) AS v1
FROM TABLE(generate_series(1, 2400)) t(n);

-- query 5
-- @skip_result_check=true
SET CATALOG mvrw_${uuid0};

-- query 6
-- @skip_result_check=true
USE ns_${uuid0};

-- query 7
-- alias-qualified definition; select list deliberately unordered: an
-- aggregate first, then the expression key, then the plain key
-- @skip_result_check=true
CREATE MATERIALIZED VIEW month_mv
DISTRIBUTED BY HASH(k3) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT SUM(a.v1) AS sv, substring(a.k4, 1, 7) AS month, a.k3
FROM t1 a
WHERE a.k2 = 'a'
GROUP BY a.k3, substring(a.k4, 1, 7);

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW month_mv WITH SYNC MODE;

-- query 9
-- same semantics written WITHOUT the alias -> still a structural match
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: month_mv
SELECT k3, substring(k4, 1, 7), SUM(v1) FROM t1 WHERE k2 = 'a' GROUP BY k3, substring(k4, 1, 7);

-- query 10
SELECT k3, substring(k4, 1, 7) AS month, SUM(v1) AS sv
FROM t1 WHERE k2 = 'a' GROUP BY k3, substring(k4, 1, 7) ORDER BY k3, month;

-- query 11
-- a DIFFERENT alias on the query side
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: month_mv
SELECT b.k3, substring(b.k4, 1, 7), SUM(b.v1) FROM t1 b WHERE b.k2 = 'a' GROUP BY b.k3, substring(b.k4, 1, 7);

-- query 12
-- rollup over the expression key only (strict subset of {k3, month})
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: month_mv
SELECT substring(k4, 1, 7), SUM(v1) FROM t1 WHERE k2 = 'a' GROUP BY substring(k4, 1, 7);

-- query 13
SELECT substring(k4, 1, 7) AS month, SUM(v1) AS sv
FROM t1 WHERE k2 = 'a' GROUP BY substring(k4, 1, 7) ORDER BY month;

-- query 14
-- query without the MV's k2='a' filter -> MV does not contain the range
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT k3, SUM(v1) FROM t1 GROUP BY k3;

-- query 15
SELECT k3, SUM(v1) AS sv FROM t1 GROUP BY k3 ORDER BY k3;

-- query 16
-- different value on the filtered non-group-key column -> disjoint, miss
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT k3, SUM(v1) FROM t1 WHERE k2 = 'b' GROUP BY k3;

-- query 17
SELECT k3, SUM(v1) AS sv FROM t1 WHERE k2 = 'b' GROUP BY k3 ORDER BY k3;

-- query 18
-- freshness: base insert advances the snapshot -> miss
-- @skip_result_check=true
INSERT INTO mvrw_${uuid0}.ns_${uuid0}.t1 VALUES ('a', 0, '2024-07-15', 5);

-- query 19
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT k3, substring(k4, 1, 7), SUM(v1) FROM t1 WHERE k2 = 'a' GROUP BY k3, substring(k4, 1, 7);

-- query 20
SELECT k3, substring(k4, 1, 7) AS month, SUM(v1) AS sv
FROM t1 WHERE k2 = 'a' GROUP BY k3, substring(k4, 1, 7) ORDER BY k3, month;

-- query 21
-- @skip_result_check=true
DROP MATERIALIZED VIEW month_mv;

-- query 22
-- @skip_result_check=true
DROP TABLE mvrw_${uuid0}.ns_${uuid0}.t1 FORCE;

-- query 23
-- @skip_result_check=true
DROP DATABASE mvrw_${uuid0}.ns_${uuid0};

-- query 24
-- @skip_result_check=true
DROP CATALOG mvrw_${uuid0};
