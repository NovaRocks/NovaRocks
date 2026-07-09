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
-- Test Objective (IMV-P4-E1-d): a materialized view DEFINED OVER A JOIN of
-- two base tables is chosen by the cost-based optimizer for a matching
-- aggregate-join query, end to end through the live standalone SQL path:
-- 1. Hit: `orders JOIN regions` aggregate query matches `order_region_mv`'s
--    join+group-by shape exactly -> rewritten, and the result equals the
--    non-rewritten (base-table) answer.
-- 2. Miss: `orders JOIN warehouses` -- a different table on the right side of
--    the join -- is a table-set mismatch against the MV and must fail open
--    regardless of cost.
--
-- Join-key nullability note: `region_id` is declared NOT NULL on both
-- `orders` and `regions`. A nullable join key would make
-- DeriveJoinNotNullPredicate (an unrelated RBO rewrite that infers `IS NOT
-- NULL` on nullable inner-join keys) attach an `IS NOT NULL` predicate to the
-- non-driving join input (`regions.region_id`). That column is not part of
-- the MV's own SELECT list (only `region_name` and the aggregates are), so
-- the rewrite's compensation-predicate step cannot re-express the derived
-- predicate against the MV's visible schema and the rewrite fails closed --
-- regardless of cost. Declaring the join key NOT NULL (realistic for a
-- dimension key) avoids the predicate being derived in the first place.
--
-- Data scale note: `orders` is populated to a couple thousand rows via
-- generate_series so the pre-joined, pre-aggregated MV (three group rows) is
-- a genuine, robust cost win for the CBO -- see mv_rewrite_hit_basic.sql for
-- the general rationale.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mvrwj_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/mvrwj_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);

-- query 2
-- @skip_result_check=true
CREATE DATABASE mvrwj_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvrwj_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  region_id BIGINT NOT NULL,
  amount BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- @skip_result_check=true
CREATE TABLE mvrwj_${uuid0}.ns_${uuid0}.regions (
  region_id BIGINT NOT NULL,
  region_name STRING
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 5
-- @skip_result_check=true
CREATE TABLE mvrwj_${uuid0}.ns_${uuid0}.warehouses (
  warehouse_id BIGINT NOT NULL,
  warehouse_name STRING
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 6
-- @skip_result_check=true
INSERT INTO mvrwj_${uuid0}.ns_${uuid0}.orders
SELECT
  number AS id,
  CAST(number % 3 AS BIGINT) AS region_id,
  CAST(number % 10 AS BIGINT) AS amount
FROM TABLE(generate_series(1, 2400)) t(number);

-- query 7
-- @skip_result_check=true
INSERT INTO mvrwj_${uuid0}.ns_${uuid0}.regions VALUES
  (0, 'east'), (1, 'west'), (2, 'north');

-- query 8
-- @skip_result_check=true
INSERT INTO mvrwj_${uuid0}.ns_${uuid0}.warehouses VALUES
  (0, 'wh-a'), (1, 'wh-b'), (2, 'wh-c');

-- query 9
-- @skip_result_check=true
SET CATALOG mvrwj_${uuid0};

-- query 10
-- @skip_result_check=true
USE ns_${uuid0};

-- query 11
-- join-defined MV: pre-joins and pre-aggregates orders with its region
-- dimension.
-- @skip_result_check=true
CREATE MATERIALIZED VIEW order_region_mv
DISTRIBUTED BY HASH(region_id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT r.region_id, r.region_name, SUM(o.amount) AS s, COUNT(*) AS c
FROM orders o JOIN regions r ON o.region_id = r.region_id
GROUP BY r.region_id, r.region_name;

-- query 12
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW order_region_mv WITH SYNC MODE;

-- query 13
-- hit: aggregate-join query matches the MV's join + group-by shape exactly
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: order_region_mv
SELECT r.region_id, r.region_name, SUM(o.amount), COUNT(*)
FROM orders o JOIN regions r ON o.region_id = r.region_id
GROUP BY r.region_id, r.region_name;

-- query 14
-- correctness: the rewritten result must equal the base-table (non-rewritten)
-- answer
SELECT r.region_id, r.region_name, SUM(o.amount) AS s, COUNT(*) AS c
FROM orders o JOIN regions r ON o.region_id = r.region_id
GROUP BY r.region_id, r.region_name
ORDER BY r.region_id;

-- query 15
-- miss: joining `warehouses` instead of `regions` is a table-set mismatch
-- against the MV's {orders, regions} join shape -> must fail open regardless
-- of cost.
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT w.warehouse_name, SUM(o.amount)
FROM orders o JOIN warehouses w ON o.region_id = w.warehouse_id
GROUP BY w.warehouse_name;

-- query 16
-- @skip_result_check=true
DROP MATERIALIZED VIEW order_region_mv;

-- query 17
-- @skip_result_check=true
DROP TABLE mvrwj_${uuid0}.ns_${uuid0}.orders FORCE;

-- query 18
-- @skip_result_check=true
DROP TABLE mvrwj_${uuid0}.ns_${uuid0}.regions FORCE;

-- query 19
-- @skip_result_check=true
DROP TABLE mvrwj_${uuid0}.ns_${uuid0}.warehouses FORCE;

-- query 20
-- @skip_result_check=true
DROP DATABASE mvrwj_${uuid0}.ns_${uuid0};

-- query 21
-- @skip_result_check=true
DROP CATALOG mvrwj_${uuid0};
