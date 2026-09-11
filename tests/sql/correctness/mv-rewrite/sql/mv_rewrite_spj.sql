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
-- Test Objective: rewrite against a non-aggregated (SPJ) MV.
-- 1. SPJ query, exact predicate match -> rewritten to scan the MV.
-- 2. SPJ query, tighter predicate on a projected column -> hit with a Filter
--    compensation over the MV scan.
-- 3. SPJG query (aggregate over the same SPJ base+predicate) -> the aggregate
--    is kept and its args are rewritten onto the SPJ MV columns -> hit.
-- 4. SPJ query with a WIDER predicate than the MV -> range not contained ->
--    no rewrite.
--
-- The SPJ MV is defined with a SELECTIVE predicate (amount > 8) so the MV is an
-- order of magnitude smaller than the base table. SPJ MVs preserve detail
-- rows, so unlike an aggregate MV they only beat the base on cost when the MV's
-- own filter already discards most rows; the in-range queries below stay inside
-- that filter. (Cost-based rewrite, like StarRocks.)

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
CREATE TABLE mvrw_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  region STRING,
  day STRING,
  amount BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- @skip_result_check=true
INSERT INTO mvrw_${uuid0}.ns_${uuid0}.orders
SELECT
  number AS id,
  CASE WHEN number % 3 = 0 THEN 'east' WHEN number % 3 = 1 THEN 'west' ELSE 'north' END AS region,
  CASE WHEN number % 2 = 0 THEN 'd1' ELSE 'd2' END AS day,
  CAST(number % 10 AS BIGINT) AS amount
FROM TABLE(generate_series(1, 1200)) t(number);

-- query 5
-- @skip_result_check=true
SET CATALOG mvrw_${uuid0};

-- query 6
-- @skip_result_check=true
USE ns_${uuid0};

-- query 7
-- @skip_result_check=true
CREATE MATERIALIZED VIEW spj_mv
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT region, day, amount FROM orders WHERE amount > 8;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW spj_mv WITH SYNC MODE;

-- query 9
-- SPJ exact predicate match -> hit
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: spj_mv
SELECT region, day, amount FROM orders WHERE amount > 8;

-- query 10
SELECT region, day, COUNT(*) AS c, SUM(amount) AS s
FROM orders WHERE amount > 8 GROUP BY region, day ORDER BY region, day;

-- query 11
-- SPJ tighter predicate on a projected column -> hit with compensation Filter
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: spj_mv
SELECT region, day, amount FROM orders WHERE amount > 8 AND region = 'east';

-- query 12
SELECT region, day, SUM(amount) AS s
FROM orders WHERE amount > 8 AND region = 'east' GROUP BY region, day ORDER BY day;

-- query 13
-- SPJG query over the SPJ MV: aggregate kept, args rewritten -> hit
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: spj_mv
SELECT region, SUM(amount) FROM orders WHERE amount > 8 GROUP BY region;

-- query 14
SELECT region, SUM(amount) AS s FROM orders WHERE amount > 8 GROUP BY region ORDER BY region;

-- query 15
-- WIDER predicate than the MV (amount >= -5) -> range not contained -> no rewrite
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT region, day, amount FROM orders WHERE amount >= -5;

-- query 16
-- @skip_result_check=true
DROP MATERIALIZED VIEW spj_mv;

-- query 17
-- @skip_result_check=true
DROP TABLE mvrw_${uuid0}.ns_${uuid0}.orders FORCE;

-- query 18
-- @skip_result_check=true
DROP DATABASE mvrw_${uuid0}.ns_${uuid0};

-- query 19
-- @skip_result_check=true
DROP CATALOG mvrw_${uuid0};
