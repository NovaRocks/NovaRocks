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
-- Test Objective: queries that must NOT be rewritten even though a related MV
-- exists. Each asserts @explain_not_contains=rewritten with mv (the scan still
-- reads the base `orders` table). Covered rejection reasons:
-- 1. AVG rollup is not in the supported aggregate white-list.
-- 2. COUNT(DISTINCT ...) over an aggregated MV is unsound.
-- 3. The query references a column (id) the MV does not expose.
-- 4. A compensation predicate lands on a non-group-by column (amount).
-- 5. A WHERE-scoped MV does not contain the (unfiltered) query range.
--
-- Data is scaled like the hit case so that, where a rewrite WOULD be valid, it
-- would also be cost-favourable; misses here are due to rule rejection, not
-- cost. (See mv_rewrite_hit_basic.sql for the scale rationale.)

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
CREATE MATERIALIZED VIEW agg_mv
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT region, day, COUNT(*) AS c, SUM(amount) AS s
FROM orders GROUP BY region, day;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_mv WITH SYNC MODE;

-- query 9
-- AVG rollup is not supported -> no rewrite
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT region, AVG(amount) FROM orders GROUP BY region;

-- query 10
-- COUNT(DISTINCT ...) over an aggregated MV is unsound -> no rewrite
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT region, COUNT(DISTINCT day) FROM orders GROUP BY region;

-- query 11
-- MV does not expose the id column -> no rewrite
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT id FROM orders;

-- query 12
-- compensation predicate on a non-group-by column (amount) -> no rewrite
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT region, SUM(amount) FROM orders WHERE amount > 5 GROUP BY region;

-- query 13
-- @skip_result_check=true
CREATE MATERIALIZED VIEW agg_mv_d1
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT region, day, COUNT(*) AS c, SUM(amount) AS s
FROM orders WHERE day = 'd1' GROUP BY region, day;

-- query 14
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_mv_d1 WITH SYNC MODE;

-- query 15
-- the WHERE-scoped MV (day='d1') does not contain the unfiltered query range;
-- the unrestricted agg_mv is still a valid hit, so assert only that the
-- restricted MV is NOT the chosen target.
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv: agg_mv_d1
SELECT region, day, SUM(amount) FROM orders GROUP BY region, day;

-- query 16
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_mv_d1;

-- query 17
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_mv;

-- query 18
-- @skip_result_check=true
DROP TABLE mvrw_${uuid0}.ns_${uuid0}.orders FORCE;

-- query 19
-- @skip_result_check=true
DROP DATABASE mvrw_${uuid0}.ns_${uuid0};

-- query 20
-- @skip_result_check=true
DROP CATALOG mvrw_${uuid0};
