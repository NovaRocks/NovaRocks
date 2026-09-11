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
-- Test Objective (migrated from materialized-view/test_mv_rewrite_bugs1 plus the
-- portable slices of test_refresh_mv_with_different_dbs and
-- test_mv_with_multi_partition_columns_basic):
-- MV candidates must never crash or derail planning of queries the rewrite
-- does not (or only partially) apply to:
-- 1. With a full-projection SPJ MV present, a CTE + DISTINCT + scalar-subquery
--    + APPROX_COUNT_DISTINCT query plans and returns correct results (no hit
--    or miss asserted — robustness only).
-- 2. A self-join query plans correctly; each join branch may legally rewrite
--    on its own, results must be identical either way.
-- 3. Cross-namespace resolution: the MV lives in a different namespace than
--    its base table and still serves queries (base_table_refs FQN matching).
-- 4. A partitioned Iceberg base table works as a rewrite source.

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
CREATE DATABASE mvrw_${uuid0}.ns2_${uuid0};

-- query 4
-- @skip_result_check=true
CREATE TABLE mvrw_${uuid0}.ns_${uuid0}.sales_data (
  customer_id BIGINT,
  order_id BIGINT,
  category_l1 STRING,
  sales_amount BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 5
-- @skip_result_check=true
INSERT INTO mvrw_${uuid0}.ns_${uuid0}.sales_data
SELECT
  CAST(10000 + (n % 50) AS BIGINT) AS customer_id,
  CAST(n AS BIGINT) AS order_id,
  CASE n % 3 WHEN 0 THEN 'Dairy' WHEN 1 THEN 'Snacks' ELSE 'Baby' END AS category_l1,
  CAST(n % 500 AS BIGINT) AS sales_amount
FROM TABLE(generate_series(1, 2400)) t(n);

-- query 6
-- @skip_result_check=true
SET CATALOG mvrw_${uuid0};

-- query 7
-- @skip_result_check=true
USE ns_${uuid0};

-- query 8
-- full-projection SPJ MV over the wide base table
-- @skip_result_check=true
CREATE MATERIALIZED VIEW sales_full_mv
DISTRIBUTED BY HASH(customer_id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT customer_id, order_id, category_l1, sales_amount FROM sales_data;

-- query 9
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW sales_full_mv WITH SYNC MODE;

-- query 10
-- CTE + DISTINCT + scalar subqueries + APPROX_COUNT_DISTINCT: must plan and
-- return exact results with the MV candidate present (robustness; no
-- hit/miss assertion)
WITH snack_buyers AS (
  SELECT DISTINCT customer_id FROM sales_data WHERE category_l1 = 'Snacks'
),
dairy_buyers AS (
  SELECT DISTINCT customer_id FROM sales_data WHERE category_l1 = 'Dairy'
)
SELECT
  (SELECT COUNT(*) FROM dairy_buyers) AS dairy_cnt,
  (SELECT COUNT(*) FROM snack_buyers) AS snack_cnt;

-- query 11
-- APPROX_COUNT_DISTINCT over the base with the candidate present
SELECT APPROX_COUNT_DISTINCT(customer_id) AS acd FROM sales_data;

-- query 12
-- self-join: each branch may rewrite independently; result must be exact
SELECT a.category_l1, COUNT(*) AS c
FROM sales_data a
JOIN sales_data b ON a.order_id = b.order_id
GROUP BY a.category_l1 ORDER BY a.category_l1;

-- query 13
-- cross-namespace MV: defined in ns2 over the ns table (three-part name)
-- @skip_result_check=true
USE ns2_${uuid0};

-- query 14
-- @skip_result_check=true
CREATE MATERIALIZED VIEW xns_mv
DISTRIBUTED BY HASH(category_l1) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT category_l1, COUNT(*) AS c, SUM(sales_amount) AS s
FROM mvrw_${uuid0}.ns_${uuid0}.sales_data GROUP BY category_l1;

-- query 15
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW xns_mv WITH SYNC MODE;

-- query 16
-- query issued from ns2 against the ns base table -> the cross-namespace MV
-- serves it
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: xns_mv
SELECT category_l1, COUNT(*), SUM(sales_amount) FROM mvrw_${uuid0}.ns_${uuid0}.sales_data GROUP BY category_l1;

-- query 17
SELECT category_l1, COUNT(*) AS c, SUM(sales_amount) AS s
FROM mvrw_${uuid0}.ns_${uuid0}.sales_data GROUP BY category_l1 ORDER BY category_l1;

-- query 18
-- partitioned base table as rewrite source
-- @skip_result_check=true
CREATE TABLE mvrw_${uuid0}.ns_${uuid0}.part_sales (
  region STRING,
  day STRING,
  amount BIGINT
)
PARTITION BY (region)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 19
-- @skip_result_check=true
INSERT INTO mvrw_${uuid0}.ns_${uuid0}.part_sales
SELECT
  CASE n % 3 WHEN 0 THEN 'east' WHEN 1 THEN 'west' ELSE 'north' END AS region,
  CASE WHEN n % 2 = 0 THEN 'd1' ELSE 'd2' END AS day,
  CAST(n % 100 AS BIGINT) AS amount
FROM TABLE(generate_series(1, 2400)) t(n);

-- query 20
-- @skip_result_check=true
USE ns_${uuid0};

-- query 21
-- @skip_result_check=true
CREATE MATERIALIZED VIEW part_mv
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT region, day, SUM(amount) AS s FROM part_sales GROUP BY region, day;

-- query 22
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW part_mv WITH SYNC MODE;

-- query 23
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: part_mv
SELECT region, SUM(amount) FROM part_sales GROUP BY region;

-- query 24
SELECT region, SUM(amount) AS s FROM part_sales GROUP BY region ORDER BY region;

-- query 25
-- @skip_result_check=true
DROP MATERIALIZED VIEW part_mv;

-- query 26
-- @skip_result_check=true
USE ns2_${uuid0};

-- query 27
-- @skip_result_check=true
DROP MATERIALIZED VIEW xns_mv;

-- query 28
-- @skip_result_check=true
USE ns_${uuid0};

-- query 29
-- @skip_result_check=true
DROP MATERIALIZED VIEW sales_full_mv;

-- query 30
-- @skip_result_check=true
DROP TABLE mvrw_${uuid0}.ns_${uuid0}.part_sales FORCE;

-- query 31
-- @skip_result_check=true
DROP TABLE mvrw_${uuid0}.ns_${uuid0}.sales_data FORCE;

-- query 32
-- @skip_result_check=true
DROP DATABASE mvrw_${uuid0}.ns2_${uuid0};

-- query 33
-- @skip_result_check=true
DROP DATABASE mvrw_${uuid0}.ns_${uuid0};

-- query 34
-- @skip_result_check=true
DROP CATALOG mvrw_${uuid0};
