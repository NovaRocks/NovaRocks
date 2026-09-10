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
-- Test Objective (migrated from the portable slice of
-- materialized-view/test_range_predicate_rewrite and test_materialized_view_rewrite2):
-- Interval-containment fine points on a WHERE-scoped SPJ MV
-- (dt >= '2021-08-01' AND dt < '2021-09-01', lexicographic ISO dates):
-- 1. Exact range match -> hit with no extra filter.
-- 2. BETWEEN strictly inside the MV range -> hit with the BETWEEN re-applied
--    as compensation.
-- 3. Boundary inclusivity: BETWEEN whose inclusive upper bound equals the
--    MV's EXCLUSIVE upper bound -> not contained -> miss.
-- 4. Query range extending below the MV's lower bound -> miss.
-- 5. An aggregate query over the SPJ MV combined with range compensation.
--
-- Data scale: ~2400 rows, dt spread across 4 ISO dates inside and one
-- outside the MV window.

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
CREATE TABLE mvrw_${uuid0}.ns_${uuid0}.events (
  dt STRING,
  region STRING,
  amount BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- dt in {2021-07-25, 2021-08-05, 2021-08-15, 2021-08-25, 2021-09-01}
-- @skip_result_check=true
INSERT INTO mvrw_${uuid0}.ns_${uuid0}.events
SELECT
  CASE n % 5
    WHEN 0 THEN '2021-07-25'
    WHEN 1 THEN '2021-08-05'
    WHEN 2 THEN '2021-08-15'
    WHEN 3 THEN '2021-08-25'
    ELSE '2021-09-01'
  END AS dt,
  CASE WHEN n % 2 = 0 THEN 'east' ELSE 'west' END AS region,
  CAST(n % 100 AS BIGINT) AS amount
FROM TABLE(generate_series(1, 2400)) t(n);

-- query 5
-- @skip_result_check=true
SET CATALOG mvrw_${uuid0};

-- query 6
-- @skip_result_check=true
USE ns_${uuid0};

-- query 7
-- @skip_result_check=true
CREATE MATERIALIZED VIEW aug_mv
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT dt, region, amount FROM events
WHERE dt >= '2021-08-01' AND dt < '2021-09-01';

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW aug_mv WITH SYNC MODE;

-- query 9
-- exact range match
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: aug_mv
SELECT dt, region, amount FROM events WHERE dt >= '2021-08-01' AND dt < '2021-09-01';

-- query 10
SELECT dt, region, COUNT(*) AS c, SUM(amount) AS s
FROM events WHERE dt >= '2021-08-01' AND dt < '2021-09-01'
GROUP BY dt, region ORDER BY dt, region;

-- query 11
-- BETWEEN strictly inside the MV window -> hit + compensation
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: aug_mv
SELECT dt, amount FROM events WHERE dt BETWEEN '2021-08-05' AND '2021-08-20';

-- query 12
SELECT dt, COUNT(*) AS c, SUM(amount) AS s
FROM events WHERE dt BETWEEN '2021-08-05' AND '2021-08-20'
GROUP BY dt ORDER BY dt;

-- query 13
-- inclusive upper bound == MV exclusive upper bound -> NOT contained
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT dt, amount FROM events WHERE dt BETWEEN '2021-08-05' AND '2021-09-01';

-- query 14
SELECT dt, COUNT(*) AS c
FROM events WHERE dt BETWEEN '2021-08-05' AND '2021-09-01'
GROUP BY dt ORDER BY dt;

-- query 15
-- lower bound below the MV window -> miss
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT dt, amount FROM events WHERE dt >= '2021-07-20' AND dt < '2021-09-01';

-- query 16
-- aggregate over the SPJ MV with a tighter range (query agg kept above)
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: aug_mv
SELECT region, SUM(amount) FROM events
WHERE dt >= '2021-08-10' AND dt < '2021-09-01' GROUP BY region;

-- query 17
SELECT region, SUM(amount) AS s FROM events
WHERE dt >= '2021-08-10' AND dt < '2021-09-01' GROUP BY region ORDER BY region;

-- query 18
-- @skip_result_check=true
DROP MATERIALIZED VIEW aug_mv;

-- query 19
-- @skip_result_check=true
DROP TABLE mvrw_${uuid0}.ns_${uuid0}.events FORCE;

-- query 20
-- @skip_result_check=true
DROP DATABASE mvrw_${uuid0}.ns_${uuid0};

-- query 21
-- @skip_result_check=true
DROP CATALOG mvrw_${uuid0};
