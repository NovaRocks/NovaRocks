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
-- @tags=mv,iceberg,ivm,row_lineage,a1_pipeline,update,merge_on_read,position_delete
-- Test Objective:
-- 1. Validate that UPDATE on the Iceberg base table produces a new data file
--    plus a position-delete file (Iceberg merge-on-read path).
-- 2. Validate A1 incremental refresh correctly applies the UPDATE delta to the
--    MV target: updated rows reflect new values, unchanged rows are preserved.
-- 3. Confirm MV aggregate result matches ground-truth base SELECT after UPDATE.
--
-- Scale: 1000 base rows + UPDATE 50 rows.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_upd_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_upd_${uuid0}",
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
CREATE DATABASE ice_ivm_upd_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_upd_${uuid0}.ns_${uuid0}.sales_${uuid0} (
  id     BIGINT NOT NULL,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_ivm_upd_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW sales_mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, region, amount
FROM ice_ivm_upd_${uuid0}.ns_${uuid0}.sales_${uuid0}
WHERE amount > 300;

-- query 2
-- @skip_result_check=true
-- Insert 1000 base rows (in a separate step so catalog metadata is refreshed)
-- id 1..1000, region alternates APAC/EMEA, amount cycles via id*7%1000+1
INSERT INTO ice_ivm_upd_${uuid0}.ns_${uuid0}.sales_${uuid0}
SELECT
  generate_series AS id,
  CASE WHEN generate_series % 3 = 0 THEN 'EMEA' ELSE 'APAC' END AS region,
  (generate_series * 7 % 1000) + 1 AS amount
FROM TABLE(generate_series(1, 1000));

-- query 3
-- @skip_result_check=true
-- Initial full refresh
REFRESH MATERIALIZED VIEW sales_mv_${uuid0};

-- query 4
-- Verify initial MV state
SELECT region, COUNT(*) AS cnt, SUM(amount) AS total
FROM sales_mv_${uuid0}
GROUP BY region
ORDER BY region;

-- query 5
-- Ground truth from base must match MV
SELECT region, COUNT(*) AS cnt, SUM(amount) AS total
FROM ice_ivm_upd_${uuid0}.ns_${uuid0}.sales_${uuid0}
WHERE amount > 300
GROUP BY region
ORDER BY region;

-- query 6
-- @skip_result_check=true
-- UPDATE 50 rows (ids 1..50): set amount=999 (all pass filter regardless of original value)
-- Iceberg produces new data file + position-delete (merge-on-read path)
UPDATE ice_ivm_upd_${uuid0}.ns_${uuid0}.sales_${uuid0}
SET amount = 999
WHERE id >= 1 AND id <= 50;

-- query 7
-- @skip_result_check=true
-- A1 incremental refresh applies UPDATE (new data file + position-delete)
REFRESH MATERIALIZED VIEW sales_mv_${uuid0};

-- query 8
-- All 50 updated rows must appear in MV with amount=999
SELECT COUNT(*) AS updated_in_mv
FROM sales_mv_${uuid0}
WHERE id BETWEEN 1 AND 50 AND amount = 999;

-- query 9
-- MV aggregates after UPDATE
SELECT region, COUNT(*) AS cnt, SUM(amount) AS total
FROM sales_mv_${uuid0}
GROUP BY region
ORDER BY region;

-- query 10
-- Ground truth from base after UPDATE must match MV
SELECT region, COUNT(*) AS cnt, SUM(amount) AS total
FROM ice_ivm_upd_${uuid0}.ns_${uuid0}.sales_${uuid0}
WHERE amount > 300
GROUP BY region
ORDER BY region;

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW sales_mv_${uuid0};
DROP TABLE ice_ivm_upd_${uuid0}.ns_${uuid0}.sales_${uuid0} FORCE;
DROP DATABASE ice_ivm_upd_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_upd_${uuid0};
