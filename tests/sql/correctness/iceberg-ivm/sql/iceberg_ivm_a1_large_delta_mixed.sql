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
-- @tags=mv,iceberg,ivm,row_lineage,a1_pipeline,large_delta,mixed_insert_delete,position_delete
-- Test Objective:
-- 1. Validate IcebergDeltaScan + IcebergMergeSink A1 pipeline handles a delta
--    containing both new data files (INSERT) and position-delete files (DELETE)
--    in a single REFRESH invocation.
-- 2. Verify position-delete reverse-projection correctly removes deleted rows
--    from the MV target.
-- 3. Confirm MV aggregate result matches ground-truth base SELECT after delta.
--
-- Scale: 5000 base rows + 500 inserts + 100 deletes.
-- Note: production 100MB+ exercise is a separate perf test; this suite is
-- designed to complete in <60s on a developer machine.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_ldm_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_ldm_${uuid0}",
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
CREATE DATABASE ice_ivm_ldm_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_ldm_${uuid0}.ns_${uuid0}.orders_${uuid0} (
  id     BIGINT NOT NULL,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_ivm_ldm_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, region, amount
FROM ice_ivm_ldm_${uuid0}.ns_${uuid0}.orders_${uuid0}
WHERE amount > 500;

-- query 2
-- @skip_result_check=true
-- Insert 5000 base rows (in a separate step so catalog metadata is refreshed)
-- id 1..5000, region alternates APAC/EMEA, amount cycles via id*7%1000+1
INSERT INTO ice_ivm_ldm_${uuid0}.ns_${uuid0}.orders_${uuid0}
SELECT
  generate_series AS id,
  CASE WHEN generate_series % 3 = 0 THEN 'EMEA' ELSE 'APAC' END AS region,
  (generate_series * 7 % 1000) + 1 AS amount
FROM TABLE(generate_series(1, 5000));

-- query 3
-- @skip_result_check=true
-- Initial full refresh
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 4
-- Verify initial MV state via deterministic aggregates
SELECT region, COUNT(*) AS cnt, SUM(amount) AS total
FROM orders_mv_${uuid0}
GROUP BY region
ORDER BY region;

-- query 5
-- Ground truth from base must match MV
SELECT region, COUNT(*) AS cnt, SUM(amount) AS total
FROM ice_ivm_ldm_${uuid0}.ns_${uuid0}.orders_${uuid0}
WHERE amount > 500
GROUP BY region
ORDER BY region;

-- query 6
-- @skip_result_check=true
-- Mixed delta: INSERT 500 new rows (all pass filter) + DELETE 100 rows (position-delete)
-- INSERT 500 rows: id 5001..5500, region=APAC, amount=600 (all pass amount>500 filter)
INSERT INTO ice_ivm_ldm_${uuid0}.ns_${uuid0}.orders_${uuid0}
SELECT generate_series + 5000, 'APAC', 600
FROM TABLE(generate_series(1, 500));
-- DELETE 100 rows from the base (ids 1..100)
DELETE FROM ice_ivm_ldm_${uuid0}.ns_${uuid0}.orders_${uuid0} WHERE id >= 1 AND id <= 100;

-- query 7
-- @skip_result_check=true
-- A1 incremental refresh processes mixed INSERT+DELETE delta
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 8
-- MV reflects the mixed delta (aggregates are deterministic)
SELECT region, COUNT(*) AS cnt, SUM(amount) AS total
FROM orders_mv_${uuid0}
GROUP BY region
ORDER BY region;

-- query 9
-- Ground truth from base after delta must match MV
SELECT region, COUNT(*) AS cnt, SUM(amount) AS total
FROM ice_ivm_ldm_${uuid0}.ns_${uuid0}.orders_${uuid0}
WHERE amount > 500
GROUP BY region
ORDER BY region;

-- query 10
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_mv_${uuid0};
DROP TABLE ice_ivm_ldm_${uuid0}.ns_${uuid0}.orders_${uuid0} FORCE;
DROP DATABASE ice_ivm_ldm_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_ldm_${uuid0};
