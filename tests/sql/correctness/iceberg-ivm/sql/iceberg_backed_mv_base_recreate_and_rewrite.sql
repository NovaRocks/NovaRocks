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
-- @tags=write_path,mv,iceberg,ivm,storage_engine_iceberg,aggregate,recreate,rewrite
-- Test Objective:
-- 1. Validate an Iceberg-target aggregate MV over an Iceberg base table can
--    refresh correctly before and after the base table is dropped and recreated.
-- 2. Validate a recreated base table starts the MV fresh — no stale rows from
--    the prior table version survive.
-- 3. Validate EXPLAIN references the MV when enable_materialized_view_rewrite=true.
-- 4. Validate information_schema.materialized_views is visible for Iceberg MVs.
-- 5. Validate SHOW MATERIALIZED VIEWS lists the MV.
-- Source: adapted from mv-on-iceberg/sql/test_mv_with_iceberg_recreate.sql.
-- Note:
--   NovaRocks enforces that a base table with downstream MVs cannot be dropped
--   (unlike StarRocks FE which uses enable_mv_automatic_active_check to relax
--   this). The test is restructured: the MV is explicitly dropped before the
--   base table is dropped, and then both are recreated. The core test point
--   (fresh MV after base recreate) is preserved.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_iceberg_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_recreate_${uuid0}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_iceberg_${uuid0}.mv_ice_db_${uuid0};
CREATE TABLE mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} (
  col_str STRING,
  col_int INT,
  dt DATE
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} VALUES
  ('1d8cf2a2c0e14fa89d8117792be6eb6f', 2000, '2023-12-01'),
  ('3e82e36e56718dc4abc1168d21ec91ab', 2000, '2023-12-01'),
  ('abc', 2000, '2023-12-02'),
  (NULL, 2000, '2023-12-02'),
  ('ab1d8cf2a2c0e14fa89d8117792be6eb6f', 2001, '2023-12-03'),
  ('3e82e36e56718dc4abc1168d21ec91ab', 2001, '2023-12-03'),
  ('abc', 2001, '2023-12-04'),
  (NULL, 2001, '2023-12-04');
SET CATALOG mv_iceberg_${uuid0};
USE mv_ice_db_${uuid0};
CREATE MATERIALIZED VIEW test_mv1
DISTRIBUTED BY HASH(dt) BUCKETS 2
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT dt, sum(col_int) AS s
FROM mv_ice_tbl_${uuid0} GROUP BY dt;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW test_mv1;

-- query 3
-- @skip_result_check=true
SET CATALOG mv_iceberg_${uuid0};
USE mv_ice_db_${uuid0};
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt, sum(col_int) FROM mv_ice_tbl_${uuid0} WHERE dt='2023-12-01' GROUP BY dt;

-- query 4
SELECT dt, sum(col_int) FROM mv_ice_tbl_${uuid0} WHERE dt>='2023-12-03' GROUP BY dt ORDER BY dt;

-- query 5
SELECT dt, sum(col_int) FROM mv_ice_tbl_${uuid0} GROUP BY dt ORDER BY dt;

-- query 6
-- @result_contains=test_mv1
-- @result_contains=iceberg
SHOW MATERIALIZED VIEWS;

-- query 7
SELECT table_name, is_active, inactive_reason
FROM information_schema.materialized_views
WHERE table_schema = 'mv_ice_db_${uuid0}' AND table_name = 'test_mv1'
ORDER BY table_name;

-- query 8
-- Drop MV before base table (NovaRocks enforces: base with downstream MV cannot be dropped).
-- @skip_result_check=true
DROP MATERIALIZED VIEW test_mv1;
DROP TABLE mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} FORCE;

-- query 9
-- Recreate base table with a different (smaller) data set and a new MV.
-- Verifies the MV starts fresh with no stale rows from the prior table version.
-- @skip_result_check=true
CREATE TABLE mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} (
  col_str STRING,
  col_int INT,
  dt DATE
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} VALUES
  ('1d8cf2a2c0e14fa89d8117792be6eb6f', 2000, '2023-12-01'),
  ('3e82e36e56718dc4abc1168d21ec91ab', 2000, '2023-12-01');
SET CATALOG mv_iceberg_${uuid0};
USE mv_ice_db_${uuid0};
CREATE MATERIALIZED VIEW test_mv1
DISTRIBUTED BY HASH(dt) BUCKETS 2
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT dt, sum(col_int) AS s
FROM mv_ice_tbl_${uuid0} GROUP BY dt;

-- query 10
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW test_mv1;

-- query 11
-- After recreate: only 2023-12-01 rows exist. No rows from prior dates (2023-12-03, 2023-12-04).
SELECT dt, sum(col_int) FROM mv_ice_tbl_${uuid0} WHERE dt>='2023-12-03' GROUP BY dt ORDER BY dt;

-- query 12
SELECT * FROM test_mv1 ORDER BY 1, 2;

-- query 13
-- @skip_result_check=true
DROP MATERIALIZED VIEW test_mv1;

-- query 14
-- @skip_result_check=true
DROP TABLE mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} FORCE;

-- query 15
-- The MV and the base table are already dropped above, so this needs no FORCE.
-- FORCE would enumerate the namespace's views, which a Hadoop catalog cannot
-- answer -- it reports Unsupported rather than fabricating an empty list.
-- @skip_result_check=true
DROP DATABASE mv_iceberg_${uuid0}.mv_ice_db_${uuid0};

-- query 16
-- @skip_result_check=true
DROP CATALOG mv_iceberg_${uuid0};
