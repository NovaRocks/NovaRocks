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
-- @tags=write_path,mv,iceberg,ivm,storage_engine_iceberg,phase4a
-- Test Objective:
-- 1. CREATE MATERIALIZED VIEW with PROPERTIES('storage_engine' = 'iceberg') succeeds.
-- 2. First REFRESH writes visible projection/filter result into the iceberg-backed MV.
-- 3. Append-only incremental REFRESH appends only new rows (only rows matching WHERE v2 > 0).
-- 4. SHOW MATERIALIZED VIEWS includes StorageEngine column with value 'iceberg'.
-- 5. DROP cleans up sqlite + iceberg.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_phase4a_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_mv_phase4a_${uuid0}",
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
CREATE DATABASE mv_phase4a_${uuid0}.ns_${uuid0};
CREATE TABLE mv_phase4a_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO mv_phase4a_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10), (1, 20), (2, 40), (3, 0);

SET CATALOG mv_phase4a_${uuid0};
USE ns_${uuid0};

CREATE MATERIALIZED VIEW proj_mv
DISTRIBUTED BY HASH(k1) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM orders WHERE v2 > 0;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW proj_mv;

-- query 3
SELECT k1, v2 FROM proj_mv ORDER BY k1, v2;

-- query 4
-- @skip_result_check=true
INSERT INTO mv_phase4a_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 70), (4, 5);

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW proj_mv;

-- query 6
SELECT k1, v2 FROM proj_mv ORDER BY k1, v2;

-- query 7
-- SHOW MATERIALIZED VIEWS includes StorageEngine column with value 'iceberg'.
-- @result_contains=proj_mv
-- @result_contains=iceberg
SHOW MATERIALIZED VIEWS;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW proj_mv;
DROP TABLE mv_phase4a_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_phase4a_${uuid0}.ns_${uuid0};
DROP CATALOG mv_phase4a_${uuid0};
