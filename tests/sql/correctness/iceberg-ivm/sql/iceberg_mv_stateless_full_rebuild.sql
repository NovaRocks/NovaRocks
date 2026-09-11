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
-- @tags=write_path,mv,iceberg,ivm,storage_engine_iceberg,statelessness
-- Test Objective:
-- 1. Validate standalone CREATE / INSERT / REFRESH over an Iceberg-target MV
--    (mirrors iceberg_backed_mv_basic_lifecycle.sql's setup).
-- 2. Dedicated coverage for the W4 lake-native statelessness `full` level:
--    `@imv_stateless_rebuild=...,level=full` drives the server to clear the
--    MV's SQLite definition and rebuild it purely from the lake (Iceberg MV
--    table descriptor properties), then
--    re-runs the same SELECT and asserts it is unchanged.
-- 3. The directive itself emits no output; the golden below is the SELECT's
--    own result, which must be identical before and after the clear+rebuild
--    round-trip the directive performs server-side.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_ice_stateless_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_ice_stateless_${uuid0}.ns_${uuid0};
CREATE TABLE mv_ice_stateless_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO mv_ice_stateless_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10),
  (2, 20),
  (3, 50);
SET CATALOG mv_ice_stateless_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_mv
DISTRIBUTED BY HASH(k1) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM orders;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv;

-- query 3
-- @imv_stateless_rebuild=orders_mv,catalog=mv_ice_stateless_${uuid0},level=full
SELECT k1, v2 FROM orders_mv ORDER BY k1;

-- query 4
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_mv;
DROP TABLE mv_ice_stateless_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_ice_stateless_${uuid0}.ns_${uuid0};
DROP CATALOG mv_ice_stateless_${uuid0};
