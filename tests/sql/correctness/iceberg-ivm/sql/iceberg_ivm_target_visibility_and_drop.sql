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
-- @tags=mv,iceberg,ivm,storage_engine_iceberg,target_catalog,drop
-- Test Objective:
-- 1. Validate an Iceberg-backed MV is queryable directly as its Iceberg table in the active catalog.
-- 2. Validate SHOW MATERIALIZED VIEWS exposes the Iceberg storage engine.
-- 3. Validate DROP MATERIALIZED VIEW removes that single MV table from the Iceberg catalog.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_target_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_target_${uuid0}",
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
CREATE DATABASE ice_ivm_target_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_target_${uuid0}.ns_${uuid0}.base_orders (
  order_id INT,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO ice_ivm_target_${uuid0}.ns_${uuid0}.base_orders VALUES
  (1, 10),
  (2, 20);

SET CATALOG ice_ivm_target_${uuid0};
USE ns_${uuid0};

CREATE MATERIALIZED VIEW target_mv
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT order_id, amount FROM base_orders;

REFRESH MATERIALIZED VIEW target_mv;

-- query 2
SELECT order_id, amount FROM target_mv ORDER BY order_id;

-- query 3
-- @result_contains=target_mv
-- @result_contains=iceberg
SHOW MATERIALIZED VIEWS;

-- query 4
SELECT COUNT(*) FROM target_mv;

-- query 5
-- @skip_result_check=true
DROP MATERIALIZED VIEW target_mv;

-- query 6
-- The drop removes the target from the Iceberg catalog, so name resolution is
-- what fails. The older "no metadata files" wording described a weaker drop
-- that left the catalog entry behind with its metadata purged; asserting it
-- would now pin that weaker behavior instead of objective 3.
-- @expect_error=unknown table
SELECT COUNT(*) FROM target_mv;

-- query 7
-- @skip_result_check=true
DROP TABLE ice_ivm_target_${uuid0}.ns_${uuid0}.base_orders FORCE;
DROP DATABASE ice_ivm_target_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_target_${uuid0};
