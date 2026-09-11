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
-- @tags=mv,iceberg,ivm,negative,v3_required
-- Test Objective:
-- 1. CREATE MATERIALIZED VIEW over a non-v3 (default v2) Iceberg base must
--    fail fast with a clear error pointing at format-version and row-lineage.
-- 2. CREATE MATERIALIZED VIEW over a v3 base without write.row-lineage=true
--    must also fail fast with the same error class.
-- IVM is Iceberg-v3-only by design; this case locks in that contract.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_reject_v2_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_reject_v2_${uuid0}",
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
CREATE DATABASE ice_ivm_reject_v2_${uuid0}.ns_${uuid0};
-- Default Iceberg format (currently v2, no row-lineage).
CREATE TABLE ice_ivm_reject_v2_${uuid0}.ns_${uuid0}.orders_v2 (
  order_id INT,
  amount BIGINT
);
INSERT INTO ice_ivm_reject_v2_${uuid0}.ns_${uuid0}.orders_v2 VALUES (1, 10);
-- Explicit v3 base but row-lineage disabled.
CREATE TABLE ice_ivm_reject_v2_${uuid0}.ns_${uuid0}.orders_v3_no_lineage (
  order_id INT,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ice_ivm_reject_v2_${uuid0}.ns_${uuid0}.orders_v3_no_lineage VALUES (1, 10);

SET CATALOG ice_ivm_reject_v2_${uuid0};
USE ns_${uuid0};

-- query 2
-- @expect_error=Iceberg format-version=3 with write.row-lineage=true
CREATE MATERIALIZED VIEW mv_over_v2
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT order_id, amount FROM orders_v2;

-- query 3
-- @expect_error=Iceberg format-version=3 with write.row-lineage=true
CREATE MATERIALIZED VIEW mv_over_v3_no_lineage
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT order_id, amount FROM orders_v3_no_lineage;

-- query 4
-- @skip_result_check=true
DROP TABLE ice_ivm_reject_v2_${uuid0}.ns_${uuid0}.orders_v2 FORCE;
DROP TABLE ice_ivm_reject_v2_${uuid0}.ns_${uuid0}.orders_v3_no_lineage FORCE;
DROP DATABASE ice_ivm_reject_v2_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_reject_v2_${uuid0};
