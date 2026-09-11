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
-- @tags=mv,iceberg,ivm,metadata_only,empty_delta,delete,update,stateless
-- Test Objective:
-- 1. Exercise a filtered Iceberg MV's native FE/BE first-refresh lifecycle.
-- 2. Advance the base with an inserted row the MV filter excludes, then
--    refresh without changing the materialized read face.
-- 3. Advance it again with a filtered-out DELETE and UPDATE, then verify the
--    incremental contents equal a full recompute.
-- 4. Rebuild from lake metadata after the net-zero refresh. This is the native
--    owner mapping for the former Core metadata-only-intent and data-free
--    watermark tests: only the FE/BE lifecycle can validate refresh staging,
--    lake provenance, and stateful reconstruction together.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_empty_delta_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_empty_delta_${uuid0}",
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
CREATE DATABASE ice_ivm_empty_delta_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_empty_delta_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  amount BIGINT,
  name STRING
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO ice_ivm_empty_delta_${uuid0}.ns_${uuid0}.orders VALUES
  (200, 200, 'keep'),
  (1, 1, 'delete-miss'),
  (2, 2, 'update-miss');
SET CATALOG ice_ivm_empty_delta_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_mv
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT id, amount, name
FROM orders
WHERE amount > 100;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv;

-- query 3
SELECT id, amount, name FROM orders_mv ORDER BY id;

-- query 4
-- Insert a row the filter excludes. The refresh must advance its admitted
-- base window without changing the visible MV result.
-- @skip_result_check=true
INSERT INTO ice_ivm_empty_delta_${uuid0}.ns_${uuid0}.orders VALUES
  (3, 3, 'insert-miss');
REFRESH MATERIALIZED VIEW orders_mv;

-- query 5
-- @imv_equivalence_check=orders_mv
SELECT id, amount, name FROM orders_mv ORDER BY id;

-- query 6
-- Both changes stay below the filter threshold: DELETE removes one excluded
-- row and UPDATE rewrites another excluded row. Their combined delta has no
-- materialized output, but must still be committed as a refresh watermark.
-- @skip_result_check=true
DELETE FROM ice_ivm_empty_delta_${uuid0}.ns_${uuid0}.orders WHERE id = 1;
UPDATE ice_ivm_empty_delta_${uuid0}.ns_${uuid0}.orders
SET name = 'update-miss-rewritten'
WHERE id = 2;
REFRESH MATERIALIZED VIEW orders_mv;

-- query 7
-- @imv_stateless_rebuild=orders_mv,catalog=ice_ivm_empty_delta_${uuid0},level=full
-- @imv_equivalence_check=orders_mv
SELECT id, amount, name FROM orders_mv ORDER BY id;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_mv;
DROP TABLE ice_ivm_empty_delta_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_empty_delta_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_empty_delta_${uuid0};
