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
-- @tags=write_path,mv,iceberg,ivm,storage_engine_iceberg,projection_filter,row_lineage,merge,cow
-- Test Point:
--   Validate projection/filter MV incremental refresh after a MERGE INTO
--   on a v3 row-lineage Iceberg base table whose update mode defaults to
--   copy-on-write. The MERGE produces a COW UPDATE snapshot followed by a
--   FastAppend INSERT snapshot; both must reach the MV in lineage order.
-- Method:
--   Create a primary-key projection MV over the base table, refresh once,
--   MERGE in a source that updates one row's amount and inserts a new
--   row, refresh, and verify the MV reflects the matched update plus the
--   new row exactly once.
-- Scope:
--   Iceberg-target projection/filter MV on an unpartitioned Iceberg v3
--   row-lineage base table updated via MERGE INTO with default
--   copy-on-write update mode.
-- MV is Iceberg-target (PROPERTIES('storage_engine'='iceberg')).

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_merge_cow_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/mv_merge_cow_${uuid0}",
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
CREATE DATABASE mv_merge_cow_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_merge_cow_ice_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  status STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO mv_merge_cow_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'open', 10),
  (2, 'open', 20);
CREATE TABLE mv_merge_cow_ice_${uuid0}.ns_${uuid0}.staging (
  id BIGINT NOT NULL,
  status STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO mv_merge_cow_ice_${uuid0}.ns_${uuid0}.staging VALUES
  (2, 'open', 25),
  (3, 'open', 30);
SET CATALOG mv_merge_cow_ice_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_merge_cow_mv
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT id, amount
FROM orders
WHERE status = 'open';

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_merge_cow_mv;

-- query 3
SELECT id, amount
FROM orders_merge_cow_mv
ORDER BY id;

-- query 4
-- @skip_result_check=true
MERGE INTO mv_merge_cow_ice_${uuid0}.ns_${uuid0}.orders AS t
USING mv_merge_cow_ice_${uuid0}.ns_${uuid0}.staging AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET amount = s.amount
WHEN NOT MATCHED THEN INSERT (id, status, amount) VALUES (s.id, s.status, s.amount);

-- query 5
SELECT id, amount
FROM mv_merge_cow_ice_${uuid0}.ns_${uuid0}.orders
ORDER BY id;

-- query 6
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_merge_cow_mv;

-- query 7
SELECT id, amount
FROM orders_merge_cow_mv
ORDER BY id;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_merge_cow_mv;
DROP TABLE mv_merge_cow_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP TABLE mv_merge_cow_ice_${uuid0}.ns_${uuid0}.staging FORCE;
DROP DATABASE mv_merge_cow_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_merge_cow_ice_${uuid0};
