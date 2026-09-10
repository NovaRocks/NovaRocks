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
-- @tags=write_path,mv,iceberg,ivm,storage_engine_iceberg,projection_filter,delete,hidden_pk
-- Test Point:
--   Validate projection/filter MV delete apply when the MV PRIMARY KEY is not
--   part of the user-visible SELECT output.
-- Method:
--   Create a projection MV over an Iceberg base table while hiding the PK
--   column from the MV output, refresh through position-delete and
--   equality-delete snapshots, and verify the visible MV rows are removed.
-- Scope:
--   Iceberg-target projection/filter MV on an unpartitioned Iceberg v3
--   row-lineage base table.
-- Note:
--   The original StarRocks table case used PRIMARY KEY on the MV; Iceberg-target
--   MVs reject PRIMARY KEY, so the MV is defined without it. The test point
--   (delete propagation to hidden-column projection MV) is preserved via the
--   row-lineage delete mechanism.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_hidden_pk_delete_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/mv_hidden_pk_delete_${uuid0}",
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
CREATE DATABASE mv_hidden_pk_delete_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_hidden_pk_delete_ice_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  customer STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO mv_hidden_pk_delete_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'A', 10),
  (2, 'A', 20),
  (3, 'B', 30);
SET CATALOG mv_hidden_pk_delete_ice_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_hidden_pk_delete_mv
DISTRIBUTED BY HASH(customer) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT customer, amount
FROM orders
WHERE amount >= 10;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_hidden_pk_delete_mv;

-- query 3
SELECT customer, amount
FROM orders_hidden_pk_delete_mv
ORDER BY customer, amount;

-- query 4
-- @skip_result_check=true
DELETE FROM mv_hidden_pk_delete_ice_${uuid0}.ns_${uuid0}.orders WHERE id = 1;

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_hidden_pk_delete_mv;

-- query 6
SELECT customer, amount
FROM orders_hidden_pk_delete_mv
ORDER BY customer, amount;

-- query 7
-- @skip_result_check=true
ALTER TABLE mv_hidden_pk_delete_ice_${uuid0}.ns_${uuid0}.orders
ADD EQUALITY DELETE (id) VALUES (2);

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_hidden_pk_delete_mv;

-- query 9
SELECT customer, amount
FROM orders_hidden_pk_delete_mv
ORDER BY customer, amount;

-- query 10
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_hidden_pk_delete_mv;
DROP TABLE mv_hidden_pk_delete_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_hidden_pk_delete_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_hidden_pk_delete_ice_${uuid0};
