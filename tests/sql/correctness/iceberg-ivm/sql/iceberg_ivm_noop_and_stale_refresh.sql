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
-- @tags=mv,iceberg,ivm,storage_engine_iceberg,noop,stale
-- Test Objective:
-- 1. Validate an Iceberg-backed MV is empty before its first manual refresh.
-- 2. Validate a no-op refresh with no base snapshot change keeps target rows stable.
-- 3. Validate append-only base changes stay invisible until the next refresh.
-- 4. Validate the refreshed target contains only rows matching the MV predicate.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_noop_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_noop_${uuid0}",
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
CREATE DATABASE ice_ivm_noop_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_noop_${uuid0}.ns_${uuid0}.orders (
  order_id INT,
  amount BIGINT,
  region STRING
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO ice_ivm_noop_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10, 'east'),
  (2, 20, 'west'),
  (3, 0, 'skip');

SET CATALOG ice_ivm_noop_${uuid0};
USE ns_${uuid0};

CREATE MATERIALIZED VIEW orders_mv
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT order_id, amount, region
FROM orders
WHERE amount > 0;

-- query 2
SELECT order_id, amount, region FROM orders_mv ORDER BY order_id;

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv;

-- query 4
SELECT order_id, amount, region FROM orders_mv ORDER BY order_id;

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv;

-- query 6
SELECT order_id, amount, region FROM orders_mv ORDER BY order_id;

-- query 7
-- @skip_result_check=true
INSERT INTO ice_ivm_noop_${uuid0}.ns_${uuid0}.orders VALUES
  (4, 40, 'north'),
  (5, 0, 'skip');

-- query 8
SELECT order_id, amount, region FROM orders_mv ORDER BY order_id;

-- query 9
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv;

-- query 10
SELECT order_id, amount, region FROM orders_mv ORDER BY order_id;

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_mv;
DROP TABLE ice_ivm_noop_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_noop_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_noop_${uuid0};
