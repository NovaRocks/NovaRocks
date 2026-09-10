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
-- @tags=write_path,mv,iceberg,ivm,storage_engine_iceberg,strategy
-- Test Objective:
-- 1. Validate aggregate MV full refresh over a v3 row-lineage Iceberg base table.
-- 2. Validate append snapshots refresh incrementally.
-- 3. Validate INSERT OVERWRITE refresh falls back to full refresh and advances metadata.
-- 4. Validate S3-backed Iceberg DELETE mutates visible base rows and MV refresh state.
-- MV is Iceberg-target (PROPERTIES('storage_engine'='iceberg')).

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_strategy_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/mv_strategy_${uuid0}",
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
CREATE DATABASE mv_strategy_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_strategy_ice_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  customer STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO mv_strategy_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'A', 10),
  (2, 'A', 20),
  (3, 'B', 30);
SET CATALOG mv_strategy_ice_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_strategy_mv
DISTRIBUTED BY HASH(customer) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT
  customer,
  COUNT(*) AS c,
  SUM(amount) AS s
FROM orders
GROUP BY customer;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_strategy_mv;

-- query 3
SELECT customer, c, s
FROM orders_strategy_mv
ORDER BY customer;

-- query 4
-- @skip_result_check=true
INSERT INTO mv_strategy_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (4, 'A', 100);

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_strategy_mv;

-- query 6
SELECT customer, c, s
FROM orders_strategy_mv
ORDER BY customer;

-- query 7
-- @skip_result_check=true
INSERT OVERWRITE mv_strategy_ice_${uuid0}.ns_${uuid0}.orders
SELECT id, customer, amount + 100
FROM mv_strategy_ice_${uuid0}.ns_${uuid0}.orders
WHERE id >= 2;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_strategy_mv;

-- query 9
SELECT customer, c, s
FROM orders_strategy_mv
ORDER BY customer;

-- query 10
-- @skip_result_check=true
DELETE FROM mv_strategy_ice_${uuid0}.ns_${uuid0}.orders WHERE id = 2;

-- query 11
SELECT id, customer, amount
FROM mv_strategy_ice_${uuid0}.ns_${uuid0}.orders
ORDER BY id;

-- query 12
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_strategy_mv;

-- query 13
SELECT customer, c, s
FROM orders_strategy_mv
ORDER BY customer;

-- query 14
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_strategy_mv;
DROP TABLE mv_strategy_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_strategy_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_strategy_ice_${uuid0};
