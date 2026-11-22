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
-- @tags=iceberg,ivm,projection,optimize,truncate
-- Exercise both replacement and full-file deletion with the same projection MV.

-- query 1
-- @skip_result_check=true
CREATE CATALOG ivm_optimize_truncate_ice_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ivm_optimize_truncate_ice_${uuid0}.ns_${uuid0};
USE ivm_optimize_truncate_ice_${uuid0}.ns_${uuid0};
SET SESSION enable_materialized_view_rewrite = false;
CREATE TABLE orders (
  id BIGINT NOT NULL,
  customer VARCHAR,
  amount BIGINT
) WITH (format_version = 3);
INSERT INTO orders VALUES ROW(1, 'A', 10), ROW(2, 'A', 20);
INSERT INTO orders VALUES ROW(3, 'B', 30);
CREATE MATERIALIZED VIEW orders_maintenance_mv AS
SELECT customer, amount FROM orders WHERE amount >= 10;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_maintenance_mv;

-- query 3
-- @imv_equivalence_check=orders_maintenance_mv
SELECT customer, amount FROM orders_maintenance_mv ORDER BY customer, amount;

-- query 4
-- The metric golden requires an actual two-file rewrite.
ALTER TABLE orders EXECUTE optimize;

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_maintenance_mv;

-- query 6
-- @imv_equivalence_check=orders_maintenance_mv
SELECT customer, amount FROM orders_maintenance_mv ORDER BY customer, amount;

-- query 7
-- @skip_result_check=true
TRUNCATE TABLE orders;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_maintenance_mv;

-- query 9
-- @imv_equivalence_check=orders_maintenance_mv
SELECT count(*) AS live_rows FROM orders_maintenance_mv;

-- query 10
SELECT count(*) AS live_rows FROM orders;

-- query 11
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_maintenance_mv;
DROP TABLE ivm_optimize_truncate_ice_${uuid0}.ns_${uuid0}.orders;
DROP SCHEMA ivm_optimize_truncate_ice_${uuid0}.ns_${uuid0};
DROP CATALOG ivm_optimize_truncate_ice_${uuid0};
