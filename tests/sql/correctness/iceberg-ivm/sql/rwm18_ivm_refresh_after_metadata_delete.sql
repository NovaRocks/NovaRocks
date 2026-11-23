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

-- @order_sensitive=true
-- @sequential=true
-- A projection MV refresh preserves a duplicate from a partition outside the metadata DELETE.

-- query 1
-- @skip_result_check=true
CREATE CATALOG rwm18_ivm_ice_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA rwm18_ivm_ice_${uuid0}.ns_${uuid0};
USE rwm18_ivm_ice_${uuid0}.ns_${uuid0};
SET SESSION enable_materialized_view_rewrite = false;
CREATE TABLE orders (id BIGINT NOT NULL, p INTEGER, customer VARCHAR, amount BIGINT)
WITH (partitioning = ARRAY['p'], format_version = 3);
INSERT INTO orders VALUES ROW(1, 10, 'A', 10), ROW(2, 10, 'A', 20),
  ROW(3, 20, 'A', 10), ROW(4, 20, 'B', 30), ROW(5, 10, 'X', 0);
CREATE MATERIALIZED VIEW orders_meta_delete_mv AS
SELECT customer, amount FROM orders WHERE amount >= 10;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_meta_delete_mv;

-- query 3
-- @imv_equivalence_check=orders_meta_delete_mv
SELECT customer, amount FROM orders_meta_delete_mv ORDER BY customer, amount;

-- query 4
-- @skip_result_check=true
-- @expect_update_count=3
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM orders WHERE p = 10;

-- query 5
SELECT s.operation, CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS deleted_rows,
       element_at(s.summary, 'added-delete-files') IS NULL AS no_delete_files
FROM "orders$snapshots" AS s JOIN "orders$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 6
-- @skip_result_check=true
SET SESSION explain_detail = 'VERBOSE';

-- query 7
-- @skip_result_check=true
-- @result_contains=source: IcebergDeltaTable from_snapshot_id=
EXPLAIN REFRESH MATERIALIZED VIEW orders_meta_delete_mv;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_meta_delete_mv;

-- query 9
-- @skip_result_check=true
USE rwm18_ivm_ice_${uuid0}.ns_${uuid0};

-- query 10
-- @imv_equivalence_check=orders_meta_delete_mv
SELECT customer, amount FROM orders_meta_delete_mv ORDER BY customer, amount;

-- query 11
SELECT id FROM orders ORDER BY id;

-- query 12
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS rwm18_ivm_ice_${uuid0}.ns_${uuid0}.orders_meta_delete_mv;
DROP TABLE IF EXISTS rwm18_ivm_ice_${uuid0}.ns_${uuid0}.orders;
DROP SCHEMA IF EXISTS rwm18_ivm_ice_${uuid0}.ns_${uuid0};
DROP CATALOG rwm18_ivm_ice_${uuid0};
