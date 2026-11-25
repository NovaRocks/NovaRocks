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
-- @tags=iceberg,ivm,projection,delete,truncate

-- query 1
-- @skip_result_check=true
CREATE CATALOG ivm_whole_file_delete_ice_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ivm_whole_file_delete_ice_${uuid0}.ns_${uuid0};
USE ivm_whole_file_delete_ice_${uuid0}.ns_${uuid0};
SET SESSION enable_materialized_view_rewrite = false;
CREATE TABLE row_delete_base (id BIGINT NOT NULL, customer VARCHAR, amount BIGINT)
WITH (format_version = 3);
CREATE TABLE truncate_base (id BIGINT NOT NULL, customer VARCHAR, amount BIGINT)
WITH (format_version = 3);
-- Each file includes a row outside the MV filter so the row-level path retains its data file.
INSERT INTO row_delete_base VALUES ROW(1, 'A', 10), ROW(2, 'B', 20), ROW(5, 'X', 0);
INSERT INTO row_delete_base VALUES ROW(3, 'A', 10), ROW(4, 'B', 20), ROW(6, 'Y', 0);
INSERT INTO truncate_base VALUES ROW(1, 'A', 10), ROW(2, 'B', 20), ROW(5, 'X', 0);
INSERT INTO truncate_base VALUES ROW(3, 'A', 10), ROW(4, 'B', 20), ROW(6, 'Y', 0);
CREATE MATERIALIZED VIEW row_delete_mv AS
SELECT customer, amount FROM row_delete_base WHERE amount >= 10;
CREATE MATERIALIZED VIEW truncate_mv AS
SELECT customer, amount FROM truncate_base WHERE amount >= 10;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW row_delete_mv;
REFRESH MATERIALIZED VIEW truncate_mv;

-- query 3
-- @imv_equivalence_check=row_delete_mv
SELECT customer, amount FROM row_delete_mv ORDER BY customer, amount;

-- query 4
-- @imv_equivalence_check=truncate_mv
SELECT customer, amount FROM truncate_mv ORDER BY customer, amount;

-- query 5
-- @skip_result_check=true
-- @expect_update_count=2
DELETE FROM row_delete_base WHERE id IN (1, 4);

-- query 6
-- @skip_result_check=true
-- @expect_update_count=2
DELETE FROM truncate_base WHERE id IN (1, 4);

-- query 7
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW row_delete_mv;
REFRESH MATERIALIZED VIEW truncate_mv;

-- query 8
-- @imv_equivalence_check=row_delete_mv
SELECT customer, amount FROM row_delete_mv ORDER BY customer, amount;

-- query 9
-- @imv_equivalence_check=truncate_mv
SELECT customer, amount FROM truncate_mv ORDER BY customer, amount;

-- query 10
-- @skip_result_check=true
-- @expect_update_count=2
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM row_delete_base WHERE amount >= 10;

-- query 11
-- @skip_result_check=true
TRUNCATE TABLE truncate_base;

-- query 12
SELECT s.operation,
       dv.vector_count >= 2 AS has_dvs,
       COALESCE(CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT), 0) = 0 AS kept_data_files
FROM "row_delete_base$snapshots" AS s JOIN "row_delete_base$refs" AS r ON s.snapshot_id = r.snapshot_id
CROSS JOIN (
  SELECT count(*) AS vector_count FROM "row_delete_base$files" WHERE content = 1 AND file_format = 'PUFFIN'
) AS dv
WHERE r.name = 'main';

-- query 13
SELECT s.operation,
       COALESCE(CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT), 0) >= 2 AS removed_data_files
FROM "truncate_base$snapshots" AS s JOIN "truncate_base$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 14
-- @skip_result_check=true
INSERT INTO row_delete_base VALUES ROW(7, 'A', 10), ROW(8, 'B', 20), ROW(9, 'A', 10), ROW(10, 'X', 0);
INSERT INTO truncate_base VALUES ROW(7, 'A', 10), ROW(8, 'B', 20), ROW(9, 'A', 10), ROW(10, 'X', 0);
REFRESH MATERIALIZED VIEW row_delete_mv;
REFRESH MATERIALIZED VIEW truncate_mv;

-- query 15
-- @imv_equivalence_check=row_delete_mv
SELECT customer, amount FROM row_delete_mv ORDER BY customer, amount;

-- query 16
-- @imv_equivalence_check=truncate_mv
SELECT customer, amount FROM truncate_mv ORDER BY customer, amount;

-- query 17
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS row_delete_mv;
DROP MATERIALIZED VIEW IF EXISTS truncate_mv;
DROP TABLE IF EXISTS ivm_whole_file_delete_ice_${uuid0}.ns_${uuid0}.row_delete_base;
DROP TABLE IF EXISTS ivm_whole_file_delete_ice_${uuid0}.ns_${uuid0}.truncate_base;
DROP SCHEMA IF EXISTS ivm_whole_file_delete_ice_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS ivm_whole_file_delete_ice_${uuid0};
