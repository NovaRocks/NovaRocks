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
-- @tags=iceberg,ivm,join,runtime_filter

-- query 1
-- @skip_result_check=true
CREATE CATALOG ivm_full_join_filter_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ivm_full_join_filter_${uuid0}.ns_${uuid0};
USE ivm_full_join_filter_${uuid0}.ns_${uuid0};
SET SESSION enable_materialized_view_rewrite = false;
SET SESSION enable_global_runtime_filter = true;
SET SESSION global_runtime_filter_wait_timeout = 5000;
SET SESSION runtime_filter_scan_wait_time = 5000;
CREATE TABLE facts (k BIGINT, amount BIGINT)
WITH (format_version = 3, partitioning = ARRAY['k']);
CREATE TABLE chosen_keys (k BIGINT, category VARCHAR) WITH (format_version = 3);
INSERT INTO facts SELECT 42, id FROM TABLE(sequence(1, 4096)) AS n(id);
INSERT INTO facts VALUES ROW(1, 10), ROW(1, 20), ROW(2, 30);
INSERT INTO chosen_keys VALUES ROW(1, 'first');
CREATE MATERIALIZED VIEW selected_facts AS
SELECT f.amount, d.category
FROM facts f JOIN chosen_keys d ON f.k = d.k;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW selected_facts;

-- query 3
-- @imv_equivalence_check=selected_facts
SELECT amount, category FROM selected_facts ORDER BY amount;

-- query 4
-- @skip_result_check=true
DELETE FROM chosen_keys WHERE k = 1;
INSERT INTO chosen_keys VALUES ROW(2, 'second');
SET SESSION ivm_full_join_filter_${uuid0}.incremental_refresh_enabled = false;
REFRESH MATERIALIZED VIEW selected_facts;

-- query 5
-- @imv_equivalence_check=selected_facts
SELECT amount, category FROM selected_facts ORDER BY amount;

-- query 6
-- @skip_result_check=true
DELETE FROM chosen_keys WHERE k = 2;
REFRESH MATERIALIZED VIEW selected_facts;
RESET SESSION ivm_full_join_filter_${uuid0}.incremental_refresh_enabled;

-- query 7
-- @imv_equivalence_check=selected_facts
SELECT count(*) AS rows_visible FROM selected_facts;

-- query 8
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS selected_facts;
DROP TABLE IF EXISTS chosen_keys;
DROP TABLE IF EXISTS facts;
DROP SCHEMA IF EXISTS ivm_full_join_filter_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS ivm_full_join_filter_${uuid0};
