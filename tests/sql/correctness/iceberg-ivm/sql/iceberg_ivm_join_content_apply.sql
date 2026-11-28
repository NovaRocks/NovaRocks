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
-- @tags=iceberg,ivm,join,content_apply

-- query 1
-- @skip_result_check=true
CREATE CATALOG ivm_join_content_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ivm_join_content_${uuid0}.ns_${uuid0};
USE ivm_join_content_${uuid0}.ns_${uuid0};
SET SESSION enable_materialized_view_rewrite = false;
CREATE TABLE edges (
  id BIGINT NOT NULL, k BIGINT, next_key BIGINT, label VARCHAR, unused_payload VARCHAR
)
WITH (format_version = 3);
INSERT INTO edges
SELECT -id, 100 + id, -id, 'irrelevant', to_hex(sha256(to_utf8(CAST(id AS VARCHAR))))
FROM TABLE(sequence(1, 4096)) AS cold(id);
INSERT INTO edges (id, k, next_key, label) VALUES ROW(1, 10, 20, 'left'), ROW(2, 10, 20, 'left'),
  ROW(3, 20, 30, 'old'), ROW(4, 20, 30, 'old'), ROW(5, 99, 98, 'unmatched');
ANALYZE edges WITH (columns = ARRAY['next_key', 'k']);
CREATE MATERIALIZED VIEW paths AS
SELECT a.label AS start_label, b.label AS end_label
FROM edges a JOIN edges b ON a.next_key = b.k WHERE b.k < 50;
REFRESH MATERIALIZED VIEW paths;

-- query 2
-- @imv_equivalence_check=paths
SELECT start_label, end_label, count(*) AS multiplicity
FROM paths GROUP BY start_label, end_label ORDER BY start_label, end_label;

-- query 3
-- @skip_result_check=true
-- @result_contains=MV refresh: ContentLocate
-- The same source window removes both a left occurrence and a right occurrence.
DELETE FROM edges WHERE id IN (1, 3);
INSERT INTO edges (id, k, next_key, label) VALUES ROW(6, 20, 30, 'new');
ANALYZE edges WITH (columns = ARRAY['next_key', 'k']);
EXPLAIN REFRESH MATERIALIZED VIEW paths;
REFRESH MATERIALIZED VIEW paths;

-- query 4
-- @imv_equivalence_check=paths
SELECT start_label, end_label, count(*) AS multiplicity
FROM paths GROUP BY start_label, end_label ORDER BY start_label, end_label;

-- query 5
-- @skip_result_check=true
DELETE FROM edges WHERE k < 50;
REFRESH MATERIALIZED VIEW paths;

-- query 6
-- @imv_equivalence_check=paths
SELECT count(*) AS rows_visible FROM paths;

-- query 7
-- @skip_result_check=true
SET SESSION ivm_join_content_${uuid0}.incremental_refresh_enabled = false;
REFRESH MATERIALIZED VIEW paths;
RESET SESSION ivm_join_content_${uuid0}.incremental_refresh_enabled;

-- query 8
-- @imv_equivalence_check=paths
SELECT count(*) AS rows_visible FROM paths;

-- query 9
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS paths;
DROP TABLE IF EXISTS edges;
DROP SCHEMA IF EXISTS ivm_join_content_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS ivm_join_content_${uuid0};
