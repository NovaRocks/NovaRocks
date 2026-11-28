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
-- @tags=iceberg,ivm,content_apply,cost

-- query 1
-- @skip_result_check=true
CREATE CATALOG ivm_cost_explain_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ivm_cost_explain_${uuid0}.ns_${uuid0};
USE ivm_cost_explain_${uuid0}.ns_${uuid0};
SET SESSION enable_materialized_view_rewrite = false;
CREATE TABLE source_rows (
  rid BIGINT NOT NULL, k BIGINT, include_row BOOLEAN, payload VARCHAR
) WITH (format_version = 3);
INSERT INTO source_rows
SELECT -id, CAST(NULL AS BIGINT), false, to_hex(sha256(to_utf8(CAST(id AS VARCHAR))))
FROM TABLE(sequence(1, 4096)) AS cold(id);
INSERT INTO source_rows VALUES
  ROW(1, NULL, true, 'delete-null'), ROW(2, 11, true, 'delete-11'),
  ROW(3, NULL, true, 'keep-null'), ROW(4, 42, true, 'keep-42');
CREATE MATERIALIZED VIEW projected_rows AS
SELECT k FROM source_rows WHERE include_row;
REFRESH MATERIALIZED VIEW projected_rows;

-- query 2
-- @skip_result_check=true
DELETE FROM source_rows WHERE rid IN (1, 2);
INSERT INTO source_rows VALUES ROW(5, 17, true, 'insert-17');

-- query 3
-- @result_contains=MV refresh: ContentLocate
-- @result_contains=MV optimizer cost ContentLocate:
-- @result_contains=exact change-window volume, source and target manifests
-- @result_contains=MV optimizer cost FullReplace:
-- @result_contains=complete source manifests
EXPLAIN REFRESH MATERIALIZED VIEW projected_rows;

-- query 4
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS projected_rows;
DROP TABLE IF EXISTS source_rows;
DROP SCHEMA IF EXISTS ivm_cost_explain_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS ivm_cost_explain_${uuid0};
