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
-- @tags=iceberg,mv,content_apply,delete_verification

-- query 1
-- @skip_result_check=true
CREATE CATALOG mv_content_insufficient_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA mv_content_insufficient_${uuid0}.ns_${uuid0};
USE mv_content_insufficient_${uuid0}.ns_${uuid0};
SET SESSION enable_materialized_view_rewrite = false;
CREATE TABLE source_rows (
  rid BIGINT NOT NULL, k BIGINT, include_row BOOLEAN, payload VARCHAR
) WITH (format_version = 3);
INSERT INTO source_rows
SELECT -id, CAST(NULL AS BIGINT), false, to_hex(sha256(to_utf8(CAST(id AS VARCHAR))))
FROM TABLE(sequence(1, 4096)) AS cold(id);
INSERT INTO source_rows VALUES ROW(1, 10, true, 'old'), ROW(2, 20, true, 'kept');
CREATE MATERIALIZED VIEW retired_mv AS SELECT k FROM source_rows WHERE include_row;
REFRESH MATERIALIZED VIEW retired_mv;

-- query 2
-- @skip_result_check=true
DELETE FROM source_rows WHERE rid = 1;
INSERT INTO source_rows VALUES ROW(3, 30, true, 'lost');

-- query 3
-- @skip_result_check=true
-- @result_contains=MV refresh: ContentLocate
EXPLAIN REFRESH MATERIALIZED VIEW retired_mv;

-- query 4
-- @skip_result_check=true
-- @query_lifecycle_fault=mv-content-apply-drop-insert,0
-- @query_lifecycle_fault=mv-content-apply-drop-insert,1
-- @query_lifecycle_fault=mv-content-apply-drop-insert,2
-- @be_log_contains=NOVAROCKS_MV_CONTENT_APPLY_INSERT_DROPPED
REFRESH MATERIALIZED VIEW retired_mv;

-- query 5
-- The injected insertion never entered the target.
SELECT k FROM retired_mv ORDER BY k;

-- query 6
-- @skip_result_check=true
DELETE FROM source_rows WHERE rid = 3;

-- query 7
-- @expect_error=MV delete verification failed and the target requires an explicit rebuild
REFRESH MATERIALIZED VIEW retired_mv;

-- query 8
-- @skip_result_check=true
SET SESSION enable_materialized_view_rewrite = true;

-- query 9
-- @skip_result_check=true
-- @result_not_contains=retired_mv
EXPLAIN SELECT k FROM source_rows WHERE include_row;

-- query 10
-- @skip_result_check=true
SET SESSION enable_materialized_view_rewrite = false;
SET SESSION mv_content_insufficient_${uuid0}.incremental_refresh_enabled = false;

-- query 11
-- @expect_error=requires an explicit rebuild
REFRESH MATERIALIZED VIEW retired_mv;

-- query 12
-- @skip_result_check=true
RESET SESSION mv_content_insufficient_${uuid0}.incremental_refresh_enabled;
DROP MATERIALIZED VIEW retired_mv;
CREATE MATERIALIZED VIEW retired_mv AS SELECT k FROM source_rows WHERE include_row;
REFRESH MATERIALIZED VIEW retired_mv;

-- query 13
-- @imv_equivalence_check=retired_mv
SELECT k FROM retired_mv ORDER BY k;

-- query 14
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS retired_mv;
DROP TABLE IF EXISTS source_rows;
DROP SCHEMA IF EXISTS mv_content_insufficient_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS mv_content_insufficient_${uuid0};
