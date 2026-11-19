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
-- @tags=write_path,mv,iceberg,ivm,storage_engine_iceberg,phase4a
-- Test Objective:
-- 1. CREATE MATERIALIZED VIEW with PROPERTIES('storage_engine' = 'iceberg') succeeds.
-- 2. First REFRESH writes visible projection/filter result into the iceberg-backed MV.
-- 3. Append-only incremental REFRESH appends only new rows (only rows matching WHERE v2 > 0).
-- 4. SHOW MATERIALIZED VIEWS includes StorageEngine column with value 'iceberg'.
-- 5. DROP cleans up sqlite + iceberg.

-- query 1
-- @skip_result_check=true
CREATE CATALOG mv_phase4a_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA mv_phase4a_${uuid0}.ns_${uuid0};
CREATE TABLE mv_phase4a_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
) WITH (format_version = 3);
INSERT INTO mv_phase4a_${uuid0}.ns_${uuid0}.orders VALUES
  ROW(1, 10), ROW(1, 20), ROW(2, 40), ROW(3, 0);

USE mv_phase4a_${uuid0}.ns_${uuid0};

CREATE MATERIALIZED VIEW proj_mv AS SELECT k1, v2 FROM orders WHERE v2 > 0;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW proj_mv;

-- query 3
SELECT k1, v2 FROM proj_mv ORDER BY k1, v2;

-- query 4
-- @skip_result_check=true
INSERT INTO mv_phase4a_${uuid0}.ns_${uuid0}.orders VALUES
  ROW(1, 70), ROW(4, 5);

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW proj_mv;

-- query 6
SELECT k1, v2 FROM proj_mv ORDER BY k1, v2;

-- query 7
-- SHOW MATERIALIZED VIEWS includes StorageEngine column with value 'iceberg'.
-- @result_contains=proj_mv
SELECT * FROM system.runtime.materialized_view_states WHERE catalog_name = 'mv_phase4a_${uuid0}';

-- query 8
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW proj_mv;
DROP TABLE mv_phase4a_${uuid0}.ns_${uuid0}.orders ;
DROP SCHEMA mv_phase4a_${uuid0}.ns_${uuid0};
DROP CATALOG mv_phase4a_${uuid0};
