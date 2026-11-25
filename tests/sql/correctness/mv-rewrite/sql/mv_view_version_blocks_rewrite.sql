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
-- @tags=mv,rewrite,iceberg
-- Keep the source snapshot and result declaration fixed across a VIEW version change.

-- query 1
-- @skip_result_check=true
CREATE CATALOG rwm10b_rw_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA rwm10b_rw_${uuid0}.ns_${uuid0};
USE rwm10b_rw_${uuid0}.ns_${uuid0};
CREATE TABLE base_rows (id BIGINT NOT NULL, x BIGINT NOT NULL) WITH (format_version = 3);
INSERT INTO base_rows SELECT number, number % 100 FROM TABLE(sequence(1, 12000)) t(number);
CREATE VIEW source_view AS SELECT id, x + CAST(1 AS BIGINT) AS y FROM base_rows WHERE x > 98;
CREATE MATERIALIZED VIEW projected_mv WITH (refresh_policy = 'MANUAL') AS SELECT id, y FROM source_view;
REFRESH MATERIALIZED VIEW projected_mv;

-- query 2
SELECT id, y FROM source_view WHERE id > 11900 ORDER BY id;

-- query 3
-- @skip_result_check=true
CREATE OR REPLACE VIEW source_view AS SELECT id, x + CAST(2 AS BIGINT) AS y FROM base_rows WHERE x > 98;

-- query 4
-- @explain_not_contains=rewritten with mv: projected_mv
SELECT id, x + CAST(1 AS BIGINT) AS y FROM base_rows WHERE x > 98 AND id > 11900 ORDER BY id;

-- query 5
-- @explain_not_contains=rewritten with mv: projected_mv
SELECT id, y FROM source_view WHERE id > 11900 ORDER BY id;

-- query 6
-- @skip_result_check=true
SET SESSION enable_materialized_view_rewrite = false;

-- query 7
SELECT id, y FROM source_view WHERE id > 11900 ORDER BY id;

-- query 8
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.projected_mv;
DROP VIEW IF EXISTS ns_${uuid0}.source_view;
DROP TABLE IF EXISTS ns_${uuid0}.base_rows;
DROP SCHEMA IF EXISTS rwm10b_rw_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS rwm10b_rw_${uuid0};
