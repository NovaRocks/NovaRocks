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
-- @tags=optimizer,mv,rewrite,iceberg,aggregate-filter

-- query 1
-- @skip_result_check=true
CREATE CATALOG mvrw_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');

-- query 2
-- @skip_result_check=true
CREATE SCHEMA mvrw_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
USE mvrw_${uuid0}.ns_${uuid0};
CREATE TABLE user_tags (dt VARCHAR, user_id INTEGER, tag_id INTEGER) WITH (format_version = 3);
INSERT INTO user_tags VALUES ROW('d', 1, 1), ROW('d', 2, 3), ROW('d', 3, NULL);

-- query 4
-- @skip_result_check=true
CREATE MATERIALIZED VIEW tag_projection_mv AS SELECT user_id, dt FROM user_tags;
REFRESH MATERIALIZED VIEW tag_projection_mv;

-- query 5
SELECT user_id, dt FROM tag_projection_mv ORDER BY user_id;

-- query 6
-- The fresh projection omits tag_id, which is required only by FILTER.
-- @explain_not_contains=rewritten with mv
SELECT count(*) FILTER (WHERE tag_id > 1) AS c, count(*) AS n FROM user_tags;

-- query 7
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS mvrw_${uuid0}.ns_${uuid0}.tag_projection_mv;
DROP TABLE IF EXISTS mvrw_${uuid0}.ns_${uuid0}.user_tags;
DROP SCHEMA IF EXISTS mvrw_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS mvrw_${uuid0};
