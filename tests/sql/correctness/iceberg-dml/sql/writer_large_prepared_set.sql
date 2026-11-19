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
-- One file per distinct partition crosses the retired 16384-fragment limit.

-- query 1
-- @skip_result_check=true
CREATE CATALOG "rwm13_large_${uuid0}" USING iceberg WITH ("iceberg.catalog.type" = 'rest', "iceberg.max-partitions-per-writer" = '16385', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA rwm13_large_${uuid0}.rwm13_${uuid0};
CREATE TABLE rwm13_large_${uuid0}.rwm13_${uuid0}.t (p BIGINT) WITH (partitioning = ARRAY['p']);
INSERT INTO rwm13_large_${uuid0}.rwm13_${uuid0}.t
SELECT generate_series FROM TABLE(generate_series(1, 16385));

-- query 2
SELECT count(*) AS n, sum(p) AS partition_sum FROM rwm13_large_${uuid0}.rwm13_${uuid0}.t;

-- query 3
SELECT count(*) AS data_files
FROM rwm13_large_${uuid0}.rwm13_${uuid0}."t$files" WHERE content = 0;

-- query 4
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS rwm13_large_${uuid0}.rwm13_${uuid0}.t ;
DROP SCHEMA IF EXISTS rwm13_large_${uuid0}.rwm13_${uuid0};
DROP CATALOG IF EXISTS rwm13_large_${uuid0};
