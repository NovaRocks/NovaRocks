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
-- @tags=iceberg,ivm,partition_replace

-- query 1
-- @skip_result_check=true
CREATE CATALOG ivm_partition_replace_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ivm_partition_replace_${uuid0}.ns_${uuid0};
USE ivm_partition_replace_${uuid0}.ns_${uuid0};
SET SESSION enable_materialized_view_rewrite = false;
CREATE TABLE partitioned_source (p INT, id BIGINT, payload VARCHAR)
WITH (format_version = 3, partitioning = ARRAY['p']);
INSERT INTO partitioned_source SELECT 2, id, to_hex(sha256(to_utf8(CAST(id AS VARCHAR))))
FROM TABLE(sequence(1, 2048)) AS retained(id);
INSERT INTO partitioned_source VALUES ROW(1, 1, 'old'), ROW(1, 2, 'old');
CREATE MATERIALIZED VIEW partitioned_mv WITH (partitioning = ARRAY['p']) AS
SELECT p, id FROM partitioned_source;
REFRESH MATERIALIZED VIEW partitioned_mv;

-- query 2
-- @skip_result_check=true
-- A metadata delete removes the whole source partition before the append.
DELETE FROM partitioned_source WHERE p = 1;
INSERT INTO partitioned_source VALUES ROW(1, 9, 'replacement');

-- query 3
-- @skip_result_check=true
-- @result_contains=MV refresh: PartitionReplace
-- @result_contains=Affected target partitions: 1
EXPLAIN REFRESH MATERIALIZED VIEW partitioned_mv;

-- query 4
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW partitioned_mv;

-- query 5
-- @imv_equivalence_check=partitioned_mv
SELECT p, count(*) AS rows_visible, sum(id) AS total
FROM partitioned_mv GROUP BY p ORDER BY p;

-- query 6
-- @skip_result_check=true
-- A single row must be removed from a file with remaining rows.
DELETE FROM partitioned_source WHERE p = 2 AND id = 1;

-- query 7
-- @skip_result_check=true
-- @result_not_contains=MV refresh: PartitionReplace
EXPLAIN REFRESH MATERIALIZED VIEW partitioned_mv;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW partitioned_mv;

-- query 9
-- @imv_equivalence_check=partitioned_mv
SELECT p, count(*) AS rows_visible, sum(id) AS total
FROM partitioned_mv GROUP BY p ORDER BY p;

-- query 10
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS partitioned_mv;
DROP TABLE IF EXISTS partitioned_source;
DROP SCHEMA IF EXISTS ivm_partition_replace_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS ivm_partition_replace_${uuid0};
