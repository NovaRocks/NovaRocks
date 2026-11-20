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

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.m06a08_default_blocks (v BIGINT) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.m06a08_default_blocks
SELECT generate_series FROM TABLE(generate_series(1, 1024));

-- query 3
-- @be_log_contains=NOVAROCKS_CONNECTOR_PARQUET_FIRST_BATCH max_read_rows=8192 max_read_bytes=16777216
SELECT count(v) AS rows, sum(v) AS total FROM ${case_db}.m06a08_default_blocks;

-- query 4
-- @skip_result_check=true
CREATE CATALOG m06a08_blocks_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}', "parquet.max-read-block-row-count" = '512', "parquet.max-read-block-size" = '32MB');
CREATE SCHEMA m06a08_blocks_${uuid0}.m06a08_reader_${uuid0};

-- query 5
-- @skip_result_check=true
CREATE TABLE m06a08_blocks_${uuid0}.m06a08_reader_${uuid0}.read_blocks (v BIGINT) WITH (format_version = 3);

-- query 6
-- @skip_result_check=true
INSERT INTO m06a08_blocks_${uuid0}.m06a08_reader_${uuid0}.read_blocks SELECT generate_series FROM TABLE(generate_series(1, 1024));

-- query 7
-- @be_log_contains=NOVAROCKS_CONNECTOR_PARQUET_FIRST_BATCH max_read_rows=512 max_read_bytes=33554432 reader_batch_rows=512 decoded_rows=512
SELECT count(v) AS rows, sum(v) AS total FROM m06a08_blocks_${uuid0}.m06a08_reader_${uuid0}.read_blocks;

-- query 8
-- @skip_result_check=true
SET SESSION m06a08_blocks_${uuid0}.parquet_max_read_block_row_count = 256;

-- query 9
-- @be_log_contains=NOVAROCKS_CONNECTOR_PARQUET_FIRST_BATCH max_read_rows=256 max_read_bytes=33554432 reader_batch_rows=256 decoded_rows=256
SELECT count(v) AS rows, sum(v) AS total FROM m06a08_blocks_${uuid0}.m06a08_reader_${uuid0}.read_blocks;

-- query 10
-- @be_log_contains=NOVAROCKS_CONNECTOR_PARQUET_FIRST_BATCH max_read_rows=512 max_read_bytes=1024 reader_batch_rows=128 decoded_rows=128
WITH SESSION m06a08_blocks_${uuid0}.parquet_max_read_block_row_count = 512, m06a08_blocks_${uuid0}.parquet_max_read_block_size = '1kB' SELECT count(v) AS rows, sum(v) AS total FROM m06a08_blocks_${uuid0}.m06a08_reader_${uuid0}.read_blocks;

-- query 11
-- @be_log_contains=NOVAROCKS_CONNECTOR_PARQUET_FIRST_BATCH max_read_rows=512 max_read_bytes=0 reader_batch_rows=1 decoded_rows=1
WITH SESSION m06a08_blocks_${uuid0}.parquet_max_read_block_row_count = 512, m06a08_blocks_${uuid0}.parquet_max_read_block_size = '0B' SELECT count(v) AS rows, sum(v) AS total FROM m06a08_blocks_${uuid0}.m06a08_reader_${uuid0}.read_blocks;

-- query 12
-- @skip_result_check=true
RESET SESSION m06a08_blocks_${uuid0}.parquet_max_read_block_row_count;

-- query 13
-- @be_log_contains=NOVAROCKS_CONNECTOR_PARQUET_FIRST_BATCH max_read_rows=512 max_read_bytes=33554432 reader_batch_rows=512 decoded_rows=512
SELECT count(v) AS rows, sum(v) AS total FROM m06a08_blocks_${uuid0}.m06a08_reader_${uuid0}.read_blocks;

-- query 14
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS m06a08_blocks_${uuid0}.m06a08_reader_${uuid0}.read_blocks;
DROP SCHEMA IF EXISTS m06a08_blocks_${uuid0}.m06a08_reader_${uuid0};
DROP CATALOG IF EXISTS m06a08_blocks_${uuid0};
