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
CREATE CATALOG m06a08_session_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}', "parquet.max-read-block-row-count" = '128', "parquet.max-read-block-size" = '0B');

-- query 2
-- @skip_result_check=true
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_row_count = 128;

-- query 3
-- @skip_result_check=true
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_row_count = 65536;

-- query 4
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_row_count = 127;

-- query 5
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_row_count = 65537;

-- query 6
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_row_count = 0;

-- query 7
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_row_count = -1;

-- query 8
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_row_count = '8192';

-- query 9
-- @skip_result_check=true
RESET SESSION m06a08_session_${uuid0}.parquet_max_read_block_row_count;

-- query 10
WITH SESSION m06a08_session_${uuid0}.parquet_max_read_block_row_count = 128 SELECT CAST(17 AS INTEGER) AS value;

-- query 11
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
WITH SESSION m06a08_session_${uuid0}.parquet_max_read_block_row_count = 127 SELECT CAST(17 AS INTEGER) AS value;

-- query 12
-- @skip_result_check=true
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_size = '16MB';

-- query 13
-- @skip_result_check=true
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_size = '1.5GB';

-- query 14
-- @skip_result_check=true
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_size = '0B';

-- query 15
-- @skip_result_check=true
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_size = '16 MB';

-- query 16
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_size = '16mb';

-- query 17
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_size = 'abc';

-- query 18
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_size = '-1MB';

-- query 19
-- @skip_result_check=true
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_size = '9223372036854775807B';

-- query 20
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_size = '9223372036854775808B';

-- query 21
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION m06a08_session_${uuid0}.parquet_max_read_block_size = 16;

-- query 22
-- @skip_result_check=true
RESET SESSION m06a08_session_${uuid0}.parquet_max_read_block_size;

-- query 23
WITH SESSION m06a08_session_${uuid0}.parquet_max_read_block_size = '1kB' SELECT CAST(17 AS INTEGER) AS value;

-- query 24
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
WITH SESSION m06a08_session_${uuid0}.parquet_max_read_block_size = 'abc' SELECT CAST(17 AS INTEGER) AS value;

-- query 25
-- @cleanup=true
-- @skip_result_check=true
DROP CATALOG IF EXISTS m06a08_session_${uuid0};
