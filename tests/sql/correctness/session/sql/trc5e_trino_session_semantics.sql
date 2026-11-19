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
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
WITH SESSION no_such_property = true SELECT CAST(1 AS integer);

-- query 2
-- @expect_trino_error=CATALOG_NOT_FOUND,USER_ERROR
WITH SESSION unknown_catalog.any_property = true SELECT CAST(1 AS integer);

-- query 3
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
EXPLAIN WITH SESSION no_such_property = true SELECT CAST(1 AS integer);

-- query 4
-- @expect_trino_error=NOT_IN_TRANSACTION,USER_ERROR
COMMIT;

-- query 5
-- @expect_trino_error=NOT_IN_TRANSACTION,USER_ERROR
ROLLBACK;

-- query 6
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION no_such_property = 1;

-- query 7
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
RESET SESSION no_such_property;


-- query 8
-- @skip_result_check=true
CREATE CATALOG trc5e_session_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA trc5e_session_${uuid0}.trc5e_session;
USE trc5e_session_${uuid0}.trc5e_session;

-- query 9
-- @skip_result_check=true
SET SESSION trc5e_session_${uuid0}.incremental_refresh_enabled = false;

-- query 10
WITH SESSION trc5e_session_${uuid0}.incremental_refresh_enabled = true SELECT CAST(17 AS integer) AS result;

-- query 11
-- @result_contains=result
EXPLAIN WITH SESSION trc5e_session_${uuid0}.incremental_refresh_enabled = true SELECT CAST(17 AS integer) AS result;

-- query 12
-- @skip_result_check=true
RESET SESSION trc5e_session_${uuid0}.incremental_refresh_enabled;

-- query 13
-- @cleanup=true
-- @skip_result_check=true
DROP SCHEMA IF EXISTS trc5e_session_${uuid0}.trc5e_session;
DROP CATALOG IF EXISTS trc5e_session_${uuid0};
