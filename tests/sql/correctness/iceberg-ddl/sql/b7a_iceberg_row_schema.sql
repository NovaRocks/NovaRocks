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

-- query 1
-- @skip_result_check=true
CREATE CATALOG b7a_rows_${uuid0} USING iceberg WITH (
    "iceberg.catalog.type" = 'rest',
    "uri" = '${iceberg_rest_uri}',
    "warehouse" = '${iceberg_rest_warehouse}',
    "credential.object-store-metadata.consumer-role" = 'frontend',
    "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}',
    "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}',
    "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}',
    "credential.object-store-data.consumer-role" = 'backend',
    "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}',
    "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}',
    "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}',
    "aws.s3.endpoint" = '${oss_endpoint}',
    "aws.s3.region" = '${oss_region}',
    "aws.s3.enable_path_style_access" = '${oss_path_style}'
);
CREATE SCHEMA b7a_rows_${uuid0}.rows_${uuid0};

-- query 2
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE TABLE b7a_rows_${uuid0}.rows_${uuid0}.b7a_anon_row AS
SELECT ROW(1) AS payload;

-- query 3
-- @expect_trino_error=DUPLICATE_COLUMN_NAME,USER_ERROR
CREATE TABLE b7a_rows_${uuid0}.rows_${uuid0}.b7a_duplicate_row AS
SELECT ROW(1 AS id, 2 AS ID) AS payload;

-- query 4
SELECT count(*) AS rejected_tables
FROM b7a_rows_${uuid0}.information_schema.tables
WHERE table_schema = 'rows_${uuid0}'
  AND table_name IN ('b7a_anon_row', 'b7a_duplicate_row');

-- query 5
-- @skip_result_check=true
CREATE TABLE b7a_rows_${uuid0}.rows_${uuid0}.b7a_named_row (
    payload ROW(id INTEGER, note VARCHAR)
) WITH (format_version = 3);

-- query 6
-- @skip_result_check=true
INSERT INTO b7a_rows_${uuid0}.rows_${uuid0}.b7a_named_row VALUES
    ROW(ROW(7, CAST('kept' AS VARCHAR))), ROW(ROW(8, NULL));

-- query 7
SELECT payload.id AS id_value, payload.note AS note
FROM b7a_rows_${uuid0}.rows_${uuid0}.b7a_named_row ORDER BY id_value;

-- query 8
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS b7a_rows_${uuid0}.rows_${uuid0}.b7a_anon_row;
DROP TABLE IF EXISTS b7a_rows_${uuid0}.rows_${uuid0}.b7a_duplicate_row;
DROP TABLE IF EXISTS b7a_rows_${uuid0}.rows_${uuid0}.b7a_named_row;
DROP SCHEMA IF EXISTS b7a_rows_${uuid0}.rows_${uuid0};
DROP CATALOG b7a_rows_${uuid0};
