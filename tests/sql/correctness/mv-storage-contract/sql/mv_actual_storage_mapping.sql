-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0.
--
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,storage-contract

-- query 1
-- @skip_result_check=true
CREATE CATALOG rwm10b_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA rwm10b_${uuid0}.ns_${uuid0};
USE rwm10b_${uuid0}.ns_${uuid0};
SET SESSION enable_materialized_view_rewrite = false;
CREATE TABLE source_table (id BIGINT NOT NULL, price DECIMAL(12, 2)) WITH (format_version = 3);
INSERT INTO source_table VALUES (1, DECIMAL '1234567890.01'), (2, DECIMAL '-0.17'), (3, NULL);

-- query 2
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=0,decimal-mapping=price
CREATE MATERIALIZED VIEW target_mv WITH (refresh_policy = 'MANUAL') AS SELECT id, price FROM source_table;

-- query 3
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=1,full-refreshes=1,source=source_table,logical-rows=3,decimal-mapping=price
REFRESH MATERIALIZED VIEW target_mv;

-- query 4
SELECT id, price FROM target_mv ORDER BY id;

-- query 5
SELECT id, price FROM source_table ORDER BY id;

-- query 6
-- @skip_result_check=true
-- @expect_update_count=0
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
CREATE TABLE IF NOT EXISTS source_table AS
SELECT JSON '1' AS payload FROM missing_json_source;

-- query 7
-- @expect_trino_error=TABLE_ALREADY_EXISTS,USER_ERROR
CREATE TABLE source_table AS SELECT JSON '1' AS payload;

-- query 8
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=Unsupported: Iceberg JSON writes are not supported yet
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
CREATE TABLE rejected_json_ctas AS SELECT id, ARRAY[JSON '1'] AS payload FROM source_table;

-- query 9
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
CREATE MATERIALIZED VIEW rejected_json_mv WITH (refresh_policy = 'MANUAL')
AS SELECT id, JSON '1' AS payload FROM source_table;

-- query 10
SELECT count(*) AS rejected_targets
FROM rwm10b_${uuid0}.information_schema.tables
WHERE table_schema = 'ns_${uuid0}'
  AND table_name IN ('rejected_json_ctas', 'rejected_json_mv');

-- query 11
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.rejected_json_mv;
DROP TABLE IF EXISTS ns_${uuid0}.rejected_json_ctas;
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.target_mv;
DROP TABLE IF EXISTS ns_${uuid0}.source_table;
DROP SCHEMA IF EXISTS rwm10b_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS rwm10b_${uuid0};
