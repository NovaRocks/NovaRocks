-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information regarding copyright
-- ownership.  The ASF licenses this file to you under the Apache License,
-- Version 2.0.

-- SQLP-5 syntax and DML admission failures use the original Trino wire.

-- query 1
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
DELETE FROM;

-- query 2
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE OR REPLACE TABLE sqlp5_duplicate_column (id INT, id BIGINT);

-- query 3
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
UPDATE sqlp5_reject_target SET target.id = 1;

-- query 4
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
MERGE INTO sqlp5_reject_target target USING sqlp5_reject_source source ON target.id = source.id
WHEN MATCHED THEN DELETE
WHEN MATCHED THEN DELETE;

-- query 5
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE TEMPORARY TABLE sqlp5_reject_target (id INT);

-- query 6
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE TABLE sqlp5_reject_target (id INT) ENGINE = olap;

-- query 7
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE TABLE sqlp5_reject_target (id INT) ORDER BY (id);

-- query 8
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE OR REPLACE TABLE reject_ice.reject_ns.t AS SELECT CAST(1 AS bigint) AS id;

-- query 9
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE EXTERNAL TABLE reject_table (id integer);

-- query 10
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE TABLE reject_table (id integer) DISTRIBUTED BY HASH(id) BUCKETS 1;

-- query 11
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE TABLE reject_table LIKE stable_source;

-- query 12
-- @skip_result_check=true
CREATE CATALOG IF NOT EXISTS "sqlp5_delete_${uuid0}" USING iceberg WITH ("iceberg.catalog.type" = '${iceberg_catalog_type}', "iceberg.catalog.warehouse" = '${iceberg_catalog_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA "sqlp5_delete_${uuid0}".reject_ns;
CREATE TABLE "sqlp5_delete_${uuid0}".reject_ns.target (id INT);

-- query 13
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
DELETE FROM "sqlp5_delete_${uuid0}".reject_ns.target
WHERE id IN (SELECT id FROM "sqlp5_delete_${uuid0}".reject_ns.target);

-- query 14
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS "sqlp5_delete_${uuid0}".reject_ns.target;
DROP SCHEMA IF EXISTS "sqlp5_delete_${uuid0}".reject_ns;
DROP CATALOG IF EXISTS "sqlp5_delete_${uuid0}";
