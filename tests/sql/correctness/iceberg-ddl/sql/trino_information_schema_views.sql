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
-- @tags=iceberg,view,system_catalog
-- Correctness owner: iceberg-ddl. Trino C03/E04/E06/E07 provide
-- independent row types, session binding and table_type filtering rules.
-- Use an explicit REST catalog because Hadoop does not provide view listing.
-- This case drops its view before the source table and restores suite context.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG trc_metadata_${uuid0}
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="rest",
    "uri"="${iceberg_rest_uri}",
    "warehouse"="${iceberg_rest_warehouse}",
    "credential.object-store-metadata.consumer-role" = "frontend",
    "credential.object-store-metadata.mode" = "${iceberg_object_store_credential_mode}",
    "credential.object-store-metadata.name" = "${iceberg_object_store_metadata_credential_name}",
    "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
    "credential.object-store-data.consumer-role" = "backend",
    "credential.object-store-data.mode" = "${iceberg_object_store_credential_mode}",
    "credential.object-store-data.name" = "${iceberg_object_store_data_credential_name}",
    "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
    "aws.s3.endpoint"="${oss_endpoint}",
    "aws.s3.region"="${oss_region}",
    "aws.s3.enable_path_style_access"="${oss_path_style}"
);
CREATE DATABASE trc_metadata_${uuid0}.metadata_${uuid0};
CREATE TABLE trc_metadata_${uuid0}.metadata_${uuid0}.metadata_base (a BIGINT);
CREATE VIEW trc_metadata_${uuid0}.metadata_${uuid0}.metadata_view
AS SELECT a FROM trc_metadata_${uuid0}.metadata_${uuid0}.metadata_base;

-- query 2
SELECT table_name, table_type
FROM trc_metadata_${uuid0}.information_schema.tables
WHERE table_schema = 'metadata_${uuid0}'
ORDER BY table_name;

-- query 3
-- @skip_result_check=true
USE trc_metadata_${uuid0}.metadata_${uuid0};

-- query 4
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'metadata_${uuid0}'
ORDER BY table_name;

-- query 5
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'metadata_${uuid0}' AND table_type = 'VIEW'
ORDER BY table_name;

-- query 6
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'metadata_${uuid0}' AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- query 7
SELECT t.table_name, t.table_type
FROM trc_metadata_${uuid0}.information_schema.tables t
JOIN trc_metadata_${uuid0}.information_schema.schemata s
  ON t.table_catalog = s.catalog_name AND t.table_schema = s.schema_name
WHERE t.table_schema = 'metadata_${uuid0}' AND s.schema_name = 'metadata_${uuid0}'
ORDER BY t.table_name;

-- query 8
SELECT table_name, table_type
FROM trc_metadata_${uuid0}.information_schema.tables
WHERE table_schema = (
  SELECT schema_name FROM trc_metadata_${uuid0}.information_schema.schemata
  WHERE schema_name = 'metadata_${uuid0}'
)
ORDER BY table_name;

-- query 9
-- @skip_result_check=true
INSERT INTO trc_metadata_${uuid0}.metadata_${uuid0}.metadata_base (a)
SELECT CAST(7 AS bigint) FROM system.information_schema.tables
WHERE table_schema = 'metadata' AND table_name = 'materialized_views';

-- query 10
SELECT a FROM trc_metadata_${uuid0}.metadata_${uuid0}.metadata_base ORDER BY a;

-- query 11
-- @skip_result_check=true
CREATE TABLE trc_metadata_${uuid0}.metadata_${uuid0}.metadata_capture AS
SELECT table_name, table_type FROM system.information_schema.tables
WHERE table_schema = 'metadata' AND table_name = 'materialized_views';

-- query 12
SELECT table_name, table_type
FROM trc_metadata_${uuid0}.metadata_${uuid0}.metadata_capture ORDER BY table_name;

-- query 13
-- @cleanup=true
-- @skip_result_check=true
DROP VIEW IF EXISTS trc_metadata_${uuid0}.metadata_${uuid0}.metadata_view;
DROP TABLE IF EXISTS trc_metadata_${uuid0}.metadata_${uuid0}.metadata_capture;
DROP TABLE IF EXISTS trc_metadata_${uuid0}.metadata_${uuid0}.metadata_base;
USE iceberg_ddl_cat_${suite_uuid0}.${case_db};
DROP DATABASE IF EXISTS trc_metadata_${uuid0}.metadata_${uuid0};
DROP CATALOG trc_metadata_${uuid0};
