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
-- @order_sensitive=false
-- @tags=column-naming,view-schema
-- Test Objective: Validate saved view headers and types, definition contexts, recursion layers, and local and Iceberg view declarations.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG trc_views_${uuid0}
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

CREATE DATABASE trc_views_${uuid0}.views_${uuid0};
SET CATALOG trc_views_${uuid0};
USE views_${uuid0};

-- query 2
CREATE TABLE trc_views_${uuid0}.views_${uuid0}.header_source (a integer, b integer);

-- query 3
INSERT INTO trc_views_${uuid0}.views_${uuid0}.header_source VALUES (1, 2);

-- query 4
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.header_view AS SELECT * FROM trc_views_${uuid0}.views_${uuid0}.header_source;

-- query 5
SELECT * FROM trc_views_${uuid0}.views_${uuid0}.header_view;

-- query 6
DROP TABLE trc_views_${uuid0}.views_${uuid0}.header_source;

-- query 7
CREATE TABLE trc_views_${uuid0}.views_${uuid0}.header_source (b integer, c integer);

-- query 8
INSERT INTO trc_views_${uuid0}.views_${uuid0}.header_source VALUES (20, 30);

-- query 9
-- @expect_trino_error=VIEW_IS_STALE,USER_ERROR
SELECT * FROM trc_views_${uuid0}.views_${uuid0}.header_view;

-- query 10
-- @expect_trino_error=VIEW_IS_STALE,USER_ERROR
SELECT * FROM trc_views_${uuid0}.views_${uuid0}.header_view AS r (a, b);

-- query 11
DROP TABLE trc_views_${uuid0}.views_${uuid0}.header_source;

-- query 12
-- @expect_trino_error=INVALID_VIEW,USER_ERROR
SELECT * FROM trc_views_${uuid0}.views_${uuid0}.header_view;

-- query 13
CREATE TABLE trc_views_${uuid0}.views_${uuid0}.type_source (a bigint);

-- query 14
INSERT INTO trc_views_${uuid0}.views_${uuid0}.type_source VALUES (CAST(1 AS integer));

-- query 15
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.type_bigint AS SELECT * FROM trc_views_${uuid0}.views_${uuid0}.type_source;

-- query 16
DROP TABLE trc_views_${uuid0}.views_${uuid0}.type_source;

-- query 17
CREATE TABLE trc_views_${uuid0}.views_${uuid0}.type_source (a integer);

-- query 18
INSERT INTO trc_views_${uuid0}.views_${uuid0}.type_source VALUES (2);

-- query 19
SELECT * FROM trc_views_${uuid0}.views_${uuid0}.type_bigint;

-- query 20
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.type_integer AS SELECT * FROM trc_views_${uuid0}.views_${uuid0}.type_source;

-- query 21
DROP TABLE trc_views_${uuid0}.views_${uuid0}.type_source;

-- query 22
CREATE TABLE trc_views_${uuid0}.views_${uuid0}.type_source (a bigint);

-- query 23
INSERT INTO trc_views_${uuid0}.views_${uuid0}.type_source VALUES (3);

-- query 24
DROP TABLE trc_views_${uuid0}.views_${uuid0}.type_source;

-- query 25
CREATE TABLE trc_views_${uuid0}.views_${uuid0}.type_source (a varchar);

-- query 26
INSERT INTO trc_views_${uuid0}.views_${uuid0}.type_source VALUES ('4');

-- query 27
-- @expect_trino_error=VIEW_IS_STALE,USER_ERROR
SELECT * FROM trc_views_${uuid0}.views_${uuid0}.type_integer;

-- query 28
-- @expect_trino_error=TABLE_NOT_FOUND,USER_ERROR
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.self_new AS SELECT * FROM trc_views_${uuid0}.views_${uuid0}.self_new;

-- query 29
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.self_existing AS SELECT CAST(1 AS integer) AS a;

-- query 30
-- @expect_trino_error=VIEW_IS_RECURSIVE,USER_ERROR
CREATE OR REPLACE VIEW trc_views_${uuid0}.views_${uuid0}.self_existing AS SELECT * FROM trc_views_${uuid0}.views_${uuid0}.self_existing;

-- query 31
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.indirect_a AS SELECT CAST(1 AS integer) AS a;

-- query 32
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.indirect_b AS SELECT * FROM trc_views_${uuid0}.views_${uuid0}.indirect_a;

-- query 33
-- @expect_trino_error=INVALID_VIEW,USER_ERROR
CREATE OR REPLACE VIEW trc_views_${uuid0}.views_${uuid0}.indirect_a AS SELECT * FROM trc_views_${uuid0}.views_${uuid0}.indirect_b;

-- query 34
SELECT * FROM trc_views_${uuid0}.views_${uuid0}.self_existing;

-- query 35
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.depth_1 AS SELECT CAST(7 AS integer) AS a;

-- query 36
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.depth_2 AS SELECT * FROM trc_views_${uuid0}.views_${uuid0}.depth_1;

-- query 37
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.depth_3 AS SELECT * FROM trc_views_${uuid0}.views_${uuid0}.depth_2;

-- query 38
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.depth_4 AS SELECT * FROM trc_views_${uuid0}.views_${uuid0}.depth_3;

-- query 39
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.depth_5 AS SELECT * FROM trc_views_${uuid0}.views_${uuid0}.depth_4;

-- query 40
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.depth_6 AS SELECT * FROM trc_views_${uuid0}.views_${uuid0}.depth_5;

-- query 41
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.depth_7 AS SELECT * FROM trc_views_${uuid0}.views_${uuid0}.depth_6;

-- query 42
SELECT * FROM trc_views_${uuid0}.views_${uuid0}.depth_7;

-- query 43
-- @expect_trino_error=MISSING_COLUMN_NAME,USER_ERROR
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.values_body AS VALUES (CAST(1 AS integer));

-- query 44
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.values_body AS SELECT * FROM (VALUES (CAST(1 AS integer))) AS t (a);

-- query 45
SELECT * FROM trc_views_${uuid0}.views_${uuid0}.values_body;

-- query 46
CREATE VIEW trc_views_${uuid0}.views_${uuid0}.set_body AS SELECT CAST(1 AS integer) AS a UNION ALL SELECT CAST(2 AS integer) AS b;

-- query 47
SELECT * FROM trc_views_${uuid0}.views_${uuid0}.set_body ORDER BY a;

-- query 48
CREATE TABLE trc_views_${uuid0}.views_${uuid0}.local_header_source (a integer, b integer);

-- query 49
INSERT INTO trc_views_${uuid0}.views_${uuid0}.local_header_source VALUES (1, 2);

-- query 50
CREATE VIEW default_catalog.default.trc_local_header_${uuid0} AS SELECT * FROM trc_views_${uuid0}.views_${uuid0}.local_header_source;

-- query 51
SELECT * FROM default_catalog.default.trc_local_header_${uuid0};

-- query 52
DROP TABLE trc_views_${uuid0}.views_${uuid0}.local_header_source;

-- query 53
CREATE TABLE trc_views_${uuid0}.views_${uuid0}.local_header_source (b integer, c integer);

-- query 54
INSERT INTO trc_views_${uuid0}.views_${uuid0}.local_header_source VALUES (20, 30);

-- query 55
-- @expect_trino_error=VIEW_IS_STALE,USER_ERROR
SELECT * FROM default_catalog.default.trc_local_header_${uuid0};

-- query 56
-- @expect_trino_error=VIEW_IS_STALE,USER_ERROR
SELECT * FROM default_catalog.default.trc_local_header_${uuid0} AS r (a, b);

-- query 57
DROP TABLE trc_views_${uuid0}.views_${uuid0}.local_header_source;

-- query 58
-- @expect_trino_error=INVALID_VIEW,USER_ERROR
SELECT * FROM default_catalog.default.trc_local_header_${uuid0};

-- query 59
-- @skip_result_check=true
CREATE DATABASE trc_views_${uuid0}.saved_${uuid0};
CREATE DATABASE trc_views_${uuid0}.caller_${uuid0};

-- query 60
CREATE TABLE trc_views_${uuid0}.saved_${uuid0}.context_source AS SELECT CAST(11 AS integer) AS v;

-- query 61
CREATE TABLE trc_views_${uuid0}.caller_${uuid0}.context_source AS SELECT CAST(22 AS integer) AS v;

-- query 62
-- @skip_result_check=true
USE saved_${uuid0};

-- query 63
CREATE VIEW trc_views_${uuid0}.saved_${uuid0}.context_view AS SELECT (SELECT max(v) FROM context_source) AS x;

-- query 64
CREATE VIEW trc_views_${uuid0}.saved_${uuid0}.context_exists AS SELECT v FROM context_source WHERE EXISTS (SELECT 1 FROM context_source s WHERE s.v = 11);

-- query 65
-- @skip_result_check=true
USE caller_${uuid0};

-- query 66
SELECT * FROM trc_views_${uuid0}.saved_${uuid0}.context_view;

-- query 67
WITH context_source(v) AS (VALUES (CAST(99 AS integer))) SELECT * FROM trc_views_${uuid0}.saved_${uuid0}.context_view;

-- query 68
SELECT * FROM trc_views_${uuid0}.saved_${uuid0}.context_exists;

-- query 69
-- @skip_result_check=true
USE saved_${uuid0};

-- query 70
CREATE VIEW default_catalog.default.trc_local_context_${uuid0} AS SELECT (SELECT max(v) FROM context_source) AS x;

-- query 71
CREATE VIEW default_catalog.default.trc_local_exists_${uuid0} AS SELECT v FROM context_source WHERE EXISTS (SELECT 1 FROM context_source s WHERE s.v = 11);

-- query 72
-- @skip_result_check=true
USE caller_${uuid0};

-- query 73
SELECT * FROM default_catalog.default.trc_local_context_${uuid0};

-- query 74
WITH context_source(v) AS (VALUES (CAST(99 AS integer))) SELECT * FROM default_catalog.default.trc_local_context_${uuid0};

-- query 75
SELECT * FROM default_catalog.default.trc_local_exists_${uuid0};

-- query 76
-- @skip_result_check=true
USE views_${uuid0};

-- query 77
-- @cleanup=true
-- @skip_result_check=true
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.header_view;
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.type_bigint;
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.type_integer;
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.self_new;
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.self_existing;
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.indirect_a;
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.indirect_b;
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.values_body;
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.set_body;
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.depth_1;
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.depth_2;
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.depth_3;
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.depth_4;
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.depth_5;
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.depth_6;
DROP VIEW IF EXISTS trc_views_${uuid0}.views_${uuid0}.depth_7;
DROP VIEW IF EXISTS default_catalog.default.trc_local_header_${uuid0};
DROP VIEW IF EXISTS trc_views_${uuid0}.saved_${uuid0}.context_view;
DROP VIEW IF EXISTS trc_views_${uuid0}.saved_${uuid0}.context_exists;
DROP VIEW IF EXISTS default_catalog.default.trc_local_context_${uuid0};
DROP VIEW IF EXISTS default_catalog.default.trc_local_exists_${uuid0};
DROP TABLE IF EXISTS trc_views_${uuid0}.views_${uuid0}.header_source;
DROP TABLE IF EXISTS trc_views_${uuid0}.views_${uuid0}.type_source;
DROP TABLE IF EXISTS trc_views_${uuid0}.views_${uuid0}.local_header_source;
DROP TABLE IF EXISTS trc_views_${uuid0}.saved_${uuid0}.context_source;
DROP TABLE IF EXISTS trc_views_${uuid0}.caller_${uuid0}.context_source;
DROP DATABASE IF EXISTS trc_views_${uuid0}.saved_${uuid0};
DROP DATABASE IF EXISTS trc_views_${uuid0}.caller_${uuid0};

-- query 78
-- @cleanup=true
-- @skip_result_check=true

SET CATALOG iceberg_ddl_cat_${suite_uuid0};
USE ${case_db};
DROP DATABASE IF EXISTS trc_views_${uuid0}.views_${uuid0};
DROP CATALOG trc_views_${uuid0};
