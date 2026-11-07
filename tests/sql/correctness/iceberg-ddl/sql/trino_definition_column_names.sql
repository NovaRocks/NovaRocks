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
-- Test Objective: Reject missing and duplicate definition names in output order; reject view definition column lists and preserve relation column lists.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG trc_names_${uuid0}
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

CREATE DATABASE trc_names_${uuid0}.names_${uuid0};
USE trc_names_${uuid0}.names_${uuid0};

-- query 2
-- @expect_trino_error=MISSING_COLUMN_NAME,USER_ERROR
CREATE TABLE trc_names_${uuid0}.names_${uuid0}.c01 AS SELECT CAST(1 AS integer);

-- query 3
-- @expect_trino_error=DUPLICATE_COLUMN_NAME,USER_ERROR
CREATE TABLE trc_names_${uuid0}.names_${uuid0}.c02 AS SELECT CAST(1 AS integer) AS a, CAST(2 AS integer) AS a;

-- query 4
-- @expect_trino_error=DUPLICATE_COLUMN_NAME,USER_ERROR
CREATE TABLE trc_names_${uuid0}.names_${uuid0}.c04 AS SELECT x, x FROM (VALUES (CAST(1 AS integer))) AS t (x);

-- query 5
-- @expect_trino_error=MISSING_COLUMN_NAME,USER_ERROR
CREATE TABLE trc_names_${uuid0}.names_${uuid0}.c05 AS SELECT * FROM (SELECT CAST(1 AS integer)) AS s;

-- query 6
-- @expect_trino_error=MISSING_COLUMN_NAME,USER_ERROR
CREATE VIEW trc_names_${uuid0}.names_${uuid0}.v01 AS SELECT CAST(1 AS integer);

-- query 7
-- @expect_trino_error=DUPLICATE_COLUMN_NAME,USER_ERROR
CREATE VIEW trc_names_${uuid0}.names_${uuid0}.v02 AS SELECT CAST(1 AS integer) AS a, CAST(2 AS integer) AS a;

-- query 8
-- @expect_trino_error=DUPLICATE_COLUMN_NAME,USER_ERROR
CREATE TABLE trc_names_${uuid0}.names_${uuid0}.duplicate_first AS SELECT 1 AS a, 2 AS a, 3;

-- query 9
-- @expect_trino_error=MISSING_COLUMN_NAME,USER_ERROR
CREATE TABLE trc_names_${uuid0}.names_${uuid0}.missing_first AS SELECT 1, 2 AS a, 3 AS a;

-- query 10
-- @expect_trino_error=DUPLICATE_COLUMN_NAME,USER_ERROR
CREATE VIEW trc_names_${uuid0}.names_${uuid0}.duplicate_view AS SELECT 1 AS a, 2 AS a, 3;

-- query 11
-- @expect_trino_error=MISSING_COLUMN_NAME,USER_ERROR
CREATE VIEW trc_names_${uuid0}.names_${uuid0}.missing_view AS SELECT 1, 2 AS a, 3 AS a;

-- query 12
-- @expect_trino_error=DUPLICATE_COLUMN_NAME,USER_ERROR
CREATE VIEW trc_names_${uuid0}.names_${uuid0}.case_duplicate AS SELECT CAST(1 AS integer) AS a, CAST(2 AS integer) AS A;

-- query 13
-- @expect_trino_error=DUPLICATE_COLUMN_NAME,USER_ERROR
CREATE VIEW default_catalog.default.trc_names_case_${uuid0} AS SELECT CAST(1 AS integer) AS a, CAST(2 AS integer) AS A;

-- query 14
CREATE VIEW trc_names_${uuid0}.names_${uuid0}.public_view AS SELECT CAST(1 AS integer) AS public_x;

-- query 15
SELECT public_x FROM trc_names_${uuid0}.names_${uuid0}.public_view;

-- query 16
SELECT outer_x FROM trc_names_${uuid0}.names_${uuid0}.public_view AS r(outer_x);

-- query 17
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE OR REPLACE VIEW public_view(a,b) AS SELECT CAST(1 AS integer);

-- query 18
SELECT public_x FROM trc_names_${uuid0}.names_${uuid0}.public_view;

-- query 19
CREATE VIEW default_catalog.default.trc_names_public_${uuid0} AS SELECT CAST(1 AS integer) AS public_x;

-- query 20
SELECT public_x FROM default_catalog.default.trc_names_public_${uuid0};

-- query 21
SELECT outer_x FROM default_catalog.default.trc_names_public_${uuid0} AS r(outer_x);

-- query 22
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CREATE OR REPLACE VIEW default_catalog.default.trc_names_public_${uuid0}(a,b) AS SELECT CAST(1 AS integer);

-- query 23
SELECT public_x FROM default_catalog.default.trc_names_public_${uuid0};

-- query 24
CREATE TABLE trc_names_${uuid0}.names_${uuid0}.relation_names AS SELECT CAST(1 AS integer) AS k;

-- query 25
SELECT kk FROM trc_names_${uuid0}.names_${uuid0}.relation_names AS r(kk);

-- query 26
-- @expect_trino_error=COLUMN_NOT_FOUND,USER_ERROR
SELECT k FROM trc_names_${uuid0}.names_${uuid0}.relation_names AS r(kk);

-- query 27
-- @expect_trino_error=MISMATCHED_COLUMN_ALIASES,USER_ERROR
SELECT * FROM trc_names_${uuid0}.names_${uuid0}.relation_names AS r(kk,extra);

-- query 28
-- @cleanup=true
-- @skip_result_check=true
DROP VIEW IF EXISTS trc_names_${uuid0}.names_${uuid0}.v01;
DROP VIEW IF EXISTS trc_names_${uuid0}.names_${uuid0}.v02;
DROP VIEW IF EXISTS trc_names_${uuid0}.names_${uuid0}.duplicate_view;
DROP VIEW IF EXISTS trc_names_${uuid0}.names_${uuid0}.missing_view;
DROP VIEW IF EXISTS trc_names_${uuid0}.names_${uuid0}.case_duplicate;
DROP VIEW IF EXISTS default_catalog.default.trc_names_case_${uuid0};
DROP VIEW IF EXISTS trc_names_${uuid0}.names_${uuid0}.public_view;
DROP VIEW IF EXISTS default_catalog.default.trc_names_public_${uuid0};
DROP TABLE IF EXISTS trc_names_${uuid0}.names_${uuid0}.c01;
DROP TABLE IF EXISTS trc_names_${uuid0}.names_${uuid0}.c02;
DROP TABLE IF EXISTS trc_names_${uuid0}.names_${uuid0}.c04;
DROP TABLE IF EXISTS trc_names_${uuid0}.names_${uuid0}.c05;
DROP TABLE IF EXISTS trc_names_${uuid0}.names_${uuid0}.duplicate_first;
DROP TABLE IF EXISTS trc_names_${uuid0}.names_${uuid0}.missing_first;
DROP TABLE IF EXISTS trc_names_${uuid0}.names_${uuid0}.relation_names;
USE iceberg_ddl_cat_${suite_uuid0}.${case_db};
DROP DATABASE IF EXISTS trc_names_${uuid0}.names_${uuid0};
DROP CATALOG trc_names_${uuid0};
