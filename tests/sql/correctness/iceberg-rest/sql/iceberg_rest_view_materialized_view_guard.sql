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
CREATE SCHEMA iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0};
CREATE TABLE iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_base (id INTEGER);
CREATE TABLE iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_storage (id INTEGER);
INSERT INTO iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_base VALUES 11, 22;
INSERT INTO iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_storage VALUES 99;

-- query 2
-- @iceberg_view_fixture=rest-mv:iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv:trino-mv
SELECT id FROM iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv ORDER BY id;

-- query 3
-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.table_already_exists
-- @expect_error_at=1:1
-- @expect_error=Materialized view already exists: 'iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv'
-- @iceberg_view_fixture_unchanged=iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv
CREATE OR REPLACE VIEW iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv AS SELECT 0 AS id;

-- query 4
SELECT id FROM iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_storage;

-- query 5
-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.table_already_exists
-- @expect_error_at=1:1
-- @expect_error=Materialized view already exists: 'iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv'
-- @iceberg_view_fixture_unchanged=iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv
CREATE VIEW iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv AS SELECT 0 AS id;

-- query 6
SELECT id FROM iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_storage;

-- query 7
-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.table_already_exists
-- @expect_error_at=1:1
-- @expect_error=Materialized view already exists: 'iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv'
-- @iceberg_view_fixture_unchanged=iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv
CREATE VIEW IF NOT EXISTS iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv AS SELECT 0 AS id;

-- query 8
SELECT id FROM iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_storage;

-- query 9
-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.materialized_view_target
-- @expect_error_at=1:1
-- @expect_error=View 'iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv' does not exist, but a materialized view with that name exists. Did you mean DROP MATERIALIZED VIEW iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv?
-- @iceberg_view_fixture_unchanged=iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv
DROP VIEW iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv;

-- query 10
SELECT id FROM iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_storage;

-- query 11
-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.materialized_view_target
-- @expect_error_at=1:1
-- @expect_error=View 'iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv' does not exist, but a materialized view with that name exists. Did you mean DROP MATERIALIZED VIEW iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv?
-- @iceberg_view_fixture_unchanged=iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv
DROP VIEW IF EXISTS iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv;

-- query 12
SELECT id FROM iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_storage;

-- query 13
-- @expect_error_tier=drift
-- @expect_error=Table 'iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv' does not exist, but a materialized view with that name exists. Did you mean DROP MATERIALIZED VIEW iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv?
-- @iceberg_view_fixture_unchanged=iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv
DROP TABLE iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv;

-- query 14
SELECT id FROM iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_storage;

-- query 15
-- @expect_error_tier=drift
-- @expect_error=materialized views exist: mv
-- @iceberg_view_fixture_unchanged=iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv
DROP SCHEMA iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0} CASCADE;

-- query 16
SELECT id FROM iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv ORDER BY id;

-- query 17
SELECT id FROM iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_storage;

-- query 18
SELECT table_name, table_type FROM iceberg_rest_mv_${suite_uuid0}.information_schema.tables WHERE table_schema = 'mv_guard_${uuid0}' ORDER BY table_name;

-- query 19
-- @iceberg_view_fixture=rest-mv:iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.spark_mv:spark-mv
-- @expect_error_tier=drift
-- @expect_error=[query.execution.unsupported] pure compilation failed in its source domain
SELECT id FROM iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.spark_mv;

-- query 20
-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.materialized_view_target
-- @expect_error_at=1:1
-- @expect_error=View 'iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.spark_mv' does not exist, but a materialized view with that name exists. Did you mean DROP MATERIALIZED VIEW iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.spark_mv?
-- @iceberg_view_fixture_unchanged=iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.spark_mv
DROP VIEW iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.spark_mv;

-- query 21
SELECT id FROM iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_storage;

-- query 22
-- @skip_result_check=true
CREATE VIEW iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.plain AS SELECT 1 AS id;

-- query 23
-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.table_already_exists
-- @expect_error_at=1:1
-- @expect_error=View already exists: 'iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.plain'
CREATE VIEW iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.plain AS SELECT 2 AS id;

-- query 24
-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.table_already_exists
-- @expect_error_at=1:1
-- @expect_error=Table already exists: 'iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_base'
CREATE VIEW iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_base AS SELECT 0 AS id;

-- query 25
-- @skip_result_check=true
CREATE OR REPLACE VIEW iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.plain AS SELECT 2 AS id;

-- query 26
SELECT id FROM iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.plain;

-- query 27
-- @skip_result_check=true
DROP VIEW iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.plain;
CREATE SCHEMA iceberg_rest_${suite_uuid0}.mv_guard_stock_${uuid0};

-- query 28
-- @iceberg_view_fixture=rest:iceberg_rest_${suite_uuid0}.mv_guard_stock_${uuid0}.stock_view:trino-mv
-- @skip_result_check=true
DROP VIEW iceberg_rest_${suite_uuid0}.mv_guard_stock_${uuid0}.stock_view;

-- query 29
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_storage;
DROP TABLE IF EXISTS iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0}.mv_base;
DROP SCHEMA IF EXISTS iceberg_rest_mv_${suite_uuid0}.mv_guard_${uuid0};
DROP SCHEMA IF EXISTS iceberg_rest_${suite_uuid0}.mv_guard_stock_${uuid0};
