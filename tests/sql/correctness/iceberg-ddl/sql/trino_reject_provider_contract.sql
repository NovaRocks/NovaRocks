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
-- @tags=iceberg_ddl,trc5e


-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_ddl_cat_${suite_uuid0}.p14_reject_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.p14_reject_${uuid0};
CREATE TABLE p14_source (id BIGINT NOT NULL) WITH (format_version = 3);

-- query 2
-- @expect_trino_error=DUPLICATE_COLUMN_NAME,USER_ERROR
CREATE TABLE p14_bad_table (id INT, id BIGINT);

-- query 3
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE TABLE p14_bad_table AS SELECT CAST(1 AS bigint) AS id WITH NO DATA;

-- query 4
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE TABLE p14_bad_table (renamed) AS SELECT CAST(1 AS bigint) AS id;

-- query 5
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE TABLE p14_bad_table (id bigint WITH (p = 1));

-- query 6
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE TABLE p14_bad_table (LIKE p14_source INCLUDING PROPERTIES);

-- query 7
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE TABLE p14_bad_table (LIKE p14_source, id bigint);

-- query 8
-- @expect_trino_error=INVALID_SCHEMA_PROPERTY,USER_ERROR
CREATE SCHEMA iceberg_ddl_cat_${suite_uuid0}.p14_bad_schema_${uuid0} WITH (no_such_property = 1);

-- query 9
-- @expect_trino_error=INVALID_TABLE_PROPERTY,USER_ERROR
CREATE TABLE p14_bad_table (id bigint) WITH (no_such_property = 1);

-- query 10
-- @expect_trino_error=INVALID_TABLE_PROPERTY,USER_ERROR
ALTER TABLE p14_source SET PROPERTIES no_such_property = 1;

-- query 11
-- @expect_trino_error=INVALID_MATERIALIZED_VIEW_PROPERTY,USER_ERROR
CREATE MATERIALIZED VIEW p14_bad_mv WITH (no_such_property = 1) AS SELECT id FROM p14_source;

-- query 12
-- @expect_trino_error=INVALID_ANALYZE_PROPERTY,USER_ERROR
ANALYZE p14_source WITH (no_such_property = 1);

-- query 13
-- @expect_trino_error=INVALID_BRANCH_PROPERTY,USER_ERROR
CREATE BRANCH b WITH (no_such_property = 1) IN TABLE p14_source;

-- query 14
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE TABLE p14_bad_table (id bigint) WITH (sorted_by = ARRAY['id']);

-- query 15
-- @expect_trino_error=PROCEDURE_NOT_FOUND,USER_ERROR
ALTER TABLE p14_source EXECUTE no_such_proc;

-- query 16
-- @expect_trino_error=INVALID_PROCEDURE_ARGUMENT,USER_ERROR
ALTER TABLE p14_source EXECUTE optimize(no_such => 1);

-- query 17
-- @expect_trino_error=INVALID_ARGUMENTS,USER_ERROR
ALTER TABLE p14_source EXECUTE optimize('10MB', no_such => 1);

-- query 18
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE p14_source EXECUTE optimize_manifests WHERE id > 0;

-- query 19
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION iceberg_ddl_cat_${suite_uuid0}.no_such = 1;

-- query 20
-- @skip_result_check=true
-- @cleanup=true
DROP MATERIALIZED VIEW IF EXISTS iceberg_ddl_cat_${suite_uuid0}.p14_reject_${uuid0}.p14_bad_mv;
DROP TABLE IF EXISTS iceberg_ddl_cat_${suite_uuid0}.p14_reject_${uuid0}.p14_bad_table;
DROP TABLE IF EXISTS iceberg_ddl_cat_${suite_uuid0}.p14_reject_${uuid0}.p14_source;
DROP SCHEMA IF EXISTS iceberg_ddl_cat_${suite_uuid0}.p14_reject_${uuid0};
DROP SCHEMA IF EXISTS iceberg_ddl_cat_${suite_uuid0}.p14_bad_schema_${uuid0};
