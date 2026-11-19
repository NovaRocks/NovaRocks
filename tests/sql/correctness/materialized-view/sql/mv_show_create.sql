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
-- @tags=mv,iceberg,lake_native

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.show_create_base (
  id integer NOT NULL,
  value bigint
) WITH (format_version = 3);
CREATE MATERIALIZED VIEW ${case_db}.show_create_mv
WITH (partitioning = ARRAY['id'], refresh_policy = 'MANUAL', gc_enabled = false)
AS SELECT /* saved query body */ id,  value
FROM ${case_db}.show_create_base;
CREATE VIEW ${case_db}.show_create_view AS SELECT /* saved view body */ id
FROM ${case_db}.show_create_base;

-- query 2
-- @result_contains=CREATE MATERIALIZED VIEW
-- @result_contains=format = 'PARQUET'
-- @result_contains=format_version = 3
-- @result_contains=gc_enabled = false
-- @result_contains=partitioning = ARRAY['id']
-- @result_contains=refresh_policy = 'MANUAL'
-- @result_contains=refresh_paused = false
-- @result_contains=SELECT /* saved query body */ id,  value
-- @result_not_contains=extra_properties
SHOW CREATE MATERIALIZED VIEW ${case_db}.show_create_mv;

-- query 3
-- @result_contains=CREATE VIEW
-- @result_contains=SELECT /* saved view body */ id
SHOW CREATE VIEW ${case_db}.show_create_view;

-- query 4
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SHOW CREATE MATERIALIZED VIEW ${case_db}.show_create_base;

-- query 5
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SHOW CREATE MATERIALIZED VIEW ${case_db}.show_create_view;

-- query 6
-- @expect_trino_error=TABLE_NOT_FOUND,USER_ERROR
SHOW CREATE MATERIALIZED VIEW ${case_db}.missing_mv;

-- query 7
-- @cleanup=true
-- @skip_result_check=true
DROP VIEW IF EXISTS ${case_db}.show_create_view;
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.show_create_mv;
DROP TABLE IF EXISTS ${case_db}.show_create_base;
