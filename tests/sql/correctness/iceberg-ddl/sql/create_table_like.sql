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

-- @tags=iceberg_ddl
-- Test Objective:
-- 1. Validate CREATE TABLE ... LIKE copies the source schema on an Iceberg table.
-- 2. Verify INSERT into the LIKE'd table works and the row count is correct.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.src;
DROP TABLE IF EXISTS ${case_db}.dst;
CREATE TABLE ${case_db}.src (
  id INT,
  name VARCHAR,
  v BIGINT
) COMMENT 'source-table';
CREATE TABLE ${case_db}.dst (LIKE ${case_db}.src);

-- query 2
-- @result_contains=id integer
-- @result_contains=name varchar
-- @result_contains=v bigint
-- @result_contains=format = 'PARQUET'
-- @result_contains=format_version = 2
SHOW CREATE TABLE ${case_db}.dst;

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.dst VALUES ROW(1, 'alice', 100), ROW(2, 'bob', 200);

-- query 4
SELECT count(1) AS n FROM ${case_db}.dst;
