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

-- @tags=iceberg_ddl,struct
-- Test Objective:
-- 1. Validate nested STRUCT field add/drop via dotted-path syntax on Iceberg.
-- 2. Verify add-field on a non-struct path is rejected.
-- 3. Verify add-field for an already-existing field is rejected.
-- 4. Verify drop-field for a non-existent field is rejected.
-- Note: NovaRocks's standalone INSERT path does not currently support STRUCT
-- column writes (see tests/sql/correctness/iceberg/sql/iceberg_schema_evolution_nested.sql),
-- so this test is DDL-only.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.tab1;
CREATE TABLE ${case_db}.tab1 (
  c0 INT,
  c1 ROW(v1 INT, v2 ROW(v3 INT, v4 INT))
);

-- query 2
-- Negative: cannot ADD COLUMN under a non-struct path (v1 is an INT, not a STRUCT).
-- @expect_error=nested Iceberg schema changes currently require a struct parent
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.v1.v5 INT;

-- query 3
-- Negative: cannot ADD COLUMN with an existing top-level field name (v2).
-- @expect_error=column `v2` already exists
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.v2 INT;

-- query 4
-- Negative: cannot ADD COLUMN with an existing nested field name (v2.v3).
-- @expect_error=column `v3` already exists
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.v2.v3 INT;

-- query 5
-- Positive: add a new top-level field.
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.val1 INT;

-- query 6
-- Negative: cannot DROP COLUMN a non-existent nested field.
-- @expect_error=Iceberg column `v5` does not exist
ALTER TABLE ${case_db}.tab1 DROP COLUMN c1.v2.v5;

-- query 7
-- Positive: drop a top-level field.
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 DROP COLUMN c1.v1;

-- query 8
-- Positive: re-add a previously-dropped field name with a new type.
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.v1 INT;
