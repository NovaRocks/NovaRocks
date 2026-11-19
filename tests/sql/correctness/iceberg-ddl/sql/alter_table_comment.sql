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
-- 1. Validate ALTER TABLE t COMMENT 'x' updates the table-level comment on an Iceberg table.
-- 2. Verify SHOW CREATE TABLE reflects both the initial CREATE TABLE COMMENT and the ALTER-applied comment.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, v INT) COMMENT 'c1';

-- query 2
-- @result_contains=COMMENT 'c1'
SHOW CREATE TABLE ${case_db}.t;

-- query 3
-- @skip_result_check=true
COMMENT ON TABLE ${case_db}.t IS 'c2';

-- query 4
-- @result_contains=COMMENT 'c2'
SHOW CREATE TABLE ${case_db}.t;
