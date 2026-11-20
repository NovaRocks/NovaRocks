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
-- @tags=iceberg_dml,lifecycle
-- Test Objective:
-- 1. Iceberg TRUNCATE TABLE clears rows but keeps the table available for new writes.
-- 2. Iceberg DROP TABLE removes the table from the catalog.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_table_lifecycle;
CREATE TABLE ${case_db}.t_table_lifecycle (
  k1 INT,
  v1 STRING
);
INSERT INTO ${case_db}.t_table_lifecycle VALUES
  ROW(1, 'a'),
  ROW(2, 'b');

-- query 2
SELECT k1, v1
FROM ${case_db}.t_table_lifecycle
ORDER BY k1;

-- query 3
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_table_lifecycle;

-- query 4
SELECT count(*)
FROM ${case_db}.t_table_lifecycle;

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.t_table_lifecycle VALUES ROW(10, 'z');

-- query 6
SELECT k1, v1
FROM ${case_db}.t_table_lifecycle
ORDER BY k1;

-- query 7
-- @skip_result_check=true
DROP TABLE ${case_db}.t_table_lifecycle;

-- query 8
-- @expect_error=unknown table
SELECT * FROM ${case_db}.t_table_lifecycle;
