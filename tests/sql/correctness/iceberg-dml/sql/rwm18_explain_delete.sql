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
-- EXPLAIN DELETE exposes each planned route without executing it.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_explain (id BIGINT, p INTEGER) WITH (partitioning = ARRAY['p'], format_version = 3);
INSERT INTO ${case_db}.rwm18_explain VALUES ROW(1, 10), ROW(2, 10), ROW(3, 20);

-- query 2
SELECT count(*) AS snapshots FROM ${case_db}."rwm18_explain$snapshots";

-- query 3
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_contains=constraint on [p]
-- @result_not_contains=RowLevelDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_explain WHERE p = 10;

-- query 4
-- @result_contains=Values (0 rows)
-- @result_not_contains=TableDelete
-- @result_not_contains=RowLevelDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_explain WHERE FALSE;

-- query 5
-- @skip_result_check=true
-- @result_contains=RowLevelDelete
-- @result_contains=DeletionVector
-- @result_not_contains=TableDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_explain WHERE id = 1;

-- query 6
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
EXPLAIN ANALYZE DELETE FROM ${case_db}.rwm18_explain WHERE p = 10;

-- query 7
SELECT count(*) AS snapshots FROM ${case_db}."rwm18_explain$snapshots";

-- query 8
SELECT id, p FROM ${case_db}.rwm18_explain ORDER BY id;

-- query 9
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.rwm18_explain;
