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
-- @tags=iceberg_dml,delete,metadata_delete
-- Void, data-only, and volatile predicates stay on the row-level path.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_void (id BIGINT, p INTEGER)
WITH (partitioning = ARRAY['id', 'void(p)'], format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_void VALUES
  ROW(1, 10),
  ROW(2, 20),
  ROW(3, NULL);

-- query 3
-- @skip_result_check=true
-- @result_contains=RowLevelDelete
-- @result_contains=DeletionVector
-- @result_not_contains=TableDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_void WHERE p = 10;

-- query 4
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM ${case_db}.rwm18_void WHERE p = 10;

-- query 5
SELECT CAST(element_at(s.summary, 'added-position-deletes') AS BIGINT) > 0 AS added_positions
FROM ${case_db}."rwm18_void$snapshots" AS s
JOIN ${case_db}."rwm18_void$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 6
SELECT count(*) > 0 AS has_delete_file FROM ${case_db}."rwm18_void$files"
WHERE content = 1 AND file_format = 'PUFFIN';

-- query 7
SELECT id FROM ${case_db}.rwm18_void ORDER BY id;

-- query 8
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_fallback_identity (id BIGINT, p INTEGER, payload INTEGER)
WITH (partitioning = ARRAY['p'], format_version = 3);

-- query 9
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_fallback_identity VALUES
  ROW(1, 10, 100),
  ROW(2, 10, 200),
  ROW(3, 20, 300);

-- query 10
-- @skip_result_check=true
-- @result_contains=RowLevelDelete
-- @result_contains=DeletionVector
-- @result_not_contains=TableDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_fallback_identity WHERE p = 10 AND payload = 100;

-- query 11
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM ${case_db}.rwm18_fallback_identity WHERE p = 10 AND payload = 100;

-- query 12
SELECT CAST(element_at(s.summary, 'added-position-deletes') AS BIGINT) > 0 AS added_positions
FROM ${case_db}."rwm18_fallback_identity$snapshots" AS s
JOIN ${case_db}."rwm18_fallback_identity$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 13
SELECT count(*) > 0 AS has_delete_file FROM ${case_db}."rwm18_fallback_identity$files"
WHERE content = 1 AND file_format = 'PUFFIN';

-- query 14
-- @skip_result_check=true
-- @result_contains=RowLevelDelete
-- @result_contains=DeletionVector
-- @result_not_contains=TableDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_fallback_identity WHERE p = 10 AND random() < 1.0;

-- query 15
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM ${case_db}.rwm18_fallback_identity WHERE p = 10 AND random() < 1.0;

-- query 16
SELECT CAST(element_at(s.summary, 'added-position-deletes') AS BIGINT) > 0 AS added_positions
FROM ${case_db}."rwm18_fallback_identity$snapshots" AS s
JOIN ${case_db}."rwm18_fallback_identity$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 17
SELECT count(*) > 0 AS has_delete_file FROM ${case_db}."rwm18_fallback_identity$files"
WHERE content = 1 AND file_format = 'PUFFIN';

-- query 18
SELECT id FROM ${case_db}.rwm18_fallback_identity ORDER BY id;

-- query 19
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
DELETE FROM ${case_db}.rwm18_fallback_identity WHERE id IN (SELECT id FROM ${case_db}.rwm18_void);

-- query 20
SELECT id FROM ${case_db}.rwm18_fallback_identity ORDER BY id;

-- query 21
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.rwm18_void;
DROP TABLE IF EXISTS ${case_db}.rwm18_fallback_identity;
