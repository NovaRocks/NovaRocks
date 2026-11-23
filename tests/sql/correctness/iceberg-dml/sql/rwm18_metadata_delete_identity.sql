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
-- @sequential=true
-- @tags=iceberg_dml,delete,metadata_delete
-- Identity predicates, empty delete commits, and independently empty filters.

-- query 1
-- @restart_fe_after_step=true
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_identity (id BIGINT, p INTEGER, q INTEGER)
WITH (partitioning = ARRAY['p', 'q'], format_version = 3);

-- query 2
-- @skip_result_check=true
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
INSERT INTO ${case_db}.rwm18_identity VALUES
  ROW(1, 10, 1),
  ROW(2, 10, 2),
  ROW(3, 20, 1),
  ROW(4, 30, 1),
  ROW(5, NULL, 1);

-- query 3
SELECT count(*) AS snapshots FROM ${case_db}."rwm18_identity$snapshots";

-- query 4
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_contains=constraint on [
EXPLAIN DELETE FROM ${case_db}.rwm18_identity WHERE p = 20;

-- query 5
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM ${case_db}.rwm18_identity WHERE p = 20;

-- query 6
SELECT s.operation,
       CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS physical_rows,
       CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT) > 0 AS removed_data_files,
       element_at(s.summary, 'added-delete-files') IS NULL AS no_new_delete_files
FROM ${case_db}."rwm18_identity$snapshots" AS s
JOIN ${case_db}."rwm18_identity$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 7
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_contains=constraint on [
EXPLAIN DELETE FROM ${case_db}.rwm18_identity WHERE p = 10 AND q = 1;

-- query 8
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM ${case_db}.rwm18_identity WHERE p = 10 AND q = 1;

-- query 9
SELECT s.operation,
       CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS physical_rows,
       CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT) > 0 AS removed_data_files,
       element_at(s.summary, 'added-delete-files') IS NULL AS no_new_delete_files
FROM ${case_db}."rwm18_identity$snapshots" AS s
JOIN ${case_db}."rwm18_identity$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 10
SELECT id FROM ${case_db}.rwm18_identity ORDER BY id;

-- query 11
SELECT count(*) AS snapshots FROM ${case_db}."rwm18_identity$snapshots";

-- query 12
-- @skip_result_check=true
-- @expect_update_count=NULL
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM ${case_db}.rwm18_identity WHERE p = 10 AND q = 1;

-- query 13
-- @skip_result_check=true
-- @expect_update_count=NULL
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM ${case_db}.rwm18_identity WHERE p = 99;

-- query 14
SELECT count(*) AS empty_delete_snapshots
FROM ${case_db}."rwm18_identity$snapshots"
WHERE operation = 'delete' AND element_at(summary, 'deleted-records') IS NULL;

-- query 15
SELECT count(*) AS snapshots FROM ${case_db}."rwm18_identity$snapshots";

-- query 16
-- @skip_result_check=true
-- @expect_update_count=0
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM ${case_db}.rwm18_identity WHERE FALSE;

-- query 17
-- @skip_result_check=true
-- @expect_update_count=0
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM ${case_db}.rwm18_identity WHERE p < 10 AND p >= 10;

-- query 18
SELECT count(*) AS snapshots FROM ${case_db}."rwm18_identity$snapshots";

-- query 19
SELECT id FROM ${case_db}.rwm18_identity ORDER BY id;

-- query 20
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_c9_empty (id BIGINT, p INTEGER)
WITH (partitioning = ARRAY['p'], format_version = 3);

-- query 21
SELECT count(*) AS snapshots FROM ${case_db}."rwm18_c9_empty$snapshots";

-- query 22
-- @skip_result_check=true
-- @result_contains=Values (0 rows)
-- @result_not_contains=TableDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_c9_empty WHERE p = 10;

-- query 23
-- @skip_result_check=true
-- @expect_update_count=0
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM ${case_db}.rwm18_c9_empty WHERE p = 10;

-- query 24
SELECT count(*) AS snapshots FROM ${case_db}."rwm18_c9_empty$snapshots";

-- query 25
SELECT id FROM ${case_db}.rwm18_c9_empty ORDER BY id;

-- query 26
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.rwm18_identity;
DROP TABLE IF EXISTS ${case_db}.rwm18_c9_empty;
