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
-- A no-WHERE DELETE commits on main, including the first snapshot of an empty table.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_delete_all (id BIGINT, p INTEGER) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
INSERT INTO ${case_db}.rwm18_delete_all VALUES
  ROW(1, 10),
  ROW(2, 20);

-- query 3
-- @skip_result_check=true
CREATE BRANCH dev IN TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.rwm18_delete_all;

-- query 4
-- @skip_result_check=true
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
INSERT INTO ${case_db}.rwm18_delete_all @dev VALUES ROW(3, 30);

-- query 5
SELECT id FROM ${case_db}.rwm18_delete_all FOR VERSION AS OF 'dev' ORDER BY id;

-- query 6
-- @skip_result_check=true
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM ${case_db}.rwm18_delete_all @dev;

-- query 7
SELECT id FROM ${case_db}.rwm18_delete_all FOR VERSION AS OF 'dev' ORDER BY id;

-- query 8
SELECT id FROM ${case_db}.rwm18_delete_all FOR VERSION AS OF 'main' ORDER BY id;

-- query 9
-- @skip_result_check=true
DROP BRANCH IF EXISTS dev IN TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.rwm18_delete_all;

-- query 10
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_not_contains=constraint on [
EXPLAIN DELETE FROM ${case_db}.rwm18_delete_all;

-- query 11
-- @skip_result_check=true
-- @expect_update_count=2
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM ${case_db}.rwm18_delete_all;

-- query 12
SELECT s.operation,
       CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS physical_rows,
       CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT) > 0 AS removed_data_files,
       element_at(s.summary, 'added-delete-files') IS NULL AS no_new_delete_files
FROM ${case_db}."rwm18_delete_all$snapshots" AS s
JOIN ${case_db}."rwm18_delete_all$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 13
SELECT id FROM ${case_db}.rwm18_delete_all ORDER BY id;

-- query 14
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_delete_all_empty (id BIGINT) WITH (format_version = 3);

-- query 15
SELECT count(*) AS snapshots FROM ${case_db}."rwm18_delete_all_empty$snapshots";

-- query 16
-- @skip_result_check=true
-- @expect_update_count=NULL
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
DELETE FROM ${case_db}.rwm18_delete_all_empty;

-- query 17
SELECT count(*) AS snapshots FROM ${case_db}."rwm18_delete_all_empty$snapshots";

-- query 18
SELECT s.operation, element_at(s.summary, 'deleted-records') IS NULL AS no_deleted_rows
FROM ${case_db}."rwm18_delete_all_empty$snapshots" AS s
JOIN ${case_db}."rwm18_delete_all_empty$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 19
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.rwm18_delete_all;
DROP TABLE IF EXISTS ${case_db}.rwm18_delete_all_empty;
