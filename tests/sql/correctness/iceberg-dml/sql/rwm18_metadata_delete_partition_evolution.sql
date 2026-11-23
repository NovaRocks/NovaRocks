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
-- A live old bucket spec prevents full absorption until OPTIMIZE rewrites its files.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_evolution (id BIGINT, p INTEGER)
WITH (partitioning = ARRAY['bucket(p, 16)'], format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_evolution VALUES
  ROW(1, 10),
  ROW(2, 20);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_evolution VALUES
  ROW(3, 20),
  ROW(4, 30);

-- query 4
-- @skip_result_check=true
ALTER TABLE ${case_db}.rwm18_evolution SET PROPERTIES partitioning = ARRAY[];
ALTER TABLE ${case_db}.rwm18_evolution SET PROPERTIES partitioning = ARRAY['p'];

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_evolution VALUES
  ROW(5, 20),
  ROW(6, 40),
  ROW(7, 30);

-- query 6
-- @result_rows_where=live_specs=2
-- @result_rows_count=1
SELECT count(DISTINCT spec_id) AS live_specs FROM ${case_db}."rwm18_evolution$files" WHERE content = 0;

-- query 7
-- @skip_result_check=true
-- @result_contains=RowLevelDelete
-- @result_contains=DeletionVector
-- @result_not_contains=TableDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_evolution WHERE p = 10;

-- query 8
-- @skip_result_check=true
-- @expect_update_count=1
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM ${case_db}.rwm18_evolution WHERE p = 10;

-- query 9
SELECT CAST(element_at(s.summary, 'added-position-deletes') AS BIGINT) > 0 AS added_positions
FROM ${case_db}."rwm18_evolution$snapshots" AS s
JOIN ${case_db}."rwm18_evolution$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 10
SELECT count(*) > 0 AS has_delete_file FROM ${case_db}."rwm18_evolution$files"
WHERE content = 1 AND file_format = 'PUFFIN';

-- query 11
SELECT id FROM ${case_db}.rwm18_evolution ORDER BY id;

-- query 12
-- @skip_result_check=true
ALTER TABLE ${case_db}.rwm18_evolution EXECUTE optimize(file_size_threshold => '1MB');

-- query 13
-- @result_rows_where=live_specs=1
-- @result_rows_count=1
-- @result_rows_expect=old_spec_inactive=true
SELECT count(DISTINCT spec_id) AS live_specs, min(spec_id) > 0 AS old_spec_inactive
FROM ${case_db}."rwm18_evolution$files" WHERE content = 0;

-- query 14
-- @result_rows_where=delete_files=0
-- @result_rows_count=1
SELECT count(*) AS delete_files FROM ${case_db}."rwm18_evolution$files" WHERE content = 1;

-- query 15
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_contains=constraint on [
EXPLAIN DELETE FROM ${case_db}.rwm18_evolution WHERE p = 20;

-- query 16
-- @skip_result_check=true
-- @expect_update_count=3
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
DELETE FROM ${case_db}.rwm18_evolution WHERE p = 20;

-- query 17
SELECT s.operation,
       CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS physical_rows,
       CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT) > 0 AS removed_data_files,
       element_at(s.summary, 'added-delete-files') IS NULL AS no_new_delete_files
FROM ${case_db}."rwm18_evolution$snapshots" AS s
JOIN ${case_db}."rwm18_evolution$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 18
SELECT id FROM ${case_db}.rwm18_evolution ORDER BY id;

-- query 19
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.rwm18_evolution;
