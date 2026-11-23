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
-- The metadata count subtracts position deletes, including a v3 deletion vector.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_existing_v2 (id BIGINT, part INTEGER)
WITH (partitioning = ARRAY['part'], format_version = 2);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_existing_v2 VALUES
  ROW(1, 10),
  ROW(2, 10),
  ROW(3, 20);

-- query 3
-- @skip_result_check=true
-- @result_contains=RowLevelDelete
-- @result_contains=PositionDelete
-- @result_not_contains=TableDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_existing_v2 WHERE id = 1;

-- query 4
-- @skip_result_check=true
-- @expect_update_count=1
DELETE FROM ${case_db}.rwm18_existing_v2 WHERE id = 1;

-- query 5
SELECT CAST(element_at(s.summary, 'added-position-deletes') AS BIGINT) > 0 AS added_positions
FROM ${case_db}."rwm18_existing_v2$snapshots" AS s
JOIN ${case_db}."rwm18_existing_v2$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 6
SELECT COALESCE(SUM(record_count), CAST(0 AS BIGINT)) AS covered_positions FROM ${case_db}."rwm18_existing_v2$files"
WHERE content = 1 AND file_format = 'PARQUET';

-- query 7
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_contains=constraint on [
EXPLAIN DELETE FROM ${case_db}.rwm18_existing_v2 WHERE part = 10;

-- query 8
-- @skip_result_check=true
-- @expect_update_count=1
DELETE FROM ${case_db}.rwm18_existing_v2 WHERE part = 10;

-- query 9
SELECT s.operation,
       CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS physical_rows,
       CAST(element_at(s.summary, 'removed-position-deletes') AS BIGINT) AS removed_positions,
       COALESCE(CAST(element_at(s.summary, 'removed-dvs') AS BIGINT), CAST(0 AS BIGINT)) AS removed_dvs
FROM ${case_db}."rwm18_existing_v2$snapshots" AS s
JOIN ${case_db}."rwm18_existing_v2$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 10
SELECT count(*) AS delete_files FROM ${case_db}."rwm18_existing_v2$files" WHERE content = 1;

-- query 11
SELECT id FROM ${case_db}.rwm18_existing_v2 ORDER BY id;

-- query 12
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_existing_v3 (id BIGINT, part INTEGER)
WITH (partitioning = ARRAY['part'], format_version = 3);

-- query 13
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_existing_v3 VALUES
  ROW(1, 10),
  ROW(2, 10),
  ROW(3, 20);

-- query 14
-- @skip_result_check=true
-- @result_contains=RowLevelDelete
-- @result_contains=DeletionVector
-- @result_not_contains=TableDelete
EXPLAIN DELETE FROM ${case_db}.rwm18_existing_v3 WHERE id = 1;

-- query 15
-- @skip_result_check=true
-- @expect_update_count=1
DELETE FROM ${case_db}.rwm18_existing_v3 WHERE id = 1;

-- query 16
SELECT CAST(element_at(s.summary, 'added-position-deletes') AS BIGINT) > 0 AS added_positions
FROM ${case_db}."rwm18_existing_v3$snapshots" AS s
JOIN ${case_db}."rwm18_existing_v3$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 17
SELECT COALESCE(SUM(record_count), CAST(0 AS BIGINT)) AS covered_positions FROM ${case_db}."rwm18_existing_v3$files"
WHERE content = 1 AND file_format = 'PUFFIN';

-- query 18
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_contains=constraint on [
EXPLAIN DELETE FROM ${case_db}.rwm18_existing_v3 WHERE part = 10;

-- query 19
-- @skip_result_check=true
-- @expect_update_count=1
DELETE FROM ${case_db}.rwm18_existing_v3 WHERE part = 10;

-- query 20
SELECT s.operation,
       CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS physical_rows,
       CAST(element_at(s.summary, 'removed-position-deletes') AS BIGINT) AS removed_positions,
       COALESCE(CAST(element_at(s.summary, 'removed-dvs') AS BIGINT), CAST(0 AS BIGINT)) AS removed_dvs
FROM ${case_db}."rwm18_existing_v3$snapshots" AS s
JOIN ${case_db}."rwm18_existing_v3$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 21
SELECT count(*) AS delete_files FROM ${case_db}."rwm18_existing_v3$files" WHERE content = 1;

-- query 22
SELECT id FROM ${case_db}.rwm18_existing_v3 ORDER BY id;

-- query 23
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_existing_v1 (id BIGINT, part INTEGER)
WITH (partitioning = ARRAY['part'], format_version = 1);

-- query 24
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_existing_v1 VALUES
  ROW(1, 10),
  ROW(2, 30);

-- query 25
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_contains=constraint on [
EXPLAIN DELETE FROM ${case_db}.rwm18_existing_v1 WHERE part = 10;

-- query 26
-- @skip_result_check=true
-- @expect_update_count=1
DELETE FROM ${case_db}.rwm18_existing_v1 WHERE part = 10;

-- query 27
SELECT s.operation,
       CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS physical_rows,
       CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT) > 0 AS removed_data_files,
       element_at(s.summary, 'added-delete-files') IS NULL AS no_new_delete_files
FROM ${case_db}."rwm18_existing_v1$snapshots" AS s
JOIN ${case_db}."rwm18_existing_v1$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 28
SELECT id FROM ${case_db}.rwm18_existing_v1 ORDER BY id;

-- query 29
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.rwm18_existing_v1;
DROP TABLE IF EXISTS ${case_db}.rwm18_existing_v2;
DROP TABLE IF EXISTS ${case_db}.rwm18_existing_v3;
