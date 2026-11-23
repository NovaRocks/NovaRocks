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
-- A SQL comparison never matches a NULL partition, with either manifest layout.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_null_shared (id BIGINT, part INTEGER)
WITH (partitioning = ARRAY['part'], format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_null_shared VALUES
  ROW(1, NULL),
  ROW(2, 10),
  ROW(3, 30);

-- query 3
SELECT count(*) AS data_manifests FROM ${case_db}."rwm18_null_shared$manifests" WHERE content = 0;

-- query 4
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_contains=constraint on [
EXPLAIN DELETE FROM ${case_db}.rwm18_null_shared WHERE part < 20;

-- query 5
-- @skip_result_check=true
-- @expect_update_count=1
DELETE FROM ${case_db}.rwm18_null_shared WHERE part < 20;

-- query 6
SELECT s.operation,
       CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS physical_rows,
       CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT) > 0 AS removed_data_files,
       element_at(s.summary, 'added-delete-files') IS NULL AS no_new_delete_files
FROM ${case_db}."rwm18_null_shared$snapshots" AS s
JOIN ${case_db}."rwm18_null_shared$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 7
SELECT id, part FROM ${case_db}.rwm18_null_shared ORDER BY id;

-- query 8
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_null_separate (id BIGINT, part INTEGER)
WITH (partitioning = ARRAY['part'], format_version = 3);

-- query 9
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_null_separate VALUES
  ROW(1, NULL);

-- query 10
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm18_null_separate VALUES
  ROW(2, 10),
  ROW(3, 30);

-- query 11
SELECT count(*) AS data_manifests FROM ${case_db}."rwm18_null_separate$manifests" WHERE content = 0;

-- query 12
-- @skip_result_check=true
-- @result_contains=TableDelete
-- @result_contains=rows:bigint
-- @result_contains=constraint on [
EXPLAIN DELETE FROM ${case_db}.rwm18_null_separate WHERE part < 20;

-- query 13
-- @skip_result_check=true
-- @expect_update_count=1
DELETE FROM ${case_db}.rwm18_null_separate WHERE part < 20;

-- query 14
SELECT s.operation,
       CAST(element_at(s.summary, 'deleted-records') AS BIGINT) AS physical_rows,
       CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT) > 0 AS removed_data_files,
       element_at(s.summary, 'added-delete-files') IS NULL AS no_new_delete_files
FROM ${case_db}."rwm18_null_separate$snapshots" AS s
JOIN ${case_db}."rwm18_null_separate$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 15
SELECT id, part FROM ${case_db}.rwm18_null_separate ORDER BY id;

-- query 16
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.rwm18_null_shared;
DROP TABLE IF EXISTS ${case_db}.rwm18_null_separate;
