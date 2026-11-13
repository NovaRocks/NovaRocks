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

-- @sequential=true
-- @order_sensitive=true
-- Correctness owner: Iceberg optimize. Trino 484 O09/O13/P06/D06/D14/E02
-- independently define file selection, metrics and no-op snapshots (plan1.4).
-- Each seed INSERT creates one tiny file; the case owns its namespace and
-- drops all tables on cleanup. The private suite fixture owns actual shutdown.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.trc_opt_${uuid0};
CREATE TABLE iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.empty_rows (k BIGINT);
CREATE TABLE iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.tiny_rows (k BIGINT);
CREATE TABLE iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.partition_rows (k BIGINT, p STRING)
PARTITION BY (p);
CREATE TABLE iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.position_rows (k BIGINT)
TBLPROPERTIES ('format-version' = '2');
CREATE TABLE iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.vector_rows (k BIGINT)
TBLPROPERTIES ('format-version' = '3', 'write.row-lineage' = 'true');

-- query 2
ALTER TABLE iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.empty_rows OPTIMIZE;

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.tiny_rows VALUES (1);
INSERT INTO iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.tiny_rows VALUES (2);
INSERT INTO iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.tiny_rows VALUES (3);

-- query 4
ALTER TABLE iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.tiny_rows OPTIMIZE;

-- query 5
-- Freeze the exact snapshot before the singleton no-op. Tag creation changes
-- reference metadata, not the data snapshot, and provides an independent oracle.
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.tiny_rows CREATE TAG before_noop;

-- query 6
-- A single current-spec file without deletes is skipped.
ALTER TABLE iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.tiny_rows OPTIMIZE;

-- query 7
-- A no-op retains the exact saved snapshot; this does not assume a history count.
SELECT count(*) AS main_matches_before_noop
FROM iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.tiny_rows$refs AS current_ref
JOIN iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.tiny_rows$refs AS before_ref
  ON current_ref.snapshot_id = before_ref.snapshot_id
WHERE current_ref.name = 'main' AND before_ref.name = 'before_noop';

-- query 8
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.partition_rows VALUES (1, 'a');
INSERT INTO iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.partition_rows VALUES (2, 'a');
INSERT INTO iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.partition_rows VALUES (3, 'b');

-- query 9
-- Only the two-file partition is selected; partition b must survive untouched.
ALTER TABLE iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.partition_rows OPTIMIZE;

-- query 10
-- Return the original partition ROW without requiring struct-field expressions.
SELECT `partition`, record_count AS physical_rows
FROM iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.partition_rows$files
WHERE content = 0 ORDER BY record_count DESC;

-- query 11
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.position_rows VALUES (1), (2);
DELETE FROM iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.position_rows WHERE k = 1;

-- query 12
-- A singleton carrying a position delete is selected.
ALTER TABLE iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.position_rows OPTIMIZE;

-- query 13
SELECT k FROM iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.position_rows ORDER BY k;

-- query 14
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.vector_rows VALUES (1), (2);
INSERT INTO iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.vector_rows VALUES (3), (4);
DELETE FROM iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.vector_rows WHERE k IN (1, 3);

-- query 15
ALTER TABLE iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.vector_rows OPTIMIZE;

-- query 16
SELECT k FROM iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.vector_rows ORDER BY k;

-- query 17
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.empty_rows FORCE;
DROP TABLE IF EXISTS iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.tiny_rows FORCE;
DROP TABLE IF EXISTS iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.partition_rows FORCE;
DROP TABLE IF EXISTS iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.position_rows FORCE;
DROP TABLE IF EXISTS iceberg_cat_${suite_uuid0}.trc_opt_${uuid0}.vector_rows FORCE;
DROP DATABASE IF EXISTS iceberg_cat_${suite_uuid0}.trc_opt_${uuid0};
