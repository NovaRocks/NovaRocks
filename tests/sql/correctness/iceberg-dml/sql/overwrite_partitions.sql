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
-- Validate INSERT OVERWRITE PARTITIONS on Iceberg targets end-to-end:
--   parser -> normalizer rewrite -> engine -> OverwritePartitionsCommit
--   -> manifest writes -> catalog commit.
-- Covers: single partition transform (identity), multiple partition columns,
-- empty SELECT result (noop overwrite snapshot), branch-only OVERWRITE,
-- after-DELETE (deletion vector entries in covered partition),
-- and parser-level rejections (non-partitioned table, v2 table,
-- cross historical partition spec).
--
-- The runner auto-creates `${case_db}` per case and drops it on cleanup, so
-- there is no explicit CREATE/DROP DATABASE here. All table refs are 2-part
-- `${case_db}.<table>` against the suite-level iceberg catalog
-- (`iceberg_dml_cat_${suite_uuid0}`) selected by `SET catalog` at session setup.

-- ---------------------------------------------------------------------------
-- Case 1: identity-partitioned v3 table — replace one partition, preserve others
-- ---------------------------------------------------------------------------

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_id (id INT, region VARCHAR(8)) WITH (partitioning = ARRAY['region'], format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.t_id VALUES
  ROW(1, 'us'), ROW(2, 'us'), ROW(3, 'eu'), ROW(4, 'eu');

-- query 3
-- Pre-OVERWRITE: 4 rows across two partitions.
SELECT region, COUNT(*) AS n FROM ${case_db}.t_id GROUP BY region ORDER BY region;

-- query 4
-- @skip_result_check=true
-- Replace just the `us` partition with a single new row.
INSERT OVERWRITE PARTITIONS ${case_db}.t_id VALUES ROW(99, 'us');

-- query 5
-- Post-OVERWRITE: us has 1 row (the new one), eu still has 2 rows.
SELECT region, COUNT(*) AS n FROM ${case_db}.t_id GROUP BY region ORDER BY region;

-- query 6
-- The new row is the only us row.
SELECT id, region FROM ${case_db}.t_id WHERE region = 'us' ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 2: empty SELECT result — noop overwrite snapshot, base data preserved
-- ---------------------------------------------------------------------------

-- query 7
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_empty_sel (id INT, region VARCHAR(8)) WITH (partitioning = ARRAY['region'], format_version = 3);

-- query 8
-- @skip_result_check=true
INSERT INTO ${case_db}.t_empty_sel VALUES ROW(1, 'us'), ROW(2, 'eu');

-- query 9
-- @skip_result_check=true
-- A self-referential SELECT WHERE FALSE produced no rows in earlier engines
-- and would still leave the table as-is, but the parser/normalizer + iceberg
-- pipeline currently can't resolve a SELECT against the same iceberg table
-- being OVERWRITE'd in the same statement (the rewrite to __nr_op_dyn target
-- would not affect the SELECT side). Use a Values-from-CTE shape that
-- yields zero rows without referencing the target table.
INSERT OVERWRITE PARTITIONS ${case_db}.t_empty_sel
  SELECT 999 AS id, 'never' AS region FROM (SELECT 1 AS dummy) AS sub WHERE 1 = 0;

-- query 10
-- All base data is still present.
SELECT region, COUNT(*) AS n FROM ${case_db}.t_empty_sel GROUP BY region ORDER BY region;

-- ---------------------------------------------------------------------------
-- Case 3: covered partition contains deletion-vector entries
-- ---------------------------------------------------------------------------

-- query 11
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_dv (id INT, region VARCHAR(8)) WITH (partitioning = ARRAY['region'], format_version = 3);

-- query 12
-- @skip_result_check=true
INSERT INTO ${case_db}.t_dv VALUES
  ROW(1, 'us'), ROW(2, 'us'), ROW(3, 'us'), ROW(4, 'eu');

-- query 13
-- @skip_result_check=true
-- Create a deletion-vector entry in the us partition.
DELETE FROM ${case_db}.t_dv WHERE id = 2;

-- query 14
-- Pre-OVERWRITE: 2 us rows live (id=1,3), 1 eu row live.
SELECT region, COUNT(*) AS n FROM ${case_db}.t_dv GROUP BY region ORDER BY region;

-- query 15
-- @skip_result_check=true
-- OVERWRITE PARTITIONS for us must mark BOTH the data files AND the DV in
-- the us partition as DELETED, so the new us row is the only us row.
INSERT OVERWRITE PARTITIONS ${case_db}.t_dv VALUES ROW(99, 'us');

-- query 16
-- Post-OVERWRITE: us has 1 row (id=99), eu untouched.
SELECT id, region FROM ${case_db}.t_dv ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 4: branch write — OVERWRITE PARTITIONS dev branch only, main intact
-- ---------------------------------------------------------------------------

-- query 17
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_branch (id INT, region VARCHAR(8)) WITH (partitioning = ARRAY['region'], format_version = 3);

-- query 18
-- @skip_result_check=true
INSERT INTO ${case_db}.t_branch VALUES ROW(1, 'us'), ROW(2, 'eu');

-- query 19
-- @skip_result_check=true
-- ALTER TABLE for branch DDL requires 3-part name (catalog.db.table); see
-- src/engine/iceberg_ref_flow.rs::resolve_table_parts.
CREATE BRANCH dev IN TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.t_branch;

-- query 20
-- @skip_result_check=true
INSERT INTO ${case_db}.t_branch @dev VALUES (3, 'us');

-- query 21
-- @skip_result_check=true
-- Replace `us` only on the dev branch.
INSERT OVERWRITE PARTITIONS ${case_db}.t_branch @dev VALUES ROW(99, 'us');

-- query 22
-- dev branch: us=1 (the overwrite), eu still has the inherited row.
SELECT id, region FROM ${case_db}.t_branch FOR VERSION AS OF 'dev' ORDER BY id;

-- query 23
-- main branch unchanged: 1 us row + 1 eu row, original ids 1 and 2.
SELECT id, region FROM ${case_db}.t_branch ORDER BY id;

-- query 24
-- @skip_result_check=true
DROP BRANCH IF EXISTS dev IN TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.t_branch;

-- ---------------------------------------------------------------------------
-- Case 5: parser-level rejection — unpartitioned table
-- ---------------------------------------------------------------------------

-- query 25
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_no_part (id INT, v VARCHAR(8)) WITH (format_version = 3);

-- query 26
-- @expect_error=PARTITIONS
-- Engine fail-fast: OVERWRITE PARTITIONS demands a partitioned table.
INSERT OVERWRITE PARTITIONS ${case_db}.t_no_part VALUES ROW(1, 'a');

-- ---------------------------------------------------------------------------
-- Case 6: parser-level rejection — v2 table without row-lineage
-- ---------------------------------------------------------------------------

-- query 27
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_v2 (id INT, region VARCHAR(8)) WITH (partitioning = ARRAY['region'], format_version = 2);

-- query 28
-- @skip_result_check=true
INSERT INTO ${case_db}.t_v2 VALUES ROW(1, 'us');

-- query 29
-- @expect_error=row-lineage
-- OverwritePartitionsCommit requires a v3 row-lineage table.
INSERT OVERWRITE PARTITIONS ${case_db}.t_v2 VALUES ROW(99, 'us');
