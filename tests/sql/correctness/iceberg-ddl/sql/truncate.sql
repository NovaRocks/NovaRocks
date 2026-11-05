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
-- Validate TRUNCATE TABLE on Iceberg targets end-to-end:
--   parser -> engine -> TruncateCommit -> manifest writes -> catalog commit.
-- Covers: v3 row-lineage table, v2 table, empty v3 table, branch-only TRUNCATE,
-- TRUNCATE after DELETE (deletion vector entries), and parser-level rejection of
-- TRUNCATE ... PARTITION (...) / TRUNCATE ... WHERE clauses.
--
-- The runner auto-creates `${case_db}` per case and drops it on cleanup, so
-- there is no explicit CREATE/DROP DATABASE here. All table refs are 2-part
-- `${case_db}.<table>` against the suite-level iceberg catalog
-- (`iceberg_ddl_cat_${suite_uuid0}`) selected by `SET catalog` at session setup.

-- ---------------------------------------------------------------------------
-- Case 1: TRUNCATE on a v3 row-lineage table
-- ---------------------------------------------------------------------------

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_v3 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 2
-- @skip_result_check=true
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
INSERT INTO ${case_db}.t_v3 VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- query 3
-- Sanity check: 3 rows present before TRUNCATE.
SELECT COUNT(*) AS n FROM ${case_db}.t_v3;

-- query 4
-- @skip_result_check=true
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
-- TRUNCATE writes an operation=delete snapshot.
TRUNCATE TABLE ${case_db}.t_v3;

-- query 5
-- After TRUNCATE the table reads as empty.
SELECT COUNT(*) AS n FROM ${case_db}.t_v3;

-- query 6
-- @skip_result_check=true
-- INSERT after TRUNCATE — proves the snapshot chain is healthy and schema preserved.
INSERT INTO ${case_db}.t_v3 VALUES (10, 'after');

-- query 7
-- Only the post-TRUNCATE row should be visible.
SELECT id, v FROM ${case_db}.t_v3 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 2: TRUNCATE on a v2 (default) table
-- ---------------------------------------------------------------------------

-- query 8
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_v2 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "2");

-- query 9
-- @skip_result_check=true
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
INSERT INTO ${case_db}.t_v2 VALUES (1, 'x'), (2, 'y'), (3, 'z');

-- query 10
SELECT COUNT(*) AS n FROM ${case_db}.t_v2;

-- query 11
-- @skip_result_check=true
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
TRUNCATE TABLE ${case_db}.t_v2;

-- query 12
SELECT COUNT(*) AS n FROM ${case_db}.t_v2;

-- query 13
-- @skip_result_check=true
INSERT INTO ${case_db}.t_v2 VALUES (42, 'reborn');

-- query 14
SELECT id, v FROM ${case_db}.t_v2 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 3: TRUNCATE on an empty v3 table
-- ---------------------------------------------------------------------------

-- query 15
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_empty (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 16
-- Empty before TRUNCATE.
SELECT COUNT(*) AS n FROM ${case_db}.t_empty;

-- query 17
-- @skip_result_check=true
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
-- TRUNCATE on empty still writes a delete snapshot (audit-trail entry).
TRUNCATE TABLE ${case_db}.t_empty;

-- query 18
SELECT COUNT(*) AS n FROM ${case_db}.t_empty;

-- query 19
-- @skip_result_check=true
-- INSERT after TRUNCATE-on-empty must still succeed.
INSERT INTO ${case_db}.t_empty VALUES (1, 'first');

-- query 20
SELECT id, v FROM ${case_db}.t_empty ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 4: TRUNCATE branch only — main untouched
-- ---------------------------------------------------------------------------

-- query 21
-- @skip_result_check=true
-- v3 + row-lineage required for branch writes.
CREATE TABLE ${case_db}.t_branch (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 22
-- @skip_result_check=true
INSERT INTO ${case_db}.t_branch VALUES (1, 'main_a'), (2, 'main_b');

-- query 23
-- @skip_result_check=true
-- ALTER TABLE for branch DDL requires 3-part name (catalog.db.table); see
-- src/engine/iceberg_ref_flow.rs::resolve_table_parts.
ALTER TABLE iceberg_ddl_cat_${suite_uuid0}.${case_db}.t_branch CREATE BRANCH dev;

-- query 24
-- @skip_result_check=true
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
INSERT INTO ${case_db}.t_branch.branch_dev VALUES (3, 'dev_only');

-- query 25
-- Pre-TRUNCATE: dev sees 3 rows.
SELECT id, v FROM ${case_db}.t_branch
  FOR VERSION AS OF 'dev' ORDER BY id;

-- query 26
-- @skip_result_check=true
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
-- TRUNCATE only the dev branch.
TRUNCATE TABLE ${case_db}.t_branch.branch_dev;

-- query 27
-- dev now empty.
SELECT COUNT(*) AS n FROM ${case_db}.t_branch
  FOR VERSION AS OF 'dev';

-- query 28
-- main intact: still has the original 2 rows.
SELECT id, v FROM ${case_db}.t_branch
  FOR VERSION AS OF 'main' ORDER BY id;

-- query 29
-- @skip_result_check=true
ALTER TABLE iceberg_ddl_cat_${suite_uuid0}.${case_db}.t_branch DROP BRANCH IF EXISTS dev;

-- ---------------------------------------------------------------------------
-- Case 5: TRUNCATE after DELETE (exercises deletion-vector / position-delete entries)
-- ---------------------------------------------------------------------------

-- query 30
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_dv (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 31
-- @skip_result_check=true
-- @be_log_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
INSERT INTO ${case_db}.t_dv VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');

-- query 32
-- @skip_result_check=true
-- DELETE creates deletion-vector / position-delete entries that TRUNCATE must
-- enumerate on the next snapshot.
DELETE FROM ${case_db}.t_dv WHERE id IN (2, 3);

-- query 33
-- After DELETE: 2 live rows.
SELECT id, v FROM ${case_db}.t_dv ORDER BY id;

-- query 34
-- @skip_result_check=true
-- @be_log_not_contains=NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED
-- @be_log_not_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
-- TRUNCATE must mark both data files and delete-content files as DELETED.
TRUNCATE TABLE ${case_db}.t_dv;

-- query 35
-- After TRUNCATE: empty.
SELECT COUNT(*) AS n FROM ${case_db}.t_dv;

-- ---------------------------------------------------------------------------
-- Case 6 & 7: parser-level rejection of PARTITION / WHERE clauses
-- ---------------------------------------------------------------------------

-- query 36
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_reject (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "2");

-- query 37
-- @expect_error=PARTITION
TRUNCATE TABLE ${case_db}.t_reject PARTITION (id=1);

-- query 38
-- @expect_error=WHERE
TRUNCATE TABLE ${case_db}.t_reject WHERE id = 1;
