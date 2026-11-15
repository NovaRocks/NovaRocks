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
-- Validate ALTER TABLE x REWRITE MANIFESTS on Iceberg targets end-to-end:
--   parser -> engine -> RewriteManifestsCommit -> manifest writes -> catalog commit.
-- Covers: v3 multi-manifest merge with data-integrity check, single-manifest noop,
-- empty-table noop, v2 table support, _row_id round-trip preservation, branch-suffix
-- reject, trailing-token reject, REWRITE across DELETE/UPDATE chain, REWRITE+INSERT
-- chain, and double-REWRITE idempotence.
--
-- Note: tbl$snapshots metadata table queries are not used here because the iceberg
-- suite's current_catalog mode causes the pre-query table-registration step to try
-- to load the metadata relation as a real Iceberg table, which fails. Instead,
-- correctness is verified through data-level assertions (SELECT COUNT(*), SELECT *).
--
-- The runner auto-creates `${case_db}` per case and drops it on cleanup, so
-- there is no explicit CREATE/DROP DATABASE here. All table refs are 2-part
-- `${case_db}.<table>` against the suite-level iceberg catalog
-- (`iceberg_cat_${suite_uuid0}`) selected by `SET catalog` at session setup.

-- ---------------------------------------------------------------------------
-- Case 1: 5 INSERTs → REWRITE MANIFESTS → data unchanged, no error
-- Verifies: multi-manifest merge executes without error and all 5 rows survive.
-- ---------------------------------------------------------------------------

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES (1, 'a');

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES (2, 'b');

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES (3, 'c');

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES (4, 'd');

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES (5, 'e');

-- query 7
-- Sanity check: 5 rows present before REWRITE.
SELECT COUNT(*) AS n FROM ${case_db}.t1;

-- query 8
-- @skip_result_check=true
-- REWRITE MANIFESTS merges 5 per-INSERT manifests into one.
ALTER TABLE ${case_db}.t1 REWRITE MANIFESTS;

-- query 9
-- All 5 rows must remain visible after the manifest rewrite.
SELECT id, v FROM ${case_db}.t1 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 2: Single-manifest table → REWRITE MANIFESTS is a noop
-- Verifies: single INSERT produces 1 manifest, REWRITE is a noop, data unchanged.
-- ---------------------------------------------------------------------------

-- query 10
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 11
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 VALUES (1, 'x'), (2, 'y'), (3, 'z');

-- query 12
-- Baseline: 3 rows before noop REWRITE.
SELECT COUNT(*) AS n FROM ${case_db}.t2;

-- query 13
-- @skip_result_check=true
-- Single manifest → noop (returns Ok immediately without emitting a snapshot).
ALTER TABLE ${case_db}.t2 REWRITE MANIFESTS;

-- query 14
-- After noop REWRITE: all 3 rows still present.
SELECT COUNT(*) AS n FROM ${case_db}.t2;

-- ---------------------------------------------------------------------------
-- Case 3: Empty table → REWRITE MANIFESTS is a noop (no snapshot, no error)
-- Verifies: empty table (no current snapshot) returns Ok without error.
-- ---------------------------------------------------------------------------

-- query 15
-- @skip_result_check=true
CREATE TABLE ${case_db}.t3 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 16
-- Table is empty before REWRITE.
SELECT COUNT(*) AS n FROM ${case_db}.t3;

-- query 17
-- @skip_result_check=true
-- Empty table has no current snapshot, REWRITE is a noop and succeeds without error.
ALTER TABLE ${case_db}.t3 REWRITE MANIFESTS;

-- query 18
-- Still empty after REWRITE.
SELECT COUNT(*) AS n FROM ${case_db}.t3;

-- ---------------------------------------------------------------------------
-- Case 4: V2 table — REWRITE MANIFESTS works (not v3-only)
-- Verifies: 3 INSERTs on a v2 table are merged, all rows survive after REWRITE.
-- ---------------------------------------------------------------------------

-- query 19
-- @skip_result_check=true
CREATE TABLE ${case_db}.t4 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "2");

-- query 20
-- @skip_result_check=true
INSERT INTO ${case_db}.t4 VALUES (1, 'p');

-- query 21
-- @skip_result_check=true
INSERT INTO ${case_db}.t4 VALUES (2, 'q');

-- query 22
-- @skip_result_check=true
INSERT INTO ${case_db}.t4 VALUES (3, 'r');

-- query 23
-- @skip_result_check=true
-- REWRITE merges 3 per-INSERT manifests on the v2 table.
ALTER TABLE ${case_db}.t4 REWRITE MANIFESTS;

-- query 24
-- All 3 rows visible after REWRITE on a v2 table.
SELECT id, v FROM ${case_db}.t4 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 5: V3 row-lineage _row_id values survive REWRITE MANIFESTS unchanged
-- Verifies: per-row _row_id values are preserved through the manifest merge.
-- Before and after REWRITE, (id, _row_id) pairs must be identical.
-- ---------------------------------------------------------------------------

-- query 25
-- @skip_result_check=true
CREATE TABLE ${case_db}.t5 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 26
-- @skip_result_check=true
INSERT INTO ${case_db}.t5 VALUES (10, 'alpha');

-- query 27
-- @skip_result_check=true
INSERT INTO ${case_db}.t5 VALUES (20, 'beta');

-- query 28
-- @skip_result_check=true
INSERT INTO ${case_db}.t5 VALUES (30, 'gamma');

-- query 29
-- Capture (id, _row_id) before REWRITE — establishes the baseline.
-- The recorded result will be compared against query 31 to confirm
-- _row_id values are preserved unchanged after the manifest merge.
SELECT id, _row_id FROM ${case_db}.t5 ORDER BY id;

-- query 30
-- @skip_result_check=true
ALTER TABLE ${case_db}.t5 REWRITE MANIFESTS;

-- query 31
-- After REWRITE: (id, _row_id) must match the pre-REWRITE baseline exactly.
-- If REWRITE incorrectly reassigns row-lineage ids, this will differ from query 29.
SELECT id, _row_id FROM ${case_db}.t5 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 6: Branch suffix → reject
-- Verifies: parser rejects `ALTER TABLE t.branch_dev REWRITE MANIFESTS`.
-- ---------------------------------------------------------------------------

-- query 32
-- @skip_result_check=true
CREATE TABLE ${case_db}.t6 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 33
-- @expect_error=does not support branch
ALTER TABLE ${case_db}.t6.branch_dev REWRITE MANIFESTS;

-- ---------------------------------------------------------------------------
-- Case 7: Trailing tokens → reject
-- Verifies: parser rejects `ALTER TABLE t REWRITE MANIFESTS WHERE ...`.
-- ---------------------------------------------------------------------------

-- query 34
-- @skip_result_check=true
CREATE TABLE ${case_db}.t7 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "2");

-- query 35
-- @expect_error=unsupported trailing
ALTER TABLE ${case_db}.t7 REWRITE MANIFESTS WHERE size_in_bytes < 100;

-- ---------------------------------------------------------------------------
-- Case 8: REWRITE preserves data correctness after INSERT + DELETE chain
-- Verifies: after multiple INSERTs and a row DELETE build up a multi-manifest
-- state with both data and delete-file manifests, REWRITE does not change
-- the visible row set.
-- ---------------------------------------------------------------------------

-- query 36
-- @skip_result_check=true
CREATE TABLE ${case_db}.t8 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 37
-- @skip_result_check=true
INSERT INTO ${case_db}.t8 VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- query 38
-- @skip_result_check=true
INSERT INTO ${case_db}.t8 VALUES (4, 'd'), (5, 'e');

-- query 39
-- @skip_result_check=true
-- DELETE one row — creates a deletion-vector entry and a delete manifest.
DELETE FROM ${case_db}.t8 WHERE id = 2;

-- query 40
-- Pre-REWRITE: 4 live rows — DELETE removed 1.
SELECT id, v FROM ${case_db}.t8 ORDER BY id;

-- query 41
-- @skip_result_check=true
-- REWRITE consolidates data and delete-content manifests.
ALTER TABLE ${case_db}.t8 REWRITE MANIFESTS;

-- query 42
-- After REWRITE: exact same rows visible — DELETE effect preserved.
SELECT id, v FROM ${case_db}.t8 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 9: REWRITE then INSERT works (parent snapshot chain is healthy)
-- Verifies: a subsequent INSERT after REWRITE succeeds and the new row is visible.
-- ---------------------------------------------------------------------------

-- query 43
-- @skip_result_check=true
CREATE TABLE ${case_db}.t9 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 44
-- @skip_result_check=true
INSERT INTO ${case_db}.t9 VALUES (1, 'first');

-- query 45
-- @skip_result_check=true
INSERT INTO ${case_db}.t9 VALUES (2, 'second');

-- query 46
-- @skip_result_check=true
INSERT INTO ${case_db}.t9 VALUES (3, 'third');

-- query 47
-- @skip_result_check=true
ALTER TABLE ${case_db}.t9 REWRITE MANIFESTS;

-- query 48
-- @skip_result_check=true
-- INSERT after REWRITE — proves the snapshot parent chain is healthy.
INSERT INTO ${case_db}.t9 VALUES (4, 'fourth');

-- query 49
-- All 4 rows visible: pre-REWRITE rows plus the post-REWRITE INSERT.
SELECT id, v FROM ${case_db}.t9 ORDER BY id;

-- query 50
-- Total row count confirms no data loss from the REWRITE.
SELECT COUNT(*) AS n FROM ${case_db}.t9;

-- ---------------------------------------------------------------------------
-- Case 10: Two consecutive REWRITEs — second is noop after first merged manifests
-- Verifies: double-REWRITE is idempotent, data is unchanged after both rewrites.
-- After the first REWRITE, the manifest count drops to 1. The second REWRITE
-- detects ≤ 1 manifest and returns early (noop) without writing a new snapshot.
-- ---------------------------------------------------------------------------

-- query 51
-- @skip_result_check=true
CREATE TABLE ${case_db}.t10 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 52
-- @skip_result_check=true
INSERT INTO ${case_db}.t10 VALUES (1, 'one');

-- query 53
-- @skip_result_check=true
INSERT INTO ${case_db}.t10 VALUES (2, 'two');

-- query 54
-- @skip_result_check=true
INSERT INTO ${case_db}.t10 VALUES (3, 'three');

-- query 55
-- @skip_result_check=true
-- First REWRITE: merges 3 manifests into 1 and adds a replace snapshot.
ALTER TABLE ${case_db}.t10 REWRITE MANIFESTS;

-- query 56
-- @skip_result_check=true
-- Second REWRITE: 1 manifest remains after first rewrite, is a noop.
ALTER TABLE ${case_db}.t10 REWRITE MANIFESTS;

-- query 57
-- Data intact after two REWRITEs: all 3 rows visible.
SELECT id, v FROM ${case_db}.t10 ORDER BY id;

-- query 58
-- Total count confirms no rows lost through the two-pass rewrite.
SELECT COUNT(*) AS n FROM ${case_db}.t10;
