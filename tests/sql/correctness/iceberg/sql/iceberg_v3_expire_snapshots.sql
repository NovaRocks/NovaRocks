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
-- Validate ALTER TABLE x EXPIRE SNAPSHOTS end-to-end:
--   parser -> engine -> ExpireSnapshotsCommit -> metadata rewrite -> catalog commit.
-- Covers: OLDER THAN all-live noop, RETAIN LAST clause acceptance, both clauses
-- together, branch+tag ancestor protection, no-clause reject, RETAIN LAST 0 reject,
-- branch-suffix reject, v2 table support, epoch-ms int timestamp, duplicate OLDER
-- THAN reject, post-EXPIRE INSERT chain.
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
-- Case 1: OLDER THAN with all-live table -> noop, data intact
-- Verifies: future cutoff does not expunge live main-chain snapshots.
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
-- Sanity: 3 rows present before EXPIRE.
SELECT COUNT() AS n FROM ${case_db}.t1;

-- query 6
-- @skip_result_check=true
-- EXPIRE OLDER THAN far-future date: all snapshots are on the live main chain
-- so this is a noop.
ALTER TABLE ${case_db}.t1 EXPIRE SNAPSHOTS OLDER THAN '2099-01-01 00:00:00';

-- query 7
-- All 3 rows must remain visible after the noop expire.
SELECT id, v FROM ${case_db}.t1 ORDER BY id;

-- query 8
-- Row count still 3 after noop expire.
SELECT COUNT() AS n FROM ${case_db}.t1;

-- ---------------------------------------------------------------------------
-- Case 2: RETAIN LAST clause syntax acceptance
-- Verifies: RETAIN LAST alone parses and executes without error on a healthy
-- table.
-- ---------------------------------------------------------------------------

-- query 9
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 10
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 VALUES (10, 'x');

-- query 11
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 VALUES (20, 'y');

-- query 12
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 VALUES (30, 'z');

-- query 13
-- Baseline: 3 rows before EXPIRE.
SELECT COUNT() AS n FROM ${case_db}.t2;

-- query 14
-- @skip_result_check=true
-- RETAIN LAST 1 on main-chain table: all snapshots are live, so noop.
ALTER TABLE ${case_db}.t2 EXPIRE SNAPSHOTS RETAIN LAST 1;

-- query 15
-- After noop EXPIRE: all 3 rows still present.
SELECT COUNT() AS n FROM ${case_db}.t2;

-- ---------------------------------------------------------------------------
-- Case 3: Both clauses together (OLDER THAN + RETAIN LAST)
-- Verifies: parser accepts both clauses, execution succeeds without error.
-- ---------------------------------------------------------------------------

-- query 16
-- @skip_result_check=true
CREATE TABLE ${case_db}.t3 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 17
-- @skip_result_check=true
INSERT INTO ${case_db}.t3 VALUES (1, 'p');

-- query 18
-- @skip_result_check=true
INSERT INTO ${case_db}.t3 VALUES (2, 'q');

-- query 19
-- @skip_result_check=true
-- Both clauses: OLDER THAN and RETAIN LAST together.
ALTER TABLE ${case_db}.t3 EXPIRE SNAPSHOTS OLDER THAN '2026-01-01 00:00:00' RETAIN LAST 5;

-- query 20
-- Data intact after combined-clause expire.
SELECT id, v FROM ${case_db}.t3 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 4: Branch + Tag protect ancestors
-- Verifies: creating a branch and tag pins ancestors -- EXPIRE OLDER THAN future
-- is still a noop because all snapshots remain reachable via refs or main chain.
-- ---------------------------------------------------------------------------

-- query 21
-- @skip_result_check=true
CREATE TABLE ${case_db}.t4 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 22
-- @skip_result_check=true
INSERT INTO ${case_db}.t4 VALUES (1, 'alpha');

-- query 23
-- @skip_result_check=true
INSERT INTO ${case_db}.t4 VALUES (2, 'beta');

-- query 24
-- @skip_result_check=true
INSERT INTO ${case_db}.t4 VALUES (3, 'gamma');

-- query 25
-- @skip_result_check=true
-- Create a branch pointing at current snapshot (3-part name required for ref DDL).
ALTER TABLE iceberg_cat_${suite_uuid0}.${case_db}.t4 CREATE BRANCH dev;

-- query 26
-- @skip_result_check=true
-- Create a tag pointing at current snapshot (3-part name required for ref DDL).
ALTER TABLE iceberg_cat_${suite_uuid0}.${case_db}.t4 CREATE TAG release_v1;

-- query 27
-- 3 rows visible through main branch.
SELECT COUNT() AS n FROM ${case_db}.t4;

-- query 28
-- @skip_result_check=true
-- EXPIRE: all snapshots protected by main + branch + tag, noop.
ALTER TABLE ${case_db}.t4 EXPIRE SNAPSHOTS OLDER THAN '2099-01-01 00:00:00';

-- query 29
-- Data unchanged after noop expire with branch+tag present.
SELECT id, v FROM ${case_db}.t4 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 5: No clause -> reject
-- Verifies: EXPIRE SNAPSHOTS with neither OLDER THAN nor RETAIN LAST is rejected.
-- ---------------------------------------------------------------------------

-- query 30
-- @skip_result_check=true
CREATE TABLE ${case_db}.t5 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "2");

-- query 31
-- @expect_error=requires at least
ALTER TABLE ${case_db}.t5 EXPIRE SNAPSHOTS;

-- ---------------------------------------------------------------------------
-- Case 6: RETAIN LAST 0 -> reject
-- Verifies: RETAIN LAST 0 is a rejected value (must be >= 1).
-- ---------------------------------------------------------------------------

-- query 32
-- @skip_result_check=true
CREATE TABLE ${case_db}.t6 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "2");

-- query 33
-- @expect_error=must be >= 1
ALTER TABLE ${case_db}.t6 EXPIRE SNAPSHOTS RETAIN LAST 0;

-- ---------------------------------------------------------------------------
-- Case 7: Branch suffix -> reject
-- Verifies: parser rejects ALTER TABLE t.branch_dev EXPIRE SNAPSHOTS.
-- ---------------------------------------------------------------------------

-- query 34
-- @skip_result_check=true
CREATE TABLE ${case_db}.t7 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 35
-- @expect_error=does not support branch
ALTER TABLE ${case_db}.t7.branch_dev EXPIRE SNAPSHOTS RETAIN LAST 5;

-- ---------------------------------------------------------------------------
-- Case 8: V2 table support
-- Verifies: EXPIRE SNAPSHOTS works on format-version 2 tables (not v3-only).
-- ---------------------------------------------------------------------------

-- query 36
-- @skip_result_check=true
CREATE TABLE ${case_db}.t8 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "2");

-- query 37
-- @skip_result_check=true
INSERT INTO ${case_db}.t8 VALUES (1, 'first');

-- query 38
-- @skip_result_check=true
INSERT INTO ${case_db}.t8 VALUES (2, 'second');

-- query 39
-- Baseline before EXPIRE on v2 table.
SELECT COUNT() AS n FROM ${case_db}.t8;

-- query 40
-- @skip_result_check=true
-- EXPIRE on v2 table: noop (all live) and no error.
ALTER TABLE ${case_db}.t8 EXPIRE SNAPSHOTS OLDER THAN '2099-01-01 00:00:00';

-- query 41
-- Data intact after EXPIRE on v2 table.
SELECT id, v FROM ${case_db}.t8 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 9: Epoch-ms integer timestamp accepted
-- Verifies: OLDER THAN accepts a bare epoch-ms integer (not just quoted strings).
-- ---------------------------------------------------------------------------

-- query 42
-- @skip_result_check=true
CREATE TABLE ${case_db}.t9 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 43
-- @skip_result_check=true
INSERT INTO ${case_db}.t9 VALUES (1, 'one');

-- query 44
-- @skip_result_check=true
INSERT INTO ${case_db}.t9 VALUES (2, 'two');

-- query 45
-- @skip_result_check=true
-- Epoch-ms integer form: 1700000000000 is 2023-11-14 which is before our data.
-- All our snapshots are newer than that cutoff but they are on the live chain
-- so they are still protected and this remains a noop in practice.
ALTER TABLE ${case_db}.t9 EXPIRE SNAPSHOTS OLDER THAN 1700000000000;

-- query 46
-- Data intact after epoch-ms EXPIRE.
SELECT id, v FROM ${case_db}.t9 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 10: Duplicate OLDER THAN -> reject
-- Verifies: duplicate OLDER THAN clause is a parse-time error.
-- ---------------------------------------------------------------------------

-- query 47
-- @skip_result_check=true
CREATE TABLE ${case_db}.t10 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 48
-- @expect_error=duplicate
ALTER TABLE ${case_db}.t10 EXPIRE SNAPSHOTS OLDER THAN '2026-01-01 00:00:00' OLDER THAN '2026-02-01 00:00:00';

-- query 49
-- A syntax rejection must not remove the target table.
SELECT COUNT() AS n FROM ${case_db}.t10;

-- ---------------------------------------------------------------------------
-- Case 11: After EXPIRE, table still readable + INSERT works
-- Verifies: snapshot chain is healthy after a noop EXPIRE and subsequent INSERT
-- adds new data correctly.
-- ---------------------------------------------------------------------------

-- query 50
-- @skip_result_check=true
CREATE TABLE ${case_db}.t11 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 51
-- @skip_result_check=true
INSERT INTO ${case_db}.t11 VALUES (1, 'row1');

-- query 52
-- @skip_result_check=true
INSERT INTO ${case_db}.t11 VALUES (2, 'row2');

-- query 53
-- @skip_result_check=true
-- EXPIRE noop: all snapshots live.
ALTER TABLE ${case_db}.t11 EXPIRE SNAPSHOTS OLDER THAN '2099-01-01 00:00:00';

-- query 54
-- @skip_result_check=true
-- INSERT after EXPIRE proves snapshot parent chain is healthy.
INSERT INTO ${case_db}.t11 VALUES (3, 'row3');

-- query 55
-- All 3 rows visible: pre-EXPIRE rows plus the post-EXPIRE INSERT.
SELECT id, v FROM ${case_db}.t11 ORDER BY id;

-- query 56
-- Total row count confirms no data loss.
SELECT COUNT() AS n FROM ${case_db}.t11;
