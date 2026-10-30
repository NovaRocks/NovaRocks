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
-- Validate the admitted REMOVE ORPHAN FILES syntax and the Provider safety
-- refusal through the real native frontend/connector path. REST/Hadoop do not
-- prove writer fencing or Unknown/unreceived-object protection, so scanned GC
-- must return Unsupported even for empty tables or historical cutoffs.
-- Existing row/count readbacks and the final INSERT independently prove that
-- this refusal preserves committed data and ordinary maintenance/write access.
-- Exact snapshot metadata maintenance remains a separate supported operation.
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
-- Case 1: Healthy table -> explicit scan refusal, data intact
-- Verifies: the rejected cleanup leaves every original row readable.
-- ---------------------------------------------------------------------------

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

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
-- Sanity: 3 rows present before REMOVE ORPHAN FILES.
SELECT COUNT(*) AS n FROM ${case_db}.t1;

-- query 6
-- @expect_sql_code=query.execution.unsupported
-- @expect_sql_phase=Execution
-- The admitted old cutoff does not supply the missing Provider safety proof.
ALTER TABLE ${case_db}.t1 REMOVE ORPHAN FILES OLDER THAN '2020-01-01 00:00:00';

-- query 7
-- All 3 rows must remain visible after the explicit scan refusal.
SELECT id, v FROM ${case_db}.t1 ORDER BY id;

-- query 8
-- Row count still 3 confirming no data files were removed.
SELECT COUNT(*) AS n FROM ${case_db}.t1;

-- ---------------------------------------------------------------------------
-- Case 2: Empty table -> the same explicit scan refusal
-- Verifies: absence of current data does not invent a safety proof.
-- ---------------------------------------------------------------------------

-- query 9
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 10
-- Table is empty before REMOVE ORPHAN FILES.
SELECT COUNT(*) AS n FROM ${case_db}.t2;

-- query 11
-- @expect_sql_code=query.execution.unsupported
-- @expect_sql_phase=Execution
-- An empty table does not grant scanned-object deletion authority.
ALTER TABLE ${case_db}.t2 REMOVE ORPHAN FILES OLDER THAN '2020-01-01 00:00:00';

-- query 12
-- Still empty after the explicit scan refusal.
SELECT COUNT(*) AS n FROM ${case_db}.t2;

-- ---------------------------------------------------------------------------
-- Case 3: Exact EXPIRE succeeds, scanned ORPHAN refuses, data is intact
-- Verifies: separate metadata maintenance does not grant scanned deletion.
-- The original readback remains required after both operations.
-- ---------------------------------------------------------------------------

-- query 13
-- @skip_result_check=true
CREATE TABLE ${case_db}.t3 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 14
-- @skip_result_check=true
INSERT INTO ${case_db}.t3 VALUES (10, 'x');

-- query 15
-- @skip_result_check=true
INSERT INTO ${case_db}.t3 VALUES (20, 'y');

-- query 16
-- @skip_result_check=true
INSERT INTO ${case_db}.t3 VALUES (30, 'z');

-- query 17
-- Baseline: 3 rows before the EXPIRE+ORPHAN pipeline.
SELECT COUNT(*) AS n FROM ${case_db}.t3;

-- query 18
-- @skip_result_check=true
-- Step 1: EXPIRE SNAPSHOTS (noop on live chain).
ALTER TABLE ${case_db}.t3 EXPIRE SNAPSHOTS OLDER THAN '2020-01-01 00:00:00';

-- query 19
-- @expect_sql_code=query.execution.unsupported
-- @expect_sql_phase=Execution
-- Exact snapshot maintenance cannot authorize a later scanned-object sweep.
ALTER TABLE ${case_db}.t3 REMOVE ORPHAN FILES OLDER THAN '2020-01-01 00:00:00';

-- query 20
-- All rows visible after exact EXPIRE and rejected scanned cleanup.
SELECT id, v FROM ${case_db}.t3 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 4: No OLDER THAN clause -> reject
-- Verifies: REMOVE ORPHAN FILES without OLDER THAN is a parse-time error.
-- ---------------------------------------------------------------------------

-- query 21
-- @skip_result_check=true
CREATE TABLE ${case_db}.t4 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "2");

-- query 22
-- @expect_error=requires OLDER THAN
ALTER TABLE ${case_db}.t4 REMOVE ORPHAN FILES;

-- ---------------------------------------------------------------------------
-- Case 5: Branch suffix -> reject
-- Verifies: parser rejects ALTER TABLE t.branch_dev REMOVE ORPHAN FILES.
-- ---------------------------------------------------------------------------

-- query 23
-- @skip_result_check=true
CREATE TABLE ${case_db}.t5 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 24
-- @expect_error=does not support branch
ALTER TABLE ${case_db}.t5.branch_dev REMOVE ORPHAN FILES OLDER THAN '2020-01-01 00:00:00';

-- ---------------------------------------------------------------------------
-- Case 6: V2 shares the safety refusal
-- Verifies: the scanned-object proof requirement is not format-version-specific.
-- ---------------------------------------------------------------------------

-- query 25
-- @skip_result_check=true
CREATE TABLE ${case_db}.t6 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "2");

-- query 26
-- @skip_result_check=true
INSERT INTO ${case_db}.t6 VALUES (1, 'first');

-- query 27
-- @skip_result_check=true
INSERT INTO ${case_db}.t6 VALUES (2, 'second');

-- query 28
-- Baseline: 2 rows before REMOVE ORPHAN FILES on v2 table.
SELECT COUNT(*) AS n FROM ${case_db}.t6;

-- query 29
-- @expect_sql_code=query.execution.unsupported
-- @expect_sql_phase=Execution
-- V2 uses the same unproven scanned-object safety boundary.
ALTER TABLE ${case_db}.t6 REMOVE ORPHAN FILES OLDER THAN '2020-01-01 00:00:00';

-- query 30
-- Data intact after the scan refusal on the v2 table.
SELECT id, v FROM ${case_db}.t6 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 7: Epoch-ms integer timestamp accepted
-- Verifies: OLDER THAN accepts a bare epoch-ms integer (e.g. 1700000000000).
-- ---------------------------------------------------------------------------

-- query 31
-- @skip_result_check=true
CREATE TABLE ${case_db}.t7 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 32
-- @skip_result_check=true
INSERT INTO ${case_db}.t7 VALUES (1, 'one');

-- query 33
-- @skip_result_check=true
INSERT INTO ${case_db}.t7 VALUES (2, 'two');

-- query 34
-- @expect_sql_code=query.execution.unsupported
-- @expect_sql_phase=Execution
-- Epoch-ms syntax is admitted; age does not prove safe scanned deletion.
ALTER TABLE ${case_db}.t7 REMOVE ORPHAN FILES OLDER THAN 1700000000000;

-- query 35
-- Data intact after the scan refusal with an admitted epoch-ms cutoff.
SELECT id, v FROM ${case_db}.t7 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 8: Trailing tokens after clause -> reject
-- Verifies: parser rejects extra tokens after the OLDER THAN timestamp.
-- ---------------------------------------------------------------------------

-- query 36
-- @skip_result_check=true
CREATE TABLE ${case_db}.t8 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "2");

-- query 37
-- @expect_error=unsupported trailing
ALTER TABLE ${case_db}.t8 REMOVE ORPHAN FILES OLDER THAN '2020-01-01 00:00:00' WHERE size_in_bytes < 100;

-- ---------------------------------------------------------------------------
-- Case 9: Historical cutoff syntax is accepted, scanned cleanup is refused
-- Verifies: age and current reachability are not Provider safety evidence.
-- ---------------------------------------------------------------------------

-- query 38
-- @skip_result_check=true
CREATE TABLE ${case_db}.t9 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 39
-- @skip_result_check=true
INSERT INTO ${case_db}.t9 VALUES (100, 'alpha');

-- query 40
-- @skip_result_check=true
INSERT INTO ${case_db}.t9 VALUES (200, 'beta');

-- query 41
-- Baseline: 2 rows.
SELECT COUNT(*) AS n FROM ${case_db}.t9;

-- query 42
-- @expect_sql_code=query.execution.unsupported
-- @expect_sql_phase=Execution
-- A historical cutoff is insufficient even while current files are live.
ALTER TABLE ${case_db}.t9 REMOVE ORPHAN FILES OLDER THAN '2020-01-01 00:00:00';

-- query 43
-- Row count unchanged after the explicit scan refusal.
SELECT COUNT(*) AS n FROM ${case_db}.t9;

-- ---------------------------------------------------------------------------
-- Case 10: Combined EXPIRE -> ORPHAN -> INSERT maintenance sequence
-- Verifies: exact EXPIRE and refused scanned cleanup preserve continued writes.
-- The subsequent INSERT and all original row/count readbacks remain required.
-- ---------------------------------------------------------------------------

-- query 44
-- @skip_result_check=true
CREATE TABLE ${case_db}.t10 (id INT, v VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 45
-- @skip_result_check=true
INSERT INTO ${case_db}.t10 VALUES (1, 'row1');

-- query 46
-- @skip_result_check=true
INSERT INTO ${case_db}.t10 VALUES (2, 'row2');

-- query 47
-- @skip_result_check=true
INSERT INTO ${case_db}.t10 VALUES (3, 'row3');

-- query 48
-- Pre-maintenance baseline: 3 rows.
SELECT COUNT(*) AS n FROM ${case_db}.t10;

-- query 49
-- @skip_result_check=true
-- Maintenance step 1: expire old snapshots (noop on live chain).
ALTER TABLE ${case_db}.t10 EXPIRE SNAPSHOTS OLDER THAN '2020-01-01 00:00:00';

-- query 50
-- @expect_sql_code=query.execution.unsupported
-- @expect_sql_phase=Execution
-- The unproven scanned-object phase refuses without authorizing deletion.
ALTER TABLE ${case_db}.t10 REMOVE ORPHAN FILES OLDER THAN '2020-01-01 00:00:00';

-- query 51
-- @skip_result_check=true
-- Maintenance step 3: new INSERT proves snapshot parent chain is healthy.
INSERT INTO ${case_db}.t10 VALUES (4, 'row4');

-- query 52
-- All 4 rows visible: 3 pre-maintenance + 1 post-maintenance.
SELECT id, v FROM ${case_db}.t10 ORDER BY id;

-- query 53
-- Total count confirms no data loss through the full maintenance pipeline.
SELECT COUNT(*) AS n FROM ${case_db}.t10;
