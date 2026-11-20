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
-- Assert empty metric results, the retained current snapshot and tagged read,
-- and the retention parameters accepted by ALTER TABLE EXECUTE.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (id INT, v VARCHAR(16)) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES ROW(1, 'a');

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES ROW(2, 'b');

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES ROW(3, 'c');

-- query 5
-- Sanity: 3 rows present before EXPIRE.
SELECT COUNT(*) AS n FROM ${case_db}.t1;

-- query 6
-- Retain the current snapshot and expire earlier snapshots. Data in the
-- current snapshot remains visible after their metadata history is removed.
SET SESSION iceberg_cat_${suite_uuid0}.expire_snapshots_min_retention = '0s';
ALTER TABLE ${case_db}.t1 EXECUTE expire_snapshots(retention_threshold => '0s', retain_last => 1);

-- query 7
SELECT count(*) AS snapshots
FROM iceberg_cat_${suite_uuid0}.${case_db}."t1$snapshots";

-- query 8
-- All 3 rows must remain visible after the snapshot expiration.
SELECT id, v FROM ${case_db}.t1 ORDER BY id;

-- query 9
-- Row count still 3 after snapshot expiration.
SELECT COUNT(*) AS n FROM ${case_db}.t1;

-- query 10
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (id INT, v VARCHAR(16)) WITH (format_version = 3);

-- query 11
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 VALUES ROW(10, 'x');

-- query 12
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 VALUES ROW(20, 'y');

-- query 13
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 VALUES ROW(30, 'z');

-- query 14
-- Baseline: 3 rows before EXPIRE.
SELECT COUNT(*) AS n FROM ${case_db}.t2;

-- query 15
-- Omitting retention_threshold uses the default threshold.
ALTER TABLE ${case_db}.t2 EXECUTE expire_snapshots(retain_last => 1);

-- query 16
-- After expiry: all 3 rows still present.
SELECT COUNT(*) AS n FROM ${case_db}.t2;

-- query 17
-- @skip_result_check=true
CREATE TABLE ${case_db}.t3 (id INT, v VARCHAR(16)) WITH (format_version = 3);

-- query 18
-- @skip_result_check=true
INSERT INTO ${case_db}.t3 VALUES ROW(1, 'p');

-- query 19
-- @skip_result_check=true
INSERT INTO ${case_db}.t3 VALUES ROW(2, 'q');

-- query 20
-- Dictionary cleanup is admitted with both retention inputs.
ALTER TABLE ${case_db}.t3 EXECUTE expire_snapshots(retention_threshold => '7d', retain_last => 5, clean_expired_metadata => true);

-- query 21
-- Data intact after combined-clause expire.
SELECT id, v FROM ${case_db}.t3 ORDER BY id;

-- query 22
-- @skip_result_check=true
CREATE TABLE ${case_db}.t4 (id INT, v VARCHAR(16)) WITH (format_version = 3);

-- query 23
-- @skip_result_check=true
INSERT INTO ${case_db}.t4 VALUES ROW(1, 'alpha');

-- query 24
-- Pin the first snapshot before appending more data.
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.${case_db}.t4 EXECUTE create_tag(name => 'release_v1', replace => false, if_not_exists => false);

-- query 25
-- @skip_result_check=true
INSERT INTO ${case_db}.t4 VALUES ROW(2, 'beta');

-- query 26
-- @skip_result_check=true
INSERT INTO ${case_db}.t4 VALUES ROW(3, 'gamma');

-- query 27
-- @skip_result_check=true
-- Create a branch pointing at current snapshot (3-part name required for ref DDL).
CREATE BRANCH dev IN TABLE iceberg_cat_${suite_uuid0}.${case_db}.t4;

-- query 28
-- 3 rows visible through main branch.
SELECT COUNT(*) AS n FROM ${case_db}.t4;

-- query 29
-- Keep the current branch head and the snapshot held by the tag.
ALTER TABLE ${case_db}.t4 EXECUTE expire_snapshots(retention_threshold => '0s', retain_last => 1);

-- query 30
SELECT count(*) AS snapshots
FROM iceberg_cat_${suite_uuid0}.${case_db}."t4$snapshots";

-- query 31
SELECT id, v FROM ${case_db}.t4 FOR VERSION AS OF 'release_v1' ORDER BY id;

-- query 32
-- Data unchanged after expiry with branch+tag present.
SELECT id, v FROM ${case_db}.t4 ORDER BY id;

-- query 33
-- @skip_result_check=true
CREATE TABLE ${case_db}.t5 (id INT, v VARCHAR(16)) WITH (format_version = 2);

-- query 34
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ALTER TABLE ${case_db}.t5 EXPIRE SNAPSHOTS;

-- query 35
-- @skip_result_check=true
CREATE TABLE ${case_db}.t6 (id INT, v VARCHAR(16)) WITH (format_version = 2);

-- query 36
-- @expect_trino_error=INVALID_PROCEDURE_ARGUMENT,USER_ERROR
ALTER TABLE ${case_db}.t6 EXECUTE expire_snapshots(retain_last => 0);

-- query 37
-- @skip_result_check=true
CREATE TABLE ${case_db}.t7 (id INT, v VARCHAR(16)) WITH (format_version = 3);

-- query 38
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ALTER TABLE ${case_db}.t7@dev EXECUTE expire_snapshots(retain_last => 5);

-- query 39
-- @skip_result_check=true
CREATE TABLE ${case_db}.t8 (id INT, v VARCHAR(16)) WITH (format_version = 2);

-- query 40
-- @skip_result_check=true
INSERT INTO ${case_db}.t8 VALUES ROW(1, 'first');

-- query 41
-- @skip_result_check=true
INSERT INTO ${case_db}.t8 VALUES ROW(2, 'second');

-- query 42
-- Baseline before EXPIRE on v2 table.
SELECT COUNT(*) AS n FROM ${case_db}.t8;

-- query 43
ALTER TABLE ${case_db}.t8 EXECUTE expire_snapshots(retention_threshold => '0s', retain_last => 1);

-- query 44
-- Data intact after EXPIRE on v2 table.
SELECT id, v FROM ${case_db}.t8 ORDER BY id;

-- query 45
-- @skip_result_check=true
CREATE TABLE ${case_db}.t9 (id INT, v VARCHAR(16)) WITH (format_version = 3);

-- query 46
-- @skip_result_check=true
INSERT INTO ${case_db}.t9 VALUES ROW(1, 'one');

-- query 47
-- @skip_result_check=true
INSERT INTO ${case_db}.t9 VALUES ROW(2, 'two');

-- query 48
-- @expect_error=table procedure 'EXPIRE_SNAPSHOTS' property 'older_than' does not exist
ALTER TABLE ${case_db}.t9 EXECUTE expire_snapshots(older_than => 1700000000000);

-- query 49
-- Data intact after the rejected property.
SELECT id, v FROM ${case_db}.t9 ORDER BY id;

-- query 50
-- @skip_result_check=true
CREATE TABLE ${case_db}.t10 (id INT, v VARCHAR(16)) WITH (format_version = 3);

-- query 51
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ALTER TABLE ${case_db}.t10 EXPIRE SNAPSHOTS OLDER THAN '2026-01-01 00:00:00' OLDER THAN '2026-02-01 00:00:00';

-- query 52
-- A syntax rejection must not remove the target table.
SELECT COUNT(*) AS n FROM ${case_db}.t10;

-- query 53
-- @skip_result_check=true
CREATE TABLE ${case_db}.t11 (id INT, v VARCHAR(16)) WITH (format_version = 3);

-- query 54
-- @skip_result_check=true
INSERT INTO ${case_db}.t11 VALUES ROW(1, 'row1');

-- query 55
-- @skip_result_check=true
INSERT INTO ${case_db}.t11 VALUES ROW(2, 'row2');

-- query 56
ALTER TABLE ${case_db}.t11 EXECUTE expire_snapshots(retention_threshold => '0s', retain_last => 1);

-- query 57
-- @skip_result_check=true
-- INSERT after EXPIRE proves snapshot parent chain is healthy.
INSERT INTO ${case_db}.t11 VALUES ROW(3, 'row3');

-- query 58
-- All 3 rows visible: pre-EXPIRE rows plus the post-EXPIRE INSERT.
SELECT id, v FROM ${case_db}.t11 ORDER BY id;

-- query 59
-- Total row count confirms no data loss.
SELECT COUNT(*) AS n FROM ${case_db}.t11;

-- query 60
-- @skip_result_check=true
RESET SESSION iceberg_cat_${suite_uuid0}.expire_snapshots_min_retention;

-- query 61
-- @expect_error=Retention specified (1.00d) is shorter than the minimum retention configured in the system (7.00d). Minimum retention can be changed with iceberg.expire-snapshots.min-retention configuration property or iceberg.expire_snapshots_min_retention session property
ALTER TABLE ${case_db}.t11 EXECUTE expire_snapshots(retention_threshold => '1d');
