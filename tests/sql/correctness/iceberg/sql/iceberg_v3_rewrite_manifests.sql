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
-- Assert manifest rewrite metrics on v2 and v3, the empty result on a skip,
-- and row integrity after rewrite. Read the delete manifest around the rewrite.

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
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES ROW(4, 'd');

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES ROW(5, 'e');

-- query 7
-- Sanity check: 5 rows present before REWRITE.
SELECT COUNT(*) AS n FROM ${case_db}.t1;

-- query 8
-- REWRITE MANIFESTS merges 5 per-INSERT manifests into one.
ALTER TABLE ${case_db}.t1 EXECUTE optimize_manifests;

-- query 9
-- All 5 rows must remain visible after the manifest rewrite.
SELECT id, v FROM ${case_db}.t1 ORDER BY id;

-- query 10
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (id INT, v VARCHAR(16)) WITH (format_version = 3);

-- query 11
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 VALUES ROW(1, 'x'), ROW(2, 'y'), ROW(3, 'z');

-- query 12
-- Baseline: 3 rows before noop REWRITE.
SELECT COUNT(*) AS n FROM ${case_db}.t2;

-- query 13
-- Single manifest → noop (returns Ok immediately without emitting a snapshot).
ALTER TABLE ${case_db}.t2 EXECUTE optimize_manifests;

-- query 14
-- After noop REWRITE: all 3 rows still present.
SELECT COUNT(*) AS n FROM ${case_db}.t2;

-- query 15
-- @skip_result_check=true
CREATE TABLE ${case_db}.t3 (id INT, v VARCHAR(16)) WITH (format_version = 3);

-- query 16
-- Table is empty before REWRITE.
SELECT COUNT(*) AS n FROM ${case_db}.t3;

-- query 17
-- Empty table has no current snapshot, REWRITE is a noop and succeeds without error.
ALTER TABLE ${case_db}.t3 EXECUTE optimize_manifests;

-- query 18
-- Still empty after REWRITE.
SELECT COUNT(*) AS n FROM ${case_db}.t3;

-- query 19
-- @skip_result_check=true
CREATE TABLE ${case_db}.t4 (id INT, v VARCHAR(16)) WITH (format_version = 2);

-- query 20
-- @skip_result_check=true
INSERT INTO ${case_db}.t4 VALUES ROW(1, 'p');

-- query 21
-- @skip_result_check=true
INSERT INTO ${case_db}.t4 VALUES ROW(2, 'q');

-- query 22
-- @skip_result_check=true
INSERT INTO ${case_db}.t4 VALUES ROW(3, 'r');

-- query 23
-- REWRITE merges 3 per-INSERT manifests on the v2 table.
ALTER TABLE ${case_db}.t4 EXECUTE optimize_manifests;

-- query 24
-- All 3 rows visible after REWRITE on a v2 table.
SELECT id, v FROM ${case_db}.t4 ORDER BY id;

-- query 25
-- @skip_result_check=true
CREATE TABLE ${case_db}.t5 (id INT, v VARCHAR(16)) WITH (format_version = 3);

-- query 26
-- @skip_result_check=true
INSERT INTO ${case_db}.t5 VALUES ROW(10, 'alpha');

-- query 27
-- @skip_result_check=true
INSERT INTO ${case_db}.t5 VALUES ROW(20, 'beta');

-- query 28
-- @skip_result_check=true
INSERT INTO ${case_db}.t5 VALUES ROW(30, 'gamma');

-- query 29
-- Capture (id, _row_id) before REWRITE — establishes the baseline.
-- The recorded result will be compared against query 31 to confirm
-- _row_id values are preserved unchanged after the manifest merge.
SELECT id, _row_id FROM ${case_db}.t5 ORDER BY id;

-- query 30
ALTER TABLE ${case_db}.t5 EXECUTE optimize_manifests;

-- query 31
-- After REWRITE: (id, _row_id) must match the pre-REWRITE baseline exactly.
-- If REWRITE incorrectly reassigns row-lineage ids, this will differ from query 29.
SELECT id, _row_id FROM ${case_db}.t5 ORDER BY id;

-- query 32
-- @skip_result_check=true
CREATE TABLE ${case_db}.t6 (id INT, v VARCHAR(16)) WITH (format_version = 3);

-- query 33
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ALTER TABLE ${case_db}.t6@dev EXECUTE optimize_manifests;

-- query 34
-- @skip_result_check=true
CREATE TABLE ${case_db}.t7 (id INT, v VARCHAR(16)) WITH (format_version = 2);

-- query 35
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ALTER TABLE ${case_db}.t7 REWRITE MANIFESTS WHERE size_in_bytes < 100;

-- query 36
-- @skip_result_check=true
CREATE TABLE ${case_db}.t8 (id INT, v VARCHAR(16)) WITH (format_version = 3);

-- query 37
-- @skip_result_check=true
INSERT INTO ${case_db}.t8 VALUES ROW(1, 'a'), ROW(2, 'b'), ROW(3, 'c');

-- query 38
-- @skip_result_check=true
INSERT INTO ${case_db}.t8 VALUES ROW(4, 'd'), ROW(5, 'e');

-- query 39
-- @skip_result_check=true
-- DELETE one row — creates a deletion-vector entry and a delete manifest.
DELETE FROM ${case_db}.t8 WHERE id = 2;

-- query 40
-- Pre-REWRITE: 4 live rows — DELETE removed 1.
SELECT id, v FROM ${case_db}.t8 ORDER BY id;

-- query 41
SELECT count(*) AS delete_manifests
FROM iceberg_cat_${suite_uuid0}.${case_db}."t8$manifests"
WHERE content = 1;

-- query 42
-- Only data manifests are rewritten. The delete manifest is kept.
ALTER TABLE ${case_db}.t8 EXECUTE optimize_manifests;

-- query 43
SELECT count(*) AS delete_manifests
FROM iceberg_cat_${suite_uuid0}.${case_db}."t8$manifests"
WHERE content = 1;

-- query 44
-- After REWRITE: exact same rows visible — DELETE effect preserved.
SELECT id, v FROM ${case_db}.t8 ORDER BY id;

-- query 45
-- @skip_result_check=true
CREATE TABLE ${case_db}.t9 (id INT, v VARCHAR(16)) WITH (format_version = 3);

-- query 46
-- @skip_result_check=true
INSERT INTO ${case_db}.t9 VALUES ROW(1, 'first');

-- query 47
-- @skip_result_check=true
INSERT INTO ${case_db}.t9 VALUES ROW(2, 'second');

-- query 48
-- @skip_result_check=true
INSERT INTO ${case_db}.t9 VALUES ROW(3, 'third');

-- query 49
ALTER TABLE ${case_db}.t9 EXECUTE optimize_manifests;

-- query 50
-- @skip_result_check=true
-- INSERT after REWRITE — proves the snapshot parent chain is healthy.
INSERT INTO ${case_db}.t9 VALUES ROW(4, 'fourth');

-- query 51
-- All 4 rows visible: pre-REWRITE rows plus the post-REWRITE INSERT.
SELECT id, v FROM ${case_db}.t9 ORDER BY id;

-- query 52
-- Total row count confirms no data loss from the REWRITE.
SELECT COUNT(*) AS n FROM ${case_db}.t9;

-- query 53
-- @skip_result_check=true
CREATE TABLE ${case_db}.t10 (id INT, v VARCHAR(16)) WITH (format_version = 3);

-- query 54
-- @skip_result_check=true
INSERT INTO ${case_db}.t10 VALUES ROW(1, 'one');

-- query 55
-- @skip_result_check=true
INSERT INTO ${case_db}.t10 VALUES ROW(2, 'two');

-- query 56
-- @skip_result_check=true
INSERT INTO ${case_db}.t10 VALUES ROW(3, 'three');

-- query 57
-- First REWRITE: merges 3 manifests into 1 and adds a replace snapshot.
ALTER TABLE ${case_db}.t10 EXECUTE optimize_manifests;

-- query 58
-- Second REWRITE: 1 manifest remains after first rewrite, is a noop.
ALTER TABLE ${case_db}.t10 EXECUTE optimize_manifests;

-- query 59
-- Data intact after two REWRITEs: all 3 rows visible.
SELECT id, v FROM ${case_db}.t10 ORDER BY id;

-- query 60
-- Total count confirms no rows lost through the two-pass rewrite.
SELECT COUNT(*) AS n FROM ${case_db}.t10;
