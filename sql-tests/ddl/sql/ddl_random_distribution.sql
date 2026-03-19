-- Migrated from dev/test/sql/test_random_distribution/T/test_random_distribution
-- Test Objective:
-- 1. Default distribution for DUPLICATE KEY tables is RANDOM (no explicit DISTRIBUTED BY).
-- 2. Explicit DISTRIBUTED BY RANDOM with/without bucket count.
-- 3. Partitioned table with RANDOM distribution: creation, insert, select.
-- 4. ADD PARTITION inherits table-level RANDOM distribution; mixing HASH distribution fails.
-- 5. Non-DUPLICATE KEY tables (PRIMARY KEY, UNIQUE KEY, AGG KEY) reject RANDOM distribution.
-- 6. Automatic partition (date_trunc) with RANDOM distribution.
-- 7. CTAS with RANDOM distribution (default, explicit, explicit with buckets).
-- 8. Join between RANDOM and HASH distributed tables.

-- query 1
-- Default distribution: create table without DISTRIBUTED BY => RANDOM.
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_default (k INT, v INT);
INSERT INTO ${case_db}.t_default VALUES (1, 1);

-- query 2
SELECT * FROM ${case_db}.t_default;

-- query 3
-- Explicit DISTRIBUTED BY RANDOM with various options.
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_dup1 (k INT, v INT) DUPLICATE KEY(k);
CREATE TABLE ${case_db}.t_rand_auto (k INT, v INT) DUPLICATE KEY(k) DISTRIBUTED BY RANDOM;
CREATE TABLE ${case_db}.t_rand_b10 (k INT, v INT) DUPLICATE KEY(k) DISTRIBUTED BY RANDOM BUCKETS 10;
CREATE TABLE ${case_db}.t_rand_no_key (k INT, v INT) DISTRIBUTED BY RANDOM;
CREATE TABLE ${case_db}.t_rand_no_key_b10 (k INT, v INT) DISTRIBUTED BY RANDOM BUCKETS 10;

-- query 4
-- SHOW CREATE TABLE shows DISTRIBUTED BY RANDOM BUCKETS 10 for explicit bucket table.
-- @result_contains=DISTRIBUTED BY RANDOM BUCKETS 10
SHOW CREATE TABLE ${case_db}.t_rand_b10;

-- query 5
-- SHOW CREATE TABLE shows DISTRIBUTED BY RANDOM BUCKETS 10 for inferred DUPLICATE KEY table.
-- @result_contains=DISTRIBUTED BY RANDOM BUCKETS 10
SHOW CREATE TABLE ${case_db}.t_rand_no_key_b10;

-- query 6
-- Partitioned table with RANDOM distribution.
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_part0 (k INT, v INT) PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN ("0"));
CREATE TABLE ${case_db}.t_part1 (k INT, v INT) PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN ("0")) DISTRIBUTED BY RANDOM;
CREATE TABLE ${case_db}.t_part2 (k INT, v INT) PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN ("0")) DISTRIBUTED BY RANDOM BUCKETS 10;

-- query 7
-- @result_contains=DISTRIBUTED BY RANDOM BUCKETS 10
SHOW CREATE TABLE ${case_db}.t_part2;

-- query 8
-- @skip_result_check=true
INSERT INTO ${case_db}.t_part2 VALUES (-1, -1);

-- query 9
SELECT * FROM ${case_db}.t_part2;

-- query 10
-- ADD PARTITION: default inherits table-level RANDOM distribution.
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_addpart (k INT, v INT)
  PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN ("0"))
  DISTRIBUTED BY RANDOM BUCKETS 10;
ALTER TABLE ${case_db}.t_addpart ADD PARTITION p2 VALUES LESS THAN ("20");

-- query 11
-- After adding p2: table distribution stays DISTRIBUTED BY RANDOM BUCKETS 10.
-- @result_contains=DISTRIBUTED BY RANDOM BUCKETS 10
SHOW CREATE TABLE ${case_db}.t_addpart;

-- query 12
-- ADD PARTITION with explicit RANDOM BUCKETS (different count) is allowed.
-- @skip_result_check=true
ALTER TABLE ${case_db}.t_addpart ADD PARTITION p3 VALUES LESS THAN ("30") DISTRIBUTED BY RANDOM BUCKETS 20;

-- query 13
-- Table default distribution still shows RANDOM BUCKETS 10.
-- @result_contains=DISTRIBUTED BY RANDOM BUCKETS 10
SHOW CREATE TABLE ${case_db}.t_addpart;

-- query 14
-- ADD PARTITION with DISTRIBUTED BY RANDOM (no explicit buckets) is allowed.
-- @skip_result_check=true
ALTER TABLE ${case_db}.t_addpart ADD PARTITION p4 VALUES LESS THAN ("40") DISTRIBUTED BY RANDOM;

-- query 15
-- @result_contains=DISTRIBUTED BY RANDOM BUCKETS 10
SHOW CREATE TABLE ${case_db}.t_addpart;

-- query 16
-- ADD PARTITION with HASH distribution on RANDOM table must fail.
-- @expect_error=Cannot assign different distribution type
ALTER TABLE ${case_db}.t_addpart ADD PARTITION p5 VALUES LESS THAN ("50") DISTRIBUTED BY HASH(k) BUCKETS 10;

-- query 17
-- @skip_result_check=true
INSERT INTO ${case_db}.t_addpart VALUES (-1,-1),(5,5),(15,15),(35,35);

-- query 18
SELECT * FROM ${case_db}.t_addpart ORDER BY k;

-- query 19
-- Non-dup key: DUPLICATE KEY baseline is allowed.
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_dup_base (k INT, v INT) DUPLICATE KEY(k);

-- query 20
-- PRIMARY KEY must use hash distribution.
-- @expect_error=PRIMARY KEY
CREATE TABLE ${case_db}.t_pk_rand (k INT, v INT) PRIMARY KEY(k) DISTRIBUTED BY RANDOM BUCKETS 20;

-- query 21
-- UNIQUE KEY default distribution is not supported.
-- @expect_error=Currently not support default distribution in UNIQUE_KEYS
CREATE TABLE ${case_db}.t_uk_def (k INT, v INT) UNIQUE KEY(k);

-- query 22
-- UNIQUE KEY must use hash distribution.
-- @expect_error=UNIQUE KEY
CREATE TABLE ${case_db}.t_uk_rand (k INT, v INT) UNIQUE KEY(k) DISTRIBUTED BY RANDOM BUCKETS 20;

-- query 23
-- AGG KEY default distribution is not supported.
-- @expect_error=Currently not support default distribution in AGG_KEYS
CREATE TABLE ${case_db}.t_agg_def (k INT, v INT SUM) AGGREGATE KEY(k);

-- query 24
-- AGG KEY must use hash distribution.
-- @expect_error=AGGREGATE KEY
CREATE TABLE ${case_db}.t_agg_rand (k INT, v INT SUM) AGGREGATE KEY(k) DISTRIBUTED BY RANDOM BUCKETS 20;

-- query 25
-- Create AGG KEY table with HASH (valid) then try to alter to RANDOM.
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_agg_hash (k INT, v INT SUM) AGGREGATE KEY(k) DISTRIBUTED BY HASH(k);

-- query 26
-- @expect_error=AGGREGATE KEY must use hash distribution
ALTER TABLE ${case_db}.t_agg_hash DISTRIBUTED BY RANDOM BUCKETS 20;

-- query 27
-- Automatic partition (date_trunc) uses RANDOM distribution by default.
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_autopart (k DATE, v INT)
  PARTITION BY date_trunc('day', k)
  PROPERTIES('replication_num'='3');
INSERT INTO ${case_db}.t_autopart VALUES ('2023-02-14', 2), ('2033-03-01', 2);

-- query 28
SELECT * FROM ${case_db}.t_autopart ORDER BY k;

-- query 29
-- @result_contains=DISTRIBUTED BY RANDOM
SHOW CREATE TABLE ${case_db}.t_autopart;

-- query 30
-- CTAS inherits RANDOM distribution from source.
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_src (k INT, v INT);
CREATE TABLE ${case_db}.t_ctas AS SELECT * FROM ${case_db}.t_src;

-- query 31
-- @result_contains=DISTRIBUTED BY RANDOM
SHOW CREATE TABLE ${case_db}.t_ctas;

-- query 32
-- CTAS with explicit DISTRIBUTED BY RANDOM.
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_ctas1 DISTRIBUTED BY RANDOM AS SELECT * FROM ${case_db}.t_src;

-- query 33
-- @result_contains=DISTRIBUTED BY RANDOM
SHOW CREATE TABLE ${case_db}.t_ctas1;

-- query 34
-- CTAS with explicit DISTRIBUTED BY RANDOM BUCKETS.
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_ctas2 DISTRIBUTED BY RANDOM BUCKETS 10 AS SELECT * FROM ${case_db}.t_src;

-- query 35
-- @result_contains=DISTRIBUTED BY RANDOM BUCKETS 10
SHOW CREATE TABLE ${case_db}.t_ctas2;

-- query 36
-- Join between RANDOM and HASH distributed tables (bucket shuffle path).
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_rand_join (k INT);
INSERT INTO ${case_db}.t_rand_join VALUES (1), (1);
CREATE TABLE ${case_db}.t_hash_join (k INT, v INT) DISTRIBUTED BY HASH(k);
INSERT INTO ${case_db}.t_hash_join VALUES (1,1),(1,2),(1,3);

-- query 37
SELECT * FROM ${case_db}.t_rand_join
  JOIN ${case_db}.t_hash_join ON ${case_db}.t_rand_join.k = ${case_db}.t_hash_join.k
  ORDER BY ${case_db}.t_hash_join.v;
