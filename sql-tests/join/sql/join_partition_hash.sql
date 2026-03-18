-- @tags=join,partition_hash,broadcast
-- Test Objective:
-- Validate partition hash join correctness with broadcast hint on large self-joins.
-- Covers: count aggregation through broadcast join with USING clause, modulus
-- filters on probe side, and both probe-side and build-side column references.
-- Test Flow:
-- 1. Create a row-generator utility table to produce 5,120,000 unique rows.
-- 2. Create a main table with bigint key and string column, self-join via broadcast.
-- 3. Assert count correctness with various modulus filters on k1.
-- 4. Assert count correctness referencing c_string from both probe and build sides.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.phj_row_util_base;

-- query 2
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.phj_row_util;

-- query 3
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.phj_t1;

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.phj_row_util_base (
  k1 BIGINT NULL
) ENGINE=OLAP
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 32
PROPERTIES ("replication_num" = "1");

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_row_util_base SELECT generate_series FROM TABLE(generate_series(0, 10000 - 1));

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_row_util_base SELECT * FROM ${case_db}.phj_row_util_base;

-- query 7
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_row_util_base SELECT * FROM ${case_db}.phj_row_util_base;

-- query 8
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_row_util_base SELECT * FROM ${case_db}.phj_row_util_base;

-- query 9
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_row_util_base SELECT * FROM ${case_db}.phj_row_util_base;

-- query 10
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_row_util_base SELECT * FROM ${case_db}.phj_row_util_base;

-- query 11
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_row_util_base SELECT * FROM ${case_db}.phj_row_util_base;

-- query 12
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_row_util_base SELECT * FROM ${case_db}.phj_row_util_base;

-- query 13
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_row_util_base SELECT * FROM ${case_db}.phj_row_util_base;

-- query 14
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_row_util_base SELECT * FROM ${case_db}.phj_row_util_base;

-- query 15
-- @skip_result_check=true
CREATE TABLE ${case_db}.phj_row_util (
  idx BIGINT NULL
) ENGINE=OLAP
DUPLICATE KEY(idx)
DISTRIBUTED BY HASH(idx) BUCKETS 32
PROPERTIES ("replication_num" = "1");

-- query 16
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_row_util SELECT row_number() OVER() AS idx FROM ${case_db}.phj_row_util_base;

-- query 17
-- @skip_result_check=true
CREATE TABLE ${case_db}.phj_t1 (
    k1 BIGINT NULL,
    c_bigint_null BIGINT NULL,
    c_string STRING
) ENGINE=OLAP
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 96
PROPERTIES ("replication_num" = "1");

-- query 18
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_t1
SELECT idx, idx, substr(uuid(), 1, 6)
FROM ${case_db}.phj_row_util;

-- query 19
-- count(k1) from probe side, no filter
SELECT count(tt1.k1) FROM ${case_db}.phj_t1 tt1 JOIN [broadcast] ${case_db}.phj_t1 tt2 USING(c_bigint_null);

-- query 20
-- count(k1) with mod 2 filter
SELECT count(tt1.k1) FROM ${case_db}.phj_t1 tt1 JOIN [broadcast] ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 2 = 0;

-- query 21
-- count(k1) with mod 3 filter
SELECT count(tt1.k1) FROM ${case_db}.phj_t1 tt1 JOIN [broadcast] ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 3 = 0;

-- query 22
-- count(k1) with mod 5 filter
SELECT count(tt1.k1) FROM ${case_db}.phj_t1 tt1 JOIN [broadcast] ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 5 = 0;

-- query 23
-- count(k1) with mod 10 filter
SELECT count(tt1.k1) FROM ${case_db}.phj_t1 tt1 JOIN [broadcast] ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 10 = 0;

-- query 24
-- count(k1) with mod 100 filter
SELECT count(tt1.k1) FROM ${case_db}.phj_t1 tt1 JOIN [broadcast] ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 100 = 0;

-- query 25
-- count(c_string) from probe side, no filter
SELECT count(tt1.c_string) FROM ${case_db}.phj_t1 tt1 JOIN [broadcast] ${case_db}.phj_t1 tt2 USING(c_bigint_null);

-- query 26
-- count(c_string) from probe side with mod 2 filter
SELECT count(tt1.c_string) FROM ${case_db}.phj_t1 tt1 JOIN [broadcast] ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 2 = 0;

-- query 27
-- count(c_string) from probe side with mod 10 filter
SELECT count(tt1.c_string) FROM ${case_db}.phj_t1 tt1 JOIN [broadcast] ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 10 = 0;

-- query 28
-- count(c_string) from probe side with mod 100 filter
SELECT count(tt1.c_string) FROM ${case_db}.phj_t1 tt1 JOIN [broadcast] ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 100 = 0;

-- query 29
-- count(c_string) from build side, no filter
SELECT count(tt2.c_string) FROM ${case_db}.phj_t1 tt1 JOIN [broadcast] ${case_db}.phj_t1 tt2 USING(c_bigint_null);

-- query 30
-- count(c_string) from build side with mod 2 filter
SELECT count(tt2.c_string) FROM ${case_db}.phj_t1 tt1 JOIN [broadcast] ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 2 = 0;

-- query 31
-- count(c_string) from build side with mod 10 filter
SELECT count(tt2.c_string) FROM ${case_db}.phj_t1 tt1 JOIN [broadcast] ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 10 = 0;

-- query 32
-- count(c_string) from build side with mod 100 filter
SELECT count(tt2.c_string) FROM ${case_db}.phj_t1 tt1 JOIN [broadcast] ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 100 = 0;
