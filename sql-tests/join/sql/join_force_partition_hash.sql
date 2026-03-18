-- @tags=join,partition_hash,null_aware_anti
-- Test Objective:
-- Validate partition hash join correctness across INNER, LEFT, RIGHT joins and
-- null-aware anti join (NOT IN) semantics under the always_use_partition_join failpoint.
-- Covers: multi-column equality join, NOT IN with and without NULLs, IN subquery,
-- correlated NOT IN, NOT EXISTS, and null-aware anti join with correlated predicates.
-- Test Flow:
-- 1. Create t0 (60961 rows) and t1 (40961 rows) with overlapping key ranges.
-- 2. Enable partition join failpoint and verify INNER/LEFT/RIGHT join counts.
-- 3. Verify NOT IN returns correct count before and after inserting NULL row.
-- 4. Create nullaware_anti_join_test with NULLs and verify IN/NOT IN/NOT EXISTS.
-- 5. Create t2 (1 NULL row) and t3 (409600 rows) for correlated NOT IN with IF().

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.fpj_t0;

-- query 2
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.fpj_t1;

-- query 3
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.fpj_nullaware;

-- query 4
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.fpj_t2;

-- query 5
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.fpj_t3;

-- query 6
-- @skip_result_check=true
CREATE TABLE ${case_db}.fpj_t0 (
  c0 INT NULL,
  c1 VARCHAR(20) NULL,
  c2 VARCHAR(200) NULL,
  c3 INT NULL
) ENGINE=OLAP
DUPLICATE KEY(c0, c1)
DISTRIBUTED BY HASH(c0) BUCKETS 4
PROPERTIES ("replication_num" = "1", "compression" = "LZ4");

-- query 7
-- @skip_result_check=true
CREATE TABLE ${case_db}.fpj_t1 (
  c0 INT NULL,
  c1 VARCHAR(20) NULL,
  c2 VARCHAR(200) NULL,
  c3 INT NULL
) ENGINE=OLAP
DUPLICATE KEY(c0, c1)
DISTRIBUTED BY HASH(c0) BUCKETS 4
PROPERTIES ("replication_num" = "1", "compression" = "LZ4");

-- query 8
-- @skip_result_check=true
INSERT INTO ${case_db}.fpj_t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1, 40960));

-- query 9
-- @skip_result_check=true
INSERT INTO ${case_db}.fpj_t1 SELECT * FROM ${case_db}.fpj_t0;

-- query 10
-- @skip_result_check=true
INSERT INTO ${case_db}.fpj_t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(40960, 40960 + 20000));

-- query 11
-- INNER JOIN with multi-column equality
SELECT count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1)
FROM ${case_db}.fpj_t0 l JOIN ${case_db}.fpj_t1 r ON l.c0 = r.c0 AND l.c1 = r.c1;

-- query 12
-- LEFT JOIN with multi-column equality
SELECT count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1)
FROM ${case_db}.fpj_t0 l LEFT JOIN ${case_db}.fpj_t1 r ON l.c0 = r.c0 AND l.c1 = r.c1;

-- query 13
-- RIGHT JOIN with multi-column equality
SELECT count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1)
FROM ${case_db}.fpj_t0 l RIGHT JOIN ${case_db}.fpj_t1 r ON l.c0 = r.c0 AND l.c1 = r.c1;

-- query 14
-- NOT IN without NULLs in subquery
SELECT count(*) FROM (SELECT * FROM ${case_db}.fpj_t0 WHERE c0 NOT IN (SELECT c0 FROM ${case_db}.fpj_t1)) t;

-- query 15
-- NOT IN with correlated predicate
SELECT count(*) FROM (SELECT * FROM ${case_db}.fpj_t0 WHERE c0 NOT IN (SELECT c0 FROM ${case_db}.fpj_t1 WHERE ${case_db}.fpj_t0.c1)) t;

-- query 16
-- @skip_result_check=true
INSERT INTO ${case_db}.fpj_t1 VALUES (NULL, NULL, NULL, NULL);

-- query 17
-- NOT IN after inserting NULL into subquery set (should return 0)
SELECT count(*) FROM (SELECT * FROM ${case_db}.fpj_t0 WHERE c0 NOT IN (SELECT c0 FROM ${case_db}.fpj_t1)) t;

-- query 18
-- @skip_result_check=true
CREATE TABLE ${case_db}.fpj_nullaware (
  k1 INT,
  k2 INT,
  k3 INT
) ENGINE=OLAP
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 19
-- @skip_result_check=true
INSERT INTO ${case_db}.fpj_nullaware VALUES (1,1,1),(1,2,1),(1,3,2),(11,1,11),(11,2,1),(2,3,2),(2,3,NULL),(NULL,NULL,NULL);

-- query 20
-- @order_sensitive=true
-- NOT IN with NULLs present (expect empty result)
SELECT * FROM ${case_db}.fpj_nullaware l1 WHERE l1.k1 NOT IN (SELECT l3.k1 FROM ${case_db}.fpj_nullaware l3) ORDER BY 1,2,3;

-- query 21
-- @order_sensitive=true
-- IN subquery with NULLs
SELECT * FROM ${case_db}.fpj_nullaware l1 WHERE l1.k1 IN (SELECT l3.k1 FROM ${case_db}.fpj_nullaware l3) ORDER BY 1,2,3;

-- query 22
-- @order_sensitive=true
-- NOT IN with correlated equality predicate
SELECT * FROM ${case_db}.fpj_nullaware l1 WHERE l1.k1 NOT IN (SELECT l3.k1 FROM ${case_db}.fpj_nullaware l3 WHERE l3.k3 = l1.k3) ORDER BY 1,2,3;

-- query 23
-- @order_sensitive=true
-- NOT IN with correlated inequality predicate
SELECT * FROM ${case_db}.fpj_nullaware l1 WHERE l1.k1 NOT IN (SELECT l3.k1 FROM ${case_db}.fpj_nullaware l3 WHERE l3.k3 != l1.k3) ORDER BY 1,2,3;

-- query 24
-- @order_sensitive=true
-- NOT EXISTS with correlated predicates
SELECT * FROM ${case_db}.fpj_nullaware l1 WHERE NOT EXISTS (SELECT 1 FROM ${case_db}.fpj_nullaware l3 WHERE l3.k1 = l1.k1 AND l3.k3 != l1.k3) ORDER BY 1,2,3;

-- query 25
-- @skip_result_check=true
CREATE TABLE ${case_db}.fpj_t2 (
  c0 INT NULL,
  c1 VARCHAR(20) NULL,
  c2 VARCHAR(200) NULL,
  c3 INT NULL
) ENGINE=OLAP
DUPLICATE KEY(c0, c1)
DISTRIBUTED BY HASH(c0) BUCKETS 4
PROPERTIES ("replication_num" = "1", "compression" = "LZ4");

-- query 26
-- @skip_result_check=true
CREATE TABLE ${case_db}.fpj_t3 (
  c0 INT NULL,
  c1 VARCHAR(20) NULL,
  c2 VARCHAR(200) NULL,
  c3 INT NULL
) ENGINE=OLAP
DUPLICATE KEY(c0, c1)
DISTRIBUTED BY HASH(c0) BUCKETS 4
PROPERTIES ("replication_num" = "1", "compression" = "LZ4");

-- query 27
-- @skip_result_check=true
INSERT INTO ${case_db}.fpj_t2 VALUES (NULL, NULL, NULL, NULL);

-- query 28
-- @skip_result_check=true
INSERT INTO ${case_db}.fpj_t3 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1, 409600));

-- query 29
-- NOT IN with correlated IF predicate (null row, expect 0)
SELECT count(*) FROM (SELECT * FROM ${case_db}.fpj_t2 WHERE c0 NOT IN (SELECT c0 FROM ${case_db}.fpj_t3 WHERE if(${case_db}.fpj_t3.c0=1, ${case_db}.fpj_t2.c1, true))) t;

-- query 30
-- NOT IN with correlated IF and IS NOT NULL predicate (null row, expect 0)
SELECT count(*) FROM (SELECT * FROM ${case_db}.fpj_t2 WHERE c0 NOT IN (SELECT c0 FROM ${case_db}.fpj_t3 WHERE if(${case_db}.fpj_t3.c0!=2, ${case_db}.fpj_t2.c0 IS NOT NULL, true))) t;
