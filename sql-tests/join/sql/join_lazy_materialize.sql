-- @tags=join,lazy_materialize,broadcast,shuffle
-- Test Objective:
-- Validate join late materialization correctness for both non-nullable and nullable
-- columns across INNER, RIGHT OUTER, and LEFT OUTER joins with broadcast/shuffle hints.
-- Covers: full column output, partial column output, empty result from other predicate,
-- empty hash table (truncated build side), and nullable column propagation.
-- Test Flow:
-- 1. Set pipeline_dop=1 and join_late_materialization=true.
-- 2. Create non-nullable t1/t2 and nullable nullable_t1/nullable_t2.
-- 3. Test inner/right outer/left outer joins with various column projections.
-- 4. Truncate build table and verify left outer join with empty hash table.
-- 5. Repeat for nullable variants.

-- query 1
-- @skip_result_check=true
SET pipeline_dop = 1;

-- query 2
-- @skip_result_check=true
SET join_late_materialization = true;

-- query 3
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.lm_t1;

-- query 4
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.lm_t2;

-- query 5
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.lm_nullable_t1;

-- query 6
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.lm_nullable_t2;

-- query 7
-- @skip_result_check=true
CREATE TABLE ${case_db}.lm_t1 (
    t1_c1 INT NOT NULL,
    t1_c2 INT NOT NULL,
    t1_c3 INT NOT NULL,
    t1_c4 INT NOT NULL,
    t1_c5 INT NOT NULL
) DUPLICATE KEY(t1_c1)
DISTRIBUTED BY HASH(t1_c1) BUCKETS 1;

-- query 8
-- @skip_result_check=true
CREATE TABLE ${case_db}.lm_t2 (
    t2_c1 INT NOT NULL,
    t2_c2 INT NOT NULL,
    t2_c3 INT NOT NULL,
    t2_c4 INT NOT NULL,
    t2_c5 INT NOT NULL
) DUPLICATE KEY(t2_c1)
DISTRIBUTED BY HASH(t2_c1) BUCKETS 1;

-- query 9
-- @skip_result_check=true
CREATE TABLE ${case_db}.lm_nullable_t1 (
    t1_c1 INT,
    t1_c2 INT,
    t1_c3 INT,
    t1_c4 INT,
    t1_c5 INT
) DUPLICATE KEY(t1_c1)
DISTRIBUTED BY HASH(t1_c1) BUCKETS 1;

-- query 10
-- @skip_result_check=true
CREATE TABLE ${case_db}.lm_nullable_t2 (
    t2_c1 INT,
    t2_c2 INT,
    t2_c3 INT,
    t2_c4 INT,
    t2_c5 INT
) DUPLICATE KEY(t2_c1)
DISTRIBUTED BY HASH(t2_c1) BUCKETS 1;

-- query 11
-- @skip_result_check=true
INSERT INTO ${case_db}.lm_t1 VALUES
(1, 11, 111, 1111, 11111),
(2, 22, 222, 2222, 22222),
(3, 33, 333, 3333, 33333),
(4, 44, 444, 4444, 44444),
(5, 55, 555, 5555, 55555);

-- query 12
-- @skip_result_check=true
INSERT INTO ${case_db}.lm_t2 VALUES
(3, 44, 444, 444, 44444),
(4, 33, 333, 333, 33333),
(5, 55, 555, 5555, 55555),
(6, 77, 777, 7777, 77777),
(7, 66, 666, 6666, 66666);

-- query 13
-- @skip_result_check=true
INSERT INTO ${case_db}.lm_nullable_t1 VALUES
(1, 11, 111, 1111, 11111),
(2, 22, 222, 2222, 22222),
(3, 33, NULL, 3333, 33333),
(4, 44, 444, 4444, 44444),
(5, 55, 555, 5555, 55555);

-- query 14
-- @skip_result_check=true
INSERT INTO ${case_db}.lm_nullable_t2 VALUES
(3, 44, 444, 4444, 44444),
(4, 33, 333, 3333, 33333),
(5, 55, 555, 5555, 55555),
(6, 77, 777, NULL, 77777),
(7, 77, 666, 6666, 66666);

-- query 15
-- @order_sensitive=true
-- inner join, output all columns
SELECT * FROM ${case_db}.lm_t1 INNER JOIN [broadcast] ${case_db}.lm_t2
  ON t1_c1 = t2_c1 AND t1_c2 > t2_c2 ORDER BY t1_c2;

-- query 16
-- @order_sensitive=true
-- inner join, output partial columns
SELECT t1_c3, t1_c4, t2_c3, t2_c4 FROM ${case_db}.lm_t1 INNER JOIN [broadcast] ${case_db}.lm_t2
  ON t1_c1 = t2_c1 AND t1_c2 > t2_c2 ORDER BY t1_c2;

-- query 17
-- @order_sensitive=true
-- inner join, empty result from other predicate
SELECT t1_c3, t1_c4, t2_c3, t2_c4 FROM ${case_db}.lm_t1 INNER JOIN [broadcast] ${case_db}.lm_t2
  ON t1_c1 = t2_c1 AND t1_c2 > t2_c2 + 55 ORDER BY t1_c2;

-- query 18
-- @order_sensitive=true
-- right outer join, output partial columns
SELECT t1_c3, t1_c4, t2_c3, t2_c4 FROM ${case_db}.lm_t1 RIGHT OUTER JOIN [shuffle] ${case_db}.lm_t2
  ON t1_c1 = t2_c1 AND t1_c2 > t2_c2 ORDER BY t2_c2;

-- query 19
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.lm_t2;

-- query 20
-- @order_sensitive=true
-- left outer join with empty build table, output all columns
SELECT * FROM ${case_db}.lm_t1 LEFT OUTER JOIN [broadcast] ${case_db}.lm_t2
  ON t1_c1 = t2_c1 AND t1_c2 > t2_c2 ORDER BY t1_c2;

-- query 21
-- @order_sensitive=true
-- left outer join with empty build table, output partial columns
SELECT t1_c3, t1_c4, t2_c3, t2_c4 FROM ${case_db}.lm_t1 LEFT OUTER JOIN [broadcast] ${case_db}.lm_t2
  ON t1_c1 = t2_c1 AND t1_c2 > t2_c2 ORDER BY t1_c2;

-- query 22
-- @order_sensitive=true
-- left outer join with empty build table and other predicate offset
SELECT t1_c3, t1_c4, t2_c3, t2_c4 FROM ${case_db}.lm_t1 LEFT OUTER JOIN [broadcast] ${case_db}.lm_t2
  ON t1_c1 = t2_c1 AND t1_c2 > t2_c2 + 55 ORDER BY t1_c2;

-- query 23
-- @order_sensitive=true
-- nullable inner join, output all columns
SELECT * FROM ${case_db}.lm_nullable_t1 INNER JOIN [broadcast] ${case_db}.lm_nullable_t2
  ON t1_c1 = t2_c1 AND t1_c2 > t2_c2 ORDER BY t1_c2;

-- query 24
-- @order_sensitive=true
-- nullable inner join, output partial columns
SELECT t1_c3, t1_c4, t2_c3, t2_c4 FROM ${case_db}.lm_nullable_t1 INNER JOIN [broadcast] ${case_db}.lm_nullable_t2
  ON t1_c1 = t2_c1 AND t1_c2 > t2_c2 ORDER BY t1_c2;

-- query 25
-- @order_sensitive=true
-- nullable inner join, empty result from other predicate
SELECT t1_c3, t1_c4, t2_c3, t2_c4 FROM ${case_db}.lm_nullable_t1 INNER JOIN [broadcast] ${case_db}.lm_nullable_t2
  ON t1_c1 = t2_c1 AND t1_c2 > t2_c2 + 55 ORDER BY t1_c2;

-- query 26
-- @order_sensitive=true
-- nullable right outer join, output partial columns
SELECT t1_c3, t1_c4, t2_c3, t2_c4 FROM ${case_db}.lm_nullable_t1 RIGHT OUTER JOIN [shuffle] ${case_db}.lm_nullable_t2
  ON t1_c1 = t2_c1 AND t1_c2 > t2_c2 ORDER BY t2_c2;

-- query 27
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.lm_nullable_t2;

-- query 28
-- @order_sensitive=true
-- nullable left outer join with empty build table, output all columns
SELECT * FROM ${case_db}.lm_nullable_t1 LEFT OUTER JOIN [broadcast] ${case_db}.lm_nullable_t2
  ON t1_c1 = t2_c1 AND t1_c2 > t2_c2 ORDER BY t1_c2;

-- query 29
-- @order_sensitive=true
-- nullable left outer join with empty build table, output partial columns
SELECT t1_c3, t1_c4, t2_c3, t2_c4 FROM ${case_db}.lm_nullable_t1 LEFT OUTER JOIN [broadcast] ${case_db}.lm_nullable_t2
  ON t1_c1 = t2_c1 AND t1_c2 > t2_c2 ORDER BY t1_c2;

-- query 30
-- @order_sensitive=true
-- nullable left outer join with empty build table and other predicate offset
SELECT t1_c3, t1_c4, t2_c3, t2_c4 FROM ${case_db}.lm_nullable_t1 LEFT OUTER JOIN [broadcast] ${case_db}.lm_nullable_t2
  ON t1_c1 = t2_c1 AND t1_c2 > t2_c2 + 55 ORDER BY t1_c2;

-- query 31
-- @skip_result_check=true
SET pipeline_dop = 0;

-- query 32
-- @skip_result_check=true
SET join_late_materialization = false;
