-- @tags=join,self_join,hash_join
-- Test Objective:
-- Validate that hash-join self-joins (same table joining itself) produce correct
-- results for all join types. Self-joins expose schema mapping bugs because both
-- sides share identical Arrow schemas, requiring slot ID disambiguation.
-- This is a regression test for the schema_is_compatible removal.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_self_join_hash;

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_self_join_hash (
  k1 BIGINT,
  v1 INT,
  v2 VARCHAR(20)
);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.t_self_join_hash VALUES
  (1, 10, 'a'),
  (2, 20, 'b'),
  (3, 30, 'c'),
  (2, 40, 'd'),
  (NULL, 50, 'e');

-- query 4
-- INNER JOIN self + other conjuncts
SELECT count(*), count(a.v1), count(b.v1)
FROM ${case_db}.t_self_join_hash a
INNER JOIN ${case_db}.t_self_join_hash b
  ON a.k1 = b.k1
  AND (a.v1 + b.v1) % 3 = 0;

-- query 5
-- LEFT OUTER JOIN self
SELECT count(*), count(a.v1), count(b.v1)
FROM ${case_db}.t_self_join_hash a
LEFT OUTER JOIN ${case_db}.t_self_join_hash b
  ON a.k1 = b.k1;

-- query 6
-- RIGHT OUTER JOIN self
SELECT count(*), count(a.v1), count(b.v1)
FROM ${case_db}.t_self_join_hash a
RIGHT OUTER JOIN ${case_db}.t_self_join_hash b
  ON a.k1 = b.k1;

-- query 7
-- FULL OUTER JOIN self
SELECT count(*), count(a.v1), count(b.v1)
FROM ${case_db}.t_self_join_hash a
FULL OUTER JOIN ${case_db}.t_self_join_hash b
  ON a.k1 = b.k1;

-- query 8
-- LEFT SEMI JOIN self + other conjuncts
SELECT count(*), count(a.v1)
FROM ${case_db}.t_self_join_hash a
LEFT SEMI JOIN ${case_db}.t_self_join_hash b
  ON a.k1 = b.k1
  AND (a.v1 + b.v1) % 3 = 0;

-- query 9
-- LEFT ANTI JOIN self + other conjuncts
SELECT count(*), count(a.v1)
FROM ${case_db}.t_self_join_hash a
LEFT ANTI JOIN ${case_db}.t_self_join_hash b
  ON a.k1 = b.k1
  AND (a.v1 + b.v1) % 3 = 0;

-- query 10
-- RIGHT SEMI JOIN self + other conjuncts (original bug scenario)
SELECT count(*), count(b.v1)
FROM ${case_db}.t_self_join_hash a
RIGHT SEMI JOIN ${case_db}.t_self_join_hash b
  ON a.k1 = b.k1
  AND (a.v1 + b.v1) % 3 = 0;

-- query 11
-- RIGHT ANTI JOIN self + other conjuncts (original bug scenario)
SELECT count(*), count(b.v1)
FROM ${case_db}.t_self_join_hash a
RIGHT ANTI JOIN ${case_db}.t_self_join_hash b
  ON a.k1 = b.k1
  AND (a.v1 + b.v1) % 3 = 0;

-- query 12
-- NULL AWARE LEFT ANTI JOIN (NOT IN subquery)
SELECT count(*), count(a.v1)
FROM ${case_db}.t_self_join_hash a
WHERE a.k1 NOT IN (SELECT b.k1 FROM ${case_db}.t_self_join_hash b WHERE b.v1 > 25);
