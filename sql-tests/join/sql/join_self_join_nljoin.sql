-- @tags=join,self_join,nljoin
-- Test Objective:
-- Validate that nested-loop join self-joins (same table joining itself) produce
-- correct results for all NL-join types. Non-equality conditions force NL Join
-- path. Self-joins expose schema mapping bugs because both sides share identical
-- Arrow schemas, requiring slot ID disambiguation.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_self_join_nl;

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_self_join_nl (
  k1 BIGINT,
  v1 INT,
  v2 VARCHAR(20)
);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.t_self_join_nl VALUES
  (1, 10, 'a'),
  (2, 20, 'b'),
  (3, 30, 'c'),
  (NULL, 40, 'd');

-- query 4
-- CROSS JOIN self
SELECT count(*), count(a.v1), count(b.v1)
FROM ${case_db}.t_self_join_nl a
CROSS JOIN ${case_db}.t_self_join_nl b;

-- query 5
-- INNER JOIN self ON non-equality
SELECT count(*), count(a.v1), count(b.v1)
FROM ${case_db}.t_self_join_nl a
INNER JOIN ${case_db}.t_self_join_nl b
  ON a.k1 > b.k1;

-- query 6
-- LEFT OUTER JOIN self ON non-equality
SELECT count(*), count(a.v1), count(b.v1)
FROM ${case_db}.t_self_join_nl a
LEFT OUTER JOIN ${case_db}.t_self_join_nl b
  ON a.k1 > b.k1;

-- query 7
-- RIGHT OUTER JOIN self ON non-equality
SELECT count(*), count(a.v1), count(b.v1)
FROM ${case_db}.t_self_join_nl a
RIGHT OUTER JOIN ${case_db}.t_self_join_nl b
  ON a.k1 > b.k1;

-- query 8
-- FULL OUTER JOIN self ON non-equality
SELECT count(*), count(a.v1), count(b.v1)
FROM ${case_db}.t_self_join_nl a
FULL OUTER JOIN ${case_db}.t_self_join_nl b
  ON a.k1 > b.k1;

-- query 9
-- LEFT SEMI JOIN self ON non-equality
SELECT count(*), count(a.v1)
FROM ${case_db}.t_self_join_nl a
LEFT SEMI JOIN ${case_db}.t_self_join_nl b
  ON a.k1 > b.k1;

-- query 10
-- LEFT ANTI JOIN self ON non-equality
SELECT count(*), count(a.v1)
FROM ${case_db}.t_self_join_nl a
LEFT ANTI JOIN ${case_db}.t_self_join_nl b
  ON a.k1 > b.k1;
