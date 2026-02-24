-- @order_sensitive=true
-- @tags=join,multiway
-- Test Objective:
-- 1. Validate multi-join chain correctness across three tables.
-- 2. Prevent regressions in join key propagation between consecutive joins.
-- Test Flow:
-- 1. Create/reset three related tables.
-- 2. Insert deterministic keys for partial overlaps.
-- 3. Execute chained INNER JOINs and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d05;
DROP TABLE IF EXISTS sql_tests_d05.t_join_chain_a;
DROP TABLE IF EXISTS sql_tests_d05.t_join_chain_b;
DROP TABLE IF EXISTS sql_tests_d05.t_join_chain_c;
CREATE TABLE sql_tests_d05.t_join_chain_a (
  id INT,
  av STRING
);
CREATE TABLE sql_tests_d05.t_join_chain_b (
  id INT,
  mid INT
);
CREATE TABLE sql_tests_d05.t_join_chain_c (
  mid INT,
  cv STRING
);
INSERT INTO sql_tests_d05.t_join_chain_a VALUES
  (1, 'A1'),
  (2, 'A2'),
  (3, 'A3');
INSERT INTO sql_tests_d05.t_join_chain_b VALUES
  (1, 10),
  (2, 20),
  (4, 40);
INSERT INTO sql_tests_d05.t_join_chain_c VALUES
  (10, 'C10'),
  (20, 'C20'),
  (30, 'C30');
SELECT a.id, a.av, c.cv
FROM sql_tests_d05.t_join_chain_a a
INNER JOIN sql_tests_d05.t_join_chain_b b
  ON a.id = b.id
INNER JOIN sql_tests_d05.t_join_chain_c c
  ON b.mid = c.mid
ORDER BY a.id;
