-- @order_sensitive=true
-- @tags=join,left_semi
-- Test Objective:
-- 1. Validate LEFT SEMI JOIN existence semantics.
-- 2. Prevent regressions where semi-join emits non-matching left rows.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic rows including duplicate right keys.
-- 3. Execute LEFT SEMI JOIN and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d05;
DROP TABLE IF EXISTS sql_tests_d05.t_join_left_semi_l;
DROP TABLE IF EXISTS sql_tests_d05.t_join_left_semi_r;
CREATE TABLE sql_tests_d05.t_join_left_semi_l (
  id INT,
  lv STRING
);
CREATE TABLE sql_tests_d05.t_join_left_semi_r (
  id INT,
  rv STRING
);
INSERT INTO sql_tests_d05.t_join_left_semi_l VALUES
  (1, 'L1'),
  (2, 'L2'),
  (3, 'L3'),
  (4, 'L4');
INSERT INTO sql_tests_d05.t_join_left_semi_r VALUES
  (2, 'R2a'),
  (2, 'R2b'),
  (4, 'R4');
SELECT l.id, l.lv
FROM sql_tests_d05.t_join_left_semi_l l
LEFT SEMI JOIN sql_tests_d05.t_join_left_semi_r r
  ON l.id = r.id
ORDER BY l.id;
