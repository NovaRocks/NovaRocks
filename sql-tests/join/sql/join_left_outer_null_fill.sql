-- @order_sensitive=true
-- @tags=join,left_outer
-- Test Objective:
-- 1. Validate LEFT OUTER JOIN null-fill semantics for unmatched right rows.
-- 2. Prevent regressions where unmatched left rows are dropped.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic rows with one unmatched left key.
-- 3. Execute LEFT OUTER JOIN and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d05;
DROP TABLE IF EXISTS sql_tests_d05.t_join_left_outer_l;
DROP TABLE IF EXISTS sql_tests_d05.t_join_left_outer_r;
CREATE TABLE sql_tests_d05.t_join_left_outer_l (
  id INT,
  lv STRING
);
CREATE TABLE sql_tests_d05.t_join_left_outer_r (
  id INT,
  rv STRING
);
INSERT INTO sql_tests_d05.t_join_left_outer_l VALUES
  (1, 'L1'),
  (2, 'L2'),
  (3, 'L3');
INSERT INTO sql_tests_d05.t_join_left_outer_r VALUES
  (2, 'R2'),
  (3, 'R3');
SELECT l.id, l.lv, r.rv
FROM sql_tests_d05.t_join_left_outer_l l
LEFT OUTER JOIN sql_tests_d05.t_join_left_outer_r r
  ON l.id = r.id
ORDER BY l.id;
