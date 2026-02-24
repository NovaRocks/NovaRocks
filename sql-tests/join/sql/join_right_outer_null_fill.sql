-- @order_sensitive=true
-- @tags=join,right_outer
-- Test Objective:
-- 1. Validate RIGHT OUTER JOIN null-fill semantics for unmatched left rows.
-- 2. Prevent regressions where unmatched right rows are dropped.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic rows with one unmatched right key.
-- 3. Execute RIGHT OUTER JOIN and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d05;
DROP TABLE IF EXISTS sql_tests_d05.t_join_right_outer_l;
DROP TABLE IF EXISTS sql_tests_d05.t_join_right_outer_r;
CREATE TABLE sql_tests_d05.t_join_right_outer_l (
  id INT,
  lv STRING
);
CREATE TABLE sql_tests_d05.t_join_right_outer_r (
  id INT,
  rv STRING
);
INSERT INTO sql_tests_d05.t_join_right_outer_l VALUES
  (2, 'L2'),
  (3, 'L3');
INSERT INTO sql_tests_d05.t_join_right_outer_r VALUES
  (2, 'R2'),
  (3, 'R3'),
  (4, 'R4');
SELECT l.id AS lid, l.lv, r.id AS rid, r.rv
FROM sql_tests_d05.t_join_right_outer_l l
RIGHT OUTER JOIN sql_tests_d05.t_join_right_outer_r r
  ON l.id = r.id
ORDER BY rid;
