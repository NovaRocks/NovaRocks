-- @order_sensitive=true
-- @tags=join,right_semi
-- Test Objective:
-- 1. Validate RIGHT SEMI JOIN existence semantics on right-side output rows.
-- 2. Prevent regressions where unmatched right rows leak into output.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic overlapping and non-overlapping keys.
-- 3. Execute RIGHT SEMI JOIN and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d05;
DROP TABLE IF EXISTS sql_tests_d05.t_join_right_semi_l;
DROP TABLE IF EXISTS sql_tests_d05.t_join_right_semi_r;
CREATE TABLE sql_tests_d05.t_join_right_semi_l (
  id INT,
  lv STRING
);
CREATE TABLE sql_tests_d05.t_join_right_semi_r (
  id INT,
  rv STRING
);
INSERT INTO sql_tests_d05.t_join_right_semi_l VALUES
  (2, 'L2'),
  (4, 'L4');
INSERT INTO sql_tests_d05.t_join_right_semi_r VALUES
  (1, 'R1'),
  (2, 'R2'),
  (3, 'R3'),
  (4, 'R4');
SELECT r.id, r.rv
FROM sql_tests_d05.t_join_right_semi_l l
RIGHT SEMI JOIN sql_tests_d05.t_join_right_semi_r r
  ON l.id = r.id
ORDER BY r.id;
