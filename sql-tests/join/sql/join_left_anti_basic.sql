-- @order_sensitive=true
-- @tags=join,left_anti
-- Test Objective:
-- 1. Validate LEFT ANTI JOIN non-existence semantics.
-- 2. Prevent regressions where matching left rows leak into anti-join output.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic overlapping and non-overlapping keys.
-- 3. Execute LEFT ANTI JOIN and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d05;
DROP TABLE IF EXISTS sql_tests_d05.t_join_left_anti_l;
DROP TABLE IF EXISTS sql_tests_d05.t_join_left_anti_r;
CREATE TABLE sql_tests_d05.t_join_left_anti_l (
  id INT,
  lv STRING
);
CREATE TABLE sql_tests_d05.t_join_left_anti_r (
  id INT,
  rv STRING
);
INSERT INTO sql_tests_d05.t_join_left_anti_l VALUES
  (1, 'L1'),
  (2, 'L2'),
  (3, 'L3'),
  (4, 'L4');
INSERT INTO sql_tests_d05.t_join_left_anti_r VALUES
  (2, 'R2'),
  (4, 'R4');
SELECT l.id, l.lv
FROM sql_tests_d05.t_join_left_anti_l l
LEFT ANTI JOIN sql_tests_d05.t_join_left_anti_r r
  ON l.id = r.id
ORDER BY l.id;
