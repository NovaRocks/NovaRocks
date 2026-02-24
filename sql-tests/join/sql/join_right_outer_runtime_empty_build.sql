-- @order_sensitive=true
-- @tags=join,right_outer,residual
-- Test Objective:
-- 1. Validate RIGHT OUTER JOIN keeps only right-side rows when right side is preserved.
-- 2. Prevent regression where probe-side rows leak when build side becomes empty at runtime.
-- Test Flow:
-- 1. Create/reset left and right tables with deterministic data.
-- 2. Build a right-side subquery filtered by a non-matching predicate to produce runtime-empty build input.
-- 3. Execute RIGHT OUTER JOIN and assert empty output.
CREATE DATABASE IF NOT EXISTS sql_tests_d05;
DROP TABLE IF EXISTS sql_tests_d05.t_join_right_outer_runtime_empty_build_l;
DROP TABLE IF EXISTS sql_tests_d05.t_join_right_outer_runtime_empty_build_r;
CREATE TABLE sql_tests_d05.t_join_right_outer_runtime_empty_build_l (
  id INT,
  lv STRING
);
CREATE TABLE sql_tests_d05.t_join_right_outer_runtime_empty_build_r (
  id INT,
  rv STRING
);
INSERT INTO sql_tests_d05.t_join_right_outer_runtime_empty_build_l VALUES
  (1, 'L1'),
  (2, 'L2');
INSERT INTO sql_tests_d05.t_join_right_outer_runtime_empty_build_r VALUES
  (1, 'R1'),
  (2, 'R2');
SELECT l.id AS lid, l.lv, r.id AS rid, r.rv
FROM sql_tests_d05.t_join_right_outer_runtime_empty_build_l l
RIGHT OUTER JOIN (
  SELECT id, rv
  FROM sql_tests_d05.t_join_right_outer_runtime_empty_build_r
  WHERE rv = 'NO_MATCH'
) r
  ON l.id = r.id
ORDER BY rid;
