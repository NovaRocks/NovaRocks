-- @order_sensitive=true
-- @tags=join,anti,not_in
-- Test Objective:
-- 1. Validate NOT IN semantics when subquery set has no NULLs.
-- 2. Prevent regressions in anti-set filtering for scalar keys.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic values without NULL on subquery side.
-- 3. Query with NOT IN and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d05;
DROP TABLE IF EXISTS sql_tests_d05.t_join_not_in_no_null_l;
DROP TABLE IF EXISTS sql_tests_d05.t_join_not_in_no_null_r;
CREATE TABLE sql_tests_d05.t_join_not_in_no_null_l (
  id INT
);
CREATE TABLE sql_tests_d05.t_join_not_in_no_null_r (
  id INT
);
INSERT INTO sql_tests_d05.t_join_not_in_no_null_l VALUES
  (1),
  (2),
  (3),
  (NULL);
INSERT INTO sql_tests_d05.t_join_not_in_no_null_r VALUES
  (2);
SELECT id
FROM sql_tests_d05.t_join_not_in_no_null_l
WHERE id NOT IN (
  SELECT id FROM sql_tests_d05.t_join_not_in_no_null_r
)
ORDER BY id;
