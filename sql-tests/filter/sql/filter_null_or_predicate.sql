-- @order_sensitive=true
-- @tags=filter,null
-- Test Objective:
-- 1. Validate OR predicate behavior when one side depends on IS NULL checks.
-- 2. Prevent regressions in boolean short-circuit style evaluation for nullable columns.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert rows covering NULL and non-NULL combinations.
-- 3. Filter with OR predicate and assert deterministic ordering.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_filter_null_or_predicate;
CREATE TABLE sql_tests_d04.t_filter_null_or_predicate (
  id INT,
  a INT,
  b STRING
);
INSERT INTO sql_tests_d04.t_filter_null_or_predicate VALUES
  (1, 1, 'x'),
  (2, NULL, 'x'),
  (3, 2, 'y'),
  (4, NULL, NULL),
  (5, 5, 'z');
SELECT id, a, b
FROM sql_tests_d04.t_filter_null_or_predicate
WHERE a IS NULL OR b = 'y'
ORDER BY id;
