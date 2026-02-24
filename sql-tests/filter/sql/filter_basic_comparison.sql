-- @order_sensitive=true
-- @tags=filter
-- Test Objective:
-- 1. Validate basic comparison filtering with nullable numeric columns.
-- 2. Prevent regressions where NULL rows are incorrectly included in range predicates.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with NULL and non-NULL values.
-- 3. Filter by numeric threshold and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_filter_basic_comparison;
CREATE TABLE sql_tests_d04.t_filter_basic_comparison (
  id INT,
  v INT,
  name STRING
);
INSERT INTO sql_tests_d04.t_filter_basic_comparison VALUES
  (1, 10, 'a'),
  (2, 20, 'b'),
  (3, NULL, 'c'),
  (4, 30, NULL);
SELECT id, v, name
FROM sql_tests_d04.t_filter_basic_comparison
WHERE v >= 20
ORDER BY id;
