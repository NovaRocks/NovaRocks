-- @order_sensitive=true
-- @tags=filter,null
-- Test Objective:
-- 1. Validate IS NOT NULL combined with range predicates.
-- 2. Prevent regressions where nullable rows leak into bounded filters.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert rows with NULL and boundary numeric values.
-- 3. Filter with IS NOT NULL and range boundaries, then assert output.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_filter_is_not_null_and_range;
CREATE TABLE sql_tests_d04.t_filter_is_not_null_and_range (
  id INT,
  k INT,
  payload STRING
);
INSERT INTO sql_tests_d04.t_filter_is_not_null_and_range VALUES
  (1, NULL, 'n1'),
  (2, 5, 'p2'),
  (3, 10, 'p3'),
  (4, 11, 'p4'),
  (5, 20, 'p5');
SELECT id, k, payload
FROM sql_tests_d04.t_filter_is_not_null_and_range
WHERE k IS NOT NULL AND k >= 10 AND k < 20
ORDER BY id;
