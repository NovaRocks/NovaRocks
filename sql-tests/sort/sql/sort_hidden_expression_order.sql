-- @order_sensitive=true
-- @tags=sort,expression,projection
-- Test Objective:
-- 1. Validate ORDER BY on expressions that are not projected in output.
-- 2. Prevent regressions where hidden sort expressions are mis-evaluated with NULL handling.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows covering NULL and non-NULL operands.
-- 3. Sort by computed distance expression and assert ordered projected ids.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_sort_hidden_expression_order;
CREATE TABLE sql_tests_d04.t_sort_hidden_expression_order (
  id INT,
  base INT,
  delta INT
);
INSERT INTO sql_tests_d04.t_sort_hidden_expression_order VALUES
  (1, 10, 1),
  (2, 8, NULL),
  (3, 5, 5),
  (4, NULL, 2),
  (5, 7, 9);
SELECT id
FROM sql_tests_d04.t_sort_hidden_expression_order
ORDER BY ABS(COALESCE(base, 0) - COALESCE(delta, 0)) ASC, id DESC
LIMIT 5;
