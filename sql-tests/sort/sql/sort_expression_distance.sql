-- @order_sensitive=true
-- @tags=sort,expression
-- Test Objective:
-- 1. Validate ORDER BY computed expression outputs.
-- 2. Prevent regressions in expression materialization before sorting.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic numeric rows.
-- 3. Sort by ABS distance expression and assert output order.
DROP TABLE IF EXISTS ${case_db}.t_sort_expression_distance;
CREATE TABLE ${case_db}.t_sort_expression_distance (
  id INT,
  v INT
);
INSERT INTO ${case_db}.t_sort_expression_distance VALUES
  (1, 7),
  (2, 12),
  (3, 9),
  (4, 15);
SELECT id, v, ABS(v - 10) AS dist
FROM ${case_db}.t_sort_expression_distance
ORDER BY dist ASC, id ASC;
