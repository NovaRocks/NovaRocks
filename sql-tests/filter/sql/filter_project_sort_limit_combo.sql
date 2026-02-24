-- @order_sensitive=true
-- @tags=filter,project,sort,limit
-- Test Objective:
-- 1. Validate combined Filter->Project->Sort->Limit pipeline behavior.
-- 2. Prevent regressions in end-to-end row selection after expression projection.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with NULL and non-NULL metrics.
-- 3. Execute combined query and assert ordered top rows.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_filter_project_sort_limit_combo;
CREATE TABLE sql_tests_d04.t_filter_project_sort_limit_combo (
  id INT,
  name STRING,
  qty INT,
  price INT
);
INSERT INTO sql_tests_d04.t_filter_project_sort_limit_combo VALUES
  (1, 'apple', 2, 5),
  (2, 'banana', NULL, 3),
  (3, 'carrot', 7, 2),
  (4, 'apple', 6, 4),
  (5, 'durian', 10, 1);
SELECT
  name,
  qty * price AS revenue
FROM sql_tests_d04.t_filter_project_sort_limit_combo
WHERE qty IS NOT NULL AND qty >= 5
ORDER BY revenue DESC, name ASC
LIMIT 3;
