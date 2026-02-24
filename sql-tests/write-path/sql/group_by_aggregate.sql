-- @order_sensitive=true
-- Test Objective:
-- 1. Validate GROUP BY aggregate semantics on BIGINT columns.
-- 2. Validate COUNT(*) vs COUNT(col) behavior with NULL values.
-- 3. Validate SUM aggregate on positive and negative values.
-- Test Flow:
-- 1. Create/reset an aggregate test table.
-- 2. Insert rows with NULL and signed numeric values.
-- 3. Execute grouped aggregates and compare deterministic ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_write_path;
DROP TABLE IF EXISTS sql_tests_write_path.t_metrics;
CREATE TABLE sql_tests_write_path.t_metrics (
  grp STRING,
  v BIGINT
);
INSERT INTO sql_tests_write_path.t_metrics VALUES
  ('A', 1),
  ('A', 2),
  ('A', NULL),
  ('B', 5),
  ('B', -1),
  ('B', NULL);
SELECT grp, COUNT(*) AS cnt_all, COUNT(v) AS cnt_v, SUM(v) AS sum_v
FROM sql_tests_write_path.t_metrics
GROUP BY grp
ORDER BY grp;
