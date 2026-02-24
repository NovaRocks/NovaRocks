-- @order_sensitive=true
-- @tags=sort
-- Test Objective:
-- 1. Validate ORDER BY on multiple keys with mixed directions.
-- 2. Prevent regressions in tie-breaking behavior for same primary sort key.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic duplicate-key rows.
-- 3. Sort by multiple keys and assert exact row order.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_sort_multi_key;
CREATE TABLE sql_tests_d04.t_sort_multi_key (
  grp STRING,
  v INT,
  id INT
);
INSERT INTO sql_tests_d04.t_sort_multi_key VALUES
  ('B', 2, 1),
  ('A', 3, 2),
  ('A', 1, 3),
  ('B', 1, 4),
  ('A', 1, 5);
SELECT grp, v, id
FROM sql_tests_d04.t_sort_multi_key
ORDER BY grp ASC, v ASC, id DESC;
