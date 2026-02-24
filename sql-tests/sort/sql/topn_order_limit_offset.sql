-- @order_sensitive=true
-- @tags=sort,limit,offset
-- Test Objective:
-- 1. Validate LIMIT with OFFSET under deterministic ordering.
-- 2. Prevent regressions in page slicing for ordered result sets.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with descending sort semantics.
-- 3. Query with ORDER BY + LIMIT + OFFSET and assert output.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_topn_order_limit_offset;
CREATE TABLE sql_tests_d04.t_topn_order_limit_offset (
  id INT,
  val INT
);
INSERT INTO sql_tests_d04.t_topn_order_limit_offset VALUES
  (1, 100),
  (2, 90),
  (3, 90),
  (4, 80),
  (5, 70);
SELECT id, val
FROM sql_tests_d04.t_topn_order_limit_offset
ORDER BY val DESC, id ASC
LIMIT 2 OFFSET 2;
