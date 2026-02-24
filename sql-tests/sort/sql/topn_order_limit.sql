-- @order_sensitive=true
-- @tags=sort,limit,topn
-- Test Objective:
-- 1. Validate TopN behavior with ORDER BY + LIMIT.
-- 2. Prevent regressions in tie-break ordering for same score rows.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic score rows including ties.
-- 3. Query top-N rows with deterministic ordering.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_topn_order_limit;
CREATE TABLE sql_tests_d04.t_topn_order_limit (
  id INT,
  score INT
);
INSERT INTO sql_tests_d04.t_topn_order_limit VALUES
  (1, 70),
  (2, 95),
  (3, 88),
  (4, 95),
  (5, 60);
SELECT id, score
FROM sql_tests_d04.t_topn_order_limit
ORDER BY score DESC, id ASC
LIMIT 3;
