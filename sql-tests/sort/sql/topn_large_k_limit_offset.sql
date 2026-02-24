-- @order_sensitive=true
-- @tags=sort,topn,limit,offset,large_k
-- Test Objective:
-- 1. Validate ORDER BY + LIMIT/OFFSET semantics when requested top-k is much larger than heap threshold.
-- 2. Prevent regressions in large-k page slicing behavior.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with score ties.
-- 3. Query ORDER BY + LIMIT/OFFSET where limit+offset is large enough to hit large-k topn path.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_topn_large_k_limit_offset;
CREATE TABLE sql_tests_d04.t_topn_large_k_limit_offset (
  id INT,
  score INT
);
INSERT INTO sql_tests_d04.t_topn_large_k_limit_offset VALUES
  (1, 100),
  (2, 95),
  (3, 95),
  (4, 90),
  (5, 85),
  (6, 85),
  (7, 80),
  (8, 70);
SELECT id, score
FROM sql_tests_d04.t_topn_large_k_limit_offset
ORDER BY score DESC, id ASC
LIMIT 1500 OFFSET 2;
