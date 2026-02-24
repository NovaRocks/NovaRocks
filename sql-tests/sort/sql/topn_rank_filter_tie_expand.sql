-- @order_sensitive=true
-- @tags=sort,topn,rank,tie
-- Test Objective:
-- 1. Validate FE rank-topn rewrite path with deterministic output.
-- 2. Cover rank-based topn behavior under duplicate order-by keys.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with duplicate scores.
-- 3. Compute RANK and filter with rk <= 2, then assert deterministic output order.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_topn_rank_filter_tie_expand;
CREATE TABLE sql_tests_d04.t_topn_rank_filter_tie_expand (
  id INT,
  score INT
);
INSERT INTO sql_tests_d04.t_topn_rank_filter_tie_expand VALUES
  (1, 100),
  (2, 95),
  (3, 95),
  (4, 90),
  (5, 80);
SELECT id, score, rk
FROM (
  SELECT
    id,
    score,
    RANK() OVER (ORDER BY score DESC) AS rk
  FROM sql_tests_d04.t_topn_rank_filter_tie_expand
) t
WHERE rk <= 2
ORDER BY rk ASC, id ASC;
