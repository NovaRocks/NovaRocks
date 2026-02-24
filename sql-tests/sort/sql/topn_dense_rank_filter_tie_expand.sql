-- @order_sensitive=true
-- @tags=sort,topn,dense_rank,tie
-- Test Objective:
-- 1. Validate dense_rank filter semantics with deterministic output order.
-- 2. Cover duplicate-key peer-group behavior for dense_rank queries.
-- 3. Document current FE behavior: StarRocks FE does not rewrite this pattern
--    to `TOP-N type: DENSE_RANK` yet, so this case validates semantic fallback.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with duplicate scores in multiple peer groups.
-- 3. Compute DENSE_RANK and filter with drk <= 3, then assert deterministic output order.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_topn_dense_rank_filter_tie_expand;
CREATE TABLE sql_tests_d04.t_topn_dense_rank_filter_tie_expand (
  id INT,
  score INT
);
INSERT INTO sql_tests_d04.t_topn_dense_rank_filter_tie_expand VALUES
  (1, 100),
  (2, 95),
  (3, 95),
  (4, 90),
  (5, 90),
  (6, 80);
SELECT id, score, drk
FROM (
  SELECT
    id,
    score,
    DENSE_RANK() OVER (ORDER BY score DESC) AS drk
  FROM sql_tests_d04.t_topn_dense_rank_filter_tie_expand
) t
WHERE drk <= 3
ORDER BY drk ASC, id ASC;
