-- @order_sensitive=true
-- @tags=sort,topn,rank,partition,tie
-- Test Objective:
-- 1. Validate rank-based partition topn behavior with tie expansion at partition boundary.
-- 2. Cover FE partition-topn rewrite path for rank window filter predicates.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with duplicated scores in each partition.
-- 3. Compute RANK over partition and filter with rk <= 2, then assert stable output order.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_topn_rank_partition_filter_tie_expand;
CREATE TABLE sql_tests_d04.t_topn_rank_partition_filter_tie_expand (
  id INT,
  grp INT,
  score INT
);
INSERT INTO sql_tests_d04.t_topn_rank_partition_filter_tie_expand VALUES
  (1, 1, 100),
  (2, 1, 95),
  (3, 1, 95),
  (4, 1, 90),
  (5, 2, 88),
  (6, 2, 88),
  (7, 2, 70),
  (8, 2, 60);
SELECT id, grp, score, rk
FROM (
  SELECT
    id,
    grp,
    score,
    RANK() OVER (PARTITION BY grp ORDER BY score DESC) AS rk
  FROM sql_tests_d04.t_topn_rank_partition_filter_tie_expand
) t
WHERE rk <= 2
ORDER BY grp ASC, rk ASC, id ASC;
