-- @tags=optimizer,window_ordering
-- Test Objective:
-- 1. Lock in physical ordering reuse for standalone window planning.
-- 2. A child Sort that already provides the required ORDER BY for a
--    non-partitioned window must not be wrapped in another equivalent Sort.
EXPLAIN VERBOSE
SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
FROM (
    SELECT id, v
    FROM (
        SELECT 2 AS id, 20 AS v
        UNION ALL SELECT 1 AS id, 10 AS v
        UNION ALL SELECT 3 AS id, 30 AS v
    ) t
    ORDER BY id
) s;
