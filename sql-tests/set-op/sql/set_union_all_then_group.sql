-- @order_sensitive=true
-- @tags=set_op,union_all,group
-- Test Objective:
-- 1. Validate downstream grouping over UNION ALL output.
-- 2. Prevent regressions where upstream multiplicity is lost before GROUP BY.
-- Test Flow:
-- 1. Build UNION ALL branch output with duplicates.
-- 2. Aggregate counts by value.
-- 3. Assert ordered grouped result.
SELECT
    x,
    COUNT(*) AS c
FROM (
    SELECT CAST(1 AS BIGINT) AS x
    UNION ALL
    SELECT CAST(1 AS BIGINT)
    UNION ALL
    SELECT CAST(2 AS BIGINT)
    UNION ALL
    SELECT CAST(2 AS BIGINT)
    UNION ALL
    SELECT CAST(2 AS BIGINT)
) t
GROUP BY x
ORDER BY x;
