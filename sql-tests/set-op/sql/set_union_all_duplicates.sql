-- @order_sensitive=true
-- @tags=set_op,union_all
-- Test Objective:
-- 1. Validate UNION ALL duplicate-preserving semantics.
-- 2. Prevent regressions where UNION ALL incorrectly deduplicates rows.
-- Test Flow:
-- 1. Build deterministic scalar branches.
-- 2. Apply UNION ALL.
-- 3. Assert ordered output with duplicates preserved.
SELECT x
FROM (
    SELECT CAST(1 AS BIGINT) AS x
    UNION ALL
    SELECT CAST(2 AS BIGINT)
    UNION ALL
    SELECT CAST(2 AS BIGINT)
    UNION ALL
    SELECT CAST(3 AS BIGINT)
) t
ORDER BY x;
