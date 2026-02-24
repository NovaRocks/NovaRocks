-- @order_sensitive=true
-- @tags=set_op,union
-- Test Objective:
-- 1. Validate UNION distinct dedup semantics.
-- 2. Prevent regressions where duplicate rows survive UNION.
-- Test Flow:
-- 1. Build deterministic scalar branches with duplicates.
-- 2. Apply UNION (distinct).
-- 3. Assert ordered deduplicated output.
SELECT x
FROM (
    SELECT CAST(1 AS BIGINT) AS x
    UNION
    SELECT CAST(2 AS BIGINT)
    UNION
    SELECT CAST(2 AS BIGINT)
    UNION
    SELECT CAST(3 AS BIGINT)
) t
ORDER BY x;
