-- @order_sensitive=true
-- @tags=set_op,union,null
-- Test Objective:
-- 1. Validate UNION distinct NULL dedup semantics.
-- 2. Prevent regressions in NULL equality behavior in set operations.
-- Test Flow:
-- 1. Build branches with NULL and non-NULL values.
-- 2. Apply UNION.
-- 3. Assert deterministic ordering with one NULL row.
SELECT x
FROM (
    SELECT CAST(NULL AS BIGINT) AS x
    UNION
    SELECT CAST(NULL AS BIGINT)
    UNION
    SELECT CAST(1 AS BIGINT)
) t
ORDER BY x IS NULL, x;
