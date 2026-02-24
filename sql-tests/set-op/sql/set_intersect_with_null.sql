-- @order_sensitive=true
-- @tags=set_op,intersect,null
-- Test Objective:
-- 1. Validate INTERSECT behavior when both sides contain NULL.
-- 2. Prevent regressions in NULL handling for INTERSECT.
-- Test Flow:
-- 1. Build two scalar sets including NULL.
-- 2. Apply INTERSECT.
-- 3. Assert deterministic ordered output.
SELECT x
FROM (
    (
        SELECT CAST(NULL AS BIGINT) AS x
        UNION ALL
        SELECT CAST(1 AS BIGINT)
    )
    INTERSECT
    (
        SELECT CAST(NULL AS BIGINT) AS x
        UNION ALL
        SELECT CAST(2 AS BIGINT)
    )
) t
ORDER BY x IS NULL, x;
