-- @order_sensitive=true
-- @tags=set_op,intersect
-- Test Objective:
-- 1. Validate INTERSECT distinct semantics.
-- 2. Prevent regressions in common-row extraction.
-- Test Flow:
-- 1. Build two scalar sets with overlap.
-- 2. Apply INTERSECT.
-- 3. Assert ordered common rows.
SELECT x
FROM (
    (
        SELECT CAST(1 AS BIGINT) AS x
        UNION ALL
        SELECT CAST(2 AS BIGINT)
        UNION ALL
        SELECT CAST(3 AS BIGINT)
    )
    INTERSECT
    (
        SELECT CAST(2 AS BIGINT) AS x
        UNION ALL
        SELECT CAST(3 AS BIGINT)
        UNION ALL
        SELECT CAST(4 AS BIGINT)
    )
) t
ORDER BY x;
