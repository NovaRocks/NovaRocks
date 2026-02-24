-- @order_sensitive=true
-- @tags=set_op,chain
-- Test Objective:
-- 1. Validate chained set-operation semantics.
-- 2. Prevent regressions in operator composition ordering.
-- Test Flow:
-- 1. Build deterministic scalar branches.
-- 2. Compose UNION/EXCEPT/INTERSECT in one query.
-- 3. Assert ordered final output.
SELECT x
FROM (
    (
        (
            SELECT CAST(1 AS BIGINT) AS x
            UNION ALL
            SELECT CAST(2 AS BIGINT)
            UNION ALL
            SELECT CAST(3 AS BIGINT)
            UNION ALL
            SELECT CAST(4 AS BIGINT)
        )
        EXCEPT
        (
            SELECT CAST(2 AS BIGINT) AS x
        )
    )
    INTERSECT
    (
        SELECT CAST(1 AS BIGINT) AS x
        UNION ALL
        SELECT CAST(3 AS BIGINT)
        UNION ALL
        SELECT CAST(5 AS BIGINT)
    )
) t
ORDER BY x;
