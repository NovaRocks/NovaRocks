-- @order_sensitive=true
-- @tags=set_op,except,null,duplicates
-- Test Objective:
-- 1. Validate chained EXCEPT semantics with duplicated rows and NULL values.
-- 2. Prevent regressions in stage gating and deletion marker propagation for EXCEPT execution.
-- Test Flow:
-- 1. Build deterministic BIGINT branches containing duplicates and NULL.
-- 2. Apply EXCEPT in two stages.
-- 3. Assert ordered final output.
SELECT x
FROM (
    (
        (
            SELECT CAST(1 AS BIGINT) AS x
            UNION ALL
            SELECT CAST(1 AS BIGINT)
            UNION ALL
            SELECT CAST(2 AS BIGINT)
            UNION ALL
            SELECT CAST(NULL AS BIGINT)
            UNION ALL
            SELECT CAST(3 AS BIGINT)
            UNION ALL
            SELECT CAST(4 AS BIGINT)
        )
        EXCEPT
        (
            SELECT CAST(1 AS BIGINT) AS x
            UNION ALL
            SELECT CAST(2 AS BIGINT)
            UNION ALL
            SELECT CAST(NULL AS BIGINT)
        )
    )
    EXCEPT
    (
        SELECT CAST(4 AS BIGINT) AS x
        UNION ALL
        SELECT CAST(NULL AS BIGINT)
    )
) t
ORDER BY x;
