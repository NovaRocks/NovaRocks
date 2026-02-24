-- @order_sensitive=true
-- @tags=set_op,except,multi_stage,null
-- Test Objective:
-- 1. Validate chained EXCEPT semantics on multi-column rows with NULL values.
-- 2. Prevent regressions in stage-by-stage deletion logic for set operations.
-- Test Flow:
-- 1. Build baseline row set with mixed NULL/non-NULL rows.
-- 2. Apply two EXCEPT stages sequentially.
-- 3. Assert deterministic ordered output.
SELECT k, v
FROM (
    (
        (
            SELECT CAST(1 AS BIGINT) AS k, CAST('a' AS VARCHAR) AS v
            UNION ALL
            SELECT CAST(1 AS BIGINT), CAST('b' AS VARCHAR)
            UNION ALL
            SELECT CAST(2 AS BIGINT), CAST(NULL AS VARCHAR)
            UNION ALL
            SELECT CAST(NULL AS BIGINT), CAST('n' AS VARCHAR)
        )
        EXCEPT
        (
            SELECT CAST(1 AS BIGINT) AS k, CAST('b' AS VARCHAR) AS v
            UNION ALL
            SELECT CAST(3 AS BIGINT), CAST('x' AS VARCHAR)
        )
    )
    EXCEPT
    (
        SELECT CAST(NULL AS BIGINT) AS k, CAST('n' AS VARCHAR) AS v
    )
) t
ORDER BY k IS NULL, k, v IS NULL, v;
