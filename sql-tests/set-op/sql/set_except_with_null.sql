-- @order_sensitive=true
-- @tags=set_op,except,null
-- Test Objective:
-- 1. Validate EXCEPT behavior with NULL rows.
-- 2. Prevent regressions where NULL rows are mishandled in set subtraction.
-- Test Flow:
-- 1. Build left and right sets including NULL.
-- 2. Apply EXCEPT.
-- 3. Assert deterministic ordered output.
SELECT x
FROM (
    (
        SELECT CAST(NULL AS BIGINT) AS x
        UNION ALL
        SELECT CAST(1 AS BIGINT)
    )
    EXCEPT
    (
        SELECT CAST(NULL AS BIGINT) AS x
    )
) t
ORDER BY x IS NULL, x;
