-- @order_sensitive=true
-- @tags=set_op,except
-- Test Objective:
-- 1. Validate EXCEPT distinct semantics.
-- 2. Prevent regressions in left-minus-right row elimination.
-- Test Flow:
-- 1. Build left and right scalar sets.
-- 2. Apply EXCEPT.
-- 3. Assert ordered remaining rows.
SELECT x
FROM (
    (
        SELECT CAST(1 AS BIGINT) AS x
        UNION ALL
        SELECT CAST(2 AS BIGINT)
        UNION ALL
        SELECT CAST(3 AS BIGINT)
    )
    EXCEPT
    (
        SELECT CAST(2 AS BIGINT) AS x
    )
) t
ORDER BY x;
