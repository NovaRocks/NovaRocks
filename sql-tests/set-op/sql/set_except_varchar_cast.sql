-- @order_sensitive=true
-- @tags=set_op,except,varchar
-- Test Objective:
-- 1. Validate set operations over explicit VARCHAR casts.
-- 2. Prevent regressions in string-typed set comparisons.
-- Test Flow:
-- 1. Build VARCHAR scalar sets.
-- 2. Apply EXCEPT.
-- 3. Assert ordered string output.
SELECT s
FROM (
    (
        SELECT CAST('a' AS VARCHAR) AS s
        UNION ALL
        SELECT CAST('b' AS VARCHAR)
        UNION ALL
        SELECT CAST('c' AS VARCHAR)
    )
    EXCEPT
    (
        SELECT CAST('b' AS VARCHAR) AS s
    )
) t
ORDER BY s;
