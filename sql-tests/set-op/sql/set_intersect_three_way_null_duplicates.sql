-- @order_sensitive=true
-- @tags=set_op,intersect,null,duplicates
-- Test Objective:
-- 1. Validate INTERSECT semantics across three inputs with duplicated rows and NULL values.
-- 2. Prevent regressions in multi-stage INTERSECT marker progression after set-op refactor.
-- Test Flow:
-- 1. Build three deterministic BIGINT branches containing duplicates and NULL.
-- 2. Apply INTERSECT across three branches.
-- 3. Assert ordered final output.
SELECT x
FROM (
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
    )
    INTERSECT
    (
        SELECT CAST(1 AS BIGINT) AS x
        UNION ALL
        SELECT CAST(NULL AS BIGINT)
        UNION ALL
        SELECT CAST(2 AS BIGINT)
        UNION ALL
        SELECT CAST(2 AS BIGINT)
    )
    INTERSECT
    (
        SELECT CAST(1 AS BIGINT) AS x
        UNION ALL
        SELECT CAST(NULL AS BIGINT)
        UNION ALL
        SELECT CAST(4 AS BIGINT)
    )
) t
ORDER BY x IS NULL, x;
