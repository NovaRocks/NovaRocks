-- @order_sensitive=true
-- @tags=analytic,min,max,cast,smallint
-- Test Objective:
-- 1. Validate MIN/MAX window output type follows FE SMALLINT return type.
-- 2. Prevent runtime schema mismatch on UNION-based inline inputs.
-- Test Flow:
-- 1. Build deterministic inline rows with CAST(... AS SMALLINT).
-- 2. Evaluate MIN/MAX over ROWS 1 PRECEDING..CURRENT ROW.
-- 3. Assert stable ordered outputs including NULL handling.
WITH t AS (
    SELECT 1 AS grp, 1 AS ord, CAST(10 AS SMALLINT) AS v
    UNION ALL
    SELECT 1, 2, CAST(20 AS SMALLINT)
    UNION ALL
    SELECT 1, 3, CAST(NULL AS SMALLINT)
    UNION ALL
    SELECT 1, 4, CAST(-5 AS SMALLINT)
)
SELECT
    grp,
    ord,
    v,
    MIN(v) OVER (
        PARTITION BY grp
        ORDER BY ord
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS mn,
    MAX(v) OVER (
        PARTITION BY grp
        ORDER BY ord
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS mx
FROM t
ORDER BY grp, ord;
