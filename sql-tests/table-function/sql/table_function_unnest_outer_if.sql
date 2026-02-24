-- @order_sensitive=true
-- @tags=table_function,unnest
-- Test Objective:
-- 1. Validate UNNEST with outer-column-dependent array expressions.
-- 2. Prevent regressions in correlated lateral table-function execution.
-- Test Flow:
-- 1. Build deterministic outer rows.
-- 2. Derive per-row arrays with IF expression.
-- 3. Expand with UNNEST and assert ordered output.
SELECT b.id, x
FROM (
    SELECT CAST(1 AS BIGINT) AS id
    UNION ALL
    SELECT CAST(2 AS BIGINT)
) b
CROSS JOIN LATERAL UNNEST(IF(b.id = 1, [10, 20], [30])) AS u(x)
ORDER BY b.id, x;
