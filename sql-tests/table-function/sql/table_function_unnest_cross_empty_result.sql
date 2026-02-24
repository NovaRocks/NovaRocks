-- @order_sensitive=true
-- @tags=table_function,unnest,empty_result
-- Test Objective:
-- 1. Validate CROSS JOIN LATERAL UNNEST behavior for empty and NULL arrays.
-- 2. Prevent regressions in empty-result handling while preserving output schema header.
-- Test Flow:
-- 1. Build deterministic rows with per-row array expressions.
-- 2. Expand using CROSS JOIN LATERAL UNNEST.
-- 3. Assert stable empty output (no rows).
SELECT b.id, x
FROM (
    SELECT CAST(1 AS BIGINT) AS id
    UNION ALL
    SELECT CAST(2 AS BIGINT)
) b
CROSS JOIN LATERAL UNNEST(
    IF(b.id = 1, [1], CAST(NULL AS ARRAY<BIGINT>))
) AS u(x)
WHERE b.id = 2
ORDER BY b.id, x;
