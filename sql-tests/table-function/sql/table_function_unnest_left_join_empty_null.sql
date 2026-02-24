-- @order_sensitive=true
-- @tags=table_function,unnest,left_join
-- Test Objective:
-- 1. Validate LEFT JOIN LATERAL UNNEST for empty/NULL arrays.
-- 2. Prevent regressions where unmatched outer rows are dropped.
-- Test Flow:
-- 1. Build deterministic outer rows.
-- 2. Produce non-empty, empty, and NULL arrays per row.
-- 3. LEFT JOIN LATERAL UNNEST and assert ordered output.
SELECT b.id, x
FROM (
    SELECT CAST(1 AS BIGINT) AS id
    UNION ALL
    SELECT CAST(2 AS BIGINT)
    UNION ALL
    SELECT CAST(3 AS BIGINT)
) b
LEFT JOIN LATERAL UNNEST(
    IF(b.id = 1, [10, 20], IF(b.id = 2, [], CAST(NULL AS ARRAY<BIGINT>)))
) AS u(x) ON TRUE
ORDER BY b.id, x;
