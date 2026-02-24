-- @order_sensitive=true
-- @tags=table_function,unnest,multi_array
-- Test Objective:
-- 1. Validate multi-argument UNNEST zip semantics.
-- 2. Prevent regressions in positional alignment across arrays.
-- Test Flow:
-- 1. Build two same-length arrays.
-- 2. Expand both via LATERAL UNNEST.
-- 3. Assert ordered zipped rows.
SELECT i, s
FROM (
    SELECT [1, 2, 3] AS ai, ['x', 'y', 'z'] AS as1
) t
CROSS JOIN LATERAL UNNEST(t.ai, t.as1) AS u(i, s)
ORDER BY i;
