-- @order_sensitive=true
-- @tags=complex,map,unnest
-- Test Objective:
-- 1. Validate MAP_KEYS/MAP_VALUES feeding UNNEST.
-- 2. Prevent regressions in zipped expansion of key/value arrays.
-- Test Flow:
-- 1. Build key/value arrays from MAP.
-- 2. Expand rows via CROSS JOIN LATERAL UNNEST.
-- 3. Assert ordered key/value rows.
SELECT k, v
FROM (
    SELECT
        MAP_KEYS(MAP('a', 1, 'b', 2)) AS ks,
        MAP_VALUES(MAP('a', 1, 'b', 2)) AS vs
) t
CROSS JOIN LATERAL UNNEST(t.ks, t.vs) AS u(k, v)
ORDER BY k;
