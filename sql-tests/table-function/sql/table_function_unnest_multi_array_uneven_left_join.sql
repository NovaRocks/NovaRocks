-- @order_sensitive=true
-- @tags=table_function,unnest,multi_array,left_join
-- Test Objective:
-- 1. Validate LEFT JOIN LATERAL UNNEST zip behavior for uneven multi-array lengths.
-- 2. Prevent regressions in NULL-padding semantics for shorter array arguments.
-- Test Flow:
-- 1. Build deterministic outer rows.
-- 2. Derive per-row multi-array arguments with uneven lengths and NULL arrays.
-- 3. LEFT JOIN LATERAL UNNEST and assert ordered padded output.
SELECT b.id, x, y
FROM (
    SELECT CAST(1 AS BIGINT) AS id
    UNION ALL
    SELECT CAST(2 AS BIGINT)
    UNION ALL
    SELECT CAST(3 AS BIGINT)
) b
LEFT JOIN LATERAL UNNEST(
    IF(
        b.id = 1,
        [1, 2, 3],
        IF(b.id = 2, [4], CAST(NULL AS ARRAY<BIGINT>))
    ),
    IF(
        b.id = 1,
        ['a'],
        IF(b.id = 2, ['b', 'c'], ['z'])
    )
) AS u(x, y) ON TRUE
ORDER BY b.id, IFNULL(x, -1), y;
