-- @order_sensitive=true
-- @tags=complex,map
-- Test Objective:
-- 1. Validate MAP_KEYS/MAP_VALUES/MAP_ENTRIES outputs for string-key maps.
-- 2. Prevent regressions that panic on map_entries struct field nullability.
-- Test Flow:
-- 1. Build a constant MAP with string keys and integer values.
-- 2. Project MAP_KEYS, MAP_VALUES, and MAP_ENTRIES.
-- 3. Assert deterministic JSON-style outputs.
SELECT
    MAP_KEYS(MAP('a', 1, 'b', 2)) AS ks,
    MAP_VALUES(MAP('a', 1, 'b', 2)) AS vs,
    MAP_ENTRIES(MAP('a', 1, 'b', 2)) AS entries_v;
