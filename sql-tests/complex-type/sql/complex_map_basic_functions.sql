-- @order_sensitive=true
-- @tags=complex,map
-- Test Objective:
-- 1. Validate MAP scalar access/size functions.
-- 2. Prevent regressions in MAP element lookup semantics.
-- Test Flow:
-- 1. Build constant MAP expressions.
-- 2. Apply size and element access functions.
-- 3. Assert scalar outputs.
SELECT
    MAP_SIZE(MAP('a', 1, 'b', 2)) AS map_size_v,
    CARDINALITY(MAP('a', 1, 'b', 2)) AS map_cardinality,
    ELEMENT_AT(MAP('a', 1, 'b', 2), 'b') AS value_b;
