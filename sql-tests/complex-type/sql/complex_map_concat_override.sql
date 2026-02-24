-- @order_sensitive=true
-- @tags=complex,map,map_concat
-- Test Objective:
-- 1. Validate MAP_CONCAT key-conflict override semantics.
-- 2. Prevent regressions where right-map values fail to override duplicate keys.
-- Test Flow:
-- 1. Build two deterministic MAP literals with overlapping keys.
-- 2. Concatenate maps.
-- 3. Assert element lookup and cardinality outputs.
SELECT
    ELEMENT_AT(
        MAP_CONCAT(MAP('a', 1, 'b', 2), MAP('b', 9, 'c', 3)),
        'a'
    ) AS a_v,
    ELEMENT_AT(
        MAP_CONCAT(MAP('a', 1, 'b', 2), MAP('b', 9, 'c', 3)),
        'b'
    ) AS b_v,
    ELEMENT_AT(
        MAP_CONCAT(MAP('a', 1, 'b', 2), MAP('b', 9, 'c', 3)),
        'c'
    ) AS c_v,
    MAP_SIZE(
        MAP_CONCAT(MAP('a', 1, 'b', 2), MAP('b', 9, 'c', 3))
    ) AS map_sz;
