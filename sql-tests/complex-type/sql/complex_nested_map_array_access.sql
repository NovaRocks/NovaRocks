-- @order_sensitive=true
-- @tags=complex,map,array
-- Test Objective:
-- 1. Validate nested MAP<key,ARRAY> access and downstream array aggregation.
-- 2. Prevent regressions in nested complex-type expression evaluation.
-- Test Flow:
-- 1. Build nested MAP and ARRAY expressions.
-- 2. Extract nested array by key.
-- 3. Aggregate extracted array and assert scalar outputs.
SELECT
    ELEMENT_AT(MAP('a', [1, 2, 3], 'b', [4]), 'a') AS arr_a,
    ARRAY_SUM(ELEMENT_AT(MAP('a', [1, 2, 3], 'b', [4]), 'a')) AS sum_a,
    ARRAY_CONCAT(ELEMENT_AT(MAP('x', [1, 2]), 'x'), [3]) AS concat_arr;
