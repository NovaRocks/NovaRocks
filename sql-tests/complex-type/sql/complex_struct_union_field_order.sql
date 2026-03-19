-- Test Objective:
-- 1. Validate that UNION ALL correctly handles STRUCT columns with same field names
--    but declared in different orders, using STRUCT_CAST_BY_NAME mode.
-- 2. Cover parse_json + CAST to STRUCT, named_struct construction, nested STRUCT,
--    and NULL values across different field orders.

-- query 1
-- @skip_result_check=true
set sql_mode='STRUCT_CAST_BY_NAME';

-- query 2
-- UNION ALL with different field order via parse_json + CAST
with source_1 as (
    select parse_json('{"product_id": "id_100", "price": 100}') as dimensions
),
source_2 as (
    select parse_json('{"product_id": "id_200", "price": 200}') as dimensions
),
source_3 as (
    select CAST(
        source_1.dimensions AS STRUCT<`product_id` VARCHAR, `price` INT>
    ) AS `product`
    from source_1
),
source_4 as (
    select CAST(
        source_2.dimensions AS STRUCT<`price` INT, `product_id` VARCHAR>
    ) AS `product`
    from source_2
)
select product.product_id, product.price from (
    select product from source_3
    union all
    select product from source_4
) t order by product.product_id;

-- query 3
-- Direct named_struct construction with different field order
with cte1 as (
    select named_struct('a', 1, 'b', 'hello') as s
),
cte2 as (
    select named_struct('b', 'world', 'a', 2) as s
)
select s.a, s.b from (
    select s from cte1
    union all
    select s from cte2
) t order by s.a;

-- query 4
-- Three-way UNION with mixed field orders
with cte1 as (
    select named_struct('x', 10, 'y', 20, 'z', 30) as s
),
cte2 as (
    select named_struct('z', 33, 'x', 11, 'y', 22) as s
),
cte3 as (
    select named_struct('y', 222, 'z', 333, 'x', 111) as s
)
select s.x, s.y, s.z from (
    select s from cte1
    union all
    select s from cte2
    union all
    select s from cte3
) t order by s.x;

-- query 5
-- Nested STRUCT with different field order
with cte1 as (
    select named_struct('outer_a', 1, 'inner', named_struct('inner_x', 100, 'inner_y', 200)) as s
),
cte2 as (
    select named_struct('inner', named_struct('inner_y', 400, 'inner_x', 300), 'outer_a', 2) as s
)
select s.outer_a, s.`inner`.inner_x, s.`inner`.inner_y from (
    select s from cte1
    union all
    select s from cte2
) t order by s.outer_a;

-- query 6
-- UNION ALL with NULL values and different field order
with cte1 as (
    select named_struct('a', 1, 'b', 'test') as s
),
cte2 as (
    select named_struct('b', null, 'a', 2) as s
)
select s.a, s.b from (
    select s from cte1
    union all
    select s from cte2
) t order by s.a;
