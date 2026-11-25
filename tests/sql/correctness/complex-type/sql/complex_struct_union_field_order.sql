-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Test Objective:
-- 1. Validate that UNION ALL correctly handles STRUCT columns with same field names
--    but declared in different orders, using STRUCT_CAST_BY_NAME mode.
-- 2. Cover parse_json + CAST to STRUCT, named_struct construction, nested STRUCT,
--    and NULL values across different field orders.

-- query 1
-- @skip_result_check=true
SET SESSION sql_mode = 'STRUCT_CAST_BY_NAME';

-- query 2
-- UNION ALL with different field order via parse_json + CAST
with source_1 as (
    select json_parse('{"product_id": "id_100", "price": 100}') as dimensions
),
source_2 as (
    select json_parse('{"product_id": "id_200", "price": 200}') as dimensions
),
source_3 as (
    select CAST(
        source_1.dimensions AS ROW("product_id" VARCHAR, "price" INT)
    ) AS "product"
    from source_1
),
source_4 as (
    select CAST(
        source_2.dimensions AS ROW("price" INT, "product_id" VARCHAR)
    ) AS "product"
    from source_2
)
select product.product_id, product.price from (
    select product from source_3
    union all
    select product from source_4
) t order by product.product_id;

-- query 3
-- Normalize field order explicitly at the UNION input.
with cte1 as (
    select CAST(ROW(1, 'hello') AS ROW(a INTEGER, b VARCHAR)) as s
),
cte2 as (
    select CAST(ROW('world', 2) AS ROW(b VARCHAR, a INTEGER)) as s
)
select s.a, s.b from (
    select s from cte1
    union all
    select CAST(ROW(s.a,s.b) AS ROW(a INTEGER,b VARCHAR)) AS s from cte2
) t order by s.a;

-- query 4
-- Three-way UNION with mixed field orders
with cte1 as (
    select CAST(ROW(10,20,30) AS ROW(x INTEGER,y INTEGER,z INTEGER)) as s
),
cte2 as (
    select CAST(ROW(33,11,22) AS ROW(z INTEGER,x INTEGER,y INTEGER)) as s
),
cte3 as (
    select CAST(ROW(222,333,111) AS ROW(y INTEGER,z INTEGER,x INTEGER)) as s
)
select s.x, s.y, s.z from (
    select s from cte1
    union all
    select CAST(ROW(s.x,s.y,s.z) AS ROW(x INTEGER,y INTEGER,z INTEGER)) AS s from cte2
    union all
    select CAST(ROW(s.x,s.y,s.z) AS ROW(x INTEGER,y INTEGER,z INTEGER)) AS s from cte3
) t order by s.x;

-- query 5
-- Nested STRUCT with different field order
with cte1 as (
    select CAST(ROW(1,ROW(100,200)) AS ROW(outer_a INTEGER,"inner" ROW(inner_x INTEGER,inner_y INTEGER))) as s
),
cte2 as (
    select CAST(ROW(ROW(400,300),2) AS ROW("inner" ROW(inner_y INTEGER,inner_x INTEGER),outer_a INTEGER)) as s
)
select s.outer_a, s."inner".inner_x, s."inner".inner_y from (
    select s from cte1
    union all
    select CAST(ROW(s.outer_a, ROW(s."inner".inner_x,s."inner".inner_y)) AS ROW(outer_a INTEGER,"inner" ROW(inner_x INTEGER,inner_y INTEGER))) AS s from cte2
) t order by s.outer_a;

-- query 6
-- UNION ALL with NULL values and different field order
with cte1 as (
    select CAST(ROW(1,'test') AS ROW(a INTEGER,b VARCHAR)) as s
),
cte2 as (
    select CAST(ROW(NULL,2) AS ROW(b VARCHAR,a INTEGER)) as s
)
select s.a, s.b from (
    select s from cte1
    union all
    select CAST(ROW(s.a,s.b) AS ROW(a INTEGER,b VARCHAR)) AS s from cte2
) t order by s.a;
