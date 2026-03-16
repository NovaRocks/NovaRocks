-- Migrated from dev/test/sql/test_cast/T/test_cast_json_to_map
-- Test Objective:
-- 1. Validate CAST(JSON as MAP<K,V>) where only JSON objects can be cast; arrays/scalars return NULL.
-- 2. Cover map<string,json>, map<int,json>, map<string,string>, map<int,string>.
-- 3. Cover complex value types: map<string,array<int>>, map<string,struct<...>>, map<string,map<...>>.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_cast_json_to_map;
CREATE TABLE ${case_db}.t_cast_json_to_map (
    c1 INT,
    c2 JSON
) PROPERTIES ("replication_num" = "1");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.t_cast_json_to_map VALUES
(1, '[1,2,3]'),
(2, '"abc"'),
(3, 'null'),
(4, 'true'),
(5, '1'),
(6, '{"1":1, "2":true, "3":null, "4":[5,6,7], "5":{"k51":"v51","k52":"v52"}}');

-- query 3
-- @order_sensitive=true
-- Only row 6 (JSON object) can be cast to map; others return NULL
select c1, cast(c2 as map<string,json>) as m from ${case_db}.t_cast_json_to_map order by c1;

-- query 4
-- @order_sensitive=true
-- Integer keys: only row 6 returns non-NULL
select c1, cast(c2 as map<int,json>) as m from ${case_db}.t_cast_json_to_map order by c1;

-- query 5
-- @order_sensitive=true
-- Map<string,string>: JSON null values in map become SQL NULL
select c1, cast(c2 as map<string,string>) as m from ${case_db}.t_cast_json_to_map order by c1;

-- query 6
-- @order_sensitive=true
-- Map<int,string>: integer key variant
select c1, cast(c2 as map<int,string>) as m from ${case_db}.t_cast_json_to_map order by c1;

-- query 7
-- @order_sensitive=true
-- Map<string,array<int>>: only key "4" has an array value; others map to null
select c1, cast(c2 as map<string,array<int>>) as m from ${case_db}.t_cast_json_to_map order by c1;

-- query 8
-- @order_sensitive=true
-- Map<string,struct<k51,k52>>: only key "5" has matching struct; others map to null
select c1, cast(c2 as map<string,struct<k51 string,k52 string>>) as m from ${case_db}.t_cast_json_to_map order by c1;

-- query 9
-- @order_sensitive=true
-- Map<string,map<string,string>>: only key "5" has a nested object; others map to null
select c1, cast(c2 as map<string,map<string,string>>) as m from ${case_db}.t_cast_json_to_map order by c1;
