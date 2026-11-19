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
) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.t_cast_json_to_map VALUES
ROW(1, '[1,2,3]'),
ROW(2, '"abc"'),
ROW(3, 'null'),
ROW(4, 'true'),
ROW(5, '1'),
ROW(6, '{"1":1, "2":true, "3":null, "4":[5,6,7], "5":{"k51":"v51","k52":"v52"}}');

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
select c1, cast(c2 as map<string,ROW(k51 string,k52 string)>) as m from ${case_db}.t_cast_json_to_map order by c1;

-- query 9
-- @order_sensitive=true
-- Map<string,map<string,string>>: only key "5" has a nested object; others map to null
select c1, cast(c2 as map<string,map<string,string>>) as m from ${case_db}.t_cast_json_to_map order by c1;
