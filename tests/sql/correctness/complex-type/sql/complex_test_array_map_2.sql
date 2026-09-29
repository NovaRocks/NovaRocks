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

-- Migrated from dev/test/sql/test_array_fn/R/test_array_map_2
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_array_map_2 @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `array_map_test` (
  `id` tinyint(4) NOT NULL COMMENT "",
  `arr_str` array<string> NULL COMMENT "",
  `arr_largeint` array<largeint> NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into array_map_test values (1, array_repeat("abcdefghasdasdasirnqwrq", 20000), array_repeat(100, 20000));

-- query 4
USE ${case_db};
select count() from array_map_test where array_length(array_map((x,y)->(id+length(x)+y), arr_str, arr_largeint)) > 10 ;

-- query 5
USE ${case_db};
select count(array_length(array_map((x,y)->(id+length(x)+y), arr_str, arr_largeint))) from array_map_test;

-- query 6
USE ${case_db};
select count() from array_map_test where any_match(x->any_match(x->x<10, arr_largeint), arr_largeint);

-- query 7
USE ${case_db};
select count(any_match(x->any_match(x->x<10, arr_largeint), arr_largeint)) from array_map_test;

-- query 8
USE ${case_db};
select count(array_map(x->array_length(array_concat(arr_str,[])), arr_largeint)) from array_map_test;

-- query 9
-- @skip_result_check=true
USE ${case_db};
set @arr=array_repeat("12345",1000000);

-- query 10
USE ${case_db};
select array_length(array_map((x,y)->x > y, @arr,@arr)) from table(generate_series(1,10,1));

-- query 11
USE ${case_db};
select count(*) from array_map_test where array_map((a1) -> concat(split(id, 'd'), split(id, 'd')), arr_str) is not null;

-- name: test_array_map_3
-- query 12
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t` (
  `k` bigint NOT NULL COMMENT "",
  `arr_0` array<bigint> NOT NULL COMMENT "",
  `arr_1` array<bigint> NULL COMMENT "",
  `arr_2` array<bigint> NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 13
-- @skip_result_check=true
USE ${case_db};
insert into t values (1, [1,2], [1,2],[2,3]), (2, [1,2], null, [2,3]), (3, [1,2],[1,2],null),(4, [1,2],[null,null],[2,3]), (5, [1], [1,2], [3]);

-- query 14
-- @expect_error=Input array element's size is not equal in array_map()
USE ${case_db};
select array_map((x,y,z)->x+y+z, arr_0, arr_1, arr_2) from t;

-- query 15
USE ${case_db};
select array_map((x,y,z)->x+y+z, arr_0, arr_1, arr_2) from t where k != 5 order by k;

-- query 16
-- @skip_result_check=true
USE ${case_db};
delete from t where k = 5;

-- query 17
USE ${case_db};
select array_map((x,y,z)->x+y+z, arr_0, arr_1, arr_2) from t order by k;

-- query 18
USE ${case_db};
select array_map((x,y,z,d)->x+y+z+d, arr_0, arr_1, arr_2, [1,2]) from t order by k;

-- query 19
-- @expect_error=Input array element's size is not equal in array_map()
USE ${case_db};
select array_map((x,y,z,d)->x+y+z+d, arr_0, arr_1, arr_2, [1]) from t order by k;

-- query 20
USE ${case_db};
select array_map((x)->x*x, arr_0) from t order by k;

-- query 21
USE ${case_db};
select k from t where coalesce(element_at(array_map(x->x+any_match(array_map(x->x<10,arr_1)), arr_1), 1),element_at(array_map(x->x+any_match(array_map(x->x<10,arr_2)), arr_2), 1),element_at(array_map(x->x+any_match(array_map(x->x<10,arr_0)), arr_0), 1))=2 order by k;

-- query 22
USE ${case_db};
select k from t where coalesce(element_at(array_map(x->x+any_match(array_map(x->x<10,arr_1)), arr_1), 1),element_at(array_map(x->x+any_match(array_map(x->x<10,arr_2)), arr_2), 1),element_at(array_map(x->x+any_match(array_map(x->x<10,arr_0)), arr_0), 1))>0 and coalesce(element_at(array_map(x->x+any_match(array_map(x->x<10,arr_1)), arr_1), 1),element_at(array_map(x->x+any_match(array_map(x->x<10,arr_2)), arr_2), 1)) < 10  order by k;

-- query 23
USE ${case_db};
select k, coalesce(element_at(array_map(x->x+any_match(array_map(x->x<10,arr_1)), arr_1), 1),element_at(array_map(x->x+any_match(array_map(x->x<10,arr_2)), arr_2), 1),element_at(array_map(x->x+any_match(array_map(x->x<10,arr_0)), arr_0), 1)) as col1 from t order by k;

-- query 24
USE ${case_db};
select k, coalesce(element_at(array_map(x->x+any_match(array_map(x->x<10,arr_1)), arr_1), 1),element_at(array_map(x->x+any_match(array_map(x->x<10,arr_2)), arr_2), 1),element_at(array_map(x->x+any_match(array_map(x->x<10,arr_0)), arr_0), 1)) as col1, coalesce(element_at(array_map(x->x+any_match(array_map(x->x<10,arr_1)), arr_1), 1),element_at(array_map(x->x+any_match(array_map(x->x<10,arr_2)), arr_2), 1)) as col2 from t order by k;

-- query 25
USE ${case_db};
select array_map(x->x, arr_0) from t order by k;

-- query 26
USE ${case_db};
select array_map((x,y,z)->10, arr_0, arr_1, arr_2) from t;

-- query 27
USE ${case_db};
select array_map((x,y)-> k, arr_0, arr_1) from t order by k;

-- query 28
USE ${case_db};
select array_map((x,y)->k, [1,2],[2,3]) from t order by k;

-- query 29
USE ${case_db};
select array_map((x,y,z)->x+y+z, [1,2],[2,3],[3,4]) from t;

-- query 30
USE ${case_db};
select array_map((x,y,z)->x+y+z, [1,2],[2,null],[3,4]) from t;

-- query 31
USE ${case_db};
select array_map((x,y,z)->x+y+z, [1,2],[2,null],null) from t;

-- query 32
USE ${case_db};
select array_map(x->array_map(x->x+100,x), [[1,2,3]]);

-- query 33
USE ${case_db};
select array_map(x->array_map(x->x+100,x), [[1,2,3], [null]]);

-- query 34
USE ${case_db};
select array_map(x->array_map(x->array_map(x->x+100,x),x), [[[1,2,3]]]);

-- query 35
USE ${case_db};
select array_map(arg0 -> array_map(arg1 -> array_map(arg2 -> array_map(arg3 -> array_length(arg1) + arg3, arg2), arg1), arg0), [[[[1,2]]]]);

-- query 36
USE ${case_db};
select array_map(arg0 -> array_map(arg1 -> array_map(arg2 -> array_map(arg3 -> array_length(arg0) + arg3, arg2), arg1), arg0), [[[[1,2]]]]);

-- query 37
USE ${case_db};
select array_map(arg0 -> array_map(arg1 -> array_map(arg2 -> array_map(arg3 -> array_length(arg2) + arg3, arg2), arg1), arg0), [[[[1,2]]]]);

-- query 38
USE ${case_db};
select array_map(arg0 -> array_map(arg1 -> array_map(arg2 -> array_map(arg3 -> array_map(arg4->array_length(arg2) + arg4, arg3), arg2), arg1), arg0), [[[[[1,2]]]]] );

-- query 39
-- @skip_result_check=true
USE ${case_db};
set @arr=array_generate(1,10000);

-- query 40
USE ${case_db};
select /*+ SET_VAR(query_mem_limit=104857600)*/array_sum(array_map(x->array_contains(@arr,x), array_generate(1,100000)));

-- query 41
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `array_map_x` (
  `id` tinyint(4) NOT NULL COMMENT "",
  `arr_str` array<varchar(65533)> NULL COMMENT "",
  `arr_largeint` array<largeint(40)> NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 42
-- @skip_result_check=true
USE ${case_db};
insert into array_map_x values (1, array_repeat("abcdefghasdasdasirnqwrq", 2), array_repeat(100, 2));

-- query 43
USE ${case_db};
select cast(if (1 > rand(), "[]", "") as array<string>) l , array_map((x)-> (concat(x,l)), arr_str) from array_map_x;

-- query 44
-- @skip_result_check=true
-- A catalog that cannot hold views cannot answer view enumeration, so
-- DROP DATABASE ... FORCE is refused here rather than silently assuming
-- the namespace holds none. Drop the tables explicitly instead.
USE ${case_db};
DROP TABLE IF EXISTS array_map_test;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS array_map_x;
