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
CREATE TABLE "array_map_test" (
  "id" TINYINT NOT NULL,
  "arr_str" array<VARCHAR>,
  "arr_largeint" array<largeint>
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into array_map_test values ROW(1, repeat('abcdefghasdasdasirnqwrq', 20000), repeat(CAST(100 AS LARGEINT), 20000));

-- query 4
USE ${case_db};
select count() from array_map_test where cardinality(transform(zip(arr_str, arr_largeint), inputs -> (id+length(inputs[1])+inputs[2]))) > 10 ;

-- query 5
USE ${case_db};
select count(cardinality(transform(zip(arr_str, arr_largeint), inputs -> (id+length(inputs[1])+inputs[2])))) from array_map_test;

-- query 6
USE ${case_db};
select count() from array_map_test where any_match(arr_largeint, x -> any_match(arr_largeint, y -> y < 10));

-- query 7
USE ${case_db};
select count(any_match(arr_largeint, x -> any_match(arr_largeint, y -> y < 10))) from array_map_test;

-- query 8
USE ${case_db};
select count(transform(arr_largeint, x->cardinality(concat(arr_str,[])))) from array_map_test;

-- query 9
USE ${case_db};
WITH chunks AS (
    SELECT repeat('12345', 100000) AS part
), arrays AS (
    SELECT concat(part, part, part, part, part, part, part, part, part, part) AS items
    FROM chunks
)
SELECT cardinality(transform(zip(items, items), inputs -> inputs[1] > inputs[2]))
FROM arrays CROSS JOIN TABLE(sequence(1, 10)) AS rows(sequential_number);

-- query 10
USE ${case_db};
select count(*) from array_map_test where transform(arr_str, (a1) -> concat(split(CAST(id AS VARCHAR), 'd'), split(CAST(id AS VARCHAR), 'd'))) is not null;

-- name: test_array_map_3
-- query 11
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "t" (
  "k" bigint NOT NULL,
  "arr_0" array<bigint> NOT NULL,
  "arr_1" array<bigint>,
  "arr_2" array<bigint>
) WITH (format_version = 3);

-- query 12
-- @skip_result_check=true
USE ${case_db};
insert into t values ROW(1, [1,2], [1,2],[2,3]), ROW(2, [1,2], null, [2,3]), ROW(3, [1,2],[1,2],null),ROW(4, [1,2],[null,null],[2,3]), ROW(5, [1], [1,2], [3]);

-- query 13
USE ${case_db};
select transform(zip(arr_0, arr_1, arr_2), inputs -> inputs[1]+inputs[2]+inputs[3]) from t where k != 5 order by k;

-- query 14
-- @skip_result_check=true
USE ${case_db};
delete from t where k = 5;

-- query 15
USE ${case_db};
select transform(zip(arr_0, arr_1, arr_2), inputs -> inputs[1]+inputs[2]+inputs[3]) from t order by k;

-- query 16
USE ${case_db};
select transform(zip(arr_0, arr_1, arr_2, [1,2]), inputs -> inputs[1]+inputs[2]+inputs[3]+inputs[4]) from t order by k;

-- query 17
USE ${case_db};
select transform(arr_0, (x)->x*x) from t order by k;

-- query 18
USE ${case_db};
select k from t where coalesce(element_at(transform(arr_1, x->x+(CASE any_match(arr_1, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1),element_at(transform(arr_2, x->x+(CASE any_match(arr_2, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1),element_at(transform(arr_0, x->x+(CASE any_match(arr_0, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1))=2 order by k;

-- query 19
USE ${case_db};
select k from t where coalesce(element_at(transform(arr_1, x->x+(CASE any_match(arr_1, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1),element_at(transform(arr_2, x->x+(CASE any_match(arr_2, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1),element_at(transform(arr_0, x->x+(CASE any_match(arr_0, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1))>0 and coalesce(element_at(transform(arr_1, x->x+(CASE any_match(arr_1, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1),element_at(transform(arr_2, x->x+(CASE any_match(arr_2, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1)) < 10  order by k;

-- query 20
USE ${case_db};
select k, coalesce(element_at(transform(arr_1, x->x+(CASE any_match(arr_1, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1),element_at(transform(arr_2, x->x+(CASE any_match(arr_2, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1),element_at(transform(arr_0, x->x+(CASE any_match(arr_0, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1)) as col1 from t order by k;

-- query 21
USE ${case_db};
select k, coalesce(element_at(transform(arr_1, x->x+(CASE any_match(arr_1, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1),element_at(transform(arr_2, x->x+(CASE any_match(arr_2, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1),element_at(transform(arr_0, x->x+(CASE any_match(arr_0, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1)) as col1, coalesce(element_at(transform(arr_1, x->x+(CASE any_match(arr_1, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1),element_at(transform(arr_2, x->x+(CASE any_match(arr_2, y -> y < 10) WHEN TRUE THEN 1 WHEN FALSE THEN 0 END)), 1)) as col2 from t order by k;

-- query 22
USE ${case_db};
select transform(arr_0, x->x) from t order by k;

-- query 23
USE ${case_db};
select transform(zip(arr_0, arr_1, arr_2), inputs -> 10) from t;

-- query 24
USE ${case_db};
select transform(zip(arr_0, arr_1), inputs -> k) from t order by k;

-- query 25
USE ${case_db};
select transform(zip([1,2], [2,3]), inputs -> k) from t order by k;

-- query 26
USE ${case_db};
select transform(zip([1,2], [2,3], [3,4]), inputs -> inputs[1]+inputs[2]+inputs[3]) from t;

-- query 27
USE ${case_db};
select transform(zip([1,2], [2,null], [3,4]), inputs -> inputs[1]+inputs[2]+inputs[3]) from t;

-- query 28
USE ${case_db};
select transform(zip([1,2], [2,null], null), inputs -> inputs[1]+inputs[2]+inputs[3]) from t;

-- query 29
USE ${case_db};
select transform([[1,2,3]], x->transform(x, x->x+100));

-- query 30
USE ${case_db};
select transform([[1,2,3], [null]], x->transform(x, x->x+100));

-- query 31
USE ${case_db};
select transform([[[1,2,3]]], x->transform(x, x->transform(x, x->x+100)));

-- query 32
USE ${case_db};
select transform([[[[1,2]]]], arg0 -> transform(arg0, arg1 -> transform(arg1, arg2 -> transform(arg2, arg3 -> cardinality(arg1) + arg3))));

-- query 33
USE ${case_db};
select transform([[[[1,2]]]], arg0 -> transform(arg0, arg1 -> transform(arg1, arg2 -> transform(arg2, arg3 -> cardinality(arg0) + arg3))));

-- query 34
USE ${case_db};
select transform([[[[1,2]]]], arg0 -> transform(arg0, arg1 -> transform(arg1, arg2 -> transform(arg2, arg3 -> cardinality(arg2) + arg3))));

-- query 35
USE ${case_db};
select transform([[[[[1,2]]]]], arg0 -> transform(arg0, arg1 -> transform(arg1, arg2 -> transform(arg2, arg3 -> transform(arg3, arg4->cardinality(arg2) + arg4)))));

-- query 36
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "array_map_x" (
  "id" TINYINT NOT NULL,
  "arr_str" array<varchar(65533)>,
  "arr_largeint" array<largeint(40)>
) WITH (format_version = 3);

-- query 37
-- @skip_result_check=true
USE ${case_db};
insert into array_map_x values ROW(1, repeat('abcdefghasdasdasirnqwrq', 2), repeat(CAST(100 AS LARGEINT), 2));

-- query 38
USE ${case_db};
select cast(if (1 > rand(), '[]', '') as ARRAY(VARCHAR)) l , transform(arr_str, (x)-> (concat(x,l))) from array_map_x;

-- query 39
-- @skip_result_check=true
-- A catalog that cannot hold views cannot answer view enumeration, so
-- DROP DATABASE ... FORCE is refused here rather than silently assuming
-- the namespace holds none. Drop the tables explicitly instead.
USE ${case_db};
DROP TABLE IF EXISTS array_map_test;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS array_map_x;
