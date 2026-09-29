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

-- Migrated from dev/test/sql/test_array_fn/R/test_array_sort_lambda
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_array_sort_lambda
-- query 2
USE ${case_db};
SELECT array_sort([3, 2, 5, 1, 2], (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)));

-- query 3
-- @skip_result_check=true
USE ${case_db};
create table t0 (
    k0 int,
    c0 array<tinyint>,
    c1 array<smallint>,
    c2 array<int>,
    c3 array<bigint>,
    c4 array<DECIMAL(38, 0)>,
    c5 array<double>,
    c6 array<float>
)
TBLPROPERTIES ("format-version" = "3");

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t0 select i
       ,[3,2,5,1,2] as c0
       ,[3,2,5,1,2] as c1
       ,[3,2,5,1,2] as c2
       ,[3,2,5,1,2] as c3
       ,[3,2,5,1,2] as c4
       ,[3,2,5,1,2] as c5
       ,[3,2,5,1,2] as c6
from table(generate_series(0,10000)) t(i);

-- query 5
USE ${case_db};
SELECT distinct array_sort(c0, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 6
USE ${case_db};
SELECT distinct array_sort(c1, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 7
USE ${case_db};
SELECT distinct array_sort(c2, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 8
USE ${case_db};
SELECT distinct array_sort(c3, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 9
USE ${case_db};
SELECT distinct array_sort(c4, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 10
USE ${case_db};
SELECT distinct array_sort(c5, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 11
USE ${case_db};
SELECT distinct array_sort(c6, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 12
USE ${case_db};
SELECT distinct array_sort(array_map(x->cast(x as decimal(7,2)),c6), (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 13
USE ${case_db};
SELECT distinct array_sort(array_map(x->cast(x as decimal(19,5)),c6), (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 14
USE ${case_db};
SELECT distinct array_sort([3, 2, 5, 1, 2], (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 15
USE ${case_db};
SELECT distinct array_sort([3, 2, 5, 1, 2], (x, y) -> IF(x < y, -1, IF(x = y, 0, 1)));

-- query 16
USE ${case_db};
SELECT distinct array_sort([], (x, y) -> IF(x < y, -1, IF(x = y, 0, 1)));

-- query 17
USE ${case_db};
SELECT distinct array_sort([5], (x, y) -> IF(x < y, -1, IF(x = y, 0, 1)));

-- query 18
USE ${case_db};
SELECT distinct array_sort([1, 1, 1, 1], (x, y) -> IF(x < y, -1, IF(x = y, 0, 1)));

-- query 19
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE ${case_db};
SELECT distinct array_min(array_sort([3, 2, 5, 1, 2], (x, y) -> k0%3-2)) <=3 from t0;

-- query 20
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE ${case_db};
SELECT distinct array_min(array_sort(c0, (x, y) -> k0%3-2))<=array_max(c0) from t0;

-- query 21
-- @expect_error=Comparator violates irreflexivity
USE ${case_db};
SELECT array_sort([1,2,3,4,5,6,7,8], (x,y)->CASE WHEN x = 1 THEN -1 ELSE x - y END);

-- query 22
-- @expect_error=Comparator violates asymmetry
USE ${case_db};
SELECT array_sort([1,2,3,4,5,6,7,8], (x,y)->CASE WHEN (x = 1 and y = 2) or (y = 1 and x = 2) THEN -1 ELSE x - y END);

-- query 23
-- @expect_error=Comparator violates incomparability transitivity
USE ${case_db};
SELECT array_sort([1,2,3,4,5,6,7,8], (x,y)->CASE  WHEN x = 1 and y = 2 THEN -1 WHEN x <= 6 and y <= 6 THEN 1  ELSE x - y END);

-- query 24
-- @expect_error=Comparator violates transitivity
USE ${case_db};
SELECT array_sort([1,2,3,4,5,6,7,8], (x,y)->CASE WHEN (x = 1 and y = 2) or (x = 2 and y = 3) or (x = 3 and y = 1) THEN -1 WHEN x = 1 and y = 3 THEN 1 ELSE x - y END);

-- query 25
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE ${case_db};
with cte as (
select array_agg(k0) as arr
from t0
)
select array_sort(arr, (x,y)->rand()-0.5)[1] <= array_max(arr) from cte;

-- query 26
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE ${case_db};
with cte as (
select array_agg(k0) as arr
from t0
)
select array_sort(arr, (x,y)->-1)[1] <= array_max(arr) from cte;

-- query 27
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE ${case_db};
with cte as (
select array_agg(k0) as arr
from t0
)
select array_sort(arr, (x,y)->1)[1] <= array_max(arr) from cte;

-- query 28
-- @skip_result_check=true
USE ${case_db};
drop table t0;

-- query 29
USE ${case_db};
SELECT array_sort(['bc', 'ab', 'dc'], (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)));

-- query 30
USE ${case_db};
SELECT array_sort(['a', 'abcd', 'abc'], (x, y) -> IF(length(x) < length(y), -1, IF(length(x) = length(y), 0, 1)));

-- query 31
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t0;
create table t0 (
    k0 int,
    c0 array<string>,
    c1 array<string>
)
TBLPROPERTIES ("format-version" = "3");

-- query 32
-- @skip_result_check=true
USE ${case_db};
insert into t0 select i
       ,['bc','ab','dc'] as c0
       ,['a','abcd','abc'] as c1
from table(generate_series(0,10000)) t(i);

-- query 33
USE ${case_db};
SELECT distinct array_sort(['bc', 'ab', 'dc'], (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 34
USE ${case_db};
SELECT distinct array_sort(['a', 'abcd', 'abc'], (x, y) -> IF(length(x) < length(y), -1, IF(length(x) = length(y), 0, 1))) from t0;

-- query 35
USE ${case_db};
SELECT distinct array_sort(c0, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 36
USE ${case_db};
SELECT distinct array_sort(c1, (x, y) -> IF(length(x) < length(y), -1, IF(length(x) = length(y), 0, 1))) from t0;

-- query 37
-- @skip_result_check=true
USE ${case_db};
drop table t0;

-- query 38
-- @expect_error=Comparator violates irreflexivity
USE ${case_db};
SELECT array_sort([3, 2, null, 5, null, 1, 2], (x, y) -> CASE WHEN x IS NULL THEN -1 WHEN y IS NULL THEN 1 WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END);

-- query 39
USE ${case_db};
SELECT array_sort([3, 2, null, 5, null, 1, 2], (x, y) -> CASE WHEN x IS NULL THEN 1 WHEN y IS NULL THEN -1 WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END);

-- query 40
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t0;
create table t0 (
    k0 int,
    c0 array<int>
)
TBLPROPERTIES ("format-version" = "3");

-- query 41
-- @skip_result_check=true
USE ${case_db};
insert into t0 select i
       ,[3,2,null,5,null,1,2] as c0
from table(generate_series(0,10000)) t(i);

-- query 42
-- @expect_error=Comparator violates irreflexivity
USE ${case_db};
SELECT distinct  array_sort([3, 2, null, 5, null, 1, 2], (x, y) -> CASE WHEN x IS NULL THEN -1 WHEN y IS NULL THEN 1 WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END) from t0;

-- query 43
USE ${case_db};
SELECT distinct array_sort([3, 2, null, 5, null, 1, 2], (x, y) -> CASE WHEN x IS NULL THEN 1 WHEN y IS NULL THEN -1 WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END) from t0;

-- query 44
-- @expect_error=Comparator violates irreflexivity
USE ${case_db};
SELECT distinct array_sort(c0, (x, y) -> CASE WHEN x IS NULL THEN -1 WHEN y IS NULL THEN 1 WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END) from t0;

-- query 45
USE ${case_db};
SELECT distinct array_sort(c0, (x, y) -> CASE WHEN x IS NULL THEN 1 WHEN y IS NULL THEN -1 WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END) from t0;

-- query 46
-- @skip_result_check=true
USE ${case_db};
drop table t0;

-- query 47
USE ${case_db};
SELECT array_sort([[2, 3, 1], [4, 2, 1, 4], [1, 2]], (x, y) -> IF(cardinality(x) < cardinality(y), -1, IF(cardinality(x) = cardinality(y), 0, 1)));

-- query 48
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t0;
create table t0 (
    k0 int,
    c0 array<array<int>>
)
TBLPROPERTIES ("format-version" = "3");

-- query 49
-- @skip_result_check=true
USE ${case_db};
insert into t0 select i
      ,[[2, 3, 1], [4, 2, 1, 4], [1, 2]] c0
from table(generate_series(0,10000)) t(i);

-- query 50
USE ${case_db};
SELECT distinct array_sort([[2, 3, 1], [4, 2, 1, 4], [1, 2]], (x, y) -> IF(cardinality(x) < cardinality(y), -1, IF(cardinality(x) = cardinality(y), 0, 1))) from t0;

-- query 51
USE ${case_db};
SELECT distinct array_sort(c0, (x, y) -> IF(cardinality(x) < cardinality(y), -1, IF(cardinality(x) = cardinality(y), 0, 1))) from t0;

-- query 52
-- @skip_result_check=true
USE ${case_db};
drop table t0;

-- query 53
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE ${case_db};
select array_sort([1,3,2,1,3,6,100,200],(x,y)->rand()-0.5)[1] <= array_max([1,3,2,1,3,6,100,200]);

-- query 54
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE ${case_db};
select array_sort([1,3,2,1,3,6,100,200],(x,y)->0)[1] <= array_max([1,3,2,1,3,6,100,200]);

-- query 55
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE ${case_db};
select array_sort([1,3,2,1,3,6,100,200],(x,y)->1)[1] <= array_max([1,3,2,1,3,6,100,200]);

-- query 56
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE ${case_db};
select array_sort([1,3,2,1,3,6,100,200],(x,y)->-1)[1] <= array_max([1,3,2,1,3,6,100,200]);

-- query 57
-- @skip_result_check=true
-- A catalog that cannot hold views cannot answer view enumeration, so
-- DROP DATABASE ... FORCE is refused here rather than silently assuming
-- the namespace holds none. Drop the tables explicitly instead.
USE ${case_db};
DROP TABLE IF EXISTS t0;
