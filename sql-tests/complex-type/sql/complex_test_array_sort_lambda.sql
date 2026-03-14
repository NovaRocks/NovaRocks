-- Migrated from dev/test/sql/test_array_fn/R/test_array_sort_lambda
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_array_sort_lambda FORCE;
CREATE DATABASE sql_tests_complex_test_array_sort_lambda;
USE sql_tests_complex_test_array_sort_lambda;

-- name: test_array_sort_lambda
-- query 2
USE sql_tests_complex_test_array_sort_lambda;
SELECT array_sort([3, 2, 5, 1, 2], (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)));

-- query 3
-- @skip_result_check=true
USE sql_tests_complex_test_array_sort_lambda;
create table t0 (
    k0 int,
    c0 array<tinyint>,
    c1 array<smallint>,
    c2 array<int>,
    c3 array<bigint>,
    c4 array<largeint>,
    c5 array<double>,
    c6 array<float>
)
properties(
   "replication_num" = "1"
);

-- query 4
-- @skip_result_check=true
USE sql_tests_complex_test_array_sort_lambda;
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
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(c0, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 6
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(c1, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 7
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(c2, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 8
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(c3, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 9
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(c4, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 10
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(c5, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 11
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(c6, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 12
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(array_map(x->cast(x as decimal(7,2)),c6), (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 13
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(array_map(x->cast(x as decimal(19,5)),c6), (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 14
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort([3, 2, 5, 1, 2], (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 15
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort([3, 2, 5, 1, 2], (x, y) -> IF(x < y, -1, IF(x = y, 0, 1)));

-- query 16
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort([], (x, y) -> IF(x < y, -1, IF(x = y, 0, 1)));

-- query 17
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort([5], (x, y) -> IF(x < y, -1, IF(x = y, 0, 1)));

-- query 18
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort([1, 1, 1, 1], (x, y) -> IF(x < y, -1, IF(x = y, 0, 1)));

-- query 19
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_min(array_sort([3, 2, 5, 1, 2], (x, y) -> k0%3-2)) <=3 from t0;

-- query 20
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_min(array_sort(c0, (x, y) -> k0%3-2))<=array_max(c0) from t0;

-- query 21
-- @expect_error=Comparator violates irreflexivity
USE sql_tests_complex_test_array_sort_lambda;
SELECT array_sort([1,2,3,4,5,6,7,8], (x,y)->CASE WHEN x = 1 THEN -1 ELSE x - y END);

-- query 22
-- @expect_error=Comparator violates asymmetry
USE sql_tests_complex_test_array_sort_lambda;
SELECT array_sort([1,2,3,4,5,6,7,8], (x,y)->CASE WHEN (x = 1 and y = 2) or (y = 1 and x = 2) THEN -1 ELSE x - y END);

-- query 23
-- @expect_error=Comparator violates incomparability transitivity
USE sql_tests_complex_test_array_sort_lambda;
SELECT array_sort([1,2,3,4,5,6,7,8], (x,y)->CASE  WHEN x = 1 and y = 2 THEN -1 WHEN x <= 6 and y <= 6 THEN 1  ELSE x - y END);

-- query 24
-- @expect_error=Comparator violates transitivity
USE sql_tests_complex_test_array_sort_lambda;
SELECT array_sort([1,2,3,4,5,6,7,8], (x,y)->CASE WHEN (x = 1 and y = 2) or (x = 2 and y = 3) or (x = 3 and y = 1) THEN -1 WHEN x = 1 and y = 3 THEN 1 ELSE x - y END);

-- query 25
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE sql_tests_complex_test_array_sort_lambda;
with cte as (
select array_agg(k0) as arr
from t0
)
select array_sort(arr, (x,y)->rand()-0.5)[1] <= array_max(arr) from cte;

-- query 26
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE sql_tests_complex_test_array_sort_lambda;
with cte as (
select array_agg(k0) as arr
from t0
)
select array_sort(arr, (x,y)->-1)[1] <= array_max(arr) from cte;

-- query 27
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE sql_tests_complex_test_array_sort_lambda;
with cte as (
select array_agg(k0) as arr
from t0
)
select array_sort(arr, (x,y)->1)[1] <= array_max(arr) from cte;

-- query 28
-- @skip_result_check=true
USE sql_tests_complex_test_array_sort_lambda;
drop table t0;

-- query 29
USE sql_tests_complex_test_array_sort_lambda;
SELECT array_sort(['bc', 'ab', 'dc'], (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)));

-- query 30
USE sql_tests_complex_test_array_sort_lambda;
SELECT array_sort(['a', 'abcd', 'abc'], (x, y) -> IF(length(x) < length(y), -1, IF(length(x) = length(y), 0, 1)));

-- query 31
-- @skip_result_check=true
USE sql_tests_complex_test_array_sort_lambda;
DROP TABLE IF EXISTS t0;
create table t0 (
    k0 int,
    c0 array<string>,
    c1 array<string>
)
properties(
   "replication_num" = "1"
);

-- query 32
-- @skip_result_check=true
USE sql_tests_complex_test_array_sort_lambda;
insert into t0 select i
       ,['bc','ab','dc'] as c0
       ,['a','abcd','abc'] as c1
from table(generate_series(0,10000)) t(i);

-- query 33
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(['bc', 'ab', 'dc'], (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 34
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(['a', 'abcd', 'abc'], (x, y) -> IF(length(x) < length(y), -1, IF(length(x) = length(y), 0, 1))) from t0;

-- query 35
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(c0, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 36
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(c1, (x, y) -> IF(length(x) < length(y), -1, IF(length(x) = length(y), 0, 1))) from t0;

-- query 37
-- @skip_result_check=true
USE sql_tests_complex_test_array_sort_lambda;
drop table t0;

-- query 38
-- @expect_error=Comparator violates irreflexivity
USE sql_tests_complex_test_array_sort_lambda;
SELECT array_sort([3, 2, null, 5, null, 1, 2], (x, y) -> CASE WHEN x IS NULL THEN -1 WHEN y IS NULL THEN 1 WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END);

-- query 39
USE sql_tests_complex_test_array_sort_lambda;
SELECT array_sort([3, 2, null, 5, null, 1, 2], (x, y) -> CASE WHEN x IS NULL THEN 1 WHEN y IS NULL THEN -1 WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END);

-- query 40
-- @skip_result_check=true
USE sql_tests_complex_test_array_sort_lambda;
DROP TABLE IF EXISTS t0;
create table t0 (
    k0 int,
    c0 array<int>
)
properties(
   "replication_num" = "1"
);

-- query 41
-- @skip_result_check=true
USE sql_tests_complex_test_array_sort_lambda;
insert into t0 select i
       ,[3,2,null,5,null,1,2] as c0
from table(generate_series(0,10000)) t(i);

-- query 42
-- @expect_error=Comparator violates irreflexivity
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct  array_sort([3, 2, null, 5, null, 1, 2], (x, y) -> CASE WHEN x IS NULL THEN -1 WHEN y IS NULL THEN 1 WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END) from t0;

-- query 43
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort([3, 2, null, 5, null, 1, 2], (x, y) -> CASE WHEN x IS NULL THEN 1 WHEN y IS NULL THEN -1 WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END) from t0;

-- query 44
-- @expect_error=Comparator violates irreflexivity
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(c0, (x, y) -> CASE WHEN x IS NULL THEN -1 WHEN y IS NULL THEN 1 WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END) from t0;

-- query 45
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(c0, (x, y) -> CASE WHEN x IS NULL THEN 1 WHEN y IS NULL THEN -1 WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END) from t0;

-- query 46
-- @skip_result_check=true
USE sql_tests_complex_test_array_sort_lambda;
drop table t0;

-- query 47
USE sql_tests_complex_test_array_sort_lambda;
SELECT array_sort([[2, 3, 1], [4, 2, 1, 4], [1, 2]], (x, y) -> IF(cardinality(x) < cardinality(y), -1, IF(cardinality(x) = cardinality(y), 0, 1)));

-- query 48
-- @skip_result_check=true
USE sql_tests_complex_test_array_sort_lambda;
DROP TABLE IF EXISTS t0;
create table t0 (
    k0 int,
    c0 array<array<int>>
)
properties(
   "replication_num" = "1"
);

-- query 49
-- @skip_result_check=true
USE sql_tests_complex_test_array_sort_lambda;
insert into t0 select i
      ,[[2, 3, 1], [4, 2, 1, 4], [1, 2]] c0
from table(generate_series(0,10000)) t(i);

-- query 50
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort([[2, 3, 1], [4, 2, 1, 4], [1, 2]], (x, y) -> IF(cardinality(x) < cardinality(y), -1, IF(cardinality(x) = cardinality(y), 0, 1))) from t0;

-- query 51
USE sql_tests_complex_test_array_sort_lambda;
SELECT distinct array_sort(c0, (x, y) -> IF(cardinality(x) < cardinality(y), -1, IF(cardinality(x) = cardinality(y), 0, 1))) from t0;

-- query 52
-- @skip_result_check=true
USE sql_tests_complex_test_array_sort_lambda;
drop table t0;

-- query 53
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE sql_tests_complex_test_array_sort_lambda;
select array_sort([1,3,2,1,3,6,100,200],(x,y)->rand()-0.5)[1] <= array_max([1,3,2,1,3,6,100,200]);

-- query 54
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE sql_tests_complex_test_array_sort_lambda;
select array_sort([1,3,2,1,3,6,100,200],(x,y)->0)[1] <= array_max([1,3,2,1,3,6,100,200]);

-- query 55
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE sql_tests_complex_test_array_sort_lambda;
select array_sort([1,3,2,1,3,6,100,200],(x,y)->1)[1] <= array_max([1,3,2,1,3,6,100,200]);

-- query 56
-- @expect_error=Lambda function in sort_array should only depend on both two arguments and contain no non-deterministic functions
USE sql_tests_complex_test_array_sort_lambda;
select array_sort([1,3,2,1,3,6,100,200],(x,y)->-1)[1] <= array_max([1,3,2,1,3,6,100,200]);

-- query 57
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_array_sort_lambda FORCE;
