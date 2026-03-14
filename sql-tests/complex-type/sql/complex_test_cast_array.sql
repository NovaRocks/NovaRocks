-- Migrated from dev/test/sql/test_array/R/test_cast_array
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_cast_array FORCE;
CREATE DATABASE sql_tests_complex_test_cast_array;
USE sql_tests_complex_test_cast_array;

-- name: test_cast_array
-- query 2
-- @skip_result_check=true
USE sql_tests_complex_test_cast_array;
CREATE TABLE `tbl` (k1 string,k2 string,k3 int) DUPLICATE KEY(`k1`) DISTRIBUTED BY HASH(`k1`) BUCKETS 3 PROPERTIES ("replication_num" = "1");

-- query 3
-- @skip_result_check=true
USE sql_tests_complex_test_cast_array;
insert into tbl values
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1);

-- query 4
-- @skip_result_check=true
USE sql_tests_complex_test_cast_array;
set @arr_str = (select array_agg(k1) from (select t1.k1 from tbl t1 join tbl t2 join tbl t3 join tbl t4) t);

-- query 5
USE sql_tests_complex_test_cast_array;
select @arr_str[1];

-- query 6
USE sql_tests_complex_test_cast_array;
select element_at(array_agg(array_length(@arr_str)), 5) from (select t1.k3 from tbl t1 join tbl t2 join tbl t3 join tbl t4) t;

-- query 7
-- @skip_result_check=true
USE sql_tests_complex_test_cast_array;
select * from (select array_length(@arr_str) array_len from (select t1.k3 from tbl t1 join tbl t2 join tbl t3 join tbl t4) t) t where array_len = 1;

-- query 8
USE sql_tests_complex_test_cast_array;
select count(*) from (select @arr_str arr from (select t1.k3 from tbl t1 join tbl t2) t) t group by arr[1];

-- query 9
USE sql_tests_complex_test_cast_array;
select /*+SET_VAR(pipeline_dop=100)*/ count(*) from (select @arr_str arr from (select t1.k3 from tbl t1 join tbl t2) t) t group by arr[1];

-- query 10
USE sql_tests_complex_test_cast_array;
select count(*), array_length(array_agg(arr)) from (select @arr_str arr from (select t1.k3 from tbl t1) t) t group by arr[1];

-- query 11
USE sql_tests_complex_test_cast_array;
select array_length(array_agg(arr)) from (select cast("[[1, 2, 3], [1, 2, 3]]" as array<array<int>>) arr from (select t1.k3 from tbl t1 join tbl t2) t)  t;

-- query 12
USE sql_tests_complex_test_cast_array;
select array_agg(arr)[1] from (select cast("[[1, 2, 3], [1, 2, 3]]" as array<array<int>>) arr from (select t1.k3 from tbl t1 join tbl t2) t)  t;

-- query 13
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_cast_array FORCE;
