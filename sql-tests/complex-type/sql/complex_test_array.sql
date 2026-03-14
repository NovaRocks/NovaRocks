-- Migrated from dev/test/sql/test_array/R/test_array
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_array FORCE;
CREATE DATABASE sql_tests_complex_test_array;
USE sql_tests_complex_test_array;

-- name: test_array_test01
-- query 2
USE sql_tests_complex_test_array;
select ARRAY<INT>[], [], ARRAY<STRING>['abc'], [123, NULL, 1.0], ['abc', NULL];

-- name: testArrayPredicate
-- query 3
-- @skip_result_check=true
USE sql_tests_complex_test_array;
CREATE TABLE array_data_type
    (c1 int,
    c2  array<bigint>, 
    c3  array<bigint>,
    c4  array<bigint> not null, 
    c5  array<bigint> not null)
    PRIMARY KEY(c1) 
    DISTRIBUTED BY HASH(c1) 
    BUCKETS 1 
    PROPERTIES ("replication_num" = "1");

-- query 4
-- @skip_result_check=true
USE sql_tests_complex_test_array;
insert into array_data_type (c1, c2, c3, c4,c5) values 
    (1,NULL,NULL,[22, 11, 33],[22, 11, 33]);

-- query 5
USE sql_tests_complex_test_array;
select c2 = c3 from array_data_type;   

insert into array_data_type (c1, c2, c3, c4,c5) values 
    (2,NULL,[22, 11, 33],[22, 11, 33],[22, 11, 33]),
    (3,[22, 11, 33],[22, 11, 33],[22, 11, 33],[22, 11, 33]),
    (4,[22, 11, 33],NULL,[22, 11, 33],[22, 11, 33]);

-- query 6
USE sql_tests_complex_test_array;
select c2 <=> c3 from array_data_type;

-- query 7
USE sql_tests_complex_test_array;
select c2 = c3 from array_data_type;

-- query 8
USE sql_tests_complex_test_array;
select c3 = c4 from array_data_type;

-- query 9
USE sql_tests_complex_test_array;
select c4 = c5 from array_data_type;

-- query 10
-- @skip_result_check=true
USE sql_tests_complex_test_array;
insert into array_data_type (c1, c2, c3, c4,c5) values 
    (5,[22, 11, 33],[22, 11, 33],[22, 11, 44],[22, 11, 33]);

-- query 11
USE sql_tests_complex_test_array;
select c4 = c5 from array_data_type;

-- query 12
USE sql_tests_complex_test_array;
select c4 > c5 from array_data_type;

-- query 13
USE sql_tests_complex_test_array;
select * from (select array_map(x -> x*2 + x*2, [1,3]) col1) t1 join (select array_map(x -> x*2 + x*2, c3) col2 from  array_data_type) t2;

-- name: testArrayVarchar
-- query 14
-- @skip_result_check=true
USE sql_tests_complex_test_array;
CREATE TABLE array_data_type_1
    (c1 int,
    c2  array<datetime>,
    c3  array<float>,
    c4  array<varchar(10)>,
    c5  array<varchar(20)>,
    c6  array<array<varchar(10)>>)
    PRIMARY KEY(c1)
    DISTRIBUTED BY HASH(c1)
    BUCKETS 1
    PROPERTIES ("replication_num" = "1");

-- query 15
-- @skip_result_check=true
USE sql_tests_complex_test_array;
insert into array_data_type_1 values
(1, ['2020-11-11', '2021-11-11', '2022-01-01'], [1.23, 1.35, 2.7894], ['a', 'b'], ['sss', 'eee', 'fff'], [['a', 'b']]),
(2, ['2020-01-11', null, '2022-11-01'], [2.23, 2.35, 3.7894], ['aa', null], ['ssss', 'eeee', null], [['a', null], null]),
(3, null, null, null, null, null);

-- query 16
USE sql_tests_complex_test_array;
select * from array_data_type_1 where c4 != ['a'] or c6 = [['a', 'b']];

-- query 17
USE sql_tests_complex_test_array;
select * from array_data_type_1 where c4 = ['a'] or c6 != [['a', 'b']];

-- query 18
USE sql_tests_complex_test_array;
select * from array_data_type_1 where c4 = cast(c4 as array<char(10)>);

-- query 19
-- @skip_result_check=true
USE sql_tests_complex_test_array;
select * from array_data_type_1 where c5 = c4 or c6 = [['a']];

-- query 20
-- @skip_result_check=true
USE sql_tests_complex_test_array;
select * from array_data_type_1 where array_map((x) -> concat(x, 'a'), c5) = c4;

-- query 21
USE sql_tests_complex_test_array;
select c6[0] = ['a'] from array_data_type_1;

-- query 22
USE sql_tests_complex_test_array;
select c6[0] > array_map((x) -> concat(x, 'a'), c5) from array_data_type_1;

-- name: testArrayTopN
-- query 23
-- @skip_result_check=true
USE sql_tests_complex_test_array;
CREATE TABLE array_top_n
    (c1 int,
    c2 array<int>)
    PRIMARY KEY(c1)
    DISTRIBUTED BY HASH(c1)
    BUCKETS 1
    PROPERTIES ("replication_num" = "1");

-- query 24
-- @skip_result_check=true
USE sql_tests_complex_test_array;
insert into array_top_n values
(1, [1]),
(2, [5, 6]),
(3, [2, 3, 4]),
(4, [12, 13, 14, 15]),
(5, [7, 8, 9, 10, 11]);

-- query 25
USE sql_tests_complex_test_array;
select * from array_top_n order by c2[1];

-- query 26
USE sql_tests_complex_test_array;
select * from array_top_n order by c2[1] limit 1,10;

-- query 27
USE sql_tests_complex_test_array;
select * from array_top_n order by c2[1] limit 2,10;

-- query 28
USE sql_tests_complex_test_array;
select * from array_top_n order by c2[1] limit 3,10;

-- query 29
USE sql_tests_complex_test_array;
select * from array_top_n order by c2[1] limit 4,10;

-- query 30
-- @skip_result_check=true
USE sql_tests_complex_test_array;
select * from array_top_n order by c2[1] limit 5,10;

-- name: testArrayExpr
-- query 31
-- @skip_result_check=true
USE sql_tests_complex_test_array;
CREATE TABLE array_exprr
    (
    c1 int not null,
    c2 int not null
    )
    PRIMARY KEY(c1)
    DISTRIBUTED BY HASH(c1)
    BUCKETS 1
    PROPERTIES ("replication_num" = "1");

-- query 32
-- @skip_result_check=true
USE sql_tests_complex_test_array;
insert into array_exprr SELECT generate_series, generate_series FROM TABLE(generate_series(1,  13336));

-- query 33
USE sql_tests_complex_test_array;
select count([CAST(if(c2 is null, c1 + c2, 0) as DECIMAL128(38,0)) + if(c1 is null, c2 ,0)] is null) from array_exprr;

-- name: testEmptyArray
-- query 34
USE sql_tests_complex_test_array;
with t0 as (
    select c1 from (values([])) as t(c1)
)
select 
array_concat(c1, [1])
from t0;

-- query 35
USE sql_tests_complex_test_array;
with t0 as (
    select c1 from (values([])) as t(c1)
)
select 
array_concat([1], c1)
from t0;

-- query 36
USE sql_tests_complex_test_array;
select array_concat(c1, [[]])
from (select c1 from (values([])) as t(c1)) t;

-- query 37
USE sql_tests_complex_test_array;
select array_concat(c1, [[1]])
from (select c1 from (values([])) as t(c1)) t;

-- query 38
USE sql_tests_complex_test_array;
select array_concat(c1, [[1]])
from (select c1 from (values([[]])) as t(c1)) t;

-- query 39
USE sql_tests_complex_test_array;
select array_concat(c1, [map{'a':1}])
from (select c1 from (values([map()])) as t(c1)) t;

-- query 40
USE sql_tests_complex_test_array;
select array_concat(c1, [map{'a':1}])
from (select c1 from (values([])) as t(c1)) t;

-- query 41
USE sql_tests_complex_test_array;
select array_concat(c1, [named_struct('a', 1, 'b', 2, 'c', 3)])
from (select c1 from (values([])) as t(c1)) t;

-- query 42
-- @skip_result_check=true
USE sql_tests_complex_test_array;
CREATE TABLE `t2` (
  `pk` bigint(20) NOT NULL COMMENT "",
  `aas_1` array<array<array<varchar(65533)>>> NULL COMMENT "",
  `aad_1` array<array<array<DECIMAL128(26,2)>>> NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`pk`)
DISTRIBUTED BY HASH(`pk`) BUCKETS 3 
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"fast_schema_evolution" = "true",
"compression" = "LZ4"
);

-- query 43
-- @skip_result_check=true
USE sql_tests_complex_test_array;
insert into t2 values
(1, [[["10"],["20"],["30"]],[["60"],["5"],["4"]],[["-100","-2"],["-20","10"],["100","23"]]], [[[1.00],[2.00],[3.00]],[[6.00],[5.00],[4.00]],[[-1.00,-2.00],[-2.00,10.00],[100.00,23.00]]]);

-- query 44
USE sql_tests_complex_test_array;
select aad_1 != aas_1  from t2;

-- name: test_array_generate
-- query 45
USE sql_tests_complex_test_array;
select array_generate(1, array_length([1,2,3]),1);

-- query 46
USE sql_tests_complex_test_array;
select array_generate(1, NULL,1);

-- query 47
USE sql_tests_complex_test_array;
select array_generate(NULL,1,1);

-- query 48
USE sql_tests_complex_test_array;
select array_generate(1,1,NULL);

-- query 49
USE sql_tests_complex_test_array;
select array_generate(1,9);

-- query 50
USE sql_tests_complex_test_array;
select array_generate(9,1);

-- query 51
USE sql_tests_complex_test_array;
select array_generate(9);

-- query 52
USE sql_tests_complex_test_array;
select array_generate(3,3);

-- query 53
-- @skip_result_check=true
USE sql_tests_complex_test_array;
select array_generate(3,2,1);

-- query 54
USE sql_tests_complex_test_array;
select array_generate("2025-10-01", "2025-10-05",1);

-- query 55
USE sql_tests_complex_test_array;
select array_generate("2025-10-01", "2025-10-05",interval 1 day);

-- query 56
-- @skip_result_check=true
USE sql_tests_complex_test_array;
select array_generate("2025-10-01", "2025-10-05",interval 0 day);

-- query 57
USE sql_tests_complex_test_array;
select array_generate("2025-10-01", 10000, interval 0 day);

-- query 58
USE sql_tests_complex_test_array;
select array_generate("2025-10-01", "abc", 1);

-- query 59
-- @expect_error=array_generate requires step parameter must be non-negative
USE sql_tests_complex_test_array;
select array_generate("2025-10-01", "2025-10-05",interval -1 day);

-- query 60
USE sql_tests_complex_test_array;
select array_generate(DATE"2025-10-01", DATE"2025-10-05",interval 1 day);

-- query 61
USE sql_tests_complex_test_array;
select array_generate(DATETIME"2025-10-01", DATETIME"2025-10-05",interval 1 day);

-- query 62
USE sql_tests_complex_test_array;
select array_generate("2025-10-01", "2027-10-05",interval 1 year);

-- query 63
USE sql_tests_complex_test_array;
select array_generate("2025-10-01", "2027-10-05",interval 1 QUARTER);

-- query 64
USE sql_tests_complex_test_array;
select array_generate("2025-10-01", "2026-10-05",interval 2 MONTH);

-- query 65
USE sql_tests_complex_test_array;
select array_generate("2025-10-01", "2025-12-05",interval 2 WEEK);

-- query 66
USE sql_tests_complex_test_array;
select array_generate("2025-10-01", "2025-10-05",interval 2 day);

-- query 67
USE sql_tests_complex_test_array;
select array_generate("2025-10-01 14:28:31", "2025-10-01 23:12:36",interval 3 hour);

-- query 68
USE sql_tests_complex_test_array;
select array_generate("2025-10-01 14:28:31", "2025-10-01 15:12:36",interval 8 MINUTE);

-- query 69
USE sql_tests_complex_test_array;
select array_generate("2025-10-01 14:28:31", "2025-10-01 14:29:36",interval 10 SECOND);

-- query 70
USE sql_tests_complex_test_array;
select array_generate("2025-10-01 14:28:31", "2025-10-01 14:28:34",interval 500 MILLISECOND);

-- query 71
USE sql_tests_complex_test_array;
select array_generate("2025-10-01 14:28:31", "2025-10-01 14:28:32.800000",interval 500000 MICROSECOND);

-- query 72
USE sql_tests_complex_test_array;
select array_generate("2027-10-05", "2025-10-01",interval 1 year);

-- query 73
USE sql_tests_complex_test_array;
select array_generate("2027-10-05", "2025-10-01",interval 1 QUARTER);

-- query 74
USE sql_tests_complex_test_array;
select array_generate("2026-10-05", "2025-10-01",interval 2 MONTH);

-- query 75
USE sql_tests_complex_test_array;
select array_generate("2025-12-05", "2025-10-01",interval 2 WEEK);

-- query 76
USE sql_tests_complex_test_array;
select array_generate("2025-10-05", "2025-10-01",interval 2 day);

-- query 77
USE sql_tests_complex_test_array;
select array_generate("2025-10-01 23:12:36", "2025-10-01 14:28:31",interval 3 hour);

-- query 78
USE sql_tests_complex_test_array;
select array_generate("2025-10-01 15:12:36", "2025-10-01 14:28:31",interval 8 MINUTE);

-- query 79
USE sql_tests_complex_test_array;
select array_generate("2025-10-01 14:29:36", "2025-10-01 14:28:31",interval 10 SECOND);

-- query 80
USE sql_tests_complex_test_array;
select array_generate("2025-10-01 14:28:34", "2025-10-01 14:28:31",interval 500 MILLISECOND);

-- query 81
USE sql_tests_complex_test_array;
select array_generate("2025-10-01 14:28:32.800000", "2025-10-01 14:28:31",interval 500000 MICROSECOND);

-- query 82
-- @expect_error=array_generate requires step parameter must be non-negative
USE sql_tests_complex_test_array;
select array_generate("2025-10-05", "2025-10-01",interval -1 day);

-- query 83
-- @expect_error=array_generate requires step parameter must be a constant integer
USE sql_tests_complex_test_array;
select array_generate("2025-10-01 14:28:32.800000", "2025-10-01 14:28:31",NULL);

-- name: test_array_repeat
-- query 84
USE sql_tests_complex_test_array;
select array_repeat(1,5);

-- query 85
USE sql_tests_complex_test_array;
select array_repeat([1,2],3);

-- query 86
-- @skip_result_check=true
USE sql_tests_complex_test_array;
select array_repeat(1,-1);

-- query 87
-- @skip_result_check=true
USE sql_tests_complex_test_array;
CREATE TABLE IF NOT EXISTS repeat_test (COLA INT, COLB INT) PROPERTIES ("replication_num"="1");

-- query 88
-- @skip_result_check=true
USE sql_tests_complex_test_array;
INSERT INTO repeat_test (COLA, COLB) VALUES (1, 3), (NULL, 3), (2, NULL);

-- query 89
USE sql_tests_complex_test_array;
SELECT array_repeat(COLA, COLB) FROM repeat_test ORDER BY COLA;

-- name: test_array_flatten
-- query 90
USE sql_tests_complex_test_array;
select array_flatten([[1, 2], [1, 4]]);

-- query 91
USE sql_tests_complex_test_array;
select array_flatten([[[1],[2]],[[3],[4]]]);

-- query 92
-- @skip_result_check=true
USE sql_tests_complex_test_array;
CREATE TABLE IF NOT EXISTS flatten_test (COLA INT, COLB ARRAY<ARRAY<INT>>) PROPERTIES ("replication_num"="1");

-- query 93
-- @skip_result_check=true
USE sql_tests_complex_test_array;
INSERT INTO flatten_test (COLA, COLB) VALUES (1, [[1, 2], [1, 4]]), (2, NULL), (3, [[5], [6, 7, 8], [9]]), (4, [[2, 3], [4, 5, 6], NULL]);

-- query 94
USE sql_tests_complex_test_array;
SELECT array_flatten(COLB) FROM flatten_test ORDER BY COLA;

-- query 95
USE sql_tests_complex_test_array;
SELECT array_slice(array_flatten(COLB), 1, 2) FROM flatten_test ORDER BY COLA;

-- query 96
USE sql_tests_complex_test_array;
SELECT array_flatten(array_slice(COLB, 1, 2)) FROM flatten_test ORDER BY COLA;

-- query 97
-- @skip_result_check=true
USE sql_tests_complex_test_array;
CREATE TABLE IF NOT EXISTS flatten_one_layer_arr_test (COLA INT, COLB ARRAY<INT>) PROPERTIES ("replication_num"="1");

-- query 98
-- @skip_result_check=true
USE sql_tests_complex_test_array;
INSERT INTO flatten_one_layer_arr_test (COLA, COLB) VALUES (1, [1, 2, 3]);

-- query 99
-- @expect_error=The only one input of array_flatten should be an array of arrays, rather than array<int(11)>
USE sql_tests_complex_test_array;
SELECT array_flatten(COLB) FROM flatten_one_layer_arr_test ORDER BY COLA;

-- query 100
-- @expect_error=The only one input of array_flatten should be an array of arrays, rather than array<int(11)>
USE sql_tests_complex_test_array;
SELECT array_slice(array_flatten(COLB), 1, 2) FROM flatten_one_layer_arr_test ORDER BY COLA;

-- query 101
-- @expect_error=The only one input of array_flatten should be an array of arrays, rather than array<int(11)>
USE sql_tests_complex_test_array;
SELECT array_flatten(array_slice(COLB, 1, 2)) FROM flatten_one_layer_arr_test ORDER BY COLA;

-- name: null_or_empty
-- query 102
USE sql_tests_complex_test_array;
select null_or_empty([1, 2]);

-- query 103
USE sql_tests_complex_test_array;
select null_or_empty(null);

-- query 104
USE sql_tests_complex_test_array;
select null_or_empty([]);

-- query 105
USE sql_tests_complex_test_array;
select null_or_empty([[1,2],[1]]);

-- query 106
-- @skip_result_check=true
USE sql_tests_complex_test_array;
CREATE TABLE IF NOT EXISTS null_or_empty_test (COLA INT, COLB ARRAY<INT>) PROPERTIES ("replication_num"="1");

-- query 107
-- @skip_result_check=true
USE sql_tests_complex_test_array;
INSERT INTO null_or_empty_test (COLA, COLB) VALUES (1, []), (2, NULL), (3, [1,2]);

-- query 108
USE sql_tests_complex_test_array;
SELECT null_or_empty(COLB) FROM flatten_test ORDER BY COLA;

-- query 109
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_array FORCE;
