-- Migrated from dev/test/sql/test_array_fn/R/test_array_contains
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_array_contains FORCE;
CREATE DATABASE sql_tests_complex_test_array_contains;
USE sql_tests_complex_test_array_contains;

-- name: test_array_contains_with_const @mac
-- query 2
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
CREATE TABLE t ( 
pk bigint not null ,
str string,
arr_bigint array<bigint>,
arr_str array<string>,
arr_decimal array<decimal(38,5)>
) ENGINE=OLAP
DUPLICATE KEY(`pk`)
DISTRIBUTED BY HASH(`pk`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);

-- query 3
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
insert into t select generate_series, md5sum(generate_series), array_repeat(generate_series, 1000),array_repeat(md5sum(generate_series), 100), array_repeat(generate_series, 1000) from table(generate_series(0, 9999));

-- query 4
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
insert into t values (10000, md5sum(10000), array_append(array_generate(1000), null), array_append(array_repeat(md5sum(10000),100), null),array_append(array_generate(1000),null));

-- query 5
USE sql_tests_complex_test_array_contains;
select array_contains([1,2,3,4], 1) from t order by pk limit 10;

-- query 6
USE sql_tests_complex_test_array_contains;
select array_position([1,2,3,4], 1) from t order by pk limit 10;

-- query 7
USE sql_tests_complex_test_array_contains;
select array_contains([1,2,3,4], null) from t order by pk limit 10;

-- query 8
USE sql_tests_complex_test_array_contains;
select array_position([1,2,3,4], null) from t order by pk limit 10;

-- query 9
USE sql_tests_complex_test_array_contains;
select array_contains([1,2,3,null], null) from t order by pk limit 10;

-- query 10
USE sql_tests_complex_test_array_contains;
select array_position([1,2,3,null], null) from t order by pk limit 10;

-- query 11
USE sql_tests_complex_test_array_contains;
select array_contains(null, null) from t order by pk limit 10;

-- query 12
USE sql_tests_complex_test_array_contains;
select array_position(null, null) from t order by pk limit 10;

-- query 13
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
set @arr = array_generate(10000);

-- query 14
USE sql_tests_complex_test_array_contains;
select sum(array_contains(@arr, pk)) from t;

-- query 15
USE sql_tests_complex_test_array_contains;
select sum(array_contains(@arr, 100)) from t;

-- query 16
USE sql_tests_complex_test_array_contains;
select sum(array_position(@arr, pk)) from t;

-- query 17
USE sql_tests_complex_test_array_contains;
select sum(array_position(@arr, 100)) from t;

-- query 18
USE sql_tests_complex_test_array_contains;
select sum(array_contains(array_append(@arr, null), pk)) from t;

-- query 19
USE sql_tests_complex_test_array_contains;
select sum(array_contains(array_append(@arr, null), null)) from t;

-- query 20
USE sql_tests_complex_test_array_contains;
select sum(array_contains(arr_bigint, 100)) from t;

-- query 21
USE sql_tests_complex_test_array_contains;
select sum(array_position(arr_bigint, 100)) from t;

-- query 22
USE sql_tests_complex_test_array_contains;
select sum(array_contains(arr_str, md5sum(100))) from t;

-- query 23
USE sql_tests_complex_test_array_contains;
select sum(array_position(arr_str, md5sum(100))) from t;

-- query 24
USE sql_tests_complex_test_array_contains;
select sum(array_contains(arr_decimal, pk)) from t;

-- query 25
USE sql_tests_complex_test_array_contains;
select sum(array_position(arr_decimal, pk)) from t;

-- query 26
USE sql_tests_complex_test_array_contains;
select sum(array_contains(arr_decimal, 100)) from t;

-- query 27
USE sql_tests_complex_test_array_contains;
select sum(array_position(arr_decimal, 100)) from t;

-- query 28
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
set @arr = array_repeat("abcdefg", 1000000);

-- query 29
USE sql_tests_complex_test_array_contains;
select sum(array_contains(@arr, "abcdefg")) from t;

-- query 30
USE sql_tests_complex_test_array_contains;
select sum(array_contains(@arr, str)) from t;

-- name: test_array_contains_with_decimal
-- query 31
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
DROP TABLE IF EXISTS t;
create table t (
    k bigint,
    v1 array<decimal(38,5)>,
    v2 array<array<decimal(38,5)>>,
    v3 array<array<array<decimal(38,5)>>>
) duplicate key (`k`)
distributed by random buckets 1
properties('replication_num'='1');

-- query 32
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
insert into t values (1,[1.1], [[1.1]],[[[1.1]]]);

-- query 33
USE sql_tests_complex_test_array_contains;
select array_contains(v1, 1.1) from t;

-- query 34
USE sql_tests_complex_test_array_contains;
select array_contains(v2, [1.1]) from t;

-- query 35
USE sql_tests_complex_test_array_contains;
select array_contains(v3, [[1.1]]) from t;

-- query 36
USE sql_tests_complex_test_array_contains;
select array_contains(v2, v1) from t;

-- query 37
USE sql_tests_complex_test_array_contains;
select array_contains(v3, v2) from t;

-- query 38
USE sql_tests_complex_test_array_contains;
select array_position(v1, 1.1) from t;

-- query 39
USE sql_tests_complex_test_array_contains;
select array_position(v2, [1.1]) from t;

-- query 40
USE sql_tests_complex_test_array_contains;
select array_position(v3, [[1.1]]) from t;

-- query 41
USE sql_tests_complex_test_array_contains;
select array_position(v2, v1) from t;

-- query 42
USE sql_tests_complex_test_array_contains;
select array_position(v3, v2) from t;

-- name: test_array_contains_all_and_seq
-- query 43
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
DROP TABLE IF EXISTS t;
CREATE TABLE t (
  k bigint(20) NOT NULL,
  arr_0 array<bigint(20)> NOT NULL,
  arr_1 array<bigint(20)>,
  arr_2 array<bigint(20)>
) ENGINE=OLAP
DUPLICATE KEY(`k`)
DISTRIBUTED BY RANDOM BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

-- query 44
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
insert into t values 
(1, [1,2,3], [1,2], [1]),
(2, [1,2,null], [1,null], [null]),
(3, [1,2,null],[3],[3]),
(4, [1,2,null], null, [1,2,null]),
(5, [1,2,null], [1,2,null], null),
(6, [1,2,3],[],[]),
(7, [null,null], [null,null,null], [null,null]),
(8, [1,1,1,1,1,2], [1,2], [1]),
(9, [1,1,1,1,1,null,2],[1,null,2],[null,2]);

-- query 45
USE sql_tests_complex_test_array_contains;
select array_contains_all(arr_0, arr_1) from t order by k;

-- query 46
USE sql_tests_complex_test_array_contains;
select array_contains_all(arr_1, arr_0) from t order by k;

-- query 47
USE sql_tests_complex_test_array_contains;
select array_contains_all(arr_0, arr_2) from t order by k;

-- query 48
USE sql_tests_complex_test_array_contains;
select array_contains_all(arr_2, arr_0) from t order by k;

-- query 49
USE sql_tests_complex_test_array_contains;
select array_contains_all(arr_1, arr_2) from t order by k;

-- query 50
USE sql_tests_complex_test_array_contains;
select array_contains_all(arr_2, arr_1) from t order by k;

-- query 51
USE sql_tests_complex_test_array_contains;
select array_contains_all([1,2,3,4], arr_0) from t order by k;

-- query 52
USE sql_tests_complex_test_array_contains;
select array_contains_all([1,2,3,4], arr_1) from t order by k;

-- query 53
USE sql_tests_complex_test_array_contains;
select array_contains_all([1,2,3,4,null], arr_1) from t order by k;

-- query 54
USE sql_tests_complex_test_array_contains;
select array_contains_all(arr_0, [1,null]) from t order by k;

-- query 55
USE sql_tests_complex_test_array_contains;
select array_contains_all(arr_0, []) from t order by k;

-- query 56
USE sql_tests_complex_test_array_contains;
select array_contains_all(null, arr_0) from t order by k;

-- query 57
USE sql_tests_complex_test_array_contains;
select array_contains_all(arr_1, null) from t order by k;

-- query 58
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
set @arr0 = array_repeat("abcdefg", 10000);

-- query 59
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
set @arr1 = array_repeat("abcdef", 100000);

-- query 60
USE sql_tests_complex_test_array_contains;
select array_contains_all(@arr0, @arr1);

-- query 61
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
set @arr0 = array_generate(10000);

-- query 62
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
set @arr1 = array_generate(20000);

-- query 63
USE sql_tests_complex_test_array_contains;
select array_contains_all(@arr0, @arr1);

-- query 64
USE sql_tests_complex_test_array_contains;
select array_contains_all(@arr1, @arr0);

-- query 65
USE sql_tests_complex_test_array_contains;
select array_contains_seq(arr_0, arr_1) from t order by k;

-- query 66
USE sql_tests_complex_test_array_contains;
select array_contains_seq(arr_1, arr_0) from t order by k;

-- query 67
USE sql_tests_complex_test_array_contains;
select array_contains_seq(arr_0, arr_2) from t order by k;

-- query 68
USE sql_tests_complex_test_array_contains;
select array_contains_seq(arr_2, arr_0) from t order by k;

-- query 69
USE sql_tests_complex_test_array_contains;
select array_contains_seq(arr_1, arr_2) from t order by k;

-- query 70
USE sql_tests_complex_test_array_contains;
select array_contains_seq(arr_2, arr_1) from t order by k;

-- query 71
USE sql_tests_complex_test_array_contains;
select array_contains_seq([1,2,3,4], arr_0) from t order by k;

-- query 72
USE sql_tests_complex_test_array_contains;
select array_contains_seq([1,2,3,4], arr_1) from t order by k;

-- query 73
USE sql_tests_complex_test_array_contains;
select array_contains_seq([1,2,3,4,null], arr_1) from t order by k;

-- query 74
USE sql_tests_complex_test_array_contains;
select array_contains_seq(arr_0, [1,null]) from t order by k;

-- query 75
USE sql_tests_complex_test_array_contains;
select array_contains_seq(arr_0, []) from t order by k;

-- query 76
USE sql_tests_complex_test_array_contains;
select array_contains_seq(null, arr_0) from t order by k;

-- query 77
USE sql_tests_complex_test_array_contains;
select array_contains_seq(arr_1, null) from t order by k;

-- query 78
USE sql_tests_complex_test_array_contains;
select array_contains_seq([1,1,2,3],[1,1]);

-- query 79
USE sql_tests_complex_test_array_contains;
select array_contains_seq([1,1,2,3],[1,2]);

-- query 80
USE sql_tests_complex_test_array_contains;
select array_contains_seq([1,1,2,3],[1,3]);

-- query 81
USE sql_tests_complex_test_array_contains;
select array_contains_seq([1,1,2,3],[2,3]);

-- query 82
USE sql_tests_complex_test_array_contains;
select array_contains_seq([1,1,2,3],[1,1,2]);

-- query 83
USE sql_tests_complex_test_array_contains;
select array_contains_seq([null,null,1,2],[null]);

-- query 84
USE sql_tests_complex_test_array_contains;
select array_contains_seq([null,null,1,2],[null,null]);

-- query 85
USE sql_tests_complex_test_array_contains;
select array_contains_seq([null,null,1,2],[null,1]);

-- query 86
USE sql_tests_complex_test_array_contains;
select array_contains_seq([null,null,1,2],[null,null,1]);

-- query 87
USE sql_tests_complex_test_array_contains;
select array_contains_seq([null,null,1,2],[null,1,2]);

-- query 88
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
set @arr0 = array_append(array_repeat(1, 10000), 2);

-- query 89
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
set @arr1 = array_append(array_repeat(1, 5000), 2);

-- query 90
USE sql_tests_complex_test_array_contains;
select array_contains_seq(@arr0, @arr1);

-- query 91
-- @expect_error=2-th input of array_contains_seq should be an array, rather than varchar
USE sql_tests_complex_test_array_contains;
select array_contains_seq(['abc'],'a');

-- query 92
-- @expect_error=1-th input of array_contains_seq should be an array, rather than varchar
USE sql_tests_complex_test_array_contains;
select array_contains_seq('abc',['a']);

-- name: test_array_contains_all_type
-- query 93
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
CREATE TABLE test_array_contains (
    id INT,
    array_boolean ARRAY<BOOLEAN>,
    array_tinyint ARRAY<TINYINT>,
    array_smallint ARRAY<SMALLINT>,
    array_int ARRAY<INT>,
    array_bigint ARRAY<BIGINT>,
    array_largeint ARRAY<LARGEINT>,
    array_decimalv2 ARRAY<DECIMALV2(10, 2)>,
    array_decimal32 ARRAY<DECIMAL32(9, 2)>,
    array_decimal64 ARRAY<DECIMAL64(18, 2)>,
    array_decimal128 ARRAY<DECIMAL128(38, 10)>,
    array_float ARRAY<FLOAT>,
    array_double ARRAY<DOUBLE>,
    array_varchar ARRAY<VARCHAR(100)>,
    array_date ARRAY<DATE>,
    array_datetime ARRAY<DATETIME>
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

-- query 94
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
INSERT INTO test_array_contains VALUES
(1, [true, false, NULL], [1, 2, NULL], [100, 200, NULL], [10, 20, NULL], [1000, 2000, NULL], [1234567890123456789, NULL, NULL],
 [12345.67, 89012.34, NULL], [123.45, 678.90, NULL], [12345678.90, 9876543.21, NULL], [1234567890.1234567890, NULL, NULL], 
 [1.23, 4.56, NULL], [123.456, 789.012, NULL], ['hello', NULL, 'starrocks'], 
 ['2025-01-01', '2025-01-02', NULL], ['2025-01-01 12:00:00', NULL, '2025-01-02 14:00:00']),
(2, NULL, [5, NULL, 3], [500, 400, NULL], [50, 40, NULL], [5000, NULL, 3000], [987654321987654321, NULL, NULL], 
 [56789.01, NULL, 45678.12], [345.67, NULL, 234.56], [56789012.34, NULL, 34567890.12], [2345678901.2345678901, NULL, NULL], 
 [2.34, NULL, 1.23], [234.567, NULL, 123.456], [NULL, 'array', 'test'], 
 [NULL, '2024-12-31', '2024-12-30'], ['2024-12-31 23:59:59', '2024-12-30 12:00:00', NULL]),
(3, [true, NULL, false], [1, 6, NULL], [150, NULL, 250], [15, NULL, 35], [1100, NULL, 2200], [2345678901234567890, NULL, NULL],
 [67890.12, NULL, 56789.01], [456.78, NULL, 345.67], [67890123.45, NULL, 56789012.34], [3456789012.3456789012, NULL, NULL],
 [1.11, NULL, 3.33], [222.333, NULL, 333.444], ['foo', 'bar', NULL], 
 ['2025-01-03', NULL, '2025-01-04'], ['2025-01-03 16:00:00', '2025-01-04 18:00:00', NULL]),
(4, [NULL, true, false], [NULL, 20, 10], [110, 220, NULL], [NULL, 12, 24], [1200, NULL, 2400], [3456789012345678901, NULL, NULL],
 [78901.23, 67890.12, NULL], [567.89, 456.78, NULL], [78901234.56, 67890123.45, NULL], [4567890123.4567890123, NULL, NULL],
 [NULL, 5.55, 4.44], [NULL, 777.888, 666.777], ['NULL', 'banana', 'apple'], 
 [NULL, '2025-01-05', '2025-01-06'], [NULL, '2025-01-06 20:00:00', '2025-01-05 18:00:00']),
(5, [false, NULL, true], [10, NULL, 30], [300, 400, NULL], [70, NULL, 90], [4000, NULL, 6000], [987654321234567890, NULL, NULL],
 [123456.78, NULL, 876543.21], [678.90, NULL, 789.01], [9876543.21, NULL, 1234567.89], [5678901234.5678901234, NULL, NULL],
 [3.21, 4.32, NULL], [111.222, NULL, 333.444], ['dog', 'cat', NULL], 
 ['2025-01-07', '2025-01-08', NULL], ['2025-01-07 10:00:00', NULL, '2025-01-08 15:00:00']),
(6, [NULL, true, true], [NULL, 40, 50], [450, 500, NULL], [80, 100, NULL], [1500, NULL, 2500], [765432198765432109, NULL, NULL],
 [34567.89, NULL, 12345.67], [123.45, NULL, 678.90], [54321.12, NULL, 12345.67], [7654321098.7654321098, NULL, NULL],
 [NULL, 6.54, 7.65], [555.666, NULL, 444.333], [NULL, 'bird', 'fish'], 
 ['2025-01-09', '2025-01-10', NULL], ['2025-01-09 12:00:00', NULL, '2025-01-10 18:00:00']),
(7, [false, false, NULL], [70, NULL, 90], [650, NULL, 750], [120, 140, NULL], [8000, NULL, 9000], [543210987654321098, NULL, NULL],
 [45678.12, NULL, 23456.78], [234.56, NULL, 456.78], [67890123.45, NULL, 34567890.12], [4321098765.4321098765, NULL, NULL],
 [7.89, 8.90, NULL], [333.222, NULL, 111.000], ['lion', NULL, 'tiger'], 
 ['2025-01-11', '2025-01-12', NULL], ['2025-01-11 20:00:00', NULL, '2025-01-12 22:00:00']),
(8, [true, NULL, false], [5, 15, NULL], [50, NULL, 150], [25, NULL, 75], [4500, NULL, 5500], [321098765432109876, NULL, NULL],
 [23456.78, NULL, 12345.67], [345.67, NULL, 456.78], [8901234.56, NULL, 7890123.45], [2109876543.2109876543, NULL, NULL],
 [5.67, NULL, 4.56], [666.555, NULL, 222.111], [NULL, 'grape', 'pear'], 
 ['2025-01-13', NULL, '2025-01-14'], ['2025-01-13 23:59:59', '2025-01-14 12:00:00', NULL]),
(9, [false, true, NULL], [25, 35, NULL], [350, NULL, 450], [100, 200, NULL], [6000, NULL, 7000], [654321098765432109, NULL, NULL],
 [67890.12, NULL, 34567.89], [456.78, NULL, 234.56], [34567890.12, NULL, 23456789.01], [8765432109.8765432109, NULL, NULL],
 [9.87, NULL, 8.76], [444.333, NULL, 555.222], ['watermelon', NULL, 'kiwi'], 
 [NULL, '2025-01-15', '2025-01-16'], ['2025-01-15 12:00:00', NULL, '2025-01-16 18:00:00']),
(10, [true, true, NULL], [50, 70, NULL], [750, 850, NULL], [300, 400, NULL], [10000, NULL, 12000], [789012345678901234, NULL, NULL],
 [78901.23, NULL, 67890.12], [567.89, NULL, 456.78], [12345678.90, NULL, 9876543.21], [1234567890.1234567890, NULL, NULL],
 [NULL, 1.11, 2.22], [777.888, 999.000, NULL], ['blueberry', 'cherry', NULL], 
 ['2025-01-17', '2025-01-18', NULL], [NULL, '2025-01-17 10:00:00', '2025-01-18 20:00:00']);

-- query 95
-- @expect_error=id
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_boolean, true) ORDER BY idSELECT id FROM test_array_contains WHERE array_contains(array_boolean, true) ORDER BY id;;

-- query 96
-- @expect_error=id
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_boolean, NULL) ORDER BY idSELECT id FROM test_array_contains WHERE array_contains(array_boolean, NULL) ORDER BY id;;

-- query 97
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_boolean, false) ORDER BY id;

-- query 98
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_tinyint, 5) ORDER BY id;

-- query 99
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_tinyint, NULL) ORDER BY id;

-- query 100
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_tinyint, 20) ORDER BY id;

-- query 101
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_smallint, 100) ORDER BY id;

-- query 102
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_smallint, NULL) ORDER BY id;

-- query 103
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_smallint, 300) ORDER BY id;

-- query 104
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_int, 50) ORDER BY id;

-- query 105
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_int, NULL) ORDER BY id;

-- query 106
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_int, 90) ORDER BY id;

-- query 107
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_bigint, 4000) ORDER BY id;

-- query 108
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_bigint, NULL) ORDER BY id;

-- query 109
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_bigint, 8000) ORDER BY id;

-- query 110
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_largeint, 1234567890123456789) ORDER BY id;

-- query 111
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_largeint, NULL) ORDER BY id;

-- query 112
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_largeint, 765432198765432109) ORDER BY id;

-- query 113
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_decimalv2, 12345.67) ORDER BY id;

-- query 114
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_decimalv2, NULL) ORDER BY id;

-- query 115
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_decimalv2, 56789.01) ORDER BY id;

-- query 116
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_decimal32, 123.45) ORDER BY id;

-- query 117
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_decimal32, NULL) ORDER BY id;

-- query 118
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_decimal32, 567.89) ORDER BY id;

-- query 119
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_decimal64, 12345678.90) ORDER BY id;

-- query 120
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_decimal64, NULL) ORDER BY id;

-- query 121
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_decimal64, 7890123.45) ORDER BY id;

-- query 122
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_decimal128, 1234567890.1234567890) ORDER BY id;

-- query 123
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_decimal128, NULL) ORDER BY id;

-- query 124
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_decimal128, 8765432109.8765432109) ORDER BY id;

-- query 125
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_float, 1.23) ORDER BY id;

-- query 126
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_float, NULL) ORDER BY id;

-- query 127
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_float, 7.89) ORDER BY id;

-- query 128
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_double, 123.456) ORDER BY id;

-- query 129
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_double, NULL) ORDER BY id;

-- query 130
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_double, 444.333) ORDER BY id;

-- query 131
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_varchar, 'hello') ORDER BY id;

-- query 132
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_varchar, NULL) ORDER BY id;

-- query 133
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_varchar, 'starrocks') ORDER BY id;

-- query 134
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_date, '2025-01-01') ORDER BY id;

-- query 135
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_date, NULL) ORDER BY id;

-- query 136
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_date, '2025-01-13') ORDER BY id;

-- query 137
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_datetime, '2025-01-01 12:00:00') ORDER BY id;

-- query 138
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_datetime, NULL) ORDER BY id;

-- query 139
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains(array_datetime, '2025-01-17 10:00:00') ORDER BY id;

-- query 140
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_boolean, true) AS position FROM test_array_contains ORDER BY id;

-- query 141
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_boolean, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 142
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_boolean, false) AS position FROM test_array_contains ORDER BY id;

-- query 143
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_tinyint, 5) AS position FROM test_array_contains ORDER BY id;

-- query 144
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_tinyint, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 145
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_tinyint, 20) AS position FROM test_array_contains ORDER BY id;

-- query 146
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_smallint, 100) AS position FROM test_array_contains ORDER BY id;

-- query 147
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_smallint, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 148
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_smallint, 300) AS position FROM test_array_contains ORDER BY id;

-- query 149
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_int, 50) AS position FROM test_array_contains ORDER BY id;

-- query 150
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_int, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 151
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_int, 90) AS position FROM test_array_contains ORDER BY id;

-- query 152
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_bigint, 4000) AS position FROM test_array_contains ORDER BY id;

-- query 153
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_bigint, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 154
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_bigint, 8000) AS position FROM test_array_contains ORDER BY id;

-- query 155
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_largeint, 1234567890123456789) AS position FROM test_array_contains ORDER BY id;

-- query 156
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_largeint, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 157
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_largeint, 765432198765432109) AS position FROM test_array_contains ORDER BY id;

-- query 158
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_decimalv2, 12345.67) AS position FROM test_array_contains ORDER BY id;

-- query 159
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_decimalv2, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 160
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_decimalv2, 56789.01) AS position FROM test_array_contains ORDER BY id;

-- query 161
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_decimal32, 123.45) AS position FROM test_array_contains ORDER BY id;

-- query 162
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_decimal32, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 163
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_decimal32, 567.89) AS position FROM test_array_contains ORDER BY id;

-- query 164
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_decimal64, 12345678.90) AS position FROM test_array_contains ORDER BY id;

-- query 165
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_decimal64, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 166
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_decimal64, 7890123.45) AS position FROM test_array_contains ORDER BY id;

-- query 167
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_decimal128, 1234567890.1234567890) AS position FROM test_array_contains ORDER BY id;

-- query 168
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_decimal128, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 169
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_decimal128, 8765432109.8765432109) AS position FROM test_array_contains ORDER BY id;

-- query 170
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_float, 1.23) AS position FROM test_array_contains ORDER BY id;

-- query 171
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_float, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 172
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_float, 7.89) AS position FROM test_array_contains ORDER BY id;

-- query 173
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_double, 123.456) AS position FROM test_array_contains ORDER BY id;

-- query 174
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_double, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 175
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_double, 444.333) AS position FROM test_array_contains ORDER BY id;

-- query 176
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_varchar, 'hello') AS position FROM test_array_contains ORDER BY id;

-- query 177
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_varchar, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 178
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_varchar, 'starrocks') AS position FROM test_array_contains ORDER BY id;

-- query 179
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_date, '2025-01-01') AS position FROM test_array_contains ORDER BY id;

-- query 180
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_date, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 181
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_date, '2025-01-13') AS position FROM test_array_contains ORDER BY id;

-- query 182
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_datetime, '2025-01-01 12:00:00') AS position FROM test_array_contains ORDER BY id;

-- query 183
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_datetime, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 184
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_datetime, '2025-01-17 10:00:00') AS position FROM test_array_contains ORDER BY id;

-- query 185
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_boolean, [true, false]) ORDER BY id;

-- query 186
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_boolean, [NULL]) ORDER BY id;

-- query 187
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_boolean, [true, false]) ORDER BY id;

-- query 188
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_tinyint, [5, 10]) ORDER BY id;

-- query 189
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_tinyint, [NULL]) ORDER BY id;

-- query 190
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_tinyint, [5, 10]) ORDER BY id;

-- query 191
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_smallint, [100, 200]) ORDER BY id;

-- query 192
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_smallint, [NULL]) ORDER BY id;

-- query 193
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_smallint, [100, 200]) ORDER BY id;

-- query 194
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_int, [50, 60]) ORDER BY id;

-- query 195
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_int, [NULL]) ORDER BY id;

-- query 196
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_int, [50, 60]) ORDER BY id;

-- query 197
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_bigint, [4000, 6000]) ORDER BY id;

-- query 198
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_bigint, [NULL]) ORDER BY id;

-- query 199
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_bigint, [4000, 6000]) ORDER BY id;

-- query 200
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_largeint, [1234567890123456789, 987654321098765432]) ORDER BY id;

-- query 201
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_largeint, [NULL]) ORDER BY id;

-- query 202
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_largeint, [1234567890123456789, 987654321098765432]) ORDER BY id;

-- query 203
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_decimalv2, [12345.67, 78901.23]) ORDER BY id;

-- query 204
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_decimalv2, [NULL]) ORDER BY id;

-- query 205
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_decimalv2, [12345.67, 78901.23]) ORDER BY id;

-- query 206
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_decimal32, [123.45, 567.89]) ORDER BY id;

-- query 207
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_decimal32, [NULL]) ORDER BY id;

-- query 208
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_decimal32, [123.45, 567.89]) ORDER BY id;

-- query 209
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_decimal64, [12345678.90, 56789012.34]) ORDER BY id;

-- query 210
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_decimal64, [NULL]) ORDER BY id;

-- query 211
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_decimal64, [12345678.90, 56789012.34]) ORDER BY id;

-- query 212
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_decimal128, [1234567890.1234567890, 8765432109.8765432109]) ORDER BY id;

-- query 213
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_decimal128, [NULL]) ORDER BY id;

-- query 214
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_decimal128, [1234567890.1234567890, 8765432109.8765432109]) ORDER BY id;

-- query 215
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_float, [1.23, 4.56]) ORDER BY id;

-- query 216
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_float, [NULL]) ORDER BY id;

-- query 217
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_float, [1.23, 4.56]) ORDER BY id;

-- query 218
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_double, [123.456, 789.012]) ORDER BY id;

-- query 219
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_double, [NULL]) ORDER BY id;

-- query 220
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_double, [123.456, 789.012]) ORDER BY id;

-- query 221
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_varchar, ['hello', 'world']) ORDER BY id;

-- query 222
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_varchar, [NULL]) ORDER BY id;

-- query 223
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_varchar, ['hello', 'world']) ORDER BY id;

-- query 224
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_date, ['2025-01-01', '2025-01-13']) ORDER BY id;

-- query 225
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_date, [NULL]) ORDER BY id;

-- query 226
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_date, ['2025-01-01', '2025-01-13']) ORDER BY id;

-- query 227
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_datetime, ['2025-01-01 12:00:00', '2025-01-17 10:00:00']) ORDER BY id;

-- query 228
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_datetime, [NULL]) ORDER BY id;

-- query 229
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_datetime, ['2025-01-01 12:00:00', '2025-01-17 10:00:00']) ORDER BY id;

-- query 230
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([true, false], array_boolean) ORDER BY id;

-- query 231
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([NULL], array_boolean) ORDER BY id;

-- query 232
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq([true, false], array_boolean) ORDER BY id;

-- query 233
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([5, 10], array_tinyint) ORDER BY id;

-- query 234
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([NULL], array_tinyint) ORDER BY id;

-- query 235
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq([5, 10], array_tinyint) ORDER BY id;

-- query 236
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([100, 200], array_smallint) ORDER BY id;

-- query 237
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([NULL], array_smallint) ORDER BY id;

-- query 238
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq([100, 200], array_smallint) ORDER BY id;

-- query 239
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([50, 60], array_int) ORDER BY id;

-- query 240
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([NULL], array_int) ORDER BY id;

-- query 241
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq([50, 60], array_int) ORDER BY id;

-- query 242
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([4000, 6000], array_bigint) ORDER BY id;

-- query 243
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([NULL], array_bigint) ORDER BY id;

-- query 244
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq([4000, 6000], array_bigint) ORDER BY id;

-- query 245
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([1234567890123456789, 987654321098765432], array_largeint) ORDER BY id;

-- query 246
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([NULL], array_largeint) ORDER BY id;

-- query 247
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq([1234567890123456789, 987654321098765432], array_largeint) ORDER BY id;

-- query 248
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([12345.67, 78901.23], array_decimalv2) ORDER BY id;

-- query 249
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([NULL], array_decimalv2) ORDER BY id;

-- query 250
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq([12345.67, 78901.23], array_decimalv2) ORDER BY id;

-- query 251
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([123.45, 567.89], array_decimal32) ORDER BY id;

-- query 252
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([NULL], array_decimal32) ORDER BY id;

-- query 253
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq([123.45, 567.89], array_decimal32) ORDER BY id;

-- query 254
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([12345678.90, 56789012.34], array_decimal64) ORDER BY id;

-- query 255
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([NULL], array_decimal64) ORDER BY id;

-- query 256
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq([12345678.90, 56789012.34], array_decimal64) ORDER BY id;

-- query 257
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([1234567890.1234567890, 8765432109.8765432109], array_decimal128) ORDER BY id;

-- query 258
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([NULL], array_decimal128) ORDER BY id;

-- query 259
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq([1234567890.1234567890, 8765432109.8765432109], array_decimal128) ORDER BY id;

-- query 260
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([1.23, 4.56], array_float) ORDER BY id;

-- query 261
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([NULL], array_float) ORDER BY id;

-- query 262
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq([1.23, 4.56], array_float) ORDER BY id;

-- query 263
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([123.456, 789.012], array_double) ORDER BY id;

-- query 264
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([NULL], array_double) ORDER BY id;

-- query 265
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq([123.456, 789.012], array_double) ORDER BY id;

-- query 266
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(['hello', 'world'], array_varchar) ORDER BY id;

-- query 267
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([NULL], array_varchar) ORDER BY id;

-- query 268
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(['hello', 'world'], array_varchar) ORDER BY id;

-- query 269
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(['2025-01-01', '2025-01-13'], array_date) ORDER BY id;

-- query 270
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([NULL], array_date) ORDER BY id;

-- query 271
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(['2025-01-01', '2025-01-13'], array_date) ORDER BY id;

-- query 272
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(['2025-01-01 12:00:00', '2025-01-17 10:00:00'], array_datetime) ORDER BY id;

-- query 273
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all([NULL], array_datetime) ORDER BY id;

-- query 274
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(['2025-01-01 12:00:00', '2025-01-17 10:00:00'], array_datetime) ORDER BY id;

-- query 275
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_boolean, []) ORDER BY id;

-- query 276
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_boolean, []) ORDER BY id;

-- query 277
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_tinyint, [1, 2, 3, 4, 5]) ORDER BY id;

-- query 278
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_tinyint, [1, 2, 3, 4, 5]) ORDER BY id;

-- query 279
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_smallint, [10, 20, 30, 40, 50]) ORDER BY id;

-- query 280
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_smallint, [10, 20, 30, 40, 50]) ORDER BY id;

-- query 281
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_int, [100, 200, 300, 400, 500]) ORDER BY id;

-- query 282
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_int, [100, 200, 300, 400, 500]) ORDER BY id;

-- query 283
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_all(array_varchar, ['a', 'b', 'c', 'd', 'e']) ORDER BY id;

-- query 284
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains WHERE array_contains_seq(array_varchar, ['a', 'b', 'c', 'd', 'e']) ORDER BY id;

-- name: test_array_contains_complex_type
-- query 285
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
CREATE TABLE test_array_contains_complex_type (
    id INT,
    array_map ARRAY<MAP<STRING, INT>>,
    array_json ARRAY<JSON>,
    array_struct ARRAY<STRUCT<f1 INT, f2 STRING>>
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 286
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
INSERT INTO test_array_contains_complex_type VALUES
(1, [map{'a': 1, 'b': 2}, map{'c': 3, 'd': 4}], ['{"key1": 100, "key2": 200}', '{"key3": 300}'], [row(10, 'hello'), row(20, 'world')]),
(2, [map{'x': 5, 'y': 6}], ['{"key4": 400}', '{"key5": 500, "key6": 600}'], [row(30,'starrocks'), row(40, 'database')]),
(3, NULL, NULL, NULL),
(4, [], [], []);

-- query 287
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_all([map{'a': 1, 'b': 2}], array_map) ORDER BY id;

-- query 288
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq([map{'c': 3, 'd': 4}], array_map) ORDER BY id;

-- query 289
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_all([NULL], array_map) ORDER BY id;

-- query 290
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq([], array_map) ORDER BY id;

-- query 291
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_all(['{"key1": 100}'], array_json) ORDER BY id;

-- query 292
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq(['{"key3": 300}'], array_json) ORDER BY id;

-- query 293
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_all(['{"key5": 500, "key6": 600}'], array_json) ORDER BY id;

-- query 294
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq([NULL], array_json) ORDER BY id;

-- query 295
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_all([], array_json) ORDER BY id;

-- query 296
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_all([row(10,'hello')], array_struct) ORDER BY id;

-- query 297
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq([row(20,'world')], array_struct) ORDER BY id;

-- query 298
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_all([row(40,'database')], array_struct) ORDER BY id;

-- query 299
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq([NULL], array_struct) ORDER BY id;

-- query 300
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_all([], array_struct) ORDER BY id;

-- query 301
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_all(array_map, [map{'a': 1}]) ORDER BY id;

-- query 302
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq(array_json, ['{"key3": 300}']) ORDER BY id;

-- query 303
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_all(array_struct, [row(10, 'hello')]) ORDER BY id;

-- query 304
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq(array_map, []) ORDER BY id;

-- query 305
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_map, map{'a': 1, 'b': 2}) ORDER BY id;

-- query 306
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_map, map{'c': 3, 'd': 4}) ORDER BY id;

-- query 307
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_map, map{'x': 5, 'y': 6}) ORDER BY id;

-- query 308
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_map, NULL) ORDER BY id;

-- query 309
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_map, map{}) ORDER BY id;

-- query 310
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_json, '{"key1": 100, "key2": 200}') ORDER BY id;

-- query 311
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_json, '{"key3": 300}') ORDER BY id;

-- query 312
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_json, '{"key5": 500, "key6": 600}') ORDER BY id;

-- query 313
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_json, NULL) ORDER BY id;

-- query 314
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_json, '{}') ORDER BY id;

-- query 315
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_struct, row(10, 'hello')) ORDER BY id;

-- query 316
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_struct, row(20, 'world')) ORDER BY id;

-- query 317
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_struct, row(40, 'database')) ORDER BY id;

-- query 318
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_struct, NULL) ORDER BY id;

-- query 319
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_map, map{'a': 1, 'b': 2}) FROM test_array_contains_complex_type ORDER BY id;

-- query 320
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_map, map{'c': 3, 'd': 4}) FROM test_array_contains_complex_type ORDER BY id;

-- query 321
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_map, map{'x': 5, 'y': 6}) FROM test_array_contains_complex_type ORDER BY id;

-- query 322
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_map, NULL) FROM test_array_contains_complex_type ORDER BY id;

-- query 323
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_map, map{}) FROM test_array_contains_complex_type ORDER BY id;

-- query 324
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_json, '{"key1": 100, "key2": 200}') FROM test_array_contains_complex_type ORDER BY id;

-- query 325
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_json, '{"key3": 300}') FROM test_array_contains_complex_type ORDER BY id;

-- query 326
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_json, '{"key5": 500, "key6": 600}') FROM test_array_contains_complex_type ORDER BY id;

-- query 327
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_json, NULL) FROM test_array_contains_complex_type ORDER BY id;

-- query 328
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_json, '{}') FROM test_array_contains_complex_type ORDER BY id;

-- query 329
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_struct, row(10, 'hello')) FROM test_array_contains_complex_type ORDER BY id;

-- query 330
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_struct, row(20, 'world')) FROM test_array_contains_complex_type ORDER BY id;

-- query 331
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_struct, row(40, 'database')) FROM test_array_contains_complex_type ORDER BY id;

-- query 332
USE sql_tests_complex_test_array_contains;
SELECT id, array_position(array_struct, NULL) FROM test_array_contains_complex_type ORDER BY id;

-- query 333
USE sql_tests_complex_test_array_contains;
select array_contains([null], null), array_position([null], null);

-- name: test_array_contains_with_null
-- query 334
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
DROP TABLE IF EXISTS t;
CREATE TABLE t ( 
pk bigint not null ,
str string,
arr_bigint array<bigint>,
arr_str array<string>,
arr_decimal array<decimal(38,5)>
) ENGINE=OLAP
DUPLICATE KEY(`pk`)
DISTRIBUTED BY HASH(`pk`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);

-- query 335
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
insert into t select generate_series, md5sum(generate_series), array_repeat(generate_series, 10),array_repeat(md5sum(generate_series), 10), array_repeat(generate_series, 1000) from table(generate_series(0, 9999));

-- query 336
-- @skip_result_check=true
USE sql_tests_complex_test_array_contains;
insert into t select 1, null, null, null, null;

-- query 337
USE sql_tests_complex_test_array_contains;
select /*+ set_var(pipeline_dop=1) */ pk, array_contains(arr_bigint, 1000), arr_bigint from t order by pk limit 1;

-- query 338
USE sql_tests_complex_test_array_contains;
select /*+ set_var(pipeline_dop=1) */ pk, array_length(arr_bigint), arr_bigint from t order by pk limit 1;

-- query 339
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_array_contains FORCE;
