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

-- Migrated from dev/test/sql/test_array_fn/R/test_array_contains
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_array_contains_with_const @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t ( 
pk bigint not null ,
str VARCHAR,
arr_bigint array<bigint>,
arr_str array<VARCHAR>,
arr_decimal array<decimal(38,5)>
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t select generate_series, concat('array-key-', CAST(generate_series AS varchar)), array_repeat(generate_series, 128),array_repeat(concat('array-key-', CAST(generate_series AS varchar)), 16), array_repeat(generate_series, 128) from table(generate_series(0, 999));

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t values ROW(1000, concat('array-key-', CAST(1000 AS varchar)), array_append(array_generate(128), null), array_append(array_repeat(concat('array-key-', CAST(1000 AS varchar)),16), null),array_append(array_generate(128),null));

-- query 5
USE ${case_db};
select array_contains([1,2,3,4], 1) from t order by pk limit 10;

-- query 6
USE ${case_db};
select array_position([1,2,3,4], 1) from t order by pk limit 10;

-- query 7
USE ${case_db};
select array_contains([1,2,3,4], null) from t order by pk limit 10;

-- query 8
USE ${case_db};
select array_position([1,2,3,4], null) from t order by pk limit 10;

-- query 9
USE ${case_db};
select array_contains([1,2,3,null], null) from t order by pk limit 10;

-- query 10
USE ${case_db};
select array_position([1,2,3,null], null) from t order by pk limit 10;

-- query 11
USE ${case_db};
select array_contains(null, null) from t order by pk limit 10;

-- query 12
USE ${case_db};
select array_position(null, null) from t order by pk limit 10;

-- query 13
USE ${case_db};
select sum(array_contains((array_generate(1000)), pk)) from t;

-- query 14
USE ${case_db};
select sum(array_contains((array_generate(1000)), 100)) from t;

-- query 15
USE ${case_db};
select sum(array_position((array_generate(1000)), pk)) from t;

-- query 16
USE ${case_db};
select sum(array_position((array_generate(1000)), 100)) from t;

-- query 17
USE ${case_db};
select sum(array_contains(array_append((array_generate(1000)), null), pk)) from t;

-- query 18
USE ${case_db};
select sum(array_contains(array_append((array_generate(1000)), null), null)) from t;

-- query 19
USE ${case_db};
select sum(array_contains(arr_bigint, 100)) from t;

-- query 20
USE ${case_db};
select sum(array_position(arr_bigint, 100)) from t;

-- query 21
USE ${case_db};
select sum(array_contains(arr_str, concat('array-key-', CAST(100 AS varchar)))) from t;

-- query 22
USE ${case_db};
select sum(array_position(arr_str, concat('array-key-', CAST(100 AS varchar)))) from t;

-- query 23
USE ${case_db};
select sum(array_contains(arr_decimal, pk)) from t;

-- query 24
USE ${case_db};
select sum(array_position(arr_decimal, pk)) from t;

-- query 25
USE ${case_db};
select sum(array_contains(arr_decimal, 100)) from t;

-- query 26
USE ${case_db};
select sum(array_position(arr_decimal, 100)) from t;

-- query 27
USE ${case_db};
select sum(array_contains((array_repeat('abcdefg', 10000)), 'abcdefg')) from t;

-- query 28
USE ${case_db};
select sum(array_contains((array_repeat('abcdefg', 10000)), str)) from t;

-- name: test_array_contains_with_decimal
-- query 29
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t;
create table t (
    k bigint,
    v1 array<decimal(38,5)>,
    v2 array<array<decimal(38,5)>>,
    v3 array<array<array<decimal(38,5)>>>
) WITH (format_version = 3);

-- query 30
-- @skip_result_check=true
USE ${case_db};
insert into t values ROW(1,[1.1], [[1.1]],[[[1.1]]]);

-- query 31
USE ${case_db};
select array_contains(v1, 1.1) from t;

-- query 32
USE ${case_db};
select array_contains(v2, [1.1]) from t;

-- query 33
USE ${case_db};
select array_contains(v3, [[1.1]]) from t;

-- query 34
USE ${case_db};
select array_contains(v2, v1) from t;

-- query 35
USE ${case_db};
select array_contains(v3, v2) from t;

-- query 36
USE ${case_db};
select array_position(v1, 1.1) from t;

-- query 37
USE ${case_db};
select array_position(v2, [1.1]) from t;

-- query 38
USE ${case_db};
select array_position(v3, [[1.1]]) from t;

-- query 39
USE ${case_db};
select array_position(v2, v1) from t;

-- query 40
USE ${case_db};
select array_position(v3, v2) from t;

-- name: test_array_contains_all_and_seq
-- query 41
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t;
CREATE TABLE t (
  k BIGINT NOT NULL,
  arr_0 array<BIGINT> NOT NULL,
  arr_1 array<BIGINT>,
  arr_2 array<BIGINT>
) WITH (format_version = 3);

-- query 42
-- @skip_result_check=true
USE ${case_db};
insert into t values 
ROW(1, [1,2,3], [1,2], [1]),
ROW(2, [1,2,null], [1,null], [null]),
ROW(3, [1,2,null],[3],[3]),
ROW(4, [1,2,null], null, [1,2,null]),
ROW(5, [1,2,null], [1,2,null], null),
ROW(6, [1,2,3],[],[]),
ROW(7, [null,null], [null,null,null], [null,null]),
ROW(8, [1,1,1,1,1,2], [1,2], [1]),
ROW(9, [1,1,1,1,1,null,2],[1,null,2],[null,2]);

-- query 43
USE ${case_db};
select array_contains_seq(arr_0, arr_1) from t order by k;

-- query 44
USE ${case_db};
select array_contains_seq(arr_1, arr_0) from t order by k;

-- query 45
USE ${case_db};
select array_contains_seq(arr_0, arr_2) from t order by k;

-- query 46
USE ${case_db};
select array_contains_seq(arr_2, arr_0) from t order by k;

-- query 47
USE ${case_db};
select array_contains_seq(arr_1, arr_2) from t order by k;

-- query 48
USE ${case_db};
select array_contains_seq(arr_2, arr_1) from t order by k;

-- query 49
USE ${case_db};
select array_contains_seq([1,2,3,4], arr_0) from t order by k;

-- query 50
USE ${case_db};
select array_contains_seq([1,2,3,4], arr_1) from t order by k;

-- query 51
USE ${case_db};
select array_contains_seq([1,2,3,4,null], arr_1) from t order by k;

-- query 52
USE ${case_db};
select array_contains_seq(arr_0, [1,null]) from t order by k;

-- query 53
USE ${case_db};
select array_contains_seq(arr_0, []) from t order by k;

-- query 54
USE ${case_db};
select array_contains_seq(null, arr_0) from t order by k;

-- query 55
USE ${case_db};
select array_contains_seq(arr_1, null) from t order by k;

-- query 56
USE ${case_db};
select array_contains_seq([1,1,2,3],[1,1]);

-- query 57
USE ${case_db};
select array_contains_seq([1,1,2,3],[1,2]);

-- query 58
USE ${case_db};
select array_contains_seq([1,1,2,3],[1,3]);

-- query 59
USE ${case_db};
select array_contains_seq([1,1,2,3],[2,3]);

-- query 60
USE ${case_db};
select array_contains_seq([1,1,2,3],[1,1,2]);

-- query 61
USE ${case_db};
select array_contains_seq([null,null,1,2],[null]);

-- query 62
USE ${case_db};
select array_contains_seq([null,null,1,2],[null,null]);

-- query 63
USE ${case_db};
select array_contains_seq([null,null,1,2],[null,1]);

-- query 64
USE ${case_db};
select array_contains_seq([null,null,1,2],[null,null,1]);

-- query 65
USE ${case_db};
select array_contains_seq([null,null,1,2],[null,1,2]);

-- query 66
USE ${case_db};
select array_contains_seq((array_append(array_repeat(1, 1000), 2)), (array_append(array_repeat(1, 500), 2)));

-- query 67
-- @expect_error=2-th input of array_contains_seq should be an array, rather than varchar
USE ${case_db};
select array_contains_seq(['abc'],'a');

-- query 68
-- @expect_error=1-th input of array_contains_seq should be an array, rather than varchar
USE ${case_db};
select array_contains_seq('abc',['a']);

-- name: test_array_contains_all_type
-- query 69
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE test_array_contains (
    id INT,
    array_boolean ARRAY<BOOLEAN>,
    array_tinyint ARRAY<TINYINT>,
    array_smallint ARRAY<SMALLINT>,
    array_int ARRAY<INT>,
    array_bigint ARRAY<BIGINT>,
    array_largeint ARRAY<DECIMAL(38, 0)>,
    array_decimalv2 ARRAY<DECIMAL(10, 2)>,
    array_decimal32 ARRAY<DECIMAL(9, 2)>,
    array_decimal64 ARRAY<DECIMAL(18, 2)>,
    array_decimal128 ARRAY<DECIMAL(38, 10)>,
    array_float ARRAY<REAL>,
    array_double ARRAY<DOUBLE>,
    array_varchar ARRAY<VARCHAR(100)>,
    array_date ARRAY<DATE>,
    array_datetime ARRAY<TIMESTAMP(6)>
) WITH (format_version = 3);

-- query 70
-- @skip_result_check=true
USE ${case_db};
INSERT INTO test_array_contains VALUES
ROW(1, [true, false, NULL], [1, 2, NULL], [100, 200, NULL], [10, 20, NULL], [1000, 2000, NULL], [1234567890123456789, NULL, NULL],
 [12345.67, 89012.34, NULL], [123.45, 678.90, NULL], [12345678.90, 9876543.21, NULL], [1234567890.1234567890, NULL, NULL], 
 [1.23, 4.56, NULL], [123.456, 789.012, NULL], ['hello', NULL, 'starrocks'], 
 ['2025-01-01', '2025-01-02', NULL], ['2025-01-01 12:00:00', NULL, '2025-01-02 14:00:00']),
ROW(2, NULL, [5, NULL, 3], [500, 400, NULL], [50, 40, NULL], [5000, NULL, 3000], [987654321987654321, NULL, NULL],
 [56789.01, NULL, 45678.12], [345.67, NULL, 234.56], [56789012.34, NULL, 34567890.12], [2345678901.2345678901, NULL, NULL], 
 [2.34, NULL, 1.23], [234.567, NULL, 123.456], [NULL, 'array', 'test'], 
 [NULL, '2024-12-31', '2024-12-30'], ['2024-12-31 23:59:59', '2024-12-30 12:00:00', NULL]),
ROW(3, [true, NULL, false], [1, 6, NULL], [150, NULL, 250], [15, NULL, 35], [1100, NULL, 2200], [2345678901234567890, NULL, NULL],
 [67890.12, NULL, 56789.01], [456.78, NULL, 345.67], [67890123.45, NULL, 56789012.34], [3456789012.3456789012, NULL, NULL],
 [1.11, NULL, 3.33], [222.333, NULL, 333.444], ['foo', 'bar', NULL], 
 ['2025-01-03', NULL, '2025-01-04'], ['2025-01-03 16:00:00', '2025-01-04 18:00:00', NULL]),
ROW(4, [NULL, true, false], [NULL, 20, 10], [110, 220, NULL], [NULL, 12, 24], [1200, NULL, 2400], [3456789012345678901, NULL, NULL],
 [78901.23, 67890.12, NULL], [567.89, 456.78, NULL], [78901234.56, 67890123.45, NULL], [4567890123.4567890123, NULL, NULL],
 [NULL, 5.55, 4.44], [NULL, 777.888, 666.777], ['NULL', 'banana', 'apple'], 
 [NULL, '2025-01-05', '2025-01-06'], [NULL, '2025-01-06 20:00:00', '2025-01-05 18:00:00']),
ROW(5, [false, NULL, true], [10, NULL, 30], [300, 400, NULL], [70, NULL, 90], [4000, NULL, 6000], [987654321234567890, NULL, NULL],
 [123456.78, NULL, 876543.21], [678.90, NULL, 789.01], [9876543.21, NULL, 1234567.89], [5678901234.5678901234, NULL, NULL],
 [3.21, 4.32, NULL], [111.222, NULL, 333.444], ['dog', 'cat', NULL], 
 ['2025-01-07', '2025-01-08', NULL], ['2025-01-07 10:00:00', NULL, '2025-01-08 15:00:00']),
ROW(6, [NULL, true, true], [NULL, 40, 50], [450, 500, NULL], [80, 100, NULL], [1500, NULL, 2500], [765432198765432109, NULL, NULL],
 [34567.89, NULL, 12345.67], [123.45, NULL, 678.90], [54321.12, NULL, 12345.67], [7654321098.7654321098, NULL, NULL],
 [NULL, 6.54, 7.65], [555.666, NULL, 444.333], [NULL, 'bird', 'fish'], 
 ['2025-01-09', '2025-01-10', NULL], ['2025-01-09 12:00:00', NULL, '2025-01-10 18:00:00']),
ROW(7, [false, false, NULL], [70, NULL, 90], [650, NULL, 750], [120, 140, NULL], [8000, NULL, 9000], [543210987654321098, NULL, NULL],
 [45678.12, NULL, 23456.78], [234.56, NULL, 456.78], [67890123.45, NULL, 34567890.12], [4321098765.4321098765, NULL, NULL],
 [7.89, 8.90, NULL], [333.222, NULL, 111.000], ['lion', NULL, 'tiger'], 
 ['2025-01-11', '2025-01-12', NULL], ['2025-01-11 20:00:00', NULL, '2025-01-12 22:00:00']),
ROW(8, [true, NULL, false], [5, 15, NULL], [50, NULL, 150], [25, NULL, 75], [4500, NULL, 5500], [321098765432109876, NULL, NULL],
 [23456.78, NULL, 12345.67], [345.67, NULL, 456.78], [8901234.56, NULL, 7890123.45], [2109876543.2109876543, NULL, NULL],
 [5.67, NULL, 4.56], [666.555, NULL, 222.111], [NULL, 'grape', 'pear'], 
 ['2025-01-13', NULL, '2025-01-14'], ['2025-01-13 23:59:59', '2025-01-14 12:00:00', NULL]),
ROW(9, [false, true, NULL], [25, 35, NULL], [350, NULL, 450], [100, 200, NULL], [6000, NULL, 7000], [654321098765432109, NULL, NULL],
 [67890.12, NULL, 34567.89], [456.78, NULL, 234.56], [34567890.12, NULL, 23456789.01], [8765432109.8765432109, NULL, NULL],
 [9.87, NULL, 8.76], [444.333, NULL, 555.222], ['watermelon', NULL, 'kiwi'], 
 [NULL, '2025-01-15', '2025-01-16'], ['2025-01-15 12:00:00', NULL, '2025-01-16 18:00:00']),
ROW(10, [true, true, NULL], [50, 70, NULL], [750, 850, NULL], [300, 400, NULL], [10000, NULL, 12000], [789012345678901234, NULL, NULL],
 [78901.23, NULL, 67890.12], [567.89, NULL, 456.78], [12345678.90, NULL, 9876543.21], [1234567890.1234567890, NULL, NULL],
 [NULL, 1.11, 2.22], [777.888, 999.000, NULL], ['blueberry', 'cherry', NULL], 
 ['2025-01-17', '2025-01-18', NULL], [NULL, '2025-01-17 10:00:00', '2025-01-18 20:00:00']);

-- query 71
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR,2
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_boolean, true) ORDER BY idSELECT id FROM test_array_contains WHERE array_contains(array_boolean, true) ORDER BY id;;

-- query 72
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR,2
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_boolean, NULL) ORDER BY idSELECT id FROM test_array_contains WHERE array_contains(array_boolean, NULL) ORDER BY id;;

-- query 73
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_boolean, false) ORDER BY id;

-- query 74
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_tinyint, 5) ORDER BY id;

-- query 75
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_tinyint, NULL) ORDER BY id;

-- query 76
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_tinyint, 20) ORDER BY id;

-- query 77
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_smallint, 100) ORDER BY id;

-- query 78
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_smallint, NULL) ORDER BY id;

-- query 79
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_smallint, 300) ORDER BY id;

-- query 80
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_int, 50) ORDER BY id;

-- query 81
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_int, NULL) ORDER BY id;

-- query 82
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_int, 90) ORDER BY id;

-- query 83
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_bigint, 4000) ORDER BY id;

-- query 84
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_bigint, NULL) ORDER BY id;

-- query 85
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_bigint, 8000) ORDER BY id;

-- query 86
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_largeint, 1234567890123456789) ORDER BY id;

-- query 87
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_largeint, NULL) ORDER BY id;

-- query 88
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_largeint, 765432198765432109) ORDER BY id;

-- query 89
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_decimalv2, 12345.67) ORDER BY id;

-- query 90
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_decimalv2, NULL) ORDER BY id;

-- query 91
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_decimalv2, 56789.01) ORDER BY id;

-- query 92
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_decimal32, 123.45) ORDER BY id;

-- query 93
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_decimal32, NULL) ORDER BY id;

-- query 94
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_decimal32, 567.89) ORDER BY id;

-- query 95
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_decimal64, 12345678.90) ORDER BY id;

-- query 96
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_decimal64, NULL) ORDER BY id;

-- query 97
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_decimal64, 7890123.45) ORDER BY id;

-- query 98
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_decimal128, 1234567890.1234567890) ORDER BY id;

-- query 99
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_decimal128, NULL) ORDER BY id;

-- query 100
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_decimal128, 8765432109.8765432109) ORDER BY id;

-- query 101
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_float, 1.23) ORDER BY id;

-- query 102
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_float, NULL) ORDER BY id;

-- query 103
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_float, 7.89) ORDER BY id;

-- query 104
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_double, 123.456) ORDER BY id;

-- query 105
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_double, NULL) ORDER BY id;

-- query 106
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_double, 444.333) ORDER BY id;

-- query 107
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_varchar, 'hello') ORDER BY id;

-- query 108
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_varchar, NULL) ORDER BY id;

-- query 109
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_varchar, 'starrocks') ORDER BY id;

-- query 110
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_date, '2025-01-01') ORDER BY id;

-- query 111
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_date, NULL) ORDER BY id;

-- query 112
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_date, '2025-01-13') ORDER BY id;

-- query 113
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_datetime, '2025-01-01 12:00:00') ORDER BY id;

-- query 114
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_datetime, NULL) ORDER BY id;

-- query 115
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains(array_datetime, '2025-01-17 10:00:00') ORDER BY id;

-- query 116
USE ${case_db};
SELECT id, array_position(array_boolean, true) AS position FROM test_array_contains ORDER BY id;

-- query 117
USE ${case_db};
SELECT id, array_position(array_boolean, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 118
USE ${case_db};
SELECT id, array_position(array_boolean, false) AS position FROM test_array_contains ORDER BY id;

-- query 119
USE ${case_db};
SELECT id, array_position(array_tinyint, 5) AS position FROM test_array_contains ORDER BY id;

-- query 120
USE ${case_db};
SELECT id, array_position(array_tinyint, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 121
USE ${case_db};
SELECT id, array_position(array_tinyint, 20) AS position FROM test_array_contains ORDER BY id;

-- query 122
USE ${case_db};
SELECT id, array_position(array_smallint, 100) AS position FROM test_array_contains ORDER BY id;

-- query 123
USE ${case_db};
SELECT id, array_position(array_smallint, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 124
USE ${case_db};
SELECT id, array_position(array_smallint, 300) AS position FROM test_array_contains ORDER BY id;

-- query 125
USE ${case_db};
SELECT id, array_position(array_int, 50) AS position FROM test_array_contains ORDER BY id;

-- query 126
USE ${case_db};
SELECT id, array_position(array_int, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 127
USE ${case_db};
SELECT id, array_position(array_int, 90) AS position FROM test_array_contains ORDER BY id;

-- query 128
USE ${case_db};
SELECT id, array_position(array_bigint, 4000) AS position FROM test_array_contains ORDER BY id;

-- query 129
USE ${case_db};
SELECT id, array_position(array_bigint, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 130
USE ${case_db};
SELECT id, array_position(array_bigint, 8000) AS position FROM test_array_contains ORDER BY id;

-- query 131
USE ${case_db};
SELECT id, array_position(array_largeint, 1234567890123456789) AS position FROM test_array_contains ORDER BY id;

-- query 132
USE ${case_db};
SELECT id, array_position(array_largeint, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 133
USE ${case_db};
SELECT id, array_position(array_largeint, 765432198765432109) AS position FROM test_array_contains ORDER BY id;

-- query 134
USE ${case_db};
SELECT id, array_position(array_decimalv2, 12345.67) AS position FROM test_array_contains ORDER BY id;

-- query 135
USE ${case_db};
SELECT id, array_position(array_decimalv2, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 136
USE ${case_db};
SELECT id, array_position(array_decimalv2, 56789.01) AS position FROM test_array_contains ORDER BY id;

-- query 137
USE ${case_db};
SELECT id, array_position(array_decimal32, 123.45) AS position FROM test_array_contains ORDER BY id;

-- query 138
USE ${case_db};
SELECT id, array_position(array_decimal32, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 139
USE ${case_db};
SELECT id, array_position(array_decimal32, 567.89) AS position FROM test_array_contains ORDER BY id;

-- query 140
USE ${case_db};
SELECT id, array_position(array_decimal64, 12345678.90) AS position FROM test_array_contains ORDER BY id;

-- query 141
USE ${case_db};
SELECT id, array_position(array_decimal64, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 142
USE ${case_db};
SELECT id, array_position(array_decimal64, 7890123.45) AS position FROM test_array_contains ORDER BY id;

-- query 143
USE ${case_db};
SELECT id, array_position(array_decimal128, 1234567890.1234567890) AS position FROM test_array_contains ORDER BY id;

-- query 144
USE ${case_db};
SELECT id, array_position(array_decimal128, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 145
USE ${case_db};
SELECT id, array_position(array_decimal128, 8765432109.8765432109) AS position FROM test_array_contains ORDER BY id;

-- query 146
USE ${case_db};
SELECT id, array_position(array_float, 1.23) AS position FROM test_array_contains ORDER BY id;

-- query 147
USE ${case_db};
SELECT id, array_position(array_float, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 148
USE ${case_db};
SELECT id, array_position(array_float, 7.89) AS position FROM test_array_contains ORDER BY id;

-- query 149
USE ${case_db};
SELECT id, array_position(array_double, 123.456) AS position FROM test_array_contains ORDER BY id;

-- query 150
USE ${case_db};
SELECT id, array_position(array_double, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 151
USE ${case_db};
SELECT id, array_position(array_double, 444.333) AS position FROM test_array_contains ORDER BY id;

-- query 152
USE ${case_db};
SELECT id, array_position(array_varchar, 'hello') AS position FROM test_array_contains ORDER BY id;

-- query 153
USE ${case_db};
SELECT id, array_position(array_varchar, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 154
USE ${case_db};
SELECT id, array_position(array_varchar, 'starrocks') AS position FROM test_array_contains ORDER BY id;

-- query 155
USE ${case_db};
SELECT id, array_position(array_date, '2025-01-01') AS position FROM test_array_contains ORDER BY id;

-- query 156
USE ${case_db};
SELECT id, array_position(array_date, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 157
USE ${case_db};
SELECT id, array_position(array_date, '2025-01-13') AS position FROM test_array_contains ORDER BY id;

-- query 158
USE ${case_db};
SELECT id, array_position(array_datetime, '2025-01-01 12:00:00') AS position FROM test_array_contains ORDER BY id;

-- query 159
USE ${case_db};
SELECT id, array_position(array_datetime, NULL) AS position FROM test_array_contains ORDER BY id;

-- query 160
USE ${case_db};
SELECT id, array_position(array_datetime, '2025-01-17 10:00:00') AS position FROM test_array_contains ORDER BY id;

-- query 161
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_boolean, [true, false]) ORDER BY id;

-- query 162
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_tinyint, [5, 10]) ORDER BY id;

-- query 163
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_smallint, [100, 200]) ORDER BY id;

-- query 164
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_int, [50, 60]) ORDER BY id;

-- query 165
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_bigint, [4000, 6000]) ORDER BY id;

-- query 166
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_largeint, [1234567890123456789, 987654321098765432]) ORDER BY id;

-- query 167
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_decimalv2, [12345.67, 78901.23]) ORDER BY id;

-- query 168
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_decimal32, [123.45, 567.89]) ORDER BY id;

-- query 169
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_decimal64, [12345678.90, 56789012.34]) ORDER BY id;

-- query 170
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_decimal128, [1234567890.1234567890, 8765432109.8765432109]) ORDER BY id;

-- query 171
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_float, [1.23, 4.56]) ORDER BY id;

-- query 172
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_double, [123.456, 789.012]) ORDER BY id;

-- query 173
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_varchar, ['hello', 'world']) ORDER BY id;

-- query 174
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_date, ['2025-01-01', '2025-01-13']) ORDER BY id;

-- query 175
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_datetime, ['2025-01-01 12:00:00', '2025-01-17 10:00:00']) ORDER BY id;

-- query 176
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq([true, false], array_boolean) ORDER BY id;

-- query 177
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq([5, 10], array_tinyint) ORDER BY id;

-- query 178
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq([100, 200], array_smallint) ORDER BY id;

-- query 179
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq([50, 60], array_int) ORDER BY id;

-- query 180
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq([4000, 6000], array_bigint) ORDER BY id;

-- query 181
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq([1234567890123456789, 987654321098765432], array_largeint) ORDER BY id;

-- query 182
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq([12345.67, 78901.23], array_decimalv2) ORDER BY id;

-- query 183
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq([123.45, 567.89], array_decimal32) ORDER BY id;

-- query 184
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq([12345678.90, 56789012.34], array_decimal64) ORDER BY id;

-- query 185
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq([1234567890.1234567890, 8765432109.8765432109], array_decimal128) ORDER BY id;

-- query 186
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq([1.23, 4.56], array_float) ORDER BY id;

-- query 187
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq([123.456, 789.012], array_double) ORDER BY id;

-- query 188
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(['hello', 'world'], array_varchar) ORDER BY id;

-- query 189
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(['2025-01-01', '2025-01-13'], array_date) ORDER BY id;

-- query 190
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(['2025-01-01 12:00:00', '2025-01-17 10:00:00'], array_datetime) ORDER BY id;

-- query 191
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_boolean, []) ORDER BY id;

-- query 192
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_tinyint, [1, 2, 3, 4, 5]) ORDER BY id;

-- query 193
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_smallint, [10, 20, 30, 40, 50]) ORDER BY id;

-- query 194
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_int, [100, 200, 300, 400, 500]) ORDER BY id;

-- query 195
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains WHERE array_contains_seq(array_varchar, ['a', 'b', 'c', 'd', 'e']) ORDER BY id;

-- name: test_array_contains_complex_type
-- query 196
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE test_array_contains_complex_type (
    id INT,
    array_map ARRAY<MAP<VARCHAR, INT>>,
    array_json ARRAY<JSON>,
    array_struct ARRAY<ROW(f1 INT, f2 VARCHAR)>
) WITH (format_version = 3);

-- query 197
-- @skip_result_check=true
USE ${case_db};
INSERT INTO test_array_contains_complex_type VALUES
ROW(1, [map('a', 1, 'b', 2), map('c', 3, 'd', 4)], ['{"key1": 100, "key2": 200}', '{"key3": 300}'], [row(10, 'hello'), row(20, 'world')]),
ROW(2, [map('x', 5, 'y', 6)], ['{"key4": 400}', '{"key5": 500, "key6": 600}'], [row(30,'starrocks'), row(40, 'database')]),
ROW(3, NULL, NULL, NULL),
ROW(4, [], [], []);

-- query 198
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq([map('c', 3, 'd', 4)], array_map) ORDER BY id;

-- query 199
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq([], array_map) ORDER BY id;

-- query 200
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq(['{"key3": 300}'], array_json) ORDER BY id;

-- query 201
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq([NULL], array_json) ORDER BY id;

-- query 202
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq([row(20,'world')], array_struct) ORDER BY id;

-- query 203
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq([NULL], array_struct) ORDER BY id;

-- query 204
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq(array_json, ['{"key3": 300}']) ORDER BY id;

-- query 205
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains_seq(array_map, []) ORDER BY id;

-- query 206
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_map, map('a', 1, 'b', 2)) ORDER BY id;

-- query 207
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_map, map('c', 3, 'd', 4)) ORDER BY id;

-- query 208
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_map, map('x', 5, 'y', 6)) ORDER BY id;

-- query 209
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_map, NULL) ORDER BY id;

-- query 210
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_map, map()) ORDER BY id;

-- query 211
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_json, '{"key1": 100, "key2": 200}') ORDER BY id;

-- query 212
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_json, '{"key3": 300}') ORDER BY id;

-- query 213
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_json, '{"key5": 500, "key6": 600}') ORDER BY id;

-- query 214
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_json, NULL) ORDER BY id;

-- query 215
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_json, '{}') ORDER BY id;

-- query 216
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_struct, row(10, 'hello')) ORDER BY id;

-- query 217
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_struct, row(20, 'world')) ORDER BY id;

-- query 218
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_struct, row(40, 'database')) ORDER BY id;

-- query 219
-- @skip_result_check=true
USE ${case_db};
SELECT id FROM test_array_contains_complex_type WHERE array_contains(array_struct, NULL) ORDER BY id;

-- query 220
USE ${case_db};
SELECT id, array_position(array_map, map('a', 1, 'b', 2)) FROM test_array_contains_complex_type ORDER BY id;

-- query 221
USE ${case_db};
SELECT id, array_position(array_map, map('c', 3, 'd', 4)) FROM test_array_contains_complex_type ORDER BY id;

-- query 222
USE ${case_db};
SELECT id, array_position(array_map, map('x', 5, 'y', 6)) FROM test_array_contains_complex_type ORDER BY id;

-- query 223
USE ${case_db};
SELECT id, array_position(array_map, NULL) FROM test_array_contains_complex_type ORDER BY id;

-- query 224
USE ${case_db};
SELECT id, array_position(array_map, map()) FROM test_array_contains_complex_type ORDER BY id;

-- query 225
USE ${case_db};
SELECT id, array_position(array_json, '{"key1": 100, "key2": 200}') FROM test_array_contains_complex_type ORDER BY id;

-- query 226
USE ${case_db};
SELECT id, array_position(array_json, '{"key3": 300}') FROM test_array_contains_complex_type ORDER BY id;

-- query 227
USE ${case_db};
SELECT id, array_position(array_json, '{"key5": 500, "key6": 600}') FROM test_array_contains_complex_type ORDER BY id;

-- query 228
USE ${case_db};
SELECT id, array_position(array_json, NULL) FROM test_array_contains_complex_type ORDER BY id;

-- query 229
USE ${case_db};
SELECT id, array_position(array_json, '{}') FROM test_array_contains_complex_type ORDER BY id;

-- query 230
USE ${case_db};
SELECT id, array_position(array_struct, row(10, 'hello')) FROM test_array_contains_complex_type ORDER BY id;

-- query 231
USE ${case_db};
SELECT id, array_position(array_struct, row(20, 'world')) FROM test_array_contains_complex_type ORDER BY id;

-- query 232
USE ${case_db};
SELECT id, array_position(array_struct, row(40, 'database')) FROM test_array_contains_complex_type ORDER BY id;

-- query 233
USE ${case_db};
SELECT id, array_position(array_struct, NULL) FROM test_array_contains_complex_type ORDER BY id;

-- query 234
USE ${case_db};
select array_contains([null], null), array_position([null], null);

-- name: test_array_contains_with_null
-- query 235
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t;
CREATE TABLE t ( 
pk bigint not null ,
str VARCHAR,
arr_bigint array<bigint>,
arr_str array<VARCHAR>,
arr_decimal array<decimal(38,5)>
) WITH (format_version = 3);

-- query 236
-- @skip_result_check=true
USE ${case_db};
insert into t select generate_series, concat('array-key-', CAST(generate_series AS varchar)), array_repeat(generate_series, 10),array_repeat(concat('array-key-', CAST(generate_series AS varchar)), 10), array_repeat(generate_series, 128) from table(generate_series(0, 999));

-- query 237
-- @skip_result_check=true
USE ${case_db};
insert into t select 1, null, null, null, null;

-- query 238
USE ${case_db};
select  pk, array_contains(arr_bigint, 1000), arr_bigint from t order by pk limit 1;

-- query 239
USE ${case_db};
select  pk, array_length(arr_bigint), arr_bigint from t order by pk limit 1;

-- query 240
-- @skip_result_check=true
-- A catalog that cannot hold views cannot answer view enumeration, so
-- DROP DATABASE ... FORCE is refused here rather than silently assuming
-- the namespace holds none. Drop the tables explicitly instead.
USE ${case_db};
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS test_array_contains;
DROP TABLE IF EXISTS test_array_contains_complex_type;
