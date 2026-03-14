-- Migrated from dev/test/sql/test_array_fn/R/test_array_fn
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_array_fn FORCE;
CREATE DATABASE sql_tests_complex_test_array_fn;
USE sql_tests_complex_test_array_fn;

-- name: test_array_functions @mac @no_arrow_flight_sql
-- query 2
-- @skip_result_check=true
USE sql_tests_complex_test_array_fn;
CREATE TABLE array_test ( 
pk bigint not null ,
s_1   Array<String>, 
i_1   Array<BigInt>,
f_1   Array<Double>,
d_1   Array<DECIMAL(26, 2)>,
d_2   Array<DECIMAL64(4, 3)>,
d_3   Array<DECIMAL128(25, 19)>,
d_4   Array<DECIMAL32(8, 5)> ,
d_5   Array<DECIMAL(16, 3)>,
d_6   Array<DECIMAL128(18, 6)> ,
ai_1  Array<Array<BigInt>>,
as_1  Array<Array<String>>,
aas_1 Array<Array<Array<String>>>,
aad_1 Array<Array<Array<DECIMAL(26, 2)>>>
) ENGINE=OLAP
DUPLICATE KEY(`pk`)
DISTRIBUTED BY HASH(`pk`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);

-- query 3
-- @skip_result_check=true
USE sql_tests_complex_test_array_fn;
CREATE TABLE array_agg_test (
pk bigint not null ,
d_1   DECIMAL(26, 2),
d_2   DECIMAL64(4, 3),
d_3   DECIMAL128(25, 19),
d_4   DECIMAL32(8, 5),
d_5   DECIMAL(16, 3),
d_6   DECIMAL128(18, 6)
) ENGINE=OLAP
DUPLICATE KEY(`pk`)
DISTRIBUTED BY HASH(`pk`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);

-- query 4
-- @skip_result_check=true
USE sql_tests_complex_test_array_fn;
insert into array_test values
(1, ['a', 'b', 'c'], [1.0, 2.0, 3.0, 4.0, 10.0], [1.0, 2.0, 3.0, 4.0, 10.0, 1.1, 2.1, 3.2, 4.3, -1, -10, 100], [4.0, 10.0, 1.1, 2.1, 3.2, 4.3, -1, -10, 100, 1.0, 2.0, 3.0], [4.0, 10.0, 1.1, -10, 100, 1.0, 2.0, 3.0, 2.1, 3.2, 4.3, -1], [4.0, 2.1, 3.2, 10.0, 1.1, -10, 100, -1, 1.0, 2.0, 3.0, 4.3], [4.0, 2.1, 3.2, 10.0, 2.0, 3.0, 1.1, -1, -10, 100, 1.0, 4.3], [4.0, 2.1, 3.0, 1.1, 4.3, 3.2, -10, 100, 1.0, 10.0, -1, 2.0], [4.0, 2.1, 100, 1.0, 4.3, 3.2, 10.0, 2.0, 3.0, 1.1, -1, -10], [[1, 2, 3, 4], [5, 2, 6, 4], [100, -1, 92, 8], [66, 4, 32, -10]], [['1', '2', '3', '4'], ['-1', 'a', '-100', '100'], ['a', 'b', 'c']], [[['1'],['2'],['3']], [['6'],['5'],['4']], [['-1', '-2'],['-2', '10'],['100','23']]], [[[1],[2],[3]], [[6],[5],[4]], [[-1, -2],[-2, 10],[100,23]]]),
(2, ['-1', '10', '1', '100', '2'], NULL, [10.0, 20.0, 30.0, 4.0, 100.0, 10.1, 2.1, 30.2, 40.3, -1, -10, 100], [40.0, 100.0, 01.1, 2.1, 30.2, 40.3, -1, -100, 1000, 1.0, 2.0, 3.0], [40.0, 100.0, 01.1, -10, 1000, 10.0, 2.0, 30.0, 20.1, 3.2, 4.3, -1], NULL, NULL, [40.0, 20.1, 30.0, 10.1, 40.30, 30.20, -100, 1000, 1.0, 100.0, -10, 2.0], [40.0, 20.1, 1000, 10.0, 40.30, 30.20, 100.0, 20.0, 3.0, 10.1, -10, -10], NULL, NULL, [[['10'],['20'],['30']], [['60'],['5'],['4']], [['-100', '-2'],['-20', '10'],['100','23']]], [[[10],[20],[30]], [[60],[50],[4]], [[-1, -2],[-2, 100],[100,23]]]),
(4, ['a', NULL, 'c', 'e', 'd'], [1.0, 2.0, 3.0, 4.0, 10.0], [1.0, 2.0, 3.0, 4.0, 10.0, NULL, 1.1, 2.1, 3.2, NULL, 4.3, -1, -10, 100], [4.0, 10.0, 1.1, 2.1,NULL, 3.2, 4.3, -1, -10, 100, 1.0, 2.0, 3.0], [4.0, 10.0, 1.1, -10, 100, 1.0, 2.0, 3.0, 2.1, 3.2, 4.3, -1], [4.0, 2.1, 3.2, 10.0, 1.1, -10, 100, -1, 1.0, 2.0, 3.0, 4.3], [4.0, 2.1, 3.2, 10.0, 2.0, 3.0, 1.1, -1, -10, 100, 1.0, 4.3], [4.0, 2.1, 3.0, 1.1, 4.3, 3.2, -10, 100, 1.0, 10.0, -1, 2.0], [4.0, 2.1, 100, NULL, 1.0, 4.3, 3.2, 10.0, 2.0, 3.0, 1.1, -1, -10], [[1, 2, 3, NULL, 4], [5, 2, 6, 4], NULL, [100, -1, 92, 8], [66, 4, 32, -10]], [['1', '2', '3', '4'], ['-1', 'a', '-100', '100'], ['a', 'b', 'c']], [[['1'],['2'],['3']], [['6'],['5'],['4']], [['-1', '-2'],NULL,['-2', '10'],['100','23']]], [[[1],NULL,[2],[3]], [[6],[5],[4]], NULL, [[-1, -2],[-2, 10],[100,23]]]),
(3, NULL, [1.0, 2.0, 3.0, 4.0, 10.0], NULL, [40.0, 10.0, 1.1, 2.1, 3.2, 4.3, -10, -10, 100, 10.0, 20.0, 3.0], [4.0, 10.0, 1.1, -10, 100, 1.0, 20.0, 3.0, 2.1, 3.2, 4.3, -1], [40.0, 20.1, 3.2, 10.0, 10.1, -10, 100, -1, 10.0, 2.0, 30.0, 4.3], [4.0, 2.1, 3.2, 10.0, 20.0, 3.0, 1.1, -10, -100, 100, 10.0, 4.3], NULL, NULL, [[1, 2, 30, 4], [50, 2, 6, 4], [100, -10, 92, 8], [66, 40, 32, -100]], [['1', '20', '3', '4'], ['-1', 'a00', '-100', '100'], ['a', 'b0', 'c']], NULL, NULL);

-- query 5
-- @skip_result_check=true
USE sql_tests_complex_test_array_fn;
insert into array_agg_test values
(1, 4.0, 0, 1.1, 2.1, 3.2, 4.3);

-- query 6
USE sql_tests_complex_test_array_fn;
select array_length(s_1) from array_test order by pk;

-- query 7
USE sql_tests_complex_test_array_fn;
select array_length(i_1) from array_test order by pk;

-- query 8
USE sql_tests_complex_test_array_fn;
select array_length(f_1) from array_test order by pk;

-- query 9
USE sql_tests_complex_test_array_fn;
select array_length(d_1) from array_test order by pk;

-- query 10
USE sql_tests_complex_test_array_fn;
select array_length(d_3) from array_test order by pk;

-- query 11
USE sql_tests_complex_test_array_fn;
select array_length(d_5) from array_test order by pk;

-- query 12
USE sql_tests_complex_test_array_fn;
select array_length(ai_1) from array_test order by pk;

-- query 13
USE sql_tests_complex_test_array_fn;
select array_length(as_1) from array_test order by pk;

-- query 14
USE sql_tests_complex_test_array_fn;
select array_length(aas_1) from array_test order by pk;

-- query 15
USE sql_tests_complex_test_array_fn;
select array_length(aad_1) from array_test order by pk;

-- query 16
USE sql_tests_complex_test_array_fn;
select array_length(NULL) from array_test order by pk;

-- query 17
USE sql_tests_complex_test_array_fn;
select array_length([1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 18
USE sql_tests_complex_test_array_fn;
select array_length(['a', 'b', 'c']) from array_test order by pk;

-- query 19
USE sql_tests_complex_test_array_fn;
select array_length([[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 20
USE sql_tests_complex_test_array_fn;
select array_sum(s_1) from array_test order by pk;

-- query 21
USE sql_tests_complex_test_array_fn;
select array_sum(i_1) from array_test order by pk;

-- query 22
USE sql_tests_complex_test_array_fn;
select array_sum(f_1) from array_test order by pk;

-- query 23
USE sql_tests_complex_test_array_fn;
select array_sum(d_1) from array_test order by pk;

-- query 24
USE sql_tests_complex_test_array_fn;
select array_sum(d_2) from array_test order by pk;

-- query 25
USE sql_tests_complex_test_array_fn;
select array_sum(d_3) from array_test order by pk;

-- query 26
USE sql_tests_complex_test_array_fn;
select array_sum(d_4) from array_test order by pk;

-- query 27
USE sql_tests_complex_test_array_fn;
select array_sum(d_5) from array_test order by pk;

-- query 28
USE sql_tests_complex_test_array_fn;
select array_sum(d_6) from array_test order by pk;

-- query 29
USE sql_tests_complex_test_array_fn;
select array_sum(NULL) from array_test order by pk;

-- query 30
USE sql_tests_complex_test_array_fn;
select array_sum([1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 31
USE sql_tests_complex_test_array_fn;
select array_avg(s_1) from array_test order by pk;

-- query 32
USE sql_tests_complex_test_array_fn;
select array_avg(i_1) from array_test order by pk;

-- query 33
USE sql_tests_complex_test_array_fn;
select array_avg(f_1) from array_test order by pk;

-- query 34
USE sql_tests_complex_test_array_fn;
select array_avg(d_1) from array_test order by pk;

-- query 35
USE sql_tests_complex_test_array_fn;
select array_avg(d_2) from array_test order by pk;

-- query 36
USE sql_tests_complex_test_array_fn;
select array_avg(d_3) from array_test order by pk;

-- query 37
USE sql_tests_complex_test_array_fn;
select array_avg(d_4) from array_test order by pk;

-- query 38
USE sql_tests_complex_test_array_fn;
select array_avg(d_5) from array_test order by pk;

-- query 39
USE sql_tests_complex_test_array_fn;
select array_avg(d_6) from array_test order by pk;

-- query 40
USE sql_tests_complex_test_array_fn;
select array_avg(NULL) from array_test order by pk;

-- query 41
USE sql_tests_complex_test_array_fn;
select array_avg([1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 42
USE sql_tests_complex_test_array_fn;
select array_avg(['a', 'b', 'c']) from array_test order by pk;

-- query 43
-- @expect_error=No matching function with signature: array_avg(array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_avg([[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 44
USE sql_tests_complex_test_array_fn;
select array_min(s_1) from array_test order by pk;

-- query 45
USE sql_tests_complex_test_array_fn;
select array_min(i_1) from array_test order by pk;

-- query 46
USE sql_tests_complex_test_array_fn;
select array_min(f_1) from array_test order by pk;

-- query 47
USE sql_tests_complex_test_array_fn;
select array_min(d_1) from array_test order by pk;

-- query 48
USE sql_tests_complex_test_array_fn;
select array_min(d_2) from array_test order by pk;

-- query 49
USE sql_tests_complex_test_array_fn;
select array_min(d_3) from array_test order by pk;

-- query 50
USE sql_tests_complex_test_array_fn;
select array_min(d_4) from array_test order by pk;

-- query 51
USE sql_tests_complex_test_array_fn;
select array_min(d_5) from array_test order by pk;

-- query 52
USE sql_tests_complex_test_array_fn;
select array_min(d_6) from array_test order by pk;

-- query 53
USE sql_tests_complex_test_array_fn;
select array_min(NULL) from array_test order by pk;

-- query 54
USE sql_tests_complex_test_array_fn;
select array_min([1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 55
USE sql_tests_complex_test_array_fn;
select array_min(['a', 'b', 'c']) from array_test order by pk;

-- query 56
-- @expect_error=No matching function with signature: array_min(array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_min([[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 57
USE sql_tests_complex_test_array_fn;
select array_max(s_1) from array_test order by pk;

-- query 58
USE sql_tests_complex_test_array_fn;
select array_max(i_1) from array_test order by pk;

-- query 59
USE sql_tests_complex_test_array_fn;
select array_max(f_1) from array_test order by pk;

-- query 60
USE sql_tests_complex_test_array_fn;
select array_max(d_1) from array_test order by pk;

-- query 61
USE sql_tests_complex_test_array_fn;
select array_max(d_2) from array_test order by pk;

-- query 62
USE sql_tests_complex_test_array_fn;
select array_max(d_3) from array_test order by pk;

-- query 63
USE sql_tests_complex_test_array_fn;
select array_max(d_4) from array_test order by pk;

-- query 64
USE sql_tests_complex_test_array_fn;
select array_max(d_5) from array_test order by pk;

-- query 65
USE sql_tests_complex_test_array_fn;
select array_max(d_6) from array_test order by pk;

-- query 66
USE sql_tests_complex_test_array_fn;
select array_max(NULL) from array_test order by pk;

-- query 67
USE sql_tests_complex_test_array_fn;
select array_max([1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 68
USE sql_tests_complex_test_array_fn;
select array_max(['a', 'b', 'c']) from array_test order by pk;

-- query 69
-- @expect_error=No matching function with signature: array_max(array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_max([[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 70
USE sql_tests_complex_test_array_fn;
select array_distinct(s_1) from array_test order by pk;

-- query 71
USE sql_tests_complex_test_array_fn;
select array_distinct(i_1) from array_test order by pk;

-- query 72
USE sql_tests_complex_test_array_fn;
select array_distinct(f_1) from array_test order by pk;

-- query 73
USE sql_tests_complex_test_array_fn;
select array_distinct(d_1) from array_test order by pk;

-- query 74
USE sql_tests_complex_test_array_fn;
select array_distinct(d_2) from array_test order by pk;

-- query 75
USE sql_tests_complex_test_array_fn;
select array_distinct(d_3) from array_test order by pk;

-- query 76
USE sql_tests_complex_test_array_fn;
select array_distinct(d_4) from array_test order by pk;

-- query 77
USE sql_tests_complex_test_array_fn;
select array_distinct(d_5) from array_test order by pk;

-- query 78
USE sql_tests_complex_test_array_fn;
select array_distinct(d_6) from array_test order by pk;

-- query 79
USE sql_tests_complex_test_array_fn;
select array_distinct(ai_1) from array_test order by pk;

-- query 80
USE sql_tests_complex_test_array_fn;
select array_distinct(as_1) from array_test order by pk;

-- query 81
USE sql_tests_complex_test_array_fn;
select array_distinct(aas_1) from array_test order by pk;

-- query 82
USE sql_tests_complex_test_array_fn;
select array_distinct(aad_1) from array_test order by pk;

-- query 83
USE sql_tests_complex_test_array_fn;
select array_distinct(NULL) from array_test order by pk;

-- query 84
USE sql_tests_complex_test_array_fn;
select array_distinct([1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 85
USE sql_tests_complex_test_array_fn;
select array_distinct(['a', 'b', 'c']) from array_test order by pk;

-- query 86
USE sql_tests_complex_test_array_fn;
select array_distinct([[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 87
USE sql_tests_complex_test_array_fn;
select array_sort(s_1) from array_test order by pk;

-- query 88
USE sql_tests_complex_test_array_fn;
select array_sort(i_1) from array_test order by pk;

-- query 89
USE sql_tests_complex_test_array_fn;
select array_sort(f_1) from array_test order by pk;

-- query 90
USE sql_tests_complex_test_array_fn;
select array_sort(d_1) from array_test order by pk;

-- query 91
USE sql_tests_complex_test_array_fn;
select array_sort(d_2) from array_test order by pk;

-- query 92
USE sql_tests_complex_test_array_fn;
select array_sort(d_3) from array_test order by pk;

-- query 93
USE sql_tests_complex_test_array_fn;
select array_sort(d_4) from array_test order by pk;

-- query 94
USE sql_tests_complex_test_array_fn;
select array_sort(d_5) from array_test order by pk;

-- query 95
USE sql_tests_complex_test_array_fn;
select array_sort(d_6) from array_test order by pk;

-- query 96
USE sql_tests_complex_test_array_fn;
select array_sort(NULL) from array_test order by pk;

-- query 97
USE sql_tests_complex_test_array_fn;
select array_sort([1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 98
USE sql_tests_complex_test_array_fn;
select array_sort(['a', 'b', 'c']) from array_test order by pk;

-- query 99
-- @expect_error=No matching function with signature: array_sort(array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_sort([[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 100
USE sql_tests_complex_test_array_fn;
select array_agg(s_1) from array_test where pk = 1;

-- query 101
USE sql_tests_complex_test_array_fn;
select array_agg(i_1) from array_test where pk = 1;

-- query 102
USE sql_tests_complex_test_array_fn;
select array_agg(f_1) from array_test where pk = 1;

-- query 103
USE sql_tests_complex_test_array_fn;
select array_agg(d_1) from array_test where pk = 1;

-- query 104
USE sql_tests_complex_test_array_fn;
select array_agg(d_2) from array_test where pk = 1;

-- query 105
USE sql_tests_complex_test_array_fn;
select array_agg(d_3) from array_test where pk = 1;

-- query 106
USE sql_tests_complex_test_array_fn;
select array_agg(d_4) from array_test where pk = 1;

-- query 107
USE sql_tests_complex_test_array_fn;
select array_agg(d_5) from array_test where pk = 1;

-- query 108
USE sql_tests_complex_test_array_fn;
select array_agg(d_6) from array_test where pk = 1;

-- query 109
USE sql_tests_complex_test_array_fn;
select array_agg(ai_1) from array_test where pk = 1;

-- query 110
USE sql_tests_complex_test_array_fn;
select array_agg(as_1) from array_test where pk = 1;

-- query 111
USE sql_tests_complex_test_array_fn;
select array_agg(aas_1) from array_test where pk = 1;

-- query 112
USE sql_tests_complex_test_array_fn;
select array_agg(aad_1) from array_test where pk = 1;

-- query 113
USE sql_tests_complex_test_array_fn;
select array_agg(NULL) from array_test where pk = 1;

-- query 114
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(s_1)) from array_test where pk = 1;

-- query 115
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(i_1)) from array_test where pk = 1;

-- query 116
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(f_1)) from array_test where pk = 1;

-- query 117
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(d_1)) from array_test where pk = 1;

-- query 118
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(d_2)) from array_test where pk = 1;

-- query 119
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(d_3)) from array_test where pk = 1;

-- query 120
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(d_4)) from array_test where pk = 1;

-- query 121
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(d_5)) from array_test where pk = 1;

-- query 122
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(d_6)) from array_test where pk = 1;

-- query 123
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(ai_1)) from array_test where pk = 1;

-- query 124
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(as_1)) from array_test where pk = 1;

-- query 125
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(aas_1)) from array_test where pk = 1;

-- query 126
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(aad_1)) from array_test where pk = 1;

-- query 127
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(NULL)) from array_test where pk = 1;

-- query 128
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(d_1)) from array_agg_test where pk = 1;

-- query 129
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(d_2)) from array_agg_test where pk = 1;

-- query 130
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(d_3)) from array_agg_test where pk = 1;

-- query 131
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(d_4)) from array_agg_test where pk = 1;

-- query 132
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(d_5)) from array_agg_test where pk = 1;

-- query 133
USE sql_tests_complex_test_array_fn;
select array_distinct(array_agg(d_6)) from array_agg_test where pk = 1;

-- query 134
USE sql_tests_complex_test_array_fn;
select array_agg(s_1 order by pk) from array_test;

-- query 135
USE sql_tests_complex_test_array_fn;
select array_agg(i_1 order by pk) from array_test;

-- query 136
USE sql_tests_complex_test_array_fn;
select array_agg(f_1 order by pk) from array_test;

-- query 137
USE sql_tests_complex_test_array_fn;
select array_agg(d_1 order by pk) from array_test;

-- query 138
USE sql_tests_complex_test_array_fn;
select array_agg(d_2 order by pk) from array_test;

-- query 139
USE sql_tests_complex_test_array_fn;
select array_agg(d_3 order by pk) from array_test;

-- query 140
USE sql_tests_complex_test_array_fn;
select array_agg(d_4 order by pk) from array_test;

-- query 141
USE sql_tests_complex_test_array_fn;
select array_agg(d_5 order by pk) from array_test;

-- query 142
USE sql_tests_complex_test_array_fn;
select array_agg(d_6 order by pk) from array_test;

-- query 143
USE sql_tests_complex_test_array_fn;
select array_agg(ai_1 order by pk) from array_test;

-- query 144
USE sql_tests_complex_test_array_fn;
select array_agg(as_1 order by pk) from array_test;

-- query 145
USE sql_tests_complex_test_array_fn;
select array_agg(aas_1 order by pk) from array_test;

-- query 146
USE sql_tests_complex_test_array_fn;
select array_agg(aad_1 order by pk) from array_test;

-- query 147
USE sql_tests_complex_test_array_fn;
select array_agg([1.0,2.1,3.2,4.3]) from array_test;

-- query 148
USE sql_tests_complex_test_array_fn;
select array_agg(['a', 'b', 'c']) from array_test;

-- query 149
USE sql_tests_complex_test_array_fn;
select array_agg([[1,2,3], [2,3,4]]) from array_test;

-- query 150
USE sql_tests_complex_test_array_fn;
select reverse(s_1) from array_test order by pk;

-- query 151
USE sql_tests_complex_test_array_fn;
select reverse(i_1) from array_test order by pk;

-- query 152
USE sql_tests_complex_test_array_fn;
select reverse(f_1) from array_test order by pk;

-- query 153
USE sql_tests_complex_test_array_fn;
select reverse(d_1) from array_test order by pk;

-- query 154
USE sql_tests_complex_test_array_fn;
select reverse(d_2) from array_test order by pk;

-- query 155
USE sql_tests_complex_test_array_fn;
select reverse(d_3) from array_test order by pk;

-- query 156
USE sql_tests_complex_test_array_fn;
select reverse(d_4) from array_test order by pk;

-- query 157
USE sql_tests_complex_test_array_fn;
select reverse(d_5) from array_test order by pk;

-- query 158
USE sql_tests_complex_test_array_fn;
select reverse(d_6) from array_test order by pk;

-- query 159
USE sql_tests_complex_test_array_fn;
select reverse(ai_1) from array_test order by pk;

-- query 160
USE sql_tests_complex_test_array_fn;
select reverse(as_1) from array_test order by pk;

-- query 161
USE sql_tests_complex_test_array_fn;
select reverse(aas_1) from array_test order by pk;

-- query 162
USE sql_tests_complex_test_array_fn;
select reverse(aad_1) from array_test order by pk;

-- query 163
USE sql_tests_complex_test_array_fn;
select reverse(NULL) from array_test order by pk;

-- query 164
USE sql_tests_complex_test_array_fn;
select reverse([1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 165
USE sql_tests_complex_test_array_fn;
select reverse(['a', 'b', 'c']) from array_test order by pk;

-- query 166
USE sql_tests_complex_test_array_fn;
select reverse([[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 167
-- @expect_error=array_difference function only support numeric array types
USE sql_tests_complex_test_array_fn;
select array_difference(s_1) from array_test order by pk;

-- query 168
USE sql_tests_complex_test_array_fn;
select array_difference(i_1) from array_test order by pk;

-- query 169
USE sql_tests_complex_test_array_fn;
select array_difference(f_1) from array_test order by pk;

-- query 170
USE sql_tests_complex_test_array_fn;
select array_difference(d_1) from array_test order by pk;

-- query 171
USE sql_tests_complex_test_array_fn;
select array_difference(d_2) from array_test order by pk;

-- query 172
USE sql_tests_complex_test_array_fn;
select array_difference(d_3) from array_test order by pk;

-- query 173
USE sql_tests_complex_test_array_fn;
select array_difference(d_4) from array_test order by pk;

-- query 174
USE sql_tests_complex_test_array_fn;
select array_difference(d_5) from array_test order by pk;

-- query 175
USE sql_tests_complex_test_array_fn;
select array_difference(d_6) from array_test order by pk;

-- query 176
-- @expect_error=No matching function with signature: array_difference(array<array<bigint(20)>>
USE sql_tests_complex_test_array_fn;
select array_difference(ai_1) from array_test order by pk;

-- query 177
-- @expect_error=No matching function with signature: array_difference(array<array<varchar(65533)>>
USE sql_tests_complex_test_array_fn;
select array_difference(as_1) from array_test order by pk;

-- query 178
-- @expect_error=No matching function with signature: array_difference(array<array<array<varchar(65533)>>>
USE sql_tests_complex_test_array_fn;
select array_difference(aas_1) from array_test order by pk;

-- query 179
-- @expect_error=No matching function with signature: array_difference(array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_difference(aad_1) from array_test order by pk;

-- query 180
USE sql_tests_complex_test_array_fn;
select array_difference(NULL) from array_test order by pk;

-- query 181
USE sql_tests_complex_test_array_fn;
select array_difference([1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 182
-- @expect_error=array_difference function only support numeric array types
USE sql_tests_complex_test_array_fn;
select array_difference(['a', 'b', 'c']) from array_test order by pk;

-- query 183
-- @expect_error=No matching function with signature: array_difference(array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_difference([[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 184
USE sql_tests_complex_test_array_fn;
select array_append(s_1, 1) from array_test order by pk;

-- query 185
USE sql_tests_complex_test_array_fn;
select array_append(s_1, -10) from array_test order by pk;

-- query 186
USE sql_tests_complex_test_array_fn;
select array_append(i_1, 100) from array_test order by pk;

-- query 187
USE sql_tests_complex_test_array_fn;
select array_append(i_1, -1) from array_test order by pk;

-- query 188
USE sql_tests_complex_test_array_fn;
select array_append(i_1, -10) from array_test order by pk;

-- query 189
USE sql_tests_complex_test_array_fn;
select array_append(d_2, 10) from array_test order by pk;

-- query 190
USE sql_tests_complex_test_array_fn;
select array_append(d_2, 100) from array_test order by pk;

-- query 191
USE sql_tests_complex_test_array_fn;
select array_append(d_2, -1) from array_test order by pk;

-- query 192
USE sql_tests_complex_test_array_fn;
select array_append(d_2, -10) from array_test order by pk;

-- query 193
USE sql_tests_complex_test_array_fn;
select array_append(d_3, 1) from array_test order by pk;

-- query 194
USE sql_tests_complex_test_array_fn;
select array_append(d_3, -1) from array_test order by pk;

-- query 195
USE sql_tests_complex_test_array_fn;
select array_append(d_3, -10) from array_test order by pk;

-- query 196
USE sql_tests_complex_test_array_fn;
select array_append(d_4, 1) from array_test order by pk;

-- query 197
USE sql_tests_complex_test_array_fn;
select array_append(d_4, 2) from array_test order by pk;

-- query 198
USE sql_tests_complex_test_array_fn;
select array_append(d_4, 3) from array_test order by pk;

-- query 199
USE sql_tests_complex_test_array_fn;
select array_append(d_6, 10) from array_test order by pk;

-- query 200
USE sql_tests_complex_test_array_fn;
select array_append(d_6, 100) from array_test order by pk;

-- query 201
USE sql_tests_complex_test_array_fn;
select array_append(d_6, -1) from array_test order by pk;

-- query 202
-- @expect_error=No matching function with signature: array_append(array<array<bigint(20)>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_append(ai_1, 1) from array_test order by pk;

-- query 203
-- @expect_error=No matching function with signature: array_append(array<array<bigint(20)>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_append(ai_1, 2) from array_test order by pk;

-- query 204
-- @expect_error=No matching function with signature: array_append(array<array<varchar(65533)>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_append(as_1, -10) from array_test order by pk;

-- query 205
-- @expect_error=No matching function with signature: array_append(array<array<array<varchar(65533)>>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_append(aas_1, 1) from array_test order by pk;

-- query 206
-- @expect_error=No matching function with signature: array_append(array<array<array<DECIMAL128(26,2)>>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_append(aad_1, -1) from array_test order by pk;

-- query 207
-- @expect_error=No matching function with signature: array_append(array<array<array<DECIMAL128(26,2)>>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_append(aad_1, -10) from array_test order by pk;

-- query 208
USE sql_tests_complex_test_array_fn;
select array_append(NULL, 1) from array_test order by pk;

-- query 209
USE sql_tests_complex_test_array_fn;
select array_append([1.0,2.1,3.2,4.3], -10) from array_test order by pk;

-- query 210
USE sql_tests_complex_test_array_fn;
select array_append(['a', 'b', 'c'], 1) from array_test order by pk;

-- query 211
-- @expect_error=No matching function with signature: array_append(array<array<tinyint(4)>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_append([[1,2,3], [2,3,4]], 1) from array_test order by pk;

-- query 212
USE sql_tests_complex_test_array_fn;
select array_contains(s_1, 1) from array_test order by pk;

-- query 213
USE sql_tests_complex_test_array_fn;
select array_contains(s_1, -10) from array_test order by pk;

-- query 214
USE sql_tests_complex_test_array_fn;
select array_contains(i_1, 1) from array_test order by pk;

-- query 215
USE sql_tests_complex_test_array_fn;
select array_contains(f_1, 100) from array_test order by pk;

-- query 216
USE sql_tests_complex_test_array_fn;
select array_contains(f_1, -10) from array_test order by pk;

-- query 217
USE sql_tests_complex_test_array_fn;
select array_contains(d_1, 100) from array_test order by pk;

-- query 218
USE sql_tests_complex_test_array_fn;
select array_contains(d_1, -1) from array_test order by pk;

-- query 219
USE sql_tests_complex_test_array_fn;
select array_contains(d_2, 100) from array_test order by pk;

-- query 220
USE sql_tests_complex_test_array_fn;
select array_contains(d_2, -1) from array_test order by pk;

-- query 221
USE sql_tests_complex_test_array_fn;
select array_contains(d_2, -10) from array_test order by pk;

-- query 222
USE sql_tests_complex_test_array_fn;
select array_contains(d_3, -10) from array_test order by pk;

-- query 223
USE sql_tests_complex_test_array_fn;
select array_contains(d_4, 100) from array_test order by pk;

-- query 224
USE sql_tests_complex_test_array_fn;
select array_contains(d_5, -1) from array_test order by pk;

-- query 225
USE sql_tests_complex_test_array_fn;
select array_contains(d_6, 100) from array_test order by pk;

-- query 226
USE sql_tests_complex_test_array_fn;
select array_contains(d_6, -1) from array_test order by pk;

-- query 227
-- @expect_error=No matching function with signature: array_contains(array<array<bigint(20)>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_contains(ai_1, 1) from array_test order by pk;

-- query 228
-- @expect_error=No matching function with signature: array_contains(array<array<varchar(65533)>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_contains(as_1, -1) from array_test order by pk;

-- query 229
-- @expect_error=No matching function with signature: array_contains(array<array<array<varchar(65533)>>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_contains(aas_1, 100) from array_test order by pk;

-- query 230
-- @expect_error=No matching function with signature: array_contains(array<array<array<DECIMAL128(26,2)>>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_contains(aad_1, -10) from array_test order by pk;

-- query 231
USE sql_tests_complex_test_array_fn;
select array_contains(NULL, -10) from array_test order by pk;

-- query 232
USE sql_tests_complex_test_array_fn;
select array_contains([1.0,2.1,3.2,4.3], 1) from array_test order by pk;

-- query 233
USE sql_tests_complex_test_array_fn;
select array_contains(['a', 'b', 'c'], -10) from array_test order by pk;

-- query 234
-- @expect_error=No matching function with signature: array_contains(array<array<tinyint(4)>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_contains([[1,2,3], [2,3,4]], 100) from array_test order by pk;

-- query 235
USE sql_tests_complex_test_array_fn;
select array_contains([parse_json('{"addr": 1}'), parse_json('{"addr": 2}')], parse_json('{"addr": 1}'));

-- query 236
USE sql_tests_complex_test_array_fn;
select array_contains([parse_json('{"addr": 1}'), parse_json('{"addr": 2}')], parse_json('{"addr": 3}'));

-- query 237
USE sql_tests_complex_test_array_fn;
select array_contains_all([parse_json('{"addr": 1}'), parse_json('{"addr": 2}')], 
                          [parse_json('{"addr": 1}')]);

-- query 238
USE sql_tests_complex_test_array_fn;
select array_contains_all([parse_json('{"addr": 1}'), parse_json('{"addr": 2}')], 
                          [parse_json('{"addr": 1}'), parse_json('{"addr": 2}')]);

-- query 239
USE sql_tests_complex_test_array_fn;
select array_contains_all([parse_json('{"addr": 1}'), parse_json('{"addr": 2}')], 
                          [parse_json('{"addr": 1}'), parse_json('{"addr": 2}'), parse_json('{"addr": 3}')]);

-- query 240
USE sql_tests_complex_test_array_fn;
select array_remove(s_1, 1) from array_test order by pk;

-- query 241
USE sql_tests_complex_test_array_fn;
select array_remove(i_1, -10) from array_test order by pk;

-- query 242
USE sql_tests_complex_test_array_fn;
select array_remove(f_1, -10) from array_test order by pk;

-- query 243
USE sql_tests_complex_test_array_fn;
select array_remove(d_1, -10) from array_test order by pk;

-- query 244
USE sql_tests_complex_test_array_fn;
select array_remove(d_2, -2) from array_test order by pk;

-- query 245
USE sql_tests_complex_test_array_fn;
select array_remove(d_3, 1) from array_test order by pk;

-- query 246
USE sql_tests_complex_test_array_fn;
select array_remove(d_6, -10) from array_test order by pk;

-- query 247
-- @expect_error=No matching function with signature: array_remove(array<array<bigint(20)>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_remove(ai_1, 1) from array_test order by pk;

-- query 248
-- @expect_error=No matching function with signature: array_remove(array<array<varchar(65533)>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_remove(as_1, 2) from array_test order by pk;

-- query 249
-- @expect_error=No matching function with signature: array_remove(array<array<array<varchar(65533)>>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_remove(aas_1, 1) from array_test order by pk;

-- query 250
-- @expect_error=No matching function with signature: array_remove(array<array<array<DECIMAL128(26,2)>>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_remove(aad_1, 3) from array_test order by pk;

-- query 251
USE sql_tests_complex_test_array_fn;
select array_remove(NULL, 1) from array_test order by pk;

-- query 252
USE sql_tests_complex_test_array_fn;
select array_remove([1.0,2.1,3.2,4.3], 1) from array_test order by pk;

-- query 253
USE sql_tests_complex_test_array_fn;
select array_remove(['a', 'b', 'c'], 'a') from array_test order by pk;

-- query 254
USE sql_tests_complex_test_array_fn;
select array_remove(['a', 'b', 'c'], -10) from array_test order by pk;

-- query 255
-- @expect_error=No matching function with signature: array_remove(array<array<tinyint(4)>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_remove([[1,2,3], [2,3,4]], 1) from array_test order by pk;

-- query 256
USE sql_tests_complex_test_array_fn;
select array_remove([parse_json('{"addr": 1}'), parse_json('{"addr": 2}')], parse_json('{"addr": 1}'));

-- query 257
USE sql_tests_complex_test_array_fn;
select array_remove([parse_json('{"addr": 1}'), parse_json('{"addr": 2}')], parse_json('{"addr": 3}'));

-- query 258
USE sql_tests_complex_test_array_fn;
select array_position(s_1, 1) from array_test order by pk;

-- query 259
USE sql_tests_complex_test_array_fn;
select array_position(d_6, -1) from array_test order by pk;

-- query 260
USE sql_tests_complex_test_array_fn;
select array_position(d_6, -10) from array_test order by pk;

-- query 261
-- @expect_error=No matching function with signature: array_position(array<array<bigint(20)>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_position(ai_1, 1) from array_test order by pk;

-- query 262
-- @expect_error=No matching function with signature: array_position(array<array<varchar(65533)>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_position(as_1, -10) from array_test order by pk;

-- query 263
-- @expect_error=No matching function with signature: array_position(array<array<array<varchar(65533)>>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_position(aas_1, 1) from array_test order by pk;

-- query 264
-- @expect_error=No matching function with signature: array_position(array<array<array<DECIMAL128(26,2)>>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_position(aad_1, -10) from array_test order by pk;

-- query 265
USE sql_tests_complex_test_array_fn;
select array_position(NULL, -10) from array_test order by pk;

-- query 266
USE sql_tests_complex_test_array_fn;
select array_position([1.0,2.1,3.2,4.3], 1) from array_test order by pk;

-- query 267
USE sql_tests_complex_test_array_fn;
select array_position(['a', 'b', 'c'], "c") from array_test order by pk;

-- query 268
-- @expect_error=No matching function with signature: array_position(array<array<tinyint(4)>>, tinyint(4
USE sql_tests_complex_test_array_fn;
select array_position([[1,2,3], [2,3,4]], 3) from array_test order by pk;

-- query 269
USE sql_tests_complex_test_array_fn;
select array_slice(s_1, 1) from array_test order by pk;

-- query 270
USE sql_tests_complex_test_array_fn;
select array_slice(s_1, 2) from array_test order by pk;

-- query 271
USE sql_tests_complex_test_array_fn;
select array_slice(s_1, 3) from array_test order by pk;

-- query 272
USE sql_tests_complex_test_array_fn;
select array_slice(i_1, -1) from array_test order by pk;

-- query 273
USE sql_tests_complex_test_array_fn;
select array_slice(i_1, -10) from array_test order by pk;

-- query 274
USE sql_tests_complex_test_array_fn;
select array_slice(f_1, 1) from array_test order by pk;

-- query 275
USE sql_tests_complex_test_array_fn;
select array_slice(f_1, 2) from array_test order by pk;

-- query 276
USE sql_tests_complex_test_array_fn;
select array_slice(f_1, 3) from array_test order by pk;

-- query 277
USE sql_tests_complex_test_array_fn;
select array_slice(f_1, 10) from array_test order by pk;

-- query 278
USE sql_tests_complex_test_array_fn;
select array_slice(f_1, 100) from array_test order by pk;

-- query 279
USE sql_tests_complex_test_array_fn;
select array_slice(d_1, -1) from array_test order by pk;

-- query 280
USE sql_tests_complex_test_array_fn;
select array_slice(d_1, -10) from array_test order by pk;

-- query 281
USE sql_tests_complex_test_array_fn;
select array_slice(d_2, 1) from array_test order by pk;

-- query 282
USE sql_tests_complex_test_array_fn;
select array_slice(d_2, 2) from array_test order by pk;

-- query 283
USE sql_tests_complex_test_array_fn;
select array_slice(d_2, 3) from array_test order by pk;

-- query 284
USE sql_tests_complex_test_array_fn;
select array_slice(d_2, 10) from array_test order by pk;

-- query 285
USE sql_tests_complex_test_array_fn;
select array_slice(d_2, 100) from array_test order by pk;

-- query 286
USE sql_tests_complex_test_array_fn;
select array_slice(d_3, -1) from array_test order by pk;

-- query 287
USE sql_tests_complex_test_array_fn;
select array_slice(d_3, -10) from array_test order by pk;

-- query 288
USE sql_tests_complex_test_array_fn;
select array_slice(d_4, 1) from array_test order by pk;

-- query 289
USE sql_tests_complex_test_array_fn;
select array_slice(d_4, 2) from array_test order by pk;

-- query 290
USE sql_tests_complex_test_array_fn;
select array_slice(d_4, 3) from array_test order by pk;

-- query 291
USE sql_tests_complex_test_array_fn;
select array_slice(d_4, 10) from array_test order by pk;

-- query 292
USE sql_tests_complex_test_array_fn;
select array_slice(d_4, 100) from array_test order by pk;

-- query 293
USE sql_tests_complex_test_array_fn;
select array_slice(d_4, -1) from array_test order by pk;

-- query 294
USE sql_tests_complex_test_array_fn;
select array_slice(d_5, -1) from array_test order by pk;

-- query 295
USE sql_tests_complex_test_array_fn;
select array_slice(d_5, -10) from array_test order by pk;

-- query 296
USE sql_tests_complex_test_array_fn;
select array_slice(d_6, 1) from array_test order by pk;

-- query 297
USE sql_tests_complex_test_array_fn;
select array_slice(d_6, 2) from array_test order by pk;

-- query 298
USE sql_tests_complex_test_array_fn;
select array_slice(d_6, -10) from array_test order by pk;

-- query 299
USE sql_tests_complex_test_array_fn;
select array_slice(ai_1, 1) from array_test order by pk;

-- query 300
USE sql_tests_complex_test_array_fn;
select array_slice(ai_1, 2) from array_test order by pk;

-- query 301
USE sql_tests_complex_test_array_fn;
select array_slice(as_1, 100) from array_test order by pk;

-- query 302
USE sql_tests_complex_test_array_fn;
select array_slice(as_1, -1) from array_test order by pk;

-- query 303
USE sql_tests_complex_test_array_fn;
select array_slice(as_1, -10) from array_test order by pk;

-- query 304
USE sql_tests_complex_test_array_fn;
select array_slice(aas_1, 1) from array_test order by pk;

-- query 305
USE sql_tests_complex_test_array_fn;
select array_slice(aas_1, 2) from array_test order by pk;

-- query 306
USE sql_tests_complex_test_array_fn;
select array_slice(aad_1, 10) from array_test order by pk;

-- query 307
USE sql_tests_complex_test_array_fn;
select array_slice(aad_1, 100) from array_test order by pk;

-- query 308
USE sql_tests_complex_test_array_fn;
select array_slice(aad_1, -1) from array_test order by pk;

-- query 309
USE sql_tests_complex_test_array_fn;
select array_slice(aad_1, -10) from array_test order by pk;

-- query 310
USE sql_tests_complex_test_array_fn;
select array_slice(NULL, 1) from array_test order by pk;

-- query 311
USE sql_tests_complex_test_array_fn;
select array_slice([1.0,2.1,3.2,4.3], 3) from array_test order by pk;

-- query 312
USE sql_tests_complex_test_array_fn;
select array_slice([1.0,2.1,3.2,4.3], 10) from array_test order by pk;

-- query 313
USE sql_tests_complex_test_array_fn;
select array_slice([1.0,2.1,3.2,4.3], 100) from array_test order by pk;

-- query 314
USE sql_tests_complex_test_array_fn;
select array_slice([1.0,2.1,3.2,4.3], -1) from array_test order by pk;

-- query 315
USE sql_tests_complex_test_array_fn;
select array_slice([1.0,2.1,3.2,4.3], -10) from array_test order by pk;

-- query 316
USE sql_tests_complex_test_array_fn;
select array_slice([[1,2,3], [2,3,4]], 2) from array_test order by pk;

-- query 317
USE sql_tests_complex_test_array_fn;
select array_slice([[1,2,3], [2,3,4]], -10) from array_test order by pk;

-- query 318
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, s_1) from array_test order by pk;

-- query 319
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, i_1) from array_test order by pk;

-- query 320
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, f_1) from array_test order by pk;

-- query 321
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, d_1) from array_test order by pk;

-- query 322
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, d_2) from array_test order by pk;

-- query 323
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, d_3) from array_test order by pk;

-- query 324
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, d_4) from array_test order by pk;

-- query 325
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, d_5) from array_test order by pk;

-- query 326
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, d_6) from array_test order by pk;

-- query 327
-- @expect_error=No matching function with signature: array_concat(array<varchar(65533)>, array<array<bigint(20)>>
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, ai_1) from array_test order by pk;

-- query 328
-- @expect_error=No matching function with signature: array_concat(array<varchar(65533)>, array<array<varchar(65533)>>
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, as_1) from array_test order by pk;

-- query 329
-- @expect_error=No matching function with signature: array_concat(array<varchar(65533)>, array<array<array<varchar(65533)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, aas_1) from array_test order by pk;

-- query 330
-- @expect_error=No matching function with signature: array_concat(array<varchar(65533)>, array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, aad_1) from array_test order by pk;

-- query 331
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, NULL) from array_test order by pk;

-- query 332
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, [1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 333
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, ['a', 'b', 'c']) from array_test order by pk;

-- query 334
-- @expect_error=No matching function with signature: array_concat(array<varchar(65533)>, array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_concat(s_1, [[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 335
USE sql_tests_complex_test_array_fn;
select array_concat(i_1, s_1) from array_test order by pk;

-- query 336
USE sql_tests_complex_test_array_fn;
select array_concat(i_1, i_1) from array_test order by pk;

-- query 337
USE sql_tests_complex_test_array_fn;
select array_concat(i_1, f_1) from array_test order by pk;

-- query 338
USE sql_tests_complex_test_array_fn;
select array_concat(i_1, d_1) from array_test order by pk;

-- query 339
USE sql_tests_complex_test_array_fn;
select array_concat(i_1, d_2) from array_test order by pk;

-- query 340
USE sql_tests_complex_test_array_fn;
select array_concat(i_1, d_3) from array_test order by pk;

-- query 341
USE sql_tests_complex_test_array_fn;
select array_concat(f_1, d_4) from array_test order by pk;

-- query 342
USE sql_tests_complex_test_array_fn;
select array_concat(f_1, d_5) from array_test order by pk;

-- query 343
USE sql_tests_complex_test_array_fn;
select array_concat(f_1, d_6) from array_test order by pk;

-- query 344
-- @expect_error=No matching function with signature: array_concat(array<double>, array<array<bigint(20)>>
USE sql_tests_complex_test_array_fn;
select array_concat(f_1, ai_1) from array_test order by pk;

-- query 345
-- @expect_error=No matching function with signature: array_concat(array<double>, array<array<varchar(65533)>>
USE sql_tests_complex_test_array_fn;
select array_concat(f_1, as_1) from array_test order by pk;

-- query 346
-- @expect_error=No matching function with signature: array_concat(array<double>, array<array<array<varchar(65533)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(f_1, aas_1) from array_test order by pk;

-- query 347
-- @expect_error=No matching function with signature: array_concat(array<double>, array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(f_1, aad_1) from array_test order by pk;

-- query 348
USE sql_tests_complex_test_array_fn;
select array_concat(f_1, NULL) from array_test order by pk;

-- query 349
USE sql_tests_complex_test_array_fn;
select array_concat(f_1, [1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 350
USE sql_tests_complex_test_array_fn;
select array_concat(f_1, ['a', 'b', 'c']) from array_test order by pk;

-- query 351
-- @expect_error=No matching function with signature: array_concat(array<double>, array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_concat(f_1, [[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 352
USE sql_tests_complex_test_array_fn;
select array_concat(d_1, s_1) from array_test order by pk;

-- query 353
USE sql_tests_complex_test_array_fn;
select array_concat(d_1, i_1) from array_test order by pk;

-- query 354
USE sql_tests_complex_test_array_fn;
select array_concat(d_1, f_1) from array_test order by pk;

-- query 355
USE sql_tests_complex_test_array_fn;
select array_concat(d_1, d_1) from array_test order by pk;

-- query 356
USE sql_tests_complex_test_array_fn;
select array_concat(d_1, d_2) from array_test order by pk;

-- query 357
USE sql_tests_complex_test_array_fn;
select array_concat(d_1, d_3) from array_test order by pk;

-- query 358
USE sql_tests_complex_test_array_fn;
select array_concat(d_3, s_1) from array_test order by pk;

-- query 359
USE sql_tests_complex_test_array_fn;
select array_concat(d_3, d_4) from array_test order by pk;

-- query 360
USE sql_tests_complex_test_array_fn;
select array_concat(d_3, d_5) from array_test order by pk;

-- query 361
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL128(25,19)>, array<array<bigint(20)>>
USE sql_tests_complex_test_array_fn;
select array_concat(d_3, ai_1) from array_test order by pk;

-- query 362
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL128(25,19)>, array<array<varchar(65533)>>
USE sql_tests_complex_test_array_fn;
select array_concat(d_3, as_1) from array_test order by pk;

-- query 363
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL128(25,19)>, array<array<array<varchar(65533)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(d_3, aas_1) from array_test order by pk;

-- query 364
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL128(25,19)>, array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(d_3, aad_1) from array_test order by pk;

-- query 365
USE sql_tests_complex_test_array_fn;
select array_concat(d_3, NULL) from array_test order by pk;

-- query 366
USE sql_tests_complex_test_array_fn;
select array_concat(d_3, [1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 367
USE sql_tests_complex_test_array_fn;
select array_concat(d_3, ['a', 'b', 'c']) from array_test order by pk;

-- query 368
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL128(25,19)>, array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_concat(d_3, [[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 369
USE sql_tests_complex_test_array_fn;
select array_concat(d_4, s_1) from array_test order by pk;

-- query 370
USE sql_tests_complex_test_array_fn;
select array_concat(d_4, i_1) from array_test order by pk;

-- query 371
USE sql_tests_complex_test_array_fn;
select array_concat(d_4, f_1) from array_test order by pk;

-- query 372
USE sql_tests_complex_test_array_fn;
select array_concat(d_4, d_3) from array_test order by pk;

-- query 373
USE sql_tests_complex_test_array_fn;
select array_concat(d_4, d_4) from array_test order by pk;

-- query 374
USE sql_tests_complex_test_array_fn;
select array_concat(d_4, d_5) from array_test order by pk;

-- query 375
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL32(8,5)>, array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(d_4, aad_1) from array_test order by pk;

-- query 376
USE sql_tests_complex_test_array_fn;
select array_concat(d_4, NULL) from array_test order by pk;

-- query 377
USE sql_tests_complex_test_array_fn;
select array_concat(d_4, [1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 378
USE sql_tests_complex_test_array_fn;
select array_concat(d_4, ['a', 'b', 'c']) from array_test order by pk;

-- query 379
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL32(8,5)>, array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_concat(d_4, [[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 380
USE sql_tests_complex_test_array_fn;
select array_concat(d_5, s_1) from array_test order by pk;

-- query 381
USE sql_tests_complex_test_array_fn;
select array_concat(d_5, i_1) from array_test order by pk;

-- query 382
USE sql_tests_complex_test_array_fn;
select array_concat(d_5, f_1) from array_test order by pk;

-- query 383
USE sql_tests_complex_test_array_fn;
select array_concat(d_5, d_2) from array_test order by pk;

-- query 384
USE sql_tests_complex_test_array_fn;
select array_concat(d_5, d_3) from array_test order by pk;

-- query 385
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL64(16,3)>, array<array<array<varchar(65533)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(d_5, aas_1) from array_test order by pk;

-- query 386
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL64(16,3)>, array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(d_5, aad_1) from array_test order by pk;

-- query 387
USE sql_tests_complex_test_array_fn;
select array_concat(d_5, NULL) from array_test order by pk;

-- query 388
USE sql_tests_complex_test_array_fn;
select array_concat(d_5, [1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 389
USE sql_tests_complex_test_array_fn;
select array_concat(d_5, ['a', 'b', 'c']) from array_test order by pk;

-- query 390
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL64(16,3)>, array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_concat(d_5, [[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 391
USE sql_tests_complex_test_array_fn;
select array_concat(d_6, s_1) from array_test order by pk;

-- query 392
USE sql_tests_complex_test_array_fn;
select array_concat(d_6, i_1) from array_test order by pk;

-- query 393
USE sql_tests_complex_test_array_fn;
select array_concat(d_6, f_1) from array_test order by pk;

-- query 394
USE sql_tests_complex_test_array_fn;
select array_concat(d_6, d_2) from array_test order by pk;

-- query 395
USE sql_tests_complex_test_array_fn;
select array_concat(d_6, d_3) from array_test order by pk;

-- query 396
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL128(18,6)>, array<array<array<varchar(65533)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(d_6, aas_1) from array_test order by pk;

-- query 397
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL128(18,6)>, array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(d_6, aad_1) from array_test order by pk;

-- query 398
USE sql_tests_complex_test_array_fn;
select array_concat(d_6, NULL) from array_test order by pk;

-- query 399
USE sql_tests_complex_test_array_fn;
select array_concat(d_6, [1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 400
USE sql_tests_complex_test_array_fn;
select array_concat(d_6, ['a', 'b', 'c']) from array_test order by pk;

-- query 401
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL128(18,6)>, array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_concat(d_6, [[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 402
-- @expect_error=No matching function with signature: array_concat(array<array<bigint(20)>>, array<varchar(65533)>
USE sql_tests_complex_test_array_fn;
select array_concat(ai_1, s_1) from array_test order by pk;

-- query 403
-- @expect_error=No matching function with signature: array_concat(array<array<bigint(20)>>, array<bigint(20)>
USE sql_tests_complex_test_array_fn;
select array_concat(ai_1, i_1) from array_test order by pk;

-- query 404
-- @expect_error=No matching function with signature: array_concat(array<array<bigint(20)>>, array<double>
USE sql_tests_complex_test_array_fn;
select array_concat(ai_1, f_1) from array_test order by pk;

-- query 405
-- @expect_error=No matching function with signature: array_concat(array<array<bigint(20)>>, array<DECIMAL64(4,3)>
USE sql_tests_complex_test_array_fn;
select array_concat(ai_1, d_2) from array_test order by pk;

-- query 406
-- @expect_error=No matching function with signature: array_concat(array<array<bigint(20)>>, array<DECIMAL128(18,6)>
USE sql_tests_complex_test_array_fn;
select array_concat(ai_1, d_6) from array_test order by pk;

-- query 407
USE sql_tests_complex_test_array_fn;
select array_concat(ai_1, ai_1) from array_test order by pk;

-- query 408
USE sql_tests_complex_test_array_fn;
select array_concat(ai_1, as_1) from array_test order by pk;

-- query 409
-- @expect_error=No matching function with signature: array_concat(array<array<bigint(20)>>, array<array<array<varchar(65533)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(ai_1, aas_1) from array_test order by pk;

-- query 410
-- @expect_error=No matching function with signature: array_concat(array<array<bigint(20)>>, array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(ai_1, aad_1) from array_test order by pk;

-- query 411
USE sql_tests_complex_test_array_fn;
select array_concat(ai_1, NULL) from array_test order by pk;

-- query 412
-- @expect_error=No matching function with signature: array_concat(array<array<bigint(20)>>, array<DECIMAL32(2,1)>
USE sql_tests_complex_test_array_fn;
select array_concat(ai_1, [1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 413
-- @expect_error=No matching function with signature: array_concat(array<array<bigint(20)>>, array<varchar>
USE sql_tests_complex_test_array_fn;
select array_concat(ai_1, ['a', 'b', 'c']) from array_test order by pk;

-- query 414
USE sql_tests_complex_test_array_fn;
select array_concat(ai_1, [[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 415
-- @expect_error=No matching function with signature: array_concat(array<array<varchar(65533)>>, array<varchar(65533)>
USE sql_tests_complex_test_array_fn;
select array_concat(as_1, s_1) from array_test order by pk;

-- query 416
-- @expect_error=No matching function with signature: array_concat(array<array<varchar(65533)>>, array<bigint(20)>
USE sql_tests_complex_test_array_fn;
select array_concat(as_1, i_1) from array_test order by pk;

-- query 417
-- @expect_error=No matching function with signature: array_concat(array<array<varchar(65533)>>, array<double>
USE sql_tests_complex_test_array_fn;
select array_concat(as_1, f_1) from array_test order by pk;

-- query 418
-- @expect_error=No matching function with signature: array_concat(array<array<varchar(65533)>>, array<DECIMAL128(18,6)>
USE sql_tests_complex_test_array_fn;
select array_concat(as_1, d_6) from array_test order by pk;

-- query 419
USE sql_tests_complex_test_array_fn;
select array_concat(as_1, ai_1) from array_test order by pk;

-- query 420
USE sql_tests_complex_test_array_fn;
select array_concat(as_1, as_1) from array_test order by pk;

-- query 421
-- @expect_error=No matching function with signature: array_concat(array<array<varchar(65533)>>, array<array<array<varchar(65533)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(as_1, aas_1) from array_test order by pk;

-- query 422
-- @expect_error=No matching function with signature: array_concat(array<array<varchar(65533)>>, array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(as_1, aad_1) from array_test order by pk;

-- query 423
USE sql_tests_complex_test_array_fn;
select array_concat(as_1, NULL) from array_test order by pk;

-- query 424
-- @expect_error=No matching function with signature: array_concat(array<array<varchar(65533)>>, array<DECIMAL32(2,1)>
USE sql_tests_complex_test_array_fn;
select array_concat(as_1, [1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 425
-- @expect_error=No matching function with signature: array_concat(array<array<varchar(65533)>>, array<varchar>
USE sql_tests_complex_test_array_fn;
select array_concat(as_1, ['a', 'b', 'c']) from array_test order by pk;

-- query 426
USE sql_tests_complex_test_array_fn;
select array_concat(as_1, [[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 427
-- @expect_error=No matching function with signature: array_concat(array<array<array<varchar(65533)>>>, array<varchar(65533)>
USE sql_tests_complex_test_array_fn;
select array_concat(aas_1, s_1) from array_test order by pk;

-- query 428
-- @expect_error=No matching function with signature: array_concat(array<array<array<varchar(65533)>>>, array<bigint(20)>
USE sql_tests_complex_test_array_fn;
select array_concat(aas_1, i_1) from array_test order by pk;

-- query 429
-- @expect_error=No matching function with signature: array_concat(array<array<array<varchar(65533)>>>, array<double>
USE sql_tests_complex_test_array_fn;
select array_concat(aas_1, f_1) from array_test order by pk;

-- query 430
-- @expect_error=No matching function with signature: array_concat(array<array<array<varchar(65533)>>>, array<DECIMAL128(26,2)>
USE sql_tests_complex_test_array_fn;
select array_concat(aas_1, d_1) from array_test order by pk;

-- query 431
USE sql_tests_complex_test_array_fn;
select array_concat(aas_1, aas_1) from array_test order by pk;

-- query 432
USE sql_tests_complex_test_array_fn;
select array_concat(aas_1, aad_1) from array_test order by pk;

-- query 433
USE sql_tests_complex_test_array_fn;
select array_concat(aas_1, NULL) from array_test order by pk;

-- query 434
-- @expect_error=No matching function with signature: array_concat(array<array<array<varchar(65533)>>>, array<DECIMAL32(2,1)>
USE sql_tests_complex_test_array_fn;
select array_concat(aas_1, [1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 435
-- @expect_error=No matching function with signature: array_concat(array<array<array<varchar(65533)>>>, array<varchar>
USE sql_tests_complex_test_array_fn;
select array_concat(aas_1, ['a', 'b', 'c']) from array_test order by pk;

-- query 436
-- @expect_error=No matching function with signature: array_concat(array<array<array<varchar(65533)>>>, array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_concat(aas_1, [[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 437
-- @expect_error=No matching function with signature: array_concat(array<array<array<DECIMAL128(26,2)>>>, array<varchar(65533)>
USE sql_tests_complex_test_array_fn;
select array_concat(aad_1, s_1) from array_test order by pk;

-- query 438
-- @expect_error=No matching function with signature: array_concat(array<array<array<DECIMAL128(26,2)>>>, array<bigint(20)>
USE sql_tests_complex_test_array_fn;
select array_concat(aad_1, i_1) from array_test order by pk;

-- query 439
-- @expect_error=No matching function with signature: array_concat(array<array<array<DECIMAL128(26,2)>>>, array<double>
USE sql_tests_complex_test_array_fn;
select array_concat(aad_1, f_1) from array_test order by pk;

-- query 440
-- @expect_error=No matching function with signature: array_concat(array<array<array<DECIMAL128(26,2)>>>, array<DECIMAL128(26,2)>
USE sql_tests_complex_test_array_fn;
select array_concat(aad_1, d_1) from array_test order by pk;

-- query 441
-- @expect_error=No matching function with signature: array_concat(array<array<array<DECIMAL128(26,2)>>>, array<DECIMAL64(16,3)>
USE sql_tests_complex_test_array_fn;
select array_concat(aad_1, d_5) from array_test order by pk;

-- query 442
-- @expect_error=No matching function with signature: array_concat(array<array<array<DECIMAL128(26,2)>>>, array<DECIMAL128(18,6)>
USE sql_tests_complex_test_array_fn;
select array_concat(aad_1, d_6) from array_test order by pk;

-- query 443
-- @expect_error=No matching function with signature: array_concat(array<array<array<DECIMAL128(26,2)>>>, array<array<bigint(20)>>
USE sql_tests_complex_test_array_fn;
select array_concat(aad_1, ai_1) from array_test order by pk;

-- query 444
-- @expect_error=No matching function with signature: array_concat(array<array<array<DECIMAL128(26,2)>>>, array<array<varchar(65533)>>
USE sql_tests_complex_test_array_fn;
select array_concat(aad_1, as_1) from array_test order by pk;

-- query 445
USE sql_tests_complex_test_array_fn;
select array_concat(aad_1, aas_1) from array_test order by pk;

-- query 446
USE sql_tests_complex_test_array_fn;
select array_concat(aad_1, aad_1) from array_test order by pk;

-- query 447
USE sql_tests_complex_test_array_fn;
select array_concat(aad_1, NULL) from array_test order by pk;

-- query 448
-- @expect_error=No matching function with signature: array_concat(array<array<array<DECIMAL128(26,2)>>>, array<DECIMAL32(2,1)>
USE sql_tests_complex_test_array_fn;
select array_concat(aad_1, [1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 449
-- @expect_error=No matching function with signature: array_concat(array<array<array<DECIMAL128(26,2)>>>, array<varchar>
USE sql_tests_complex_test_array_fn;
select array_concat(aad_1, ['a', 'b', 'c']) from array_test order by pk;

-- query 450
-- @expect_error=No matching function with signature: array_concat(array<array<array<DECIMAL128(26,2)>>>, array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_concat(aad_1, [[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 451
USE sql_tests_complex_test_array_fn;
select array_concat(NULL, NULL) from array_test order by pk;

-- query 452
USE sql_tests_complex_test_array_fn;
select array_concat(NULL, [1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 453
USE sql_tests_complex_test_array_fn;
select array_concat(NULL, ['a', 'b', 'c']) from array_test order by pk;

-- query 454
USE sql_tests_complex_test_array_fn;
select array_concat(NULL, [[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 455
USE sql_tests_complex_test_array_fn;
select array_concat([1.0,2.1,3.2,4.3], s_1) from array_test order by pk;

-- query 456
USE sql_tests_complex_test_array_fn;
select array_concat([1.0,2.1,3.2,4.3], i_1) from array_test order by pk;

-- query 457
USE sql_tests_complex_test_array_fn;
select array_concat([1.0,2.1,3.2,4.3], f_1) from array_test order by pk;

-- query 458
USE sql_tests_complex_test_array_fn;
select array_concat([1.0,2.1,3.2,4.3], d_6) from array_test order by pk;

-- query 459
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL32(2,1)>, array<array<bigint(20)>>
USE sql_tests_complex_test_array_fn;
select array_concat([1.0,2.1,3.2,4.3], ai_1) from array_test order by pk;

-- query 460
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL32(2,1)>, array<array<varchar(65533)>>
USE sql_tests_complex_test_array_fn;
select array_concat([1.0,2.1,3.2,4.3], as_1) from array_test order by pk;

-- query 461
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL32(2,1)>, array<array<array<varchar(65533)>>>
USE sql_tests_complex_test_array_fn;
select array_concat([1.0,2.1,3.2,4.3], aas_1) from array_test order by pk;

-- query 462
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL32(2,1)>, array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_concat([1.0,2.1,3.2,4.3], aad_1) from array_test order by pk;

-- query 463
USE sql_tests_complex_test_array_fn;
select array_concat([1.0,2.1,3.2,4.3], NULL) from array_test order by pk;

-- query 464
USE sql_tests_complex_test_array_fn;
select array_concat([1.0,2.1,3.2,4.3], [1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 465
USE sql_tests_complex_test_array_fn;
select array_concat([1.0,2.1,3.2,4.3], ['a', 'b', 'c']) from array_test order by pk;

-- query 466
-- @expect_error=No matching function with signature: array_concat(array<DECIMAL32(2,1)>, array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_concat([1.0,2.1,3.2,4.3], [[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 467
USE sql_tests_complex_test_array_fn;
select array_concat(['a', 'b', 'c'], s_1) from array_test order by pk;

-- query 468
USE sql_tests_complex_test_array_fn;
select array_concat(['a', 'b', 'c'], f_1) from array_test order by pk;

-- query 469
USE sql_tests_complex_test_array_fn;
select array_concat(['a', 'b', 'c'], d_1) from array_test order by pk;

-- query 470
-- @expect_error=No matching function with signature: array_concat(array<varchar>, array<array<bigint(20)>>
USE sql_tests_complex_test_array_fn;
select array_concat(['a', 'b', 'c'], ai_1) from array_test order by pk;

-- query 471
-- @expect_error=No matching function with signature: array_concat(array<varchar>, array<array<varchar(65533)>>
USE sql_tests_complex_test_array_fn;
select array_concat(['a', 'b', 'c'], as_1) from array_test order by pk;

-- query 472
-- @expect_error=No matching function with signature: array_concat(array<varchar>, array<array<array<varchar(65533)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(['a', 'b', 'c'], aas_1) from array_test order by pk;

-- query 473
-- @expect_error=No matching function with signature: array_concat(array<varchar>, array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_concat(['a', 'b', 'c'], aad_1) from array_test order by pk;

-- query 474
USE sql_tests_complex_test_array_fn;
select array_concat(['a', 'b', 'c'], NULL) from array_test order by pk;

-- query 475
USE sql_tests_complex_test_array_fn;
select array_concat(['a', 'b', 'c'], [1.0,2.1,3.2,4.3]) from array_test order by pk;

-- query 476
USE sql_tests_complex_test_array_fn;
select array_concat(['a', 'b', 'c'], ['a', 'b', 'c']) from array_test order by pk;

-- query 477
-- @expect_error=No matching function with signature: array_concat(array<varchar>, array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_concat(['a', 'b', 'c'], [[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 478
-- @expect_error=No matching function with signature: array_concat(array<array<tinyint(4)>>, array<varchar(65533)>
USE sql_tests_complex_test_array_fn;
select array_concat([[1,2,3], [2,3,4]], s_1) from array_test order by pk;

-- query 479
-- @expect_error=No matching function with signature: array_concat(array<array<tinyint(4)>>, array<DECIMAL128(26,2)>
USE sql_tests_complex_test_array_fn;
select array_concat([[1,2,3], [2,3,4]], d_1) from array_test order by pk;

-- query 480
-- @expect_error=No matching function with signature: array_concat(array<array<tinyint(4)>>, array<DECIMAL64(4,3)>
USE sql_tests_complex_test_array_fn;
select array_concat([[1,2,3], [2,3,4]], d_2) from array_test order by pk;

-- query 481
-- @expect_error=No matching function with signature: array_concat(array<array<tinyint(4)>>, array<varchar>
USE sql_tests_complex_test_array_fn;
select array_concat([[1,2,3], [2,3,4]], ['a', 'b', 'c']) from array_test order by pk;

-- query 482
USE sql_tests_complex_test_array_fn;
select array_concat([[1,2,3], [2,3,4]], [[1,2,3], [2,3,4]]) from array_test order by pk;

-- query 483
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(s_1, s_1)) from array_test order by pk;

-- query 484
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(s_1, i_1)) from array_test order by pk;

-- query 485
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(s_1, f_1)) from array_test order by pk;

-- query 486
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_5, d_3)) from array_test order by pk;

-- query 487
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_5, d_4)) from array_test order by pk;

-- query 488
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_5, d_5)) from array_test order by pk;

-- query 489
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_5, d_6)) from array_test order by pk;

-- query 490
-- @expect_error=No matching function with signature: array_intersect(array<DECIMAL64(16,3)>, array<array<bigint(20)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_5, ai_1)) from array_test order by pk;

-- query 491
-- @expect_error=No matching function with signature: array_intersect(array<DECIMAL64(16,3)>, array<array<varchar(65533)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_5, as_1)) from array_test order by pk;

-- query 492
-- @expect_error=No matching function with signature: array_intersect(array<DECIMAL64(16,3)>, array<array<array<varchar(65533)>>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_5, aas_1)) from array_test order by pk;

-- query 493
-- @expect_error=No matching function with signature: array_intersect(array<DECIMAL64(16,3)>, array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_5, aad_1)) from array_test order by pk;

-- query 494
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_5, NULL)) from array_test order by pk;

-- query 495
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_6, d_1)) from array_test order by pk;

-- query 496
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_6, d_2)) from array_test order by pk;

-- query 497
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_6, d_3)) from array_test order by pk;

-- query 498
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_6, d_4)) from array_test order by pk;

-- query 499
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_6, [1.0,2.1,3.2,4.3])) from array_test order by pk;

-- query 500
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_6, ['a', 'b', 'c'])) from array_test order by pk;

-- query 501
-- @expect_error=No matching function with signature: array_intersect(array<DECIMAL128(18,6)>, array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(d_6, [[1,2,3], [2,3,4]])) from array_test order by pk;

-- query 502
-- @expect_error=No matching function with signature: array_intersect(array<array<bigint(20)>>, array<bigint(20)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(ai_1, i_1)) from array_test order by pk;

-- query 503
-- @expect_error=No matching function with signature: array_intersect(array<array<bigint(20)>>, array<DECIMAL64(4,3)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(ai_1, d_2)) from array_test order by pk;

-- query 504
-- @expect_error=No matching function with signature: array_sort(array<array<bigint(20)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(ai_1, NULL)) from array_test order by pk;

-- query 505
-- @expect_error=No matching function with signature: array_intersect(array<array<bigint(20)>>, array<DECIMAL32(2,1)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(ai_1, [1.0,2.1,3.2,4.3])) from array_test order by pk;

-- query 506
-- @expect_error=No matching function with signature: array_intersect(array<array<bigint(20)>>, array<varchar>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(ai_1, ['a', 'b', 'c'])) from array_test order by pk;

-- query 507
-- @expect_error=No matching function with signature: array_sort(array<array<bigint(20)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(ai_1, [[1,2,3], [2,3,4]])) from array_test order by pk;

-- query 508
-- @expect_error=No matching function with signature: array_intersect(array<array<varchar(65533)>>, array<varchar(65533)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(as_1, s_1)) from array_test order by pk;

-- query 509
-- @expect_error=No matching function with signature: array_intersect(array<array<varchar(65533)>>, array<bigint(20)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(as_1, i_1)) from array_test order by pk;

-- query 510
-- @expect_error=No matching function with signature: array_sort(array<array<varchar(65533)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(as_1, NULL)) from array_test order by pk;

-- query 511
-- @expect_error=No matching function with signature: array_intersect(array<array<varchar(65533)>>, array<DECIMAL32(2,1)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(as_1, [1.0,2.1,3.2,4.3])) from array_test order by pk;

-- query 512
-- @expect_error=No matching function with signature: array_intersect(array<array<varchar(65533)>>, array<varchar>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(as_1, ['a', 'b', 'c'])) from array_test order by pk;

-- query 513
-- @expect_error=No matching function with signature: array_sort(array<array<varchar>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(as_1, [[1,2,3], [2,3,4]])) from array_test order by pk;

-- query 514
-- @expect_error=No matching function with signature: array_intersect(array<array<array<varchar(65533)>>>, array<varchar(65533)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aas_1, s_1)) from array_test order by pk;

-- query 515
-- @expect_error=No matching function with signature: array_intersect(array<array<array<varchar(65533)>>>, array<bigint(20)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aas_1, i_1)) from array_test order by pk;

-- query 516
-- @expect_error=No matching function with signature: array_intersect(array<array<array<varchar(65533)>>>, array<double>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aas_1, f_1)) from array_test order by pk;

-- query 517
-- @expect_error=No matching function with signature: array_intersect(array<array<array<varchar(65533)>>>, array<DECIMAL64(4,3)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aas_1, d_2)) from array_test order by pk;

-- query 518
-- @expect_error=No matching function with signature: array_intersect(array<array<array<DECIMAL128(26,2)>>>, array<varchar(65533)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aad_1, s_1)) from array_test order by pk;

-- query 519
-- @expect_error=No matching function with signature: array_intersect(array<array<array<DECIMAL128(26,2)>>>, array<bigint(20)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aad_1, i_1)) from array_test order by pk;

-- query 520
-- @expect_error=No matching function with signature: array_intersect(array<array<array<DECIMAL128(26,2)>>>, array<double>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aad_1, f_1)) from array_test order by pk;

-- query 521
-- @expect_error=No matching function with signature: array_intersect(array<array<array<DECIMAL128(26,2)>>>, array<DECIMAL128(26,2)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aad_1, d_1)) from array_test order by pk;

-- query 522
-- @expect_error=No matching function with signature: array_intersect(array<array<array<DECIMAL128(26,2)>>>, array<DECIMAL64(4,3)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aad_1, d_2)) from array_test order by pk;

-- query 523
-- @expect_error=No matching function with signature: array_intersect(array<array<array<DECIMAL128(26,2)>>>, array<array<bigint(20)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aad_1, ai_1)) from array_test order by pk;

-- query 524
-- @expect_error=No matching function with signature: array_intersect(array<array<array<DECIMAL128(26,2)>>>, array<array<varchar(65533)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aad_1, as_1)) from array_test order by pk;

-- query 525
-- @expect_error=No matching function with signature: array_sort(array<array<array<varchar>>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aad_1, aas_1)) from array_test order by pk;

-- query 526
-- @expect_error=No matching function with signature: array_sort(array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aad_1, aad_1)) from array_test order by pk;

-- query 527
-- @expect_error=No matching function with signature: array_sort(array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aad_1, NULL)) from array_test order by pk;

-- query 528
-- @expect_error=No matching function with signature: array_intersect(array<array<array<DECIMAL128(26,2)>>>, array<DECIMAL32(2,1)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aad_1, [1.0,2.1,3.2,4.3])) from array_test order by pk;

-- query 529
-- @expect_error=No matching function with signature: array_intersect(array<array<array<DECIMAL128(26,2)>>>, array<varchar>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aad_1, ['a', 'b', 'c'])) from array_test order by pk;

-- query 530
-- @expect_error=No matching function with signature: array_intersect(array<array<array<DECIMAL128(26,2)>>>, array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(aad_1, [[1,2,3], [2,3,4]])) from array_test order by pk;

-- query 531
-- @expect_error=No matching function with signature: array_sort(array<array<varchar(65533)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(NULL, as_1)) from array_test order by pk;

-- query 532
-- @expect_error=No matching function with signature: array_sort(array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(NULL, aad_1)) from array_test order by pk;

-- query 533
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(NULL, NULL)) from array_test order by pk;

-- query 534
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(NULL, ['a', 'b', 'c'])) from array_test order by pk;

-- query 535
-- @expect_error=No matching function with signature: array_sort(array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(NULL, [[1,2,3], [2,3,4]])) from array_test order by pk;

-- query 536
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect([1.0,2.1,3.2,4.3], s_1)) from array_test order by pk;

-- query 537
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect([1.0,2.1,3.2,4.3], i_1)) from array_test order by pk;

-- query 538
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect([1.0,2.1,3.2,4.3], f_1)) from array_test order by pk;

-- query 539
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect([1.0,2.1,3.2,4.3], d_1)) from array_test order by pk;

-- query 540
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(['a', 'b', 'c'], s_1)) from array_test order by pk;

-- query 541
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(['a', 'b', 'c'], d_1)) from array_test order by pk;

-- query 542
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(['a', 'b', 'c'], d_6)) from array_test order by pk;

-- query 543
-- @expect_error=No matching function with signature: array_intersect(array<varchar>, array<array<bigint(20)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(['a', 'b', 'c'], ai_1)) from array_test order by pk;

-- query 544
-- @expect_error=No matching function with signature: array_intersect(array<varchar>, array<array<varchar(65533)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(['a', 'b', 'c'], as_1)) from array_test order by pk;

-- query 545
-- @expect_error=No matching function with signature: array_intersect(array<varchar>, array<array<array<varchar(65533)>>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(['a', 'b', 'c'], aas_1)) from array_test order by pk;

-- query 546
-- @expect_error=No matching function with signature: array_intersect(array<varchar>, array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(['a', 'b', 'c'], aad_1)) from array_test order by pk;

-- query 547
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(['a', 'b', 'c'], NULL)) from array_test order by pk;

-- query 548
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(['a', 'b', 'c'], [1.0,2.1,3.2,4.3])) from array_test order by pk;

-- query 549
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(['a', 'b', 'c'], ['a', 'b', 'c'])) from array_test order by pk;

-- query 550
-- @expect_error=No matching function with signature: array_intersect(array<varchar>, array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect(['a', 'b', 'c'], [[1,2,3], [2,3,4]])) from array_test order by pk;

-- query 551
-- @expect_error=No matching function with signature: array_intersect(array<array<tinyint(4)>>, array<varchar(65533)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect([[1,2,3], [2,3,4]], s_1)) from array_test order by pk;

-- query 552
-- @expect_error=No matching function with signature: array_intersect(array<array<tinyint(4)>>, array<bigint(20)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect([[1,2,3], [2,3,4]], i_1)) from array_test order by pk;

-- query 553
-- @expect_error=No matching function with signature: array_intersect(array<array<tinyint(4)>>, array<DECIMAL128(18,6)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect([[1,2,3], [2,3,4]], d_6)) from array_test order by pk;

-- query 554
-- @expect_error=No matching function with signature: array_sort(array<array<bigint(20)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect([[1,2,3], [2,3,4]], ai_1)) from array_test order by pk;

-- query 555
-- @expect_error=No matching function with signature: array_sort(array<array<varchar>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect([[1,2,3], [2,3,4]], as_1)) from array_test order by pk;

-- query 556
-- @expect_error=No matching function with signature: array_intersect(array<array<tinyint(4)>>, array<array<array<varchar(65533)>>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect([[1,2,3], [2,3,4]], aas_1)) from array_test order by pk;

-- query 557
-- @expect_error=No matching function with signature: array_intersect(array<array<tinyint(4)>>, array<array<array<DECIMAL128(26,2)>>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect([[1,2,3], [2,3,4]], aad_1)) from array_test order by pk;

-- query 558
-- @expect_error=No matching function with signature: array_sort(array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect([[1,2,3], [2,3,4]], NULL)) from array_test order by pk;

-- query 559
-- @expect_error=No matching function with signature: array_intersect(array<array<tinyint(4)>>, array<DECIMAL32(2,1)>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect([[1,2,3], [2,3,4]], [1.0,2.1,3.2,4.3])) from array_test order by pk;

-- query 560
-- @expect_error=No matching function with signature: array_intersect(array<array<tinyint(4)>>, array<varchar>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect([[1,2,3], [2,3,4]], ['a', 'b', 'c'])) from array_test order by pk;

-- query 561
-- @expect_error=No matching function with signature: array_sort(array<array<tinyint(4)>>
USE sql_tests_complex_test_array_fn;
select array_sort(array_intersect([[1,2,3], [2,3,4]], [[1,2,3], [2,3,4]])) from array_test order by pk;

-- query 562
USE sql_tests_complex_test_array_fn;
select all_match((x,y) -> x < y, null, [4,5,6]);

-- query 563
USE sql_tests_complex_test_array_fn;
select all_match((x,y) -> x < y, [], []);

-- query 564
USE sql_tests_complex_test_array_fn;
select all_match((x,y) -> x < y, null, []);

-- query 565
USE sql_tests_complex_test_array_fn;
select all_match((x,y) -> x < y, null, null);

-- query 566
USE sql_tests_complex_test_array_fn;
select any_match((x,y) -> x < y, null, [4,5,6]);

-- query 567
USE sql_tests_complex_test_array_fn;
select any_match((x,y) -> x < y, [], []);

-- query 568
USE sql_tests_complex_test_array_fn;
select any_match((x,y) -> x < y, null, []);

-- query 569
USE sql_tests_complex_test_array_fn;
select any_match((x,y) -> x < y, null, null);

-- query 570
USE sql_tests_complex_test_array_fn;
select all_match([0],x->1);

-- query 571
USE sql_tests_complex_test_array_fn;
select any_match([0],x->1);

-- query 572
USE sql_tests_complex_test_array_fn;
select any_match([],x->1);

-- query 573
USE sql_tests_complex_test_array_fn;
select any_match(null);

-- query 574
USE sql_tests_complex_test_array_fn;
select any_match([]);

-- query 575
USE sql_tests_complex_test_array_fn;
select all_match([]);

-- query 576
USE sql_tests_complex_test_array_fn;
select any_match((x,y) -> x < y, [1,2,8], [4,5,6]);

-- query 577
USE sql_tests_complex_test_array_fn;
select any_match((x,y) -> x < y, [11,12,8], [4,5,6]);

-- query 578
USE sql_tests_complex_test_array_fn;
select any_match((x,y) -> x < y, [11,12,null], [4,5,6]);

-- query 579
USE sql_tests_complex_test_array_fn;
select all_match((x,y) -> x < y, [1,2,null], [4,5,6]);

-- query 580
USE sql_tests_complex_test_array_fn;
select all_match(s_1, x->x is null) from array_test order by pk;

-- query 581
USE sql_tests_complex_test_array_fn;
select s_1, any_match(s_1, x->x is null) from array_test order by pk;

-- query 582
-- @expect_error=Input array element's size is not equal in array_map()
USE sql_tests_complex_test_array_fn;
select d_6, d_5, all_match(d_6,d_5, (x,y)->x >y) from array_test order by pk;

-- query 583
-- @expect_error=Input array element's size is not equal in array_map()
USE sql_tests_complex_test_array_fn;
select d_6, d_5, any_match(d_6,d_5, (x,y)->x >y) from array_test order by pk;

-- query 584
-- @expect_error=Lambda arguments should equal to lambda input arrays in all_match((x, y) -> x < y, []
USE sql_tests_complex_test_array_fn;
select all_match((x,y) -> x < y, []);

-- query 585
-- @expect_error=}
USE sql_tests_complex_test_array_fn;
select all_match((x,y) -> x < y, [],{});

-- query 586
-- @expect_error=all_match should have a input array
USE sql_tests_complex_test_array_fn;
select all_match([],null);

-- query 587
-- @expect_error=}
USE sql_tests_complex_test_array_fn;
select all_match({});

-- query 588
-- @expect_error=all_match should have a input array
USE sql_tests_complex_test_array_fn;
select all_match();

-- query 589
-- @expect_error=all_match should have a input array
USE sql_tests_complex_test_array_fn;
select all_match(null,[]);

-- query 590
-- @expect_error=all_match should have a input array
USE sql_tests_complex_test_array_fn;
select all_match(null,null);

-- query 591
-- @expect_error=Lambda arguments should equal to lambda input arrays in any_match((x, y) -> x < y, []
USE sql_tests_complex_test_array_fn;
select any_match((x,y) -> x < y, []);

-- query 592
-- @expect_error=}
USE sql_tests_complex_test_array_fn;
select any_match((x,y) -> x < y, [],{});

-- query 593
-- @expect_error=any_match should have a input array
USE sql_tests_complex_test_array_fn;
select any_match([],null);

-- query 594
-- @expect_error=}
USE sql_tests_complex_test_array_fn;
select any_match({});

-- query 595
-- @expect_error=any_match should have a input array
USE sql_tests_complex_test_array_fn;
select any_match();

-- query 596
-- @expect_error=any_match should have a input array
USE sql_tests_complex_test_array_fn;
select any_match(null,[]);

-- query 597
-- @expect_error=any_match should have a input array
USE sql_tests_complex_test_array_fn;
select any_match(null,null);

-- query 598
-- @expect_error=s input [[]] can
USE sql_tests_complex_test_array_fn;
select all_match([[]]);

-- query 599
USE sql_tests_complex_test_array_fn;
select array_sum(array_map(x->not like(x, 'starRocks'),s_1)) from array_test order by 1;

-- query 600
USE sql_tests_complex_test_array_fn;
select array_contains( cast ('[40360,40361]' as array<int>), 40360);

-- query 601
-- @expect_error=2-th input of array_contains_all should be an array, rather than int(11
USE sql_tests_complex_test_array_fn;
select array_contains_all( cast ('[40360,40361]' as array<int>), 40360);

-- query 602
USE sql_tests_complex_test_array_fn;
select array_append( cast ('[40360,40361]' as array<int>), 40360);

-- query 603
-- @expect_error=array_avg should have only one input
USE sql_tests_complex_test_array_fn;
select array_avg( cast ('[40360,40361]' as array<int>), 40360);

-- query 604
USE sql_tests_complex_test_array_fn;
select array_concat( cast ('[40360,40361]' as array<int>), [40360]);

-- query 605
USE sql_tests_complex_test_array_fn;
select array_cum_sum( cast ('[40360,40361]' as array<int>));

-- query 606
USE sql_tests_complex_test_array_fn;
select array_difference( cast ('[40360,40361]' as array<int>));

-- query 607
USE sql_tests_complex_test_array_fn;
select ARRAY_DISTINCT( cast ('[40360,40361]' as array<int>));

-- query 608
USE sql_tests_complex_test_array_fn;
select array_filter(cast ('[40360,40361]' as array<int>),[0,1]);

-- query 609
USE sql_tests_complex_test_array_fn;
select array_intersect( cast ('[40360,40361]' as array<int>), [40360]);

-- query 610
USE sql_tests_complex_test_array_fn;
select array_join( cast ('[40360,40361]' as array<int>), '-');

-- query 611
USE sql_tests_complex_test_array_fn;
select array_length( cast ('[40360,40361]' as array<int>));

-- query 612
USE sql_tests_complex_test_array_fn;
select array_map(x->x+1, cast ('[40360,40361]' as array<int>));

-- query 613
USE sql_tests_complex_test_array_fn;
select array_max( cast ('[40360,40361]' as array<int>));

-- query 614
USE sql_tests_complex_test_array_fn;
select array_min( cast ('[40360,40361]' as array<int>));

-- query 615
USE sql_tests_complex_test_array_fn;
select array_position( cast ('[40360,40361]' as array<int>), 40360);

-- query 616
USE sql_tests_complex_test_array_fn;
select array_remove( cast ('[40360,40361]' as array<int>), 40360);

-- query 617
USE sql_tests_complex_test_array_fn;
select array_slice( cast ('[40360,40361]' as array<int>), 1,1);

-- query 618
USE sql_tests_complex_test_array_fn;
select ARRAY_SORT( cast ('[40360,40361]' as array<int>));

-- query 619
USE sql_tests_complex_test_array_fn;
select array_sortby( cast ('[40360,40361]' as array<int>), [40360,1]);

-- query 620
USE sql_tests_complex_test_array_fn;
select array_sortby([40360,1], cast ('[40360,40361]' as array<int>));

-- query 621
USE sql_tests_complex_test_array_fn;
select array_sum( cast ('[40360,40361]' as array<int>));

-- query 622
USE sql_tests_complex_test_array_fn;
select array_to_bitmap( cast ('[40360,40361]' as array<int>));

-- query 623
USE sql_tests_complex_test_array_fn;
select bitmap_to_array(array_to_bitmap(cast ('[40360,40361]' as array<int>)));

-- query 624
USE sql_tests_complex_test_array_fn;
select REVERSE( cast ('[40360,40361]' as array<int>));

-- query 625
USE sql_tests_complex_test_array_fn;
select bitmap_to_string(array_to_bitmap( cast ('[40360,40361]' as array<int>))) from table(generate_series(1,10,1));

-- query 626
USE sql_tests_complex_test_array_fn;
select array_contains( cast ('null' as array<int>), 40360);

-- query 627
-- @expect_error=2-th input of array_contains_all should be an array, rather than int(11
USE sql_tests_complex_test_array_fn;
select array_contains_all( cast ('null' as array<int>), 40360);

-- query 628
USE sql_tests_complex_test_array_fn;
select array_append( cast ('null' as array<int>), 40360);

-- query 629
-- @expect_error=array_avg should have only one input
USE sql_tests_complex_test_array_fn;
select array_avg( cast ('null' as array<int>), 40360);

-- query 630
USE sql_tests_complex_test_array_fn;
select array_concat( cast ('null' as array<int>), [40360]);

-- query 631
USE sql_tests_complex_test_array_fn;
select array_cum_sum( cast ('null' as array<int>));

-- query 632
USE sql_tests_complex_test_array_fn;
select array_difference( cast ('null' as array<int>));

-- query 633
USE sql_tests_complex_test_array_fn;
select ARRAY_DISTINCT( cast ('null' as array<int>));

-- query 634
USE sql_tests_complex_test_array_fn;
select array_filter(cast ('null' as array<int>),[0,1]);

-- query 635
USE sql_tests_complex_test_array_fn;
select array_intersect( cast ('null' as array<int>), [40360]);

-- query 636
USE sql_tests_complex_test_array_fn;
select array_join( cast ('null' as array<int>), '-');

-- query 637
USE sql_tests_complex_test_array_fn;
select array_length( cast ('null' as array<int>));

-- query 638
USE sql_tests_complex_test_array_fn;
select array_map(x->x+1, cast ('null' as array<int>));

-- query 639
USE sql_tests_complex_test_array_fn;
select array_max( cast ('null' as array<int>));

-- query 640
USE sql_tests_complex_test_array_fn;
select array_min( cast ('null' as array<int>));

-- query 641
USE sql_tests_complex_test_array_fn;
select array_position( cast ('null' as array<int>), 40360);

-- query 642
USE sql_tests_complex_test_array_fn;
select array_remove( cast ('null' as array<int>), 40360);

-- query 643
USE sql_tests_complex_test_array_fn;
select array_slice( cast ('null' as array<int>), 1,1);

-- query 644
USE sql_tests_complex_test_array_fn;
select ARRAY_SORT( cast ('null' as array<int>));

-- query 645
USE sql_tests_complex_test_array_fn;
select array_sortby( cast ('null' as array<int>), [40360,1]);

-- query 646
USE sql_tests_complex_test_array_fn;
select array_sortby([40360,1], cast ('null' as array<int>));

-- query 647
USE sql_tests_complex_test_array_fn;
select array_sum( cast ('null' as array<int>));

-- query 648
USE sql_tests_complex_test_array_fn;
select array_to_bitmap( cast ('null' as array<int>));

-- query 649
USE sql_tests_complex_test_array_fn;
select bitmap_to_array(array_to_bitmap(cast ('null' as array<int>)));

-- query 650
USE sql_tests_complex_test_array_fn;
select REVERSE( cast ('null' as array<int>));

-- query 651
USE sql_tests_complex_test_array_fn;
select array_length([map{1:2, 2:3, 3:4}]);

-- query 652
USE sql_tests_complex_test_array_fn;
select array_append([map{1:2, 2:3, 3:4}], map{3: null, 4:5});

-- query 653
USE sql_tests_complex_test_array_fn;
select array_contains([map{1:2, 2:3, 3:4}, map{3:4, 4:5}], map{3:4, 4:5});

-- query 654
USE sql_tests_complex_test_array_fn;
select array_contains([map{1:2, 2:3, 3:4}, map{3:4, 4:5}], map{3:4, 5:5});

-- query 655
USE sql_tests_complex_test_array_fn;
select array_contains([map{1:2, 2:3, 3:4}, map{3:4, 4:null}], map{3:4, 4:null});

-- query 656
USE sql_tests_complex_test_array_fn;
select array_remove([map{1:2, 2:3, 3:4}, map{3:4, 4:5}], map{3:4, 4:5});

-- query 657
USE sql_tests_complex_test_array_fn;
select array_position([map{1:2, 2:3, 3:4}, map{3:4, 4:5}], map{3:4, 4:5});

-- query 658
USE sql_tests_complex_test_array_fn;
select array_distinct([map{1:2, 2:3, 3:4}, map{3:4, 4:5}, map{3:4, 4:5}, map{1:2, 2:3, 3:4}]);

-- query 659
USE sql_tests_complex_test_array_fn;
select reverse([map{1:2, 2:3, 3:4}, map{3:4, 4:5}]);

-- query 660
USE sql_tests_complex_test_array_fn;
select array_slice([map{1:2, 2:3, 3:4}, map{3:4, 4:5}, map{3:4, 4:5}, map{1:2, 2:3, 3:4}], 2, 3);

-- query 661
USE sql_tests_complex_test_array_fn;
select array_concat([map{1:2, 2:3, 3:4}], [map{null:3, 4:null}]);

-- query 662
USE sql_tests_complex_test_array_fn;
select array_intersect([map{1:2, 2:3, 3:4}, map{3:4, 4:5}, map{3:4, 4:5}, map{1:2, 2:3, 3:4}], [map{1:2, 2:3, 3:4}]);

-- query 663
USE sql_tests_complex_test_array_fn;
select array_contains_all([map{1:2, 2:3, 3:4}, map{3:4, 4:5}, map{3:4, 4:5}, map{1:2, 2:3, 3:4}], [map{1:2, 2:3, 3:4}, map{3:4, 4:5}]);

-- query 664
USE sql_tests_complex_test_array_fn;
select array_filter([map{1:2, 2:3, 3:4}, map{3:4, 4:5}, map{3:4, 4:5}, map{1:2, 2:3, 3:4}], [1,0,0,0]);

-- query 665
USE sql_tests_complex_test_array_fn;
select cardinality([map{1:2, 2:3, 3:4}, map{3:4, 4:5}, map{3:4, 4:5}, map{1:2, 2:3, 3:4}]);

-- query 666
USE sql_tests_complex_test_array_fn;
select array_length([row(1,2,3), row(3,4,5)]);

-- query 667
USE sql_tests_complex_test_array_fn;
select array_append([row(1,2,3), row(3,4,5)], row(5,6,7));

-- query 668
USE sql_tests_complex_test_array_fn;
select array_contains([row(1,2,3), row(3,4,5)], row(5,6,7));

-- query 669
USE sql_tests_complex_test_array_fn;
select array_contains([row(1,2,3), row(3,4,5)], row(3,4,5));

-- query 670
USE sql_tests_complex_test_array_fn;
select array_contains([row(1,2,3), row(3,4,null)], row(3,4,null));

-- query 671
USE sql_tests_complex_test_array_fn;
select array_remove([row(1,2,3), row(3,4,5)], row(5,6,7));

-- query 672
USE sql_tests_complex_test_array_fn;
select array_remove([row(1,2,3), row(3,4,5)], row(3,4,5));

-- query 673
USE sql_tests_complex_test_array_fn;
select array_remove([row(1,2,3), row(3,4,null)], row(3,4,null));

-- query 674
USE sql_tests_complex_test_array_fn;
select array_position([row(1,2,3), row(3,4,5)], row(3,4,5));

-- query 675
USE sql_tests_complex_test_array_fn;
select array_distinct([row(1,2,3), row(3,4,5), row(3,4,5)]);

-- query 676
USE sql_tests_complex_test_array_fn;
select reverse([row(1,2,3), row(3,4,5)]);

-- query 677
USE sql_tests_complex_test_array_fn;
select array_slice([row(1,2,3), row(3,4,5),row(4,5,6)] ,1,2);

-- query 678
USE sql_tests_complex_test_array_fn;
select array_concat([row(1,2,3), row(3,4,5)], [row(4,5,6), row(5,6,7)]);

-- query 679
USE sql_tests_complex_test_array_fn;
select array_intersect([row(1,2,3), row(3,4,5)], [row(3,4,5)]);

-- query 680
USE sql_tests_complex_test_array_fn;
select array_contains_all([row(1,2,3), row(3,4,5)], [row(3,4,5)]);

-- query 681
USE sql_tests_complex_test_array_fn;
select array_filter([row(1,2,3), row(3,4,5), row(4,5,6)], [0,1,0]);

-- query 682
USE sql_tests_complex_test_array_fn;
select cardinality([row(1,2,3), row(3,4,5)]);

-- query 683
USE sql_tests_complex_test_array_fn;
select array_contains_seq([1,2,3,4], [2,3]);

-- query 684
USE sql_tests_complex_test_array_fn;
select array_contains_seq([1,2,3,4], [3,2]);

-- query 685
USE sql_tests_complex_test_array_fn;
select array_contains_seq([1,2,3,4], [1,2,3]);

-- query 686
USE sql_tests_complex_test_array_fn;
select array_contains_seq([1,2,3,4], [1,2,4]);

-- query 687
USE sql_tests_complex_test_array_fn;
select array_contains_seq([], []);

-- query 688
USE sql_tests_complex_test_array_fn;
select array_contains_seq([1,null], [null]);

-- query 689
USE sql_tests_complex_test_array_fn;
select array_contains_seq([1.0,2,3,4], [1]);

-- query 690
USE sql_tests_complex_test_array_fn;
select array_contains_seq([cast(1.0 as decimal),2,3,4], [cast(1 as int)]);

-- query 691
USE sql_tests_complex_test_array_fn;
select array_contains_seq(['a','b','c'], ['a','b']);

-- query 692
USE sql_tests_complex_test_array_fn;
select array_contains_seq(['a','b','c'], ['a','c']);

-- query 693
USE sql_tests_complex_test_array_fn;
select array_contains_seq([[1, 2], [3, 4], [5, 6]], [[1, 2], [3, 4]]);

-- query 694
USE sql_tests_complex_test_array_fn;
select array_contains_seq([json_keys('{"a":1,"b":2}')], [json_keys('{"a":1}')]);

-- query 695
USE sql_tests_complex_test_array_fn;
select array_contains_seq([json_keys('{"a":1,"b":2}')], [json_keys('{"a":1,"b":2}')]);

-- query 696
USE sql_tests_complex_test_array_fn;
select array_contains_seq([map(1,[2,4,5])], [map(1,[2,4,5])]);

-- query 697
USE sql_tests_complex_test_array_fn;
select array_contains_seq([map(1,[2,4,5])], [map(2,[2,4])]);

-- query 698
USE sql_tests_complex_test_array_fn;
select array_contains_seq([1, 2, NULL, 3, 4], ['a']);

-- query 699
USE sql_tests_complex_test_array_fn;
select array_contains_seq([1, 2, NULL, 3, 4], [2,3]);

-- query 700
USE sql_tests_complex_test_array_fn;
select array_contains_seq([1, 2, NULL, 3, 4], null);

-- query 701
USE sql_tests_complex_test_array_fn;
select array_contains_seq(null, [2,3]);

-- query 702
USE sql_tests_complex_test_array_fn;
select array_contains_seq([1, 2, NULL, 3, 4], [null,null]);

-- query 703
USE sql_tests_complex_test_array_fn;
select array_contains_seq([1, 2, NULL], [null,2]);

-- query 704
USE sql_tests_complex_test_array_fn;
select array_contains_seq(null, null);

-- query 705
USE sql_tests_complex_test_array_fn;
select array_contains_seq([1, 1, 2, NULL], [1,2]);

-- query 706
-- @skip_result_check=true
USE sql_tests_complex_test_array_fn;
CREATE TABLE array_test_01 (
pk bigint not null ,
i_0   Array<BigInt>,
i_1   Array<BigInt>,
ai_0  Array<Array<BigInt>>,
ai_1  Array<Array<BigInt>>
) ENGINE=OLAP
DUPLICATE KEY(`pk`)
DISTRIBUTED BY HASH(`pk`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);

-- query 707
-- @skip_result_check=true
USE sql_tests_complex_test_array_fn;
insert into array_test_01 values
(1,[null,1],[null],[null],[[]]),
(2,[null],[1,3,4,5,1,2],[[null,1],null],[[1,null]]),
(3,[],[],[[],null,[1,1]],[[1,1]]),
(4,null,[],[[1,1]],[[1,1],null]),
(5,[4,4,4],[4,null],[null],[null]),
(6,[1,1,2,1,1,2,3,3],[1,2,3],[[1]],[[1],[2],null,[null]]);

-- query 708
USE sql_tests_complex_test_array_fn;
select array_contains_seq(i_0,i_1) from array_test_01 order by pk;

-- query 709
USE sql_tests_complex_test_array_fn;
select array_contains_seq(i_0,i_0) from array_test_01 order by pk;

-- query 710
USE sql_tests_complex_test_array_fn;
select array_contains_seq(i_1,i_0) from array_test_01 order by pk;

-- query 711
USE sql_tests_complex_test_array_fn;
select array_contains_seq(ai_0, ai_1) from array_test_01 order by pk;

-- query 712
USE sql_tests_complex_test_array_fn;
select array_contains_seq(ai_1, ai_1) from array_test_01 order by pk;

-- query 713
USE sql_tests_complex_test_array_fn;
select array_contains_seq(ai_1, ai_0) from array_test_01 order by pk;

-- query 714
USE sql_tests_complex_test_array_fn;
select array_contains_seq(ai_0,null) from array_test_01 order by pk;

-- query 715
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_array_fn FORCE;
