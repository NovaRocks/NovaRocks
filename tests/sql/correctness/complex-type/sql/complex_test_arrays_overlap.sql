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

-- Migrated from dev/test/sql/test_array_fn/R/test_arrays_overlap
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_arrays_overlap @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
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
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into array_test values
(1, ['a', 'b', 'c'], [1.0, 2.0, 3.0, 4.0, 10.0], [1.0, 2.0, 3.0, 4.0, 10.0, 1.1, 2.1, 3.2, 4.3, -1, -10, 100], [4.0, 10.0, 1.1, 2.1, 3.2, 4.3, -1, -10, 100, 1.0, 2.0, 3.0], [4.0, 10.0, 1.1, -10, 100, 1.0, 2.0, 3.0, 2.1, 3.2, 4.3, -1], [4.0, 2.1, 3.2, 10.0, 1.1, -10, 100, -1, 1.0, 2.0, 3.0, 4.3], [4.0, 2.1, 3.2, 10.0, 2.0, 3.0, 1.1, -1, -10, 100, 1.0, 4.3], [4.0, 2.1, 3.0, 1.1, 4.3, 3.2, -10, 100, 1.0, 10.0, -1, 2.0], [4.0, 2.1, 100, 1.0, 4.3, 3.2, 10.0, 2.0, 3.0, 1.1, -1, -10], [[1, 2, 3, 4], [5, 2, 6, 4], [100, -1, 92, 8], [66, 4, 32, -10]], [['1', '2', '3', '4'], ['-1', 'a', '-100', '100'], ['a', 'b', 'c']], [[['1'],['2'],['3']], [['6'],['5'],['4']], [['-1', '-2'],['-2', '10'],['100','23']]], [[[1],[2],[3]], [[6],[5],[4]], [[-1, -2],[-2, 10],[100,23]]]),
(2, ['-1', '10', '1', '100', '2'], NULL, [10.0, 20.0, 30.0, 4.0, 100.0, 10.1, 2.1, 30.2, 40.3, -1, -10, 100], [40.0, 100.0, 01.1, 2.1, 30.2, 40.3, -1, -100, 1000, 1.0, 2.0, 3.0], [40.0, 100.0, 01.1, -10, 1000, 10.0, 2.0, 30.0, 20.1, 3.2, 4.3, -1], NULL, NULL, [40.0, 20.1, 30.0, 10.1, 40.30, 30.20, -100, 1000, 1.0, 100.0, -10, 2.0], [40.0, 20.1, 1000, 10.0, 40.30, 30.20, 100.0, 20.0, 3.0, 10.1, -10, -10], NULL, NULL, [[['10'],['20'],['30']], [['60'],['5'],['4']], [['-100', '-2'],['-20', '10'],['100','23']]], [[[10],[20],[30]], [[60],[50],[4]], [[-1, -2],[-2, 100],[100,23]]]),
(4, ['a', NULL, 'c', 'e', 'd'], [1.0, 2.0, 3.0, 4.0, 10.0], [1.0, 2.0, 3.0, 4.0, 10.0, NULL, 1.1, 2.1, 3.2, NULL, 4.3, -1, -10, 100], [4.0, 10.0, 1.1, 2.1,NULL, 3.2, 4.3, -1, -10, 100, 1.0, 2.0, 3.0], [4.0, 10.0, 1.1, -10, 100, 1.0, 2.0, 3.0, 2.1, 3.2, 4.3, -1], [4.0, 2.1, 3.2, 10.0, 1.1, -10, 100, -1, 1.0, 2.0, 3.0, 4.3], [4.0, 2.1, 3.2, 10.0, 2.0, 3.0, 1.1, -1, -10, 100, 1.0, 4.3], [4.0, 2.1, 3.0, 1.1, 4.3, 3.2, -10, 100, 1.0, 10.0, -1, 2.0], [4.0, 2.1, 100, NULL, 1.0, 4.3, 3.2, 10.0, 2.0, 3.0, 1.1, -1, -10], [[1, 2, 3, NULL, 4], [5, 2, 6, 4], NULL, [100, -1, 92, 8], [66, 4, 32, -10]], [['1', '2', '3', '4'], ['-1', 'a', '-100', '100'], ['a', 'b', 'c']], [[['1'],['2'],['3']], [['6'],['5'],['4']], [['-1', '-2'],NULL,['-2', '10'],['100','23']]], [[[1],NULL,[2],[3]], [[6],[5],[4]], NULL, [[-1, -2],[-2, 10],[100,23]]]),
(3, NULL, [1.0, 2.0, 3.0, 4.0, 10.0], NULL, [40.0, 10.0, 1.1, 2.1, 3.2, 4.3, -10, -10, 100, 10.0, 20.0, 3.0], [4.0, 10.0, 1.1, -10, 100, 1.0, 20.0, 3.0, 2.1, 3.2, 4.3, -1], [40.0, 20.1, 3.2, 10.0, 10.1, -10, 100, -1, 10.0, 2.0, 30.0, 4.3], [4.0, 2.1, 3.2, 10.0, 20.0, 3.0, 1.1, -10, -100, 100, 10.0, 4.3], NULL, NULL, [[1, 2, 30, 4], [50, 2, 6, 4], [100, -10, 92, 8], [66, 40, 32, -100]], [['1', '20', '3', '4'], ['-1', 'a00', '-100', '100'], ['a', 'b0', 'c']], NULL, NULL);

-- query 4
-- @expect_error=2-th input of arrays_overlap should be an array, rather than tinyint(4
USE ${case_db};
select arrays_overlap(s_1, 1) from array_test order by pk;

-- query 5
-- @expect_error=2-th input of arrays_overlap should be an array, rather than tinyint(4
USE ${case_db};
select arrays_overlap(d_1, 3) from array_test order by pk;

-- query 6
-- @expect_error=2-th input of arrays_overlap should be an array, rather than tinyint(4
USE ${case_db};
select arrays_overlap(as_1, 100) from array_test order by pk;

-- query 7
-- @expect_error=2-th input of arrays_overlap should be an array, rather than tinyint(4
USE ${case_db};
select arrays_overlap(aas_1, -10) from array_test order by pk;

-- query 8
-- @expect_error=2-th input of arrays_overlap should be an array, rather than tinyint(4
USE ${case_db};
select arrays_overlap(NULL, -1) from array_test order by pk;

-- query 9
-- @expect_error=2-th input of arrays_overlap should be an array, rather than tinyint(4
USE ${case_db};
select arrays_overlap([1.0,2.1,3.2,4.3], 1) from array_test order by pk;

-- query 10
-- @expect_error=2-th input of arrays_overlap should be an array, rather than tinyint(4
USE ${case_db};
select arrays_overlap(['a', 'b', 'c'], 1) from array_test order by pk;

-- query 11
-- @expect_error=2-th input of arrays_overlap should be an array, rather than tinyint(4
USE ${case_db};
select arrays_overlap([[1,2,3], [2,3,4]], 3) from array_test order by pk;

-- query 12
USE ${case_db};
select arrays_overlap(s_1, s_1) from array_test order by pk;

-- query 13
USE ${case_db};
select arrays_overlap(s_1, i_1) from array_test order by pk;

-- query 14
USE ${case_db};
select arrays_overlap(s_1, f_1) from array_test order by pk;

-- query 15
USE ${case_db};
select arrays_overlap(s_1, d_3) from array_test order by pk;

-- query 16
USE ${case_db};
select arrays_overlap(s_1, d_6) from array_test order by pk;

-- query 17
-- @expect_error=No matching function with signature: arrays_overlap(array<varchar(65533)>, array<array<bigint(20)>>
USE ${case_db};
select arrays_overlap(s_1, ai_1) from array_test order by pk;

-- query 18
-- @expect_error=No matching function with signature: arrays_overlap(array<varchar(65533)>, array<array<varchar(65533)>>
USE ${case_db};
select arrays_overlap(s_1, as_1) from array_test order by pk;

-- query 19
-- @expect_error=No matching function with signature: arrays_overlap(array<varchar(65533)>, array<array<array<varchar(65533)>>>
USE ${case_db};
select arrays_overlap(s_1, aas_1) from array_test order by pk;

-- query 20
-- @expect_error=No matching function with signature: arrays_overlap(array<varchar(65533)>, array<array<array<DECIMAL128(26,2)>>>
USE ${case_db};
select arrays_overlap(s_1, aad_1) from array_test order by pk;

-- query 21
USE ${case_db};
select arrays_overlap(s_1, ['a', 'c', 'd']) from array_test order by pk;

-- query 22
USE ${case_db};
select arrays_overlap(s_1, [4, 5, 2, null, 1]) from array_test order by pk;

-- query 23
USE ${case_db};
select arrays_overlap(s_1, NULL) from array_test order by pk;

-- query 24
USE ${case_db};
select arrays_overlap(i_1, d_3) from array_test order by pk;

-- query 25
USE ${case_db};
select arrays_overlap(i_1, d_4) from array_test order by pk;

-- query 26
USE ${case_db};
select arrays_overlap(i_1, ['a', 'c', 'd']) from array_test order by pk;

-- query 27
USE ${case_db};
select arrays_overlap(i_1, [4, 5, 2, null, 1]) from array_test order by pk;

-- query 28
USE ${case_db};
select arrays_overlap(i_1, NULL) from array_test order by pk;

-- query 29
USE ${case_db};
select arrays_overlap(f_1, s_1) from array_test order by pk;

-- query 30
USE ${case_db};
select arrays_overlap(f_1, i_1) from array_test order by pk;

-- query 31
USE ${case_db};
select arrays_overlap(f_1, f_1) from array_test order by pk;

-- query 32
USE ${case_db};
select arrays_overlap(f_1, d_4) from array_test order by pk;

-- query 33
USE ${case_db};
select arrays_overlap(f_1, d_5) from array_test order by pk;

-- query 34
-- @expect_error=No matching function with signature: arrays_overlap(array<double>, array<array<bigint(20)>>
USE ${case_db};
select arrays_overlap(f_1, ai_1) from array_test order by pk;

-- query 35
-- @expect_error=No matching function with signature: arrays_overlap(array<double>, array<array<varchar(65533)>>
USE ${case_db};
select arrays_overlap(f_1, as_1) from array_test order by pk;

-- query 36
-- @expect_error=No matching function with signature: arrays_overlap(array<double>, array<array<array<varchar(65533)>>>
USE ${case_db};
select arrays_overlap(f_1, aas_1) from array_test order by pk;

-- query 37
-- @expect_error=No matching function with signature: arrays_overlap(array<double>, array<array<array<DECIMAL128(26,2)>>>
USE ${case_db};
select arrays_overlap(f_1, aad_1) from array_test order by pk;

-- query 38
USE ${case_db};
select arrays_overlap(f_1, ['a', 'c', 'd']) from array_test order by pk;

-- query 39
USE ${case_db};
select arrays_overlap(f_1, [4, 5, 2, null, 1]) from array_test order by pk;

-- query 40
USE ${case_db};
select arrays_overlap(f_1, NULL) from array_test order by pk;

-- query 41
USE ${case_db};
select arrays_overlap(d_1, d_1) from array_test order by pk;

-- query 42
USE ${case_db};
select arrays_overlap(d_1, d_2) from array_test order by pk;

-- query 43
-- @expect_error=No matching function with signature: arrays_overlap(array<DECIMAL128(26,2)>, array<array<bigint(20)>>
USE ${case_db};
select arrays_overlap(d_1, ai_1) from array_test order by pk;

-- query 44
-- @expect_error=No matching function with signature: arrays_overlap(array<DECIMAL128(26,2)>, array<array<varchar(65533)>>
USE ${case_db};
select arrays_overlap(d_1, as_1) from array_test order by pk;

-- query 45
-- @expect_error=No matching function with signature: arrays_overlap(array<DECIMAL128(26,2)>, array<array<array<varchar(65533)>>>
USE ${case_db};
select arrays_overlap(d_1, aas_1) from array_test order by pk;

-- query 46
-- @expect_error=No matching function with signature: arrays_overlap(array<DECIMAL128(26,2)>, array<array<array<DECIMAL128(26,2)>>>
USE ${case_db};
select arrays_overlap(d_1, aad_1) from array_test order by pk;

-- query 47
USE ${case_db};
select arrays_overlap(d_1, ['a', 'c', 'd']) from array_test order by pk;

-- query 48
USE ${case_db};
select arrays_overlap(d_1, [4, 5, 2, null, 1]) from array_test order by pk;

-- query 49
USE ${case_db};
select arrays_overlap(d_1, NULL) from array_test order by pk;

-- query 50
USE ${case_db};
select arrays_overlap(d_2, s_1) from array_test order by pk;

-- query 51
USE ${case_db};
select arrays_overlap(d_2, d_2) from array_test order by pk;

-- query 52
USE ${case_db};
select arrays_overlap(d_2, d_3) from array_test order by pk;

-- query 53
USE ${case_db};
select arrays_overlap(d_2, [4, 5, 2, null, 1]) from array_test order by pk;

-- query 54
USE ${case_db};
select arrays_overlap(d_2, NULL) from array_test order by pk;

-- query 55
USE ${case_db};
select arrays_overlap(d_3, d_3) from array_test order by pk;

-- query 56
USE ${case_db};
select arrays_overlap(d_4, d_4) from array_test order by pk;

-- query 57
USE ${case_db};
select arrays_overlap(d_4, d_5) from array_test order by pk;

-- query 58
USE ${case_db};
select arrays_overlap(d_5, d_1) from array_test order by pk;

-- query 59
USE ${case_db};
select arrays_overlap(d_5, d_2) from array_test order by pk;

-- query 60
USE ${case_db};
select arrays_overlap(d_5, d_4) from array_test order by pk;

-- query 61
USE ${case_db};
select arrays_overlap(d_5, d_5) from array_test order by pk;

-- query 62
USE ${case_db};
select arrays_overlap(d_5, d_6) from array_test order by pk;

-- query 63
-- @expect_error=No matching function with signature: arrays_overlap(array<DECIMAL64(16,3)>, array<array<array<DECIMAL128(26,2)>>>
USE ${case_db};
select arrays_overlap(d_5, aad_1) from array_test order by pk;

-- query 64
USE ${case_db};
select arrays_overlap(d_6, s_1) from array_test order by pk;

-- query 65
USE ${case_db};
select arrays_overlap(d_6, i_1) from array_test order by pk;

-- query 66
USE ${case_db};
select arrays_overlap(d_6, f_1) from array_test order by pk;

-- query 67
USE ${case_db};
select arrays_overlap(d_6, d_5) from array_test order by pk;

-- query 68
USE ${case_db};
select arrays_overlap(d_6, d_6) from array_test order by pk;

-- query 69
-- @expect_error=No matching function with signature: arrays_overlap(array<array<bigint(20)>>, array<varchar(65533)>
USE ${case_db};
select arrays_overlap(ai_1, s_1) from array_test order by pk;

-- query 70
-- @expect_error=No matching function with signature: arrays_overlap(array<array<bigint(20)>>, array<bigint(20)>
USE ${case_db};
select arrays_overlap(ai_1, i_1) from array_test order by pk;

-- query 71
-- @expect_error=No matching function with signature: arrays_overlap(array<array<bigint(20)>>, array<DECIMAL128(18,6)>
USE ${case_db};
select arrays_overlap(ai_1, d_6) from array_test order by pk;

-- query 72
USE ${case_db};
select arrays_overlap(ai_1, ai_1) from array_test order by pk;

-- query 73
USE ${case_db};
select arrays_overlap(ai_1, as_1) from array_test order by pk;

-- query 74
-- @expect_error=No matching function with signature: arrays_overlap(array<array<bigint(20)>>, array<array<array<varchar(65533)>>>
USE ${case_db};
select arrays_overlap(ai_1, aas_1) from array_test order by pk;

-- query 75
-- @expect_error=No matching function with signature: arrays_overlap(array<array<bigint(20)>>, array<array<array<DECIMAL128(26,2)>>>
USE ${case_db};
select arrays_overlap(ai_1, aad_1) from array_test order by pk;

-- query 76
-- @expect_error=No matching function with signature: arrays_overlap(array<array<varchar(65533)>>, array<varchar(65533)>
USE ${case_db};
select arrays_overlap(as_1, s_1) from array_test order by pk;

-- query 77
USE ${case_db};
select arrays_overlap(as_1, ai_1) from array_test order by pk;

-- query 78
USE ${case_db};
select arrays_overlap(as_1, as_1) from array_test order by pk;

-- query 79
-- @expect_error=No matching function with signature: arrays_overlap(array<array<varchar(65533)>>, array<array<array<varchar(65533)>>>
USE ${case_db};
select arrays_overlap(as_1, aas_1) from array_test order by pk;

-- query 80
-- @expect_error=No matching function with signature: arrays_overlap(array<array<varchar(65533)>>, array<array<array<DECIMAL128(26,2)>>>
USE ${case_db};
select arrays_overlap(as_1, aad_1) from array_test order by pk;

-- query 81
-- @expect_error=No matching function with signature: arrays_overlap(array<array<array<varchar(65533)>>>, array<varchar(65533)>
USE ${case_db};
select arrays_overlap(aas_1, s_1) from array_test order by pk;

-- query 82
-- @expect_error=No matching function with signature: arrays_overlap(array<array<array<varchar(65533)>>>, array<array<varchar(65533)>>
USE ${case_db};
select arrays_overlap(aas_1, as_1) from array_test order by pk;

-- query 83
USE ${case_db};
select arrays_overlap(aas_1, aas_1) from array_test order by pk;

-- query 84
-- @expect_error=No matching function with signature: arrays_overlap(array<array<array<DECIMAL128(26,2)>>>, array<varchar(65533)>
USE ${case_db};
select arrays_overlap(aad_1, s_1) from array_test order by pk;

-- query 85
-- @expect_error=No matching function with signature: arrays_overlap(array<array<array<DECIMAL128(26,2)>>>, array<double>
USE ${case_db};
select arrays_overlap(aad_1, f_1) from array_test order by pk;

-- query 86
-- @expect_error=No matching function with signature: arrays_overlap(array<array<array<DECIMAL128(26,2)>>>, array<DECIMAL128(26,2)>
USE ${case_db};
select arrays_overlap(aad_1, d_1) from array_test order by pk;

-- query 87
-- @expect_error=No matching function with signature: arrays_overlap(array<array<array<DECIMAL128(26,2)>>>, array<DECIMAL64(4,3)>
USE ${case_db};
select arrays_overlap(aad_1, d_2) from array_test order by pk;

-- query 88
-- @expect_error=No matching function with signature: arrays_overlap(array<array<array<DECIMAL128(26,2)>>>, array<array<varchar(65533)>>
USE ${case_db};
select arrays_overlap(aad_1, as_1) from array_test order by pk;

-- query 89
USE ${case_db};
select arrays_overlap(aad_1, aas_1) from array_test order by pk;

-- query 90
USE ${case_db};
select arrays_overlap(aad_1, aad_1) from array_test order by pk;

-- query 91
-- @expect_error=No matching function with signature: arrays_overlap(array<array<array<DECIMAL128(26,2)>>>, array<varchar>
USE ${case_db};
select arrays_overlap(aad_1, ['a', 'c', 'd']) from array_test order by pk;

-- query 92
-- @expect_error=No matching function with signature: arrays_overlap(array<array<array<DECIMAL128(26,2)>>>, array<tinyint(4)>
USE ${case_db};
select arrays_overlap(aad_1, [4, 5, 2, null, 1]) from array_test order by pk;

-- query 93
USE ${case_db};
select arrays_overlap(aad_1, NULL) from array_test order by pk;

-- query 94
USE ${case_db};
select arrays_overlap(['a', 'c', 'd'], s_1) from array_test order by pk;

-- query 95
USE ${case_db};
select arrays_overlap([4, 5, 2, null, 1], f_1) from array_test order by pk;

-- query 96
-- @expect_error=No matching function with signature: arrays_overlap(array<tinyint(4)>, array<array<array<DECIMAL128(26,2)>>>
USE ${case_db};
select arrays_overlap([4, 5, 2, null, 1], aad_1) from array_test order by pk;

-- query 97
USE ${case_db};
select arrays_overlap([4, 5, 2, null, 1], ['a', 'c', 'd']) from array_test order by pk;

-- query 98
USE ${case_db};
select arrays_overlap([4, 5, 2, null, 1], [4, 5, 2, null, 1]) from array_test order by pk;

-- query 99
USE ${case_db};
select arrays_overlap([4, 5, 2, null, 1], NULL) from array_test order by pk;

-- query 100
USE ${case_db};
select arrays_overlap(NULL, s_1) from array_test order by pk;

-- query 101
USE ${case_db};
select arrays_overlap(NULL, i_1) from array_test order by pk;

-- query 102
USE ${case_db};
select arrays_overlap(NULL, d_1) from array_test order by pk;

-- query 103
USE ${case_db};
select arrays_overlap(NULL, d_2) from array_test order by pk;

-- query 104
USE ${case_db};
select arrays_overlap(NULL, ai_1) from array_test order by pk;

-- query 105
USE ${case_db};
select arrays_overlap(NULL, as_1) from array_test order by pk;

-- query 106
USE ${case_db};
select arrays_overlap(NULL, aas_1) from array_test order by pk;

-- query 107
USE ${case_db};
select arrays_overlap(NULL, aad_1) from array_test order by pk;

-- query 108
USE ${case_db};
select arrays_overlap(NULL, ['a', 'c', 'd']) from array_test order by pk;

-- query 109
USE ${case_db};
select arrays_overlap(NULL, [4, 5, 2, null, 1]) from array_test order by pk;

-- query 110
USE ${case_db};
select arrays_overlap(NULL, NULL) from array_test order by pk;

-- query 111
USE ${case_db};
select arrays_overlap([parse_json('{"addr": 1}'), parse_json('{"addr": 2}')],
                        [parse_json('{"addr": 2}'), parse_json('{"addr": 3}')]);

-- query 112
USE ${case_db};
select arrays_overlap([parse_json('{"addr": 1}'), parse_json('{"addr": 2}')],
                        [parse_json('{"addr": 3}'), parse_json('{"addr": 4}')]);

-- query 113
USE ${case_db};
select arrays_overlap( cast ('[40360,40361]' as array<int>), [40360]);

-- query 114
USE ${case_db};
select arrays_overlap( cast ('null' as array<int>), [40360]);

-- query 115
USE ${case_db};
select arrays_overlap([map{1:2, 2:3, 3:4}, map{3:4, 4:5}, map{3:4, 4:5}, map{1:2, 2:3, 3:4}], [map{1:2, 2:3, 3:4}]);

-- query 116
USE ${case_db};
select arrays_overlap([row(1,2,3), row(3,4,5), row(4,5,6)], [row(3,4,5)]);

-- query 117
USE ${case_db};
select arrays_overlap([1, null], [4, 3]);

-- query 118
USE ${case_db};
select arrays_overlap([4, 3], [1, null]);

-- name: test_arrays_overlap_constant_columns
-- query 119
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t1` (
  `k1` int(11) NULL,
  `a1` array<String> NULL,
  `a2` array<String> NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 120
-- @skip_result_check=true
USE ${case_db};
insert into t1 values
    (1, null, ['a', 'c', 'b', null]),
    (2, null, ['a', 'c', 'b']),
    (3, null, ['a', 'c']),
    (4, null, ['a']);

-- query 121
USE ${case_db};
select k1, a1, a2, arrays_overlap(ifnull(a1, []), ['a', 'c', 'b', null]) from t1 order by k1;

-- query 122
USE ${case_db};
select k1, a1, a2, arrays_overlap(['a', 'c', 'b', null], ifnull(a1, [])) from t1 order by k1;

-- query 123
USE ${case_db};
select k1, a1, a2, arrays_overlap(ifnull(a1, [null]), ['a', 'c', 'b', null]) from t1 order by k1;

-- query 124
USE ${case_db};
select k1, a1, a2, arrays_overlap(['a', 'c', 'b', null], ifnull(a1, [null])) from t1 order by k1;

-- query 125
USE ${case_db};
select k1, a1, a2, arrays_overlap(ifnull(a1, null), ['a', 'c', 'b', null]) from t1 order by k1;

-- query 126
USE ${case_db};
select k1, a1, a2, arrays_overlap(['a', 'c', 'b', null], ifnull(a1, null)) from t1 order by k1;

-- query 127
USE ${case_db};
select k1, a1, a2, arrays_overlap(ifnull(a1, []), a2) from t1 order by k1;

-- query 128
USE ${case_db};
select k1, a1, a2, arrays_overlap(a2, ifnull(a1, [])) from t1 order by k1;

-- query 129
USE ${case_db};
select k1, a1, a2, arrays_overlap(ifnull(a1, null), a2) from t1 order by k1;

-- query 130
USE ${case_db};
select k1, a1, a2, arrays_overlap(a2, ifnull(a1, null)) from t1 order by k1;

-- query 131
USE ${case_db};
select k1, a1, a2, arrays_overlap(ifnull(a1, [null]), a2) from t1 order by k1;

-- query 132
USE ${case_db};
select k1, a1, a2, arrays_overlap(a2, ifnull(a1, [null])) from t1 order by k1;

-- query 133
USE ${case_db};
select k1, a1, a2, arrays_overlap(ifnull(a1, ['a']), ifnull(a1, ['a', 'b'])) from t1 order by k1;

-- query 134
USE ${case_db};
select k1, a1, a2, arrays_overlap(ifnull(a1, null), ifnull(a1, ['a', 'b'])) from t1 order by k1;

-- query 135
USE ${case_db};
select k1, a1, a2, arrays_overlap(ifnull(a1, ['a', null]), ifnull(a1, ['a', 'b'])) from t1 order by k1;

-- query 136
USE ${case_db};
select k1, a1, a2, arrays_overlap(ifnull(a1, ['a', null]), ifnull(a1, ['a', 'b', null])) from t1 order by k1;

-- query 137
-- @skip_result_check=true
-- A catalog that cannot hold views cannot answer view enumeration, so
-- DROP DATABASE ... FORCE is refused here rather than silently assuming
-- the namespace holds none. Drop the tables explicitly instead.
USE ${case_db};
DROP TABLE IF EXISTS array_test;
DROP TABLE IF EXISTS t1;
