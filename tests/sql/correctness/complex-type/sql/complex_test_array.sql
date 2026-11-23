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

-- Migrated from dev/test/sql/test_array/R/test_array
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_array_test01
-- query 2
USE ${case_db};
select CAST([] AS ARRAY<INT>), [], CAST(['abc'] AS ARRAY<VARCHAR>), [123, NULL, 1.0], ['abc', NULL];

-- name: testArrayPredicate
-- query 3
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE array_data_type
    (c1 int,
    c2  array<bigint>, 
    c3  array<bigint>,
    c4  array<bigint> not null, 
    c5  array<bigint> not null) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into array_data_type (c1, c2, c3, c4,c5) values 
    ROW(1,NULL,NULL,[22, 11, 33],[22, 11, 33]);

-- query 5
USE ${case_db};
select c2 = c3 from array_data_type;   

insert into array_data_type (c1, c2, c3, c4,c5) values 
    ROW(2,NULL,[22, 11, 33],[22, 11, 33],[22, 11, 33]),
    ROW(3,[22, 11, 33],[22, 11, 33],[22, 11, 33],[22, 11, 33]),
    ROW(4,[22, 11, 33],NULL,[22, 11, 33],[22, 11, 33]);

-- query 6
USE ${case_db};
select c2  IS NOT DISTINCT FROM  c3 from array_data_type;

-- query 7
USE ${case_db};
select c2 = c3 from array_data_type;

-- query 8
USE ${case_db};
select c3 = c4 from array_data_type;

-- query 9
USE ${case_db};
select c4 = c5 from array_data_type;

-- query 10
-- @skip_result_check=true
USE ${case_db};
insert into array_data_type (c1, c2, c3, c4,c5) values 
    ROW(5,[22, 11, 33],[22, 11, 33],[22, 11, 44],[22, 11, 33]);

-- query 11
USE ${case_db};
select c4 = c5 from array_data_type;

-- query 12
-- @expect_error=comparison operator
USE ${case_db};
select c4 > c5 from array_data_type;

-- query 13
USE ${case_db};
select * from (select transform([1,3], x -> x*2 + x*2) col1) t1 CROSS JOIN (select transform(c3, x -> x*2 + x*2) col2 from  array_data_type) t2;

-- name: testArrayVarchar
-- query 14
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE array_data_type_1
    (c1 int,
    c2  array<TIMESTAMP(6)>,
    c3  array<REAL>,
    c4  array<varchar(10)>,
    c5  array<varchar(20)>,
    c6  array<array<varchar(10)>>) WITH (format_version = 3);

-- query 15
-- @skip_result_check=true
USE ${case_db};
insert into array_data_type_1 values
ROW(1, ['2020-11-11', '2021-11-11', '2022-01-01'], [1.23, 1.35, 2.7894], ['a', 'b'], ['sss', 'eee', 'fff'], [['a', 'b']]),
ROW(2, ['2020-01-11', null, '2022-11-01'], [2.23, 2.35, 3.7894], ['aa', null], ['ssss', 'eeee', null], [['a', null], null]),
ROW(3, null, null, null, null, null);

-- query 16
USE ${case_db};
select * from array_data_type_1 where c4 != ['a'] or c6 = [['a', 'b']];

-- query 17
USE ${case_db};
select * from array_data_type_1 where c4 = ['a'] or c6 != [['a', 'b']];

-- query 18
USE ${case_db};
select * from array_data_type_1 where c4 = cast(c4 as array<char(10)>);

-- query 19
-- @skip_result_check=true
USE ${case_db};
select * from array_data_type_1 where c5 = c4 or c6 = [['a']];

-- query 20
-- @skip_result_check=true
USE ${case_db};
select * from array_data_type_1 where transform(c5, (x) -> concat(x, 'a')) = c4;

-- query 21
USE ${case_db};
select c6[0] = ['a'] from array_data_type_1;

-- query 22
-- @expect_error=comparison operator
USE ${case_db};
select c6[0] > transform(c5, (x) -> concat(x, 'a')) from array_data_type_1;

-- name: testArrayTopN
-- query 23
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE array_top_n
    (c1 int,
    c2 array<int>) WITH (format_version = 3);

-- query 24
-- @skip_result_check=true
USE ${case_db};
insert into array_top_n values
ROW(1, [1]),
ROW(2, [5, 6]),
ROW(3, [2, 3, 4]),
ROW(4, [12, 13, 14, 15]),
ROW(5, [7, 8, 9, 10, 11]);

-- query 25
USE ${case_db};
select * from array_top_n order by c2[1];

-- query 26
USE ${case_db};
select * from array_top_n order by c2[1] OFFSET 1
LIMIT 10;

-- query 27
USE ${case_db};
select * from array_top_n order by c2[1] OFFSET 2
LIMIT 10;

-- query 28
USE ${case_db};
select * from array_top_n order by c2[1] OFFSET 3
LIMIT 10;

-- query 29
USE ${case_db};
select * from array_top_n order by c2[1] OFFSET 4
LIMIT 10;

-- query 30
-- @skip_result_check=true
USE ${case_db};
select * from array_top_n order by c2[1] OFFSET 5
LIMIT 10;

-- name: testArrayExpr
-- query 31
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE array_exprr
    (
    c1 int not null,
    c2 int not null
    ) WITH (format_version = 3);

-- query 32
-- @skip_result_check=true
USE ${case_db};
insert into array_exprr SELECT generate_series, generate_series FROM TABLE(sequence(1,  13336)) AS generate_series(generate_series);

-- query 33
USE ${case_db};
select count([CAST(if(c2 is null, c1 + c2, 0) as DECIMAL(38,0)) + if(c1 is null, c2 ,0)] is null) from array_exprr;

-- name: testEmptyArray
-- query 34
USE ${case_db};
with t0 as (
    select c1 from (values ROW([])) as t(c1)
)
select 
concat(c1, [1])
from t0;

-- query 35
USE ${case_db};
with t0 as (
    select c1 from (values ROW([])) as t(c1)
)
select 
concat([1], c1)
from t0;

-- query 36
USE ${case_db};
select concat(c1, [[]])
from (select c1 from (values ROW([])) as t(c1)) t;

-- query 37
USE ${case_db};
select concat(c1, [[1]])
from (select c1 from (values ROW([])) as t(c1)) t;

-- query 38
USE ${case_db};
select concat(c1, [[1]])
from (select c1 from (values ROW([[]])) as t(c1)) t;

-- query 39
USE ${case_db};
select concat(c1, [MAP(ARRAY['a'], ARRAY[1])])
from (select c1 from (values ROW([map()])) as t(c1)) t;

-- query 40
USE ${case_db};
select concat(c1, [MAP(ARRAY['a'], ARRAY[1])])
from (select c1 from (values ROW([])) as t(c1)) t;

-- query 41
USE ${case_db};
select concat(c1, [CAST(ROW(1,2,3) AS ROW(a INTEGER,b INTEGER,c INTEGER))])
from (select c1 from (values ROW([])) as t(c1)) t;

-- query 42
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "t2" (
  "pk" BIGINT NOT NULL,
  "aas_1" array<array<array<varchar(65533)>>>,
  "aad_1" array<array<array<DECIMAL(26,2)>>>
) WITH (format_version = 3);

-- query 43
-- @skip_result_check=true
USE ${case_db};
insert into t2 VALUES ROW(1, ARRAY[ARRAY[ARRAY['10'], ARRAY['20'], ARRAY['30']], ARRAY[ARRAY['60'], ARRAY['5'], ARRAY['4']], ARRAY[ARRAY['-100', '-2'], ARRAY['-20', '10'], ARRAY['100', '23']]], ARRAY[ARRAY[ARRAY[DECIMAL '1.00'], ARRAY[DECIMAL '2.00'], ARRAY[DECIMAL '3.00']], ARRAY[ARRAY[DECIMAL '6.00'], ARRAY[DECIMAL '5.00'], ARRAY[DECIMAL '4.00']], ARRAY[ARRAY[(- (DECIMAL '1.00')), (- (DECIMAL '2.00'))], ARRAY[(- (DECIMAL '2.00')), DECIMAL '10.00'], ARRAY[DECIMAL '100.00', DECIMAL '23.00']]]);

-- query 44
USE ${case_db};
select aad_1 != aas_1  from t2;

-- name: test_array_generate
-- query 45
USE ${case_db};
select sequence(1, cardinality([1,2,3]),1);

-- query 46
USE ${case_db};
select sequence(1, NULL,1);

-- query 47
USE ${case_db};
select sequence(NULL,1,1);

-- query 48
USE ${case_db};
select sequence(1,1,NULL);

-- query 49
USE ${case_db};
select sequence(1,9);

-- query 50
USE ${case_db};
select sequence(9,1);

-- query 51
USE ${case_db};
select sequence(1, 9);

-- query 52
USE ${case_db};
select sequence(3,3);

-- query 53
-- @skip_result_check=true
USE ${case_db};
select sequence(3,2,1);

-- query 54
USE ${case_db};
select sequence('2025-10-01', '2025-10-05',1);

-- query 55
USE ${case_db};
select sequence('2025-10-01', '2025-10-05',1, 'day');

-- query 56
-- @skip_result_check=true
USE ${case_db};
select sequence('2025-10-01', '2025-10-05',0, 'day');

-- query 57
USE ${case_db};
select sequence('2025-10-01', 10000, 0, 'day');

-- query 58
USE ${case_db};
select sequence('2025-10-01', 'abc', 1);

-- query 59
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.invalid_data
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.invalid_data]
USE ${case_db};
select sequence('2025-10-01', '2025-10-05',-1, 'day');

-- query 60
USE ${case_db};
select sequence(DATE'2025-10-01', DATE'2025-10-05',1, 'day');

-- query 61
USE ${case_db};
select sequence(TIMESTAMP'2025-10-01', TIMESTAMP'2025-10-05',1, 'day');

-- query 62
USE ${case_db};
select sequence('2025-10-01', '2027-10-05',1, 'year');

-- query 63
USE ${case_db};
select sequence('2025-10-01', '2027-10-05',1, 'quarter');

-- query 64
USE ${case_db};
select sequence('2025-10-01', '2026-10-05',2, 'month');

-- query 65
USE ${case_db};
select sequence('2025-10-01', '2025-12-05',2, 'week');

-- query 66
USE ${case_db};
select sequence('2025-10-01', '2025-10-05',2, 'day');

-- query 67
USE ${case_db};
select sequence('2025-10-01 14:28:31', '2025-10-01 23:12:36',3, 'hour');

-- query 68
USE ${case_db};
select sequence('2025-10-01 14:28:31', '2025-10-01 15:12:36',8, 'minute');

-- query 69
USE ${case_db};
select sequence('2025-10-01 14:28:31', '2025-10-01 14:29:36',10, 'second');

-- query 70
USE ${case_db};
select sequence('2025-10-01 14:28:31', '2025-10-01 14:28:34',500, 'millisecond');

-- query 71
USE ${case_db};
select sequence('2025-10-01 14:28:31', '2025-10-01 14:28:32.800000',500000, 'microsecond');

-- query 72
USE ${case_db};
select sequence('2027-10-05', '2025-10-01',1, 'year');

-- query 73
USE ${case_db};
select sequence('2027-10-05', '2025-10-01',1, 'quarter');

-- query 74
USE ${case_db};
select sequence('2026-10-05', '2025-10-01',2, 'month');

-- query 75
USE ${case_db};
select sequence('2025-12-05', '2025-10-01',2, 'week');

-- query 76
USE ${case_db};
select sequence('2025-10-05', '2025-10-01',2, 'day');

-- query 77
USE ${case_db};
select sequence('2025-10-01 23:12:36', '2025-10-01 14:28:31',3, 'hour');

-- query 78
USE ${case_db};
select sequence('2025-10-01 15:12:36', '2025-10-01 14:28:31',8, 'minute');

-- query 79
USE ${case_db};
select sequence('2025-10-01 14:29:36', '2025-10-01 14:28:31',10, 'second');

-- query 80
USE ${case_db};
select sequence('2025-10-01 14:28:34', '2025-10-01 14:28:31',500, 'millisecond');

-- query 81
USE ${case_db};
select sequence('2025-10-01 14:28:32.800000', '2025-10-01 14:28:31',500000, 'microsecond');

-- query 82
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.invalid_data
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.invalid_data]
USE ${case_db};
select sequence('2025-10-05', '2025-10-01',-1, 'day');

-- query 83
-- @expect_error=array_generate requires step parameter must be a constant integer
USE ${case_db};
select sequence('2025-10-01 14:28:32.800000', '2025-10-01 14:28:31',NULL);

-- name: test_array_repeat
-- query 84
USE ${case_db};
select repeat(1,5);

-- query 85
USE ${case_db};
select repeat([1,2],3);

-- query 86
-- @skip_result_check=true
USE ${case_db};
select repeat(1,-1);

-- query 87
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE IF NOT EXISTS repeat_test (COLA INT, COLB INT) WITH (format_version = 3);

-- query 88
-- @skip_result_check=true
USE ${case_db};
INSERT INTO repeat_test (COLA, COLB) VALUES ROW(1, 3), ROW(NULL, 3), ROW(2, NULL);

-- query 89
USE ${case_db};
SELECT repeat(COLA, COLB) FROM repeat_test ORDER BY COLA;

-- name: test_array_flatten
-- query 90
USE ${case_db};
select flatten([[1, 2], [1, 4]]);

-- query 91
USE ${case_db};
select flatten([[[1],[2]],[[3],[4]]]);

-- query 92
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE IF NOT EXISTS flatten_test (COLA INT, COLB ARRAY<ARRAY<INT>>) WITH (format_version = 3);

-- query 93
-- @skip_result_check=true
USE ${case_db};
INSERT INTO flatten_test (COLA, COLB) VALUES ROW(1, [[1, 2], [1, 4]]), ROW(2, NULL), ROW(3, [[5], [6, 7, 8], [9]]), ROW(4, [[2, 3], [4, 5, 6], NULL]);

-- query 94
USE ${case_db};
SELECT flatten(COLB) FROM flatten_test ORDER BY COLA;

-- query 95
USE ${case_db};
SELECT slice(flatten(COLB), 1, 2) FROM flatten_test ORDER BY COLA;

-- query 96
USE ${case_db};
SELECT flatten(slice(COLB, 1, 2)) FROM flatten_test ORDER BY COLA;

-- query 97
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE IF NOT EXISTS flatten_one_layer_arr_test (COLA INT, COLB ARRAY<INT>) WITH (format_version = 3);

-- query 98
-- @skip_result_check=true
USE ${case_db};
INSERT INTO flatten_one_layer_arr_test (COLA, COLB) VALUES ROW(1, [1, 2, 3]);

-- query 99
-- @expect_error=The only one input of array_flatten should be an array of arrays, rather than array<int(11)>
USE ${case_db};
SELECT flatten(COLB) FROM flatten_one_layer_arr_test ORDER BY COLA;

-- query 100
-- @expect_error=The only one input of array_flatten should be an array of arrays, rather than array<int(11)>
USE ${case_db};
SELECT slice(flatten(COLB), 1, 2) FROM flatten_one_layer_arr_test ORDER BY COLA;

-- query 101
-- @expect_error=The only one input of array_flatten should be an array of arrays, rather than array<int(11)>
USE ${case_db};
SELECT flatten(slice(COLB, 1, 2)) FROM flatten_one_layer_arr_test ORDER BY COLA;

-- name: null_or_empty
-- query 102
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE IF NOT EXISTS null_or_empty_test (COLA INT, COLB ARRAY<INT>) WITH (format_version = 3);

-- query 103
-- @skip_result_check=true
USE ${case_db};
INSERT INTO null_or_empty_test (COLA, COLB) VALUES ROW(1, []), ROW(2, NULL), ROW(3, [1,2]);

-- query 104
-- @skip_result_check=true
-- A catalog that cannot hold views cannot answer view enumeration, so
-- DROP DATABASE ... FORCE is refused here rather than silently assuming
-- the namespace holds none. Drop the tables explicitly instead.
USE ${case_db};
DROP TABLE IF EXISTS array_data_type;
DROP TABLE IF EXISTS array_data_type_1;
DROP TABLE IF EXISTS array_top_n;
DROP TABLE IF EXISTS array_exprr;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS repeat_test;
DROP TABLE IF EXISTS flatten_test;
DROP TABLE IF EXISTS flatten_one_layer_arr_test;
DROP TABLE IF EXISTS null_or_empty_test;
