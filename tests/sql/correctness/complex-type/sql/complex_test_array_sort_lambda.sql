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
    c6 array<REAL>
) WITH (format_version = 3);

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
from table(sequence(0,10000)) t(i);

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
SELECT distinct array_sort(transform(c6, x->cast(x as decimal(7,2))), (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

-- query 13
USE ${case_db};
SELECT distinct array_sort(transform(c6, x->cast(x as decimal(19,5))), (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) from t0;

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
USE ${case_db};
SELECT k0 AS id, array_sort(ARRAY[3, 2, 5, 1, 2], (x, y) -> IF(x = y, 0, IF((x < y) = (k0 = 0), 1, -1))) AS result
FROM t0 WHERE k0 IN (0, 1) ORDER BY k0;

-- query 20
USE ${case_db};
SELECT k0 AS id, array_sort(c0, (x, y) -> IF(x = y, 0, IF((x < y) = (k0 = 0), 1, -1))) AS result
FROM t0 WHERE k0 IN (0, 1) ORDER BY k0;

-- query 21
USE ${case_db};
SELECT array_sort(ARRAY[ROW(2, 'a'), ROW(1, 'b'), ROW(2, 'c')],
                  (x, y) -> IF(x[1] < y[1], -1, IF(x[1] = y[1], 0, 1))) AS result;

-- query 22
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT array_sort(ARRAY[1, 2], (x, y) -> 2);

-- query 23
USE ${case_db};
SELECT array_sort(ARRAY[3, 1, 2, 0, 5, 4],
                  (x, y) -> IF(x / 2 < y / 2, -1, IF(x / 2 = y / 2, 0, 1))) AS result;

-- query 24
USE ${case_db};
SELECT array_sort(ARRAY[ROW(1, 3), ROW(1, 1), ROW(2, 2), ROW(1, 2)],
                  (x, y) -> CASE
                      WHEN x[1] < y[1] THEN -1 WHEN x[1] > y[1] THEN 1
                      WHEN x[2] > y[2] THEN -1 WHEN x[2] < y[2] THEN 1
                      ELSE 0 END) AS result;

-- query 25
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
WITH input AS (SELECT array_agg(k0) AS a FROM ${case_db}.t0)
SELECT array_sort(a, (x, y) -> random()) FROM input;

-- query 26
USE ${case_db};
WITH input AS (SELECT array_agg(k0) AS a FROM t0)
SELECT array_sort(a, (x, y) -> IF(x < y, -1, IF(x = y, 0, 1)))[1] AS first_item FROM input;

-- query 27
USE ${case_db};
WITH input AS (SELECT array_agg(k0) AS a FROM t0)
SELECT array_sort(a, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))[1] AS first_item FROM input;

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
    c0 array<VARCHAR>,
    c1 array<VARCHAR>
) WITH (format_version = 3);

-- query 32
-- @skip_result_check=true
USE ${case_db};
insert into t0 select i
       ,['bc','ab','dc'] as c0
       ,['a','abcd','abc'] as c1
from table(sequence(0,10000)) t(i);

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
USE ${case_db};
SELECT array_sort(ARRAY[3, 2, NULL, 5, NULL, 1, 2],
    (x, y) -> CASE WHEN x IS NULL AND y IS NULL THEN 0
                   WHEN x IS NULL THEN -1 WHEN y IS NULL THEN 1
                   WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END) AS result;

-- query 39
USE ${case_db};
SELECT array_sort(ARRAY[3, 2, NULL, 5, NULL, 1, 2],
    (x, y) -> CASE WHEN x IS NULL AND y IS NULL THEN 0
                   WHEN x IS NULL THEN 1 WHEN y IS NULL THEN -1
                   WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END) AS result;

-- query 40
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t0;
create table t0 (
    k0 int,
    c0 array<int>
) WITH (format_version = 3);

-- query 41
-- @skip_result_check=true
USE ${case_db};
insert into t0 select i
       ,[3,2,null,5,null,1,2] as c0
from table(sequence(0,10000)) t(i);

-- query 42
USE ${case_db};
SELECT DISTINCT array_sort(ARRAY[3, 2, NULL, 5, NULL, 1, 2],
    (x, y) -> CASE WHEN x IS NULL AND y IS NULL THEN 0
                   WHEN x IS NULL THEN -1 WHEN y IS NULL THEN 1
                   WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END) AS result FROM t0;

-- query 43
USE ${case_db};
SELECT DISTINCT array_sort(ARRAY[3, 2, NULL, 5, NULL, 1, 2],
    (x, y) -> CASE WHEN x IS NULL AND y IS NULL THEN 0
                   WHEN x IS NULL THEN 1 WHEN y IS NULL THEN -1
                   WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END) AS result FROM t0;

-- query 44
USE ${case_db};
SELECT DISTINCT array_sort(c0,
    (x, y) -> CASE WHEN x IS NULL AND y IS NULL THEN 0
                   WHEN x IS NULL THEN -1 WHEN y IS NULL THEN 1
                   WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END) AS result FROM t0;

-- query 45
USE ${case_db};
SELECT DISTINCT array_sort(c0,
    (x, y) -> CASE WHEN x IS NULL AND y IS NULL THEN 0
                   WHEN x IS NULL THEN 1 WHEN y IS NULL THEN -1
                   WHEN x < y THEN 1 WHEN x = y THEN 0 ELSE -1 END) AS result FROM t0;

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
) WITH (format_version = 3);

-- query 49
-- @skip_result_check=true
USE ${case_db};
insert into t0 select i
      ,[[2, 3, 1], [4, 2, 1, 4], [1, 2]] c0
from table(sequence(0,10000)) t(i);

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
USE ${case_db};
SELECT array_sort(ARRAY[1, 3, 2, 1, 3, 6, 100, 200],
    (x, y) -> IF(random() >= DOUBLE '0', IF(x < y, -1, IF(x = y, 0, 1)), 0)) AS result;

-- query 54
USE ${case_db};
SELECT array_sort(ARRAY[1, 3, 2, 1, 3, 6, 100, 200], (x, y) -> 0) AS result;

-- query 55
USE ${case_db};
SELECT array_sort(ARRAY[1, 3, 2, 1, 3, 6, 100, 200], (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))) AS result;

-- query 56
USE ${case_db};
SELECT array_sort(ARRAY[1, 3, 2, 1, 3, 6, 100, 200], (x, y) -> IF(x < y, -1, IF(x = y, 0, 1))) AS result;

-- query 57
-- @skip_result_check=true
-- A catalog that cannot hold views cannot answer view enumeration, so
-- DROP DATABASE ... FORCE is refused here rather than silently assuming
-- the namespace holds none. Drop the tables explicitly instead.
USE ${case_db};
DROP TABLE IF EXISTS t0;
